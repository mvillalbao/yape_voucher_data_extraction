"""Process new Yape voucher submissions into a structured worksheet."""

from __future__ import annotations

import base64
import hashlib
import io
import json
import logging
import os
import random
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler
from typing import Any

import gspread
import cv2
import numpy as np
from dotenv import load_dotenv
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from openai import OpenAI
from pydantic import BaseModel, ConfigDict, Field

try:
    import streamlit as st
except Exception:
    st = None


REQUIRED_RAW_COLUMNS = ["timestamp", "comprobante yape", "email address"]
DEFAULT_OPENAI_MODEL = "gpt-5-mini"
DEFAULT_MAX_WORKERS = 50
DEFAULT_OPENAI_TIMEOUT_SECONDS = 90
DEFAULT_OPENAI_MAX_ATTEMPTS = 8
DEFAULT_BLUR_THRESHOLD = 200.0
BLUR_INNER_CROP_LEFT_RATIO = 0.10
BLUR_INNER_CROP_RIGHT_RATIO = 0.10
BLUR_INNER_CROP_TOP_RATIO = 0.08
BLUR_INNER_CROP_BOTTOM_RATIO = 0.08
BLUR_GRID_ROWS = 6
BLUR_GRID_COLS = 8
BLUR_WINDOW_WIDTH_RATIO = 0.55
BLUR_WINDOW_HEIGHT_RATIO = 0.55

PROCESSED_HEADERS = [
    "submission_id",
    "raw_row_number",
    "raw_timestamp",
    "uploader_email",
    "voucher_drive_link",
    "voucher_drive_file_id",
    "extracted_operation_number",
    "extracted_amount",
    "extracted_currency",
    "extracted_date",
    "extracted_time",
    "extracted_phone_or_recipient",
    "openai_model",
    "openai_input_tokens",
    "openai_cached_input_tokens",
    "openai_output_tokens",
    "openai_total_tokens",
    "openai_retry_count",
    "image_blur_score",
    "image_is_blurry",
    "extracted_has_blank_values",
    "processed_at_utc",
    "status",
    "error_message",
    "manually_reviewed",
]

LEGACY_PROCESSED_HEADERS_PRE_MANUAL_REVIEW = [
    "submission_id",
    "raw_row_number",
    "raw_timestamp",
    "uploader_email",
    "voucher_drive_link",
    "voucher_drive_file_id",
    "extracted_operation_number",
    "extracted_amount",
    "extracted_currency",
    "extracted_date",
    "extracted_time",
    "extracted_phone_or_recipient",
    "openai_model",
    "openai_input_tokens",
    "openai_cached_input_tokens",
    "openai_output_tokens",
    "openai_total_tokens",
    "openai_retry_count",
    "processed_at_utc",
    "status",
    "error_message",
]

LEGACY_PROCESSED_HEADERS_WITH_MANUAL_REVIEW = [
    *LEGACY_PROCESSED_HEADERS_PRE_MANUAL_REVIEW,
    "manually_reviewed",
]

LEGACY_PROCESSED_HEADERS_WITH_BLUR = [
    "submission_id",
    "raw_row_number",
    "raw_timestamp",
    "uploader_email",
    "voucher_drive_link",
    "voucher_drive_file_id",
    "extracted_operation_number",
    "extracted_amount",
    "extracted_currency",
    "extracted_date",
    "extracted_time",
    "extracted_phone_or_recipient",
    "openai_model",
    "openai_input_tokens",
    "openai_cached_input_tokens",
    "openai_output_tokens",
    "openai_total_tokens",
    "openai_retry_count",
    "image_blur_score",
    "image_is_blurry",
    "processed_at_utc",
    "status",
    "error_message",
    "manually_reviewed",
]

DRIVE_ID_PATTERNS = [
    re.compile(r"/d/([a-zA-Z0-9_-]+)"),
    re.compile(r"/file/d/([a-zA-Z0-9_-]+)"),
    re.compile(r"[?&]id=([a-zA-Z0-9_-]+)"),
]

EXTRACTION_SYSTEM_PROMPT = """You are an expert at extracting structured data from Yape payment vouchers.

CRITICAL RULES:
1. Extract only what is clearly visible in the document. Never guess or infer missing values.
2. If a field is absent or unreadable, return null.
3. Return the transaction amount as a plain decimal string using a dot separator and no currency symbol.
4. Return dates in ISO format YYYY-MM-DD whenever the voucher provides enough information.
5. Return time in 24-hour HH:MM format whenever the voucher provides enough information.
6. Preserve operation numbers exactly as shown, including leading zeroes.
7. Return currency as a 3-letter uppercase code when visible.
8. For phone_or_recipient, prefer the recipient name if shown clearly; otherwise return the phone number.
9. Do not include explanatory text outside the schema.
10. Be sure to only return the transfer amount if you are certain. If there is any doubt, return null for the amount."""

EXTRACTION_USER_PROMPT = (
    "Extract the payment details from this Yape voucher and return them in the requested schema."
)

_THREAD_CONTEXT = threading.local()


class ConfigError(Exception):
    """Raised when required configuration is missing."""


class ExtractionCallError(Exception):
    """Raised when the extraction call fails after one or more attempts."""

    def __init__(self, message: str, *, retry_count: int) -> None:
        super().__init__(message)
        self.retry_count = retry_count


@dataclass(frozen=True)
class AppConfig:
    openai_api_key: str
    spreadsheet_id: str
    raw_sheet_name: str
    processed_sheet_name: str
    service_account_file: str | None
    service_account_json: str | None
    openai_model: str
    max_workers: int
    openai_timeout_seconds: int
    openai_max_attempts: int


@dataclass(frozen=True)
class PendingSubmission:
    drive_file_id: str | None
    link_index: int
    raw_row_number: str
    timestamp: str
    email: str
    drive_link: str


@dataclass(frozen=True)
class DriveDocument:
    file_id: str
    file_name: str
    mime_type: str
    content: bytes


@dataclass
class DedupeState:
    submission_ids: set[str]
    operation_numbers: set[str]
    lock: threading.Lock


@dataclass(frozen=True)
class OpenAIUsage:
    input_tokens: int | None = None
    cached_input_tokens: int | None = None
    output_tokens: int | None = None
    total_tokens: int | None = None
    retry_count: int = 0


@dataclass(frozen=True)
class UpdateSummary:
    dataset_size_before_update: int
    total_submissions_to_analyze: int
    appended_rows: int
    accepted_rows: int
    blank_operation_number_rows: int
    duplicate_operation_number_rows: int
    duplicate_file_content_rows: int
    invalid_link_rows: int
    processing_error_rows: int
    rows_requiring_review: int


@dataclass(frozen=True)
class ProcessedDatasetView:
    total_rows: int
    rows: list[dict[str, Any]]


@dataclass(frozen=True)
class DrivePreview:
    mime_type: str
    content: bytes


class VoucherExtraction(BaseModel):
    """Structured voucher data returned by the model."""

    model_config = ConfigDict(extra="forbid")

    operation_number: str | None = Field(
        default=None,
        description=(
            "Transaction or operation number exactly as shown on the voucher. "
            "Preserve leading zeroes and keep it as text. Example: 00123456789."
        ),
    )
    amount: float | None = Field(
        default=None,
        description=(
            "Transferred amount. Be absolutely sure of this value, if you have any doubt, return none. Example: 24.90."
        ),
    )
    currency: str | None = Field(
        default=None,
        pattern=r"^[A-Z]{3}$",
        description=(
            "Return the detected currency as a 3-letter uppercase code. "
            "Examples: PEN, USD."
        ),
    )
    date: str | None = Field(
        default=None,
        pattern=r"^\d{4}-\d{2}-\d{2}$",
        description=(
            "Transaction date in ISO 8601 format YYYY-MM-DD. Example: 2026-03-29. If year not shown, assume current year"
        ),
    )
    time: str | None = Field(
        default=None,
        pattern=r"^\d{2}:\d{2}$",
        description=(
            "Transaction time in 24-hour HH:MM format. Example: 14:37."
        ),
    )
    phone_or_recipient: str | None = Field(
        default=None,
        description=(
            "Recipient name when clearly visible, otherwise the recipient phone number. "
            "Examples: Juan Perez, 987654321."
        ),
    )


def configure_logging() -> None:
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    root_logger.handlers.clear()

    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(threadName)s | %(message)s")

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)

    file_handler = RotatingFileHandler(
        "voucher_processing.log",
        maxBytes=1_000_000,
        backupCount=3,
        encoding="utf-8",
    )
    file_handler.setFormatter(formatter)

    root_logger.addHandler(stream_handler)
    root_logger.addHandler(file_handler)


def get_env(name: str) -> str:
    value = get_secret(name).strip()
    if not value:
        raise ConfigError(f"Missing required environment variable: {name}")
    return value


def get_env_optional(name: str, default: str) -> str:
    value = get_secret(name).strip()
    return value or default


def get_env_int(name: str, default: int, *, minimum: int = 1) -> int:
    raw_value = get_secret(name).strip()
    if not raw_value:
        return default

    try:
        parsed = int(raw_value)
    except ValueError as exc:
        raise ConfigError(f"Environment variable {name} must be an integer.") from exc

    if parsed < minimum:
        raise ConfigError(f"Environment variable {name} must be >= {minimum}.")

    return parsed


def get_secret(name: str) -> str:
    env_value = os.getenv(name)
    if env_value is not None:
        return str(env_value)

    if st is not None:
        try:
            secret_value = st.secrets.get(name)
        except Exception:
            secret_value = None
        if secret_value is not None:
            return str(secret_value)

    return ""


def load_config() -> AppConfig:
    load_dotenv()
    return AppConfig(
        openai_api_key=get_env("OPENAI_API_KEY"),
        spreadsheet_id=get_env("SPREADSHEET_ID"),
        raw_sheet_name=get_env("RAW_SHEET_NAME"),
        processed_sheet_name=get_env("PROCESSED_SHEET_NAME"),
        service_account_file=get_env_optional("GOOGLE_SERVICE_ACCOUNT_FILE", "") or None,
        service_account_json=get_env_optional("GOOGLE_SERVICE_ACCOUNT_JSON", "") or None,
        openai_model=get_env_optional("OPENAI_MODEL", DEFAULT_OPENAI_MODEL),
        max_workers=get_env_int("MAX_WORKERS", DEFAULT_MAX_WORKERS),
        openai_timeout_seconds=get_env_int(
            "OPENAI_TIMEOUT_SECONDS",
            DEFAULT_OPENAI_TIMEOUT_SECONDS,
        ),
        openai_max_attempts=get_env_int(
            "OPENAI_MAX_ATTEMPTS",
            DEFAULT_OPENAI_MAX_ATTEMPTS,
        ),
    )


def build_google_credentials(
    *,
    service_account_file: str | None,
    service_account_json: str | None,
) -> Credentials:
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive.readonly",
    ]
    if service_account_json:
        try:
            service_account_info = json.loads(service_account_json)
        except json.JSONDecodeError as exc:
            raise ConfigError("GOOGLE_SERVICE_ACCOUNT_JSON is not valid JSON.") from exc
        return Credentials.from_service_account_info(service_account_info, scopes=scopes)

    if service_account_file:
        return Credentials.from_service_account_file(service_account_file, scopes=scopes)

    raise ConfigError(
        "Missing Google credentials. Set GOOGLE_SERVICE_ACCOUNT_JSON or GOOGLE_SERVICE_ACCOUNT_FILE."
    )


def get_sheets_client(credentials: Credentials) -> gspread.Client:
    return gspread.authorize(credentials)


def get_worker_clients(config: AppConfig) -> tuple[Any, OpenAI]:
    drive_client = getattr(_THREAD_CONTEXT, "drive_client", None)
    openai_client = getattr(_THREAD_CONTEXT, "openai_client", None)

    if drive_client is None:
        creds = build_google_credentials(
            service_account_file=config.service_account_file,
            service_account_json=config.service_account_json,
        )
        drive_client = build("drive", "v3", credentials=creds)
        _THREAD_CONTEXT.drive_client = drive_client

    if openai_client is None:
        openai_client = OpenAI(
            api_key=config.openai_api_key,
            timeout=config.openai_timeout_seconds,
        )
        _THREAD_CONTEXT.openai_client = openai_client

    return drive_client, openai_client


def normalize_header(header: str) -> str:
    return header.strip().lower()


def get_raw_rows(raw_ws: gspread.Worksheet) -> list[dict[str, str]]:
    values = raw_ws.get_all_values()
    if not values:
        raise ValueError("Raw sheet is empty.")

    headers = values[0]
    normalized_headers = [normalize_header(header) for header in headers]

    for required in REQUIRED_RAW_COLUMNS:
        if required not in normalized_headers:
            raise ValueError(f"Raw sheet missing required column: {required}")

    rows: list[dict[str, str]] = []
    for row_number, row in enumerate(values[1:], start=2):
        padded_row = row + [""] * (len(headers) - len(row))
        row_map = {
            normalize_header(headers[index]): padded_row[index].strip()
            for index in range(len(headers))
        }
        row_map["_raw_row_number"] = str(row_number)
        rows.append(row_map)

    return rows


def make_submission_id(file_bytes: bytes) -> str:
    return hashlib.sha256(file_bytes).hexdigest()


def make_link_submission_id(drive_link: str) -> str:
    normalized_link = drive_link.strip()
    return hashlib.sha256(f"link:{normalized_link}".encode("utf-8")).hexdigest()


def decode_image_bytes(image_bytes: bytes) -> np.ndarray | None:
    buffer = np.frombuffer(image_bytes, dtype=np.uint8)
    if buffer.size == 0:
        return None
    return cv2.imdecode(buffer, cv2.IMREAD_COLOR)


def crop_image_borders(
    image: np.ndarray,
    *,
    left_ratio: float = BLUR_INNER_CROP_LEFT_RATIO,
    right_ratio: float = BLUR_INNER_CROP_RIGHT_RATIO,
    top_ratio: float = BLUR_INNER_CROP_TOP_RATIO,
    bottom_ratio: float = BLUR_INNER_CROP_BOTTOM_RATIO,
) -> np.ndarray:
    height, width = image.shape[:2]
    x1 = min(width - 1, max(0, int(width * left_ratio)))
    x2 = max(x1 + 1, min(width, int(width * (1.0 - right_ratio))))
    y1 = min(height - 1, max(0, int(height * top_ratio)))
    y2 = max(y1 + 1, min(height, int(height * (1.0 - bottom_ratio))))
    return image[y1:y2, x1:x2]


def generate_overlapping_grid_crops(
    image: np.ndarray,
    *,
    rows: int = BLUR_GRID_ROWS,
    cols: int = BLUR_GRID_COLS,
    window_width_ratio: float = BLUR_WINDOW_WIDTH_RATIO,
    window_height_ratio: float = BLUR_WINDOW_HEIGHT_RATIO,
) -> list[np.ndarray]:
    height, width = image.shape[:2]
    window_width = max(1, int(width * window_width_ratio))
    window_height = max(1, int(height * window_height_ratio))

    max_x = max(0, width - window_width)
    max_y = max(0, height - window_height)

    x_positions = [int(round(value)) for value in np.linspace(0, max_x, num=max(cols, 1))]
    y_positions = [int(round(value)) for value in np.linspace(0, max_y, num=max(rows, 1))]

    crops: list[np.ndarray] = []
    for y1 in y_positions:
        for x1 in x_positions:
            x2 = min(width, x1 + window_width)
            y2 = min(height, y1 + window_height)
            crops.append(image[y1:y2, x1:x2])
    return crops


def variance_of_laplacian_score(image: np.ndarray) -> float | None:
    if image.size == 0:
        return None
    gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
    return float(cv2.Laplacian(gray, cv2.CV_64F).var())


def compute_blur_score(image_bytes: bytes) -> float | None:
    image = decode_image_bytes(image_bytes)
    if image is None:
        return None

    inner_cropped = crop_image_borders(image)
    if inner_cropped.size == 0:
        return None

    crop_scores: list[float] = []
    for cropped in generate_overlapping_grid_crops(inner_cropped):
        score = variance_of_laplacian_score(cropped)
        if score is not None:
            crop_scores.append(score)

    if not crop_scores:
        return None

    return float(np.median(np.array(crop_scores, dtype=np.float64)))


def is_blurry_image(blur_score: float | None, *, threshold: float = DEFAULT_BLUR_THRESHOLD) -> bool | None:
    if blur_score is None:
        return None
    return blur_score < threshold


def normalize_processed_row_values(row_values: list[str]) -> dict[str, str]:
    variants_by_length: dict[int, list[str]] = {
        len(PROCESSED_HEADERS): PROCESSED_HEADERS,
        len(LEGACY_PROCESSED_HEADERS_WITH_BLUR): LEGACY_PROCESSED_HEADERS_WITH_BLUR,
        len(LEGACY_PROCESSED_HEADERS_WITH_MANUAL_REVIEW): LEGACY_PROCESSED_HEADERS_WITH_MANUAL_REVIEW,
        len(LEGACY_PROCESSED_HEADERS_PRE_MANUAL_REVIEW): LEGACY_PROCESSED_HEADERS_PRE_MANUAL_REVIEW,
    }

    source_headers = variants_by_length.get(len(row_values))
    normalized = {header: "" for header in PROCESSED_HEADERS}

    if source_headers is None:
        padded_row = row_values + [""] * max(0, len(PROCESSED_HEADERS) - len(row_values))
        for index, header in enumerate(PROCESSED_HEADERS[: len(padded_row)]):
            normalized[header] = padded_row[index]
        return normalized

    padded_row = row_values + [""] * max(0, len(source_headers) - len(row_values))
    for index, header in enumerate(source_headers):
        normalized[header] = padded_row[index]
    return normalized


def has_blank_extracted_values(extraction: VoucherExtraction | None) -> bool:
    extraction = extraction or VoucherExtraction()
    values = [
        extraction.operation_number or "",
        "" if extraction.amount is None else str(extraction.amount),
        extraction.currency or "",
        extraction.date or "",
        extraction.time or "",
        extraction.phone_or_recipient or "",
    ]
    return any(not str(value).strip() for value in values)


def ensure_processed_sheet(
    spreadsheet: gspread.Spreadsheet,
    sheet_name: str,
) -> gspread.Worksheet:
    try:
        worksheet = spreadsheet.worksheet(sheet_name)
    except gspread.WorksheetNotFound:
        logging.info("Processed sheet '%s' not found, creating it.", sheet_name)
        worksheet = spreadsheet.add_worksheet(title=sheet_name, rows=1000, cols=30)

    current_headers = worksheet.row_values(1)
    if current_headers != PROCESSED_HEADERS:
        worksheet.update(values=[PROCESSED_HEADERS], range_name=f"A1:{column_number_to_letter(len(PROCESSED_HEADERS))}1")

    return worksheet


def column_number_to_letter(column_number: int) -> str:
    result = ""
    while column_number > 0:
        column_number, remainder = divmod(column_number - 1, 26)
        result = chr(65 + remainder) + result
    return result

def normalize_operation_number(value: str) -> str:
    return re.sub(r"[^a-z0-9]", "", value.strip().lower())


def get_processed_sheet_indexes(
    processed_ws: gspread.Worksheet,
) -> tuple[set[str], set[str], set[str], int]:
    values = processed_ws.get_all_values()
    if len(values) <= 1:
        return set(), set(), set(), 0

    submission_ids: set[str] = set()
    drive_file_ids: set[str] = set()
    operation_numbers: set[str] = set()

    for row in values[1:]:
        row_map = normalize_processed_row_values(row)

        submission_id = row_map.get("submission_id", "").strip()
        if submission_id:
            submission_ids.add(submission_id)

        drive_file_id = row_map.get("voucher_drive_file_id", "").strip()
        if drive_file_id:
            drive_file_ids.add(drive_file_id)

        normalized = normalize_operation_number(row_map.get("extracted_operation_number", ""))
        if normalized:
            operation_numbers.add(normalized)

    return submission_ids, drive_file_ids, operation_numbers, len(values) - 1


def split_drive_links(raw_value: str) -> list[str]:
    return [link.strip() for link in raw_value.split(",") if link.strip()]


def build_pending_submissions(
    raw_rows: list[dict[str, str]],
    existing_submission_ids: set[str],
    existing_drive_file_ids: set[str],
) -> list[PendingSubmission]:
    pending: list[PendingSubmission] = []

    for row in raw_rows:
        timestamp = row.get("timestamp", "")
        drive_links = split_drive_links(row.get("comprobante yape", ""))
        email = row.get("email address", "")
        raw_row_number = row.get("_raw_row_number", "")

        for link_index, drive_link in enumerate(drive_links):
            fallback_submission_id = make_link_submission_id(drive_link)
            try:
                drive_file_id = extract_drive_file_id(drive_link)
            except Exception:
                if fallback_submission_id in existing_submission_ids:
                    logging.info(
                        "Skipping previously recorded invalid link in raw row %s: %s",
                        raw_row_number,
                        drive_link,
                    )
                    continue
                drive_file_id = None
            else:
                if drive_file_id in existing_drive_file_ids:
                    logging.info(
                        "Skipping already processed file_id %s from raw row %s.",
                        drive_file_id,
                        raw_row_number,
                    )
                    continue

            pending.append(
                PendingSubmission(
                    drive_file_id=drive_file_id,
                    link_index=link_index,
                    raw_row_number=raw_row_number,
                    timestamp=timestamp,
                    email=email,
                    drive_link=drive_link,
                )
            )

    return pending


def extract_drive_file_id(drive_link: str) -> str:
    for pattern in DRIVE_ID_PATTERNS:
        match = pattern.search(drive_link)
        if match:
            return match.group(1)
    raise ValueError("Could not extract Google Drive file ID from link.")


def fetch_drive_document(drive_client: Any, file_id: str) -> DriveDocument:
    metadata = drive_client.files().get(
        fileId=file_id,
        fields="id,name,mimeType",
        supportsAllDrives=True,
    ).execute()

    request = drive_client.files().get_media(fileId=file_id, supportsAllDrives=True)
    buffer = io.BytesIO()
    downloader = MediaIoBaseDownload(buffer, request)

    done = False
    while not done:
        _, done = downloader.next_chunk()

    return DriveDocument(
        file_id=file_id,
        file_name=metadata.get("name") or f"{file_id}.bin",
        mime_type=metadata.get("mimeType") or "application/octet-stream",
        content=buffer.getvalue(),
    )


def build_response_input(document: DriveDocument) -> list[dict[str, Any]]:
    encoded_document = base64.b64encode(document.content).decode("utf-8")

    if document.mime_type.startswith("image/"):
        document_part: dict[str, Any] = {
            "type": "input_image",
            "image_url": f"data:{document.mime_type};base64,{encoded_document}",
        }
    else:
        document_part = {
            "type": "input_file",
            "filename": document.file_name,
            "file_data": f"data:{document.mime_type};base64,{encoded_document}",
        }

    return [
        {
            "role": "system",
            "content": [{"type": "input_text", "text": EXTRACTION_SYSTEM_PROMPT}],
        },
        {
            "role": "user",
            "content": [
                document_part,
                {"type": "input_text", "text": EXTRACTION_USER_PROMPT},
            ],
        },
    ]


def is_retryable_error(exc: Exception) -> bool:
    message = str(exc).lower()
    if "unsupported_file" in message or "file type you uploaded is not supported" in message:
        return False

    try:
        import openai

        if isinstance(
            exc,
            (
                openai.RateLimitError,
                openai.APIConnectionError,
                openai.APITimeoutError,
                openai.InternalServerError,
                openai.APIError,
            ),
        ):
            return True
    except Exception:
        pass

    try:
        import httpx

        if isinstance(
            exc,
            (
                httpx.ConnectError,
                httpx.ReadTimeout,
                httpx.WriteError,
                httpx.RemoteProtocolError,
                getattr(httpx, "PoolTimeout", tuple()),
            ),
        ):
            return True
    except Exception:
        pass

    status = getattr(exc, "status", None) or getattr(exc, "status_code", None)
    if isinstance(status, int) and (
        status == 429 or status in (408, 502, 503, 504) or status >= 500
    ):
        return True

    return isinstance(exc, (TimeoutError, ConnectionError))


def normalize_usage(usage: Any) -> OpenAIUsage:
    if usage is None:
        return OpenAIUsage()

    if hasattr(usage, "model_dump"):
        usage_dict = usage.model_dump()
    elif isinstance(usage, dict):
        usage_dict = usage
    else:
        usage_dict = {
            "input_tokens": getattr(usage, "input_tokens", None),
            "output_tokens": getattr(usage, "output_tokens", None),
            "total_tokens": getattr(usage, "total_tokens", None),
            "input_tokens_details": getattr(usage, "input_tokens_details", None),
        }

    input_details = usage_dict.get("input_tokens_details") or {}
    if hasattr(input_details, "model_dump"):
        input_details = input_details.model_dump()

    return OpenAIUsage(
        input_tokens=usage_dict.get("input_tokens"),
        cached_input_tokens=input_details.get("cached_tokens"),
        output_tokens=usage_dict.get("output_tokens"),
        total_tokens=usage_dict.get("total_tokens"),
    )


def normalize_currency(value: str | None) -> str | None:
    if value is None:
        return None

    normalized = value.strip().upper()
    if not normalized:
        return None

    if normalized in {"PEN", "S/.", "S/", "SOLES", "SOL", "NUEVOS SOLES", "NUEVO SOL"}:
        return "PEN"

    return normalized


def call_openai_extract(
    openai_client: OpenAI,
    model: str,
    document: DriveDocument,
    *,
    max_attempts: int,
) -> tuple[VoucherExtraction, OpenAIUsage]:
    retry_count = 0
    messages = build_response_input(document)

    for attempt in range(1, max_attempts + 1):
        try:
            response = openai_client.responses.parse(
                model=model,
                input=messages,
                text_format=VoucherExtraction,
            )
            parsed = response.output_parsed
            if parsed is None:
                raise ValueError("Model returned no structured output.")

            extraction = (
                parsed
                if isinstance(parsed, VoucherExtraction)
                else VoucherExtraction.model_validate(parsed)
            )
            extraction = extraction.model_copy(
                update={"currency": normalize_currency(extraction.currency)}
            )
            usage = normalize_usage(getattr(response, "usage", None))
            usage = OpenAIUsage(
                input_tokens=usage.input_tokens,
                cached_input_tokens=usage.cached_input_tokens,
                output_tokens=usage.output_tokens,
                total_tokens=usage.total_tokens,
                retry_count=retry_count,
            )
            return extraction, usage
        except Exception as exc:
            if attempt >= max_attempts or not is_retryable_error(exc):
                raise ExtractionCallError(str(exc), retry_count=retry_count) from exc

            retry_count += 1
            sleep_seconds = min(10.0, (2 ** (attempt - 1)) + random.uniform(0.0, 1.0))
            logging.warning(
                "Transient extraction error for file %s on attempt %s/%s: %s. Retrying in %.2fs.",
                document.file_name,
                attempt,
                max_attempts,
                exc,
                sleep_seconds,
            )
            time.sleep(sleep_seconds)

    raise RuntimeError("OpenAI extraction retry loop exited unexpectedly.")


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def row_from_result(
    *,
    submission_id: str,
    submission: PendingSubmission,
    drive_file_id: str,
    openai_model: str,
    status: str,
    error_message: str,
    extracted: VoucherExtraction | None = None,
    usage: OpenAIUsage | None = None,
    blur_score: float | None = None,
    blurry_flag: bool | None = None,
) -> list[str]:
    extraction = extracted or VoucherExtraction()
    usage = usage or OpenAIUsage()
    has_blank_values = has_blank_extracted_values(extraction)
    result_map = {column: "" for column in PROCESSED_HEADERS}
    result_map["submission_id"] = submission_id
    result_map["raw_row_number"] = submission.raw_row_number
    result_map["raw_timestamp"] = submission.timestamp
    result_map["uploader_email"] = submission.email
    result_map["voucher_drive_link"] = submission.drive_link
    result_map["voucher_drive_file_id"] = drive_file_id
    result_map["extracted_operation_number"] = extraction.operation_number or ""
    result_map["extracted_amount"] = "" if extraction.amount is None else extraction.amount
    result_map["extracted_currency"] = extraction.currency or ""
    result_map["extracted_date"] = extraction.date or ""
    result_map["extracted_time"] = extraction.time or ""
    result_map["extracted_phone_or_recipient"] = extraction.phone_or_recipient or ""
    result_map["openai_model"] = openai_model
    result_map["openai_input_tokens"] = "" if usage.input_tokens is None else str(usage.input_tokens)
    result_map["openai_cached_input_tokens"] = (
        "" if usage.cached_input_tokens is None else str(usage.cached_input_tokens)
    )
    result_map["openai_output_tokens"] = "" if usage.output_tokens is None else str(usage.output_tokens)
    result_map["openai_total_tokens"] = "" if usage.total_tokens is None else str(usage.total_tokens)
    result_map["openai_retry_count"] = str(usage.retry_count)
    result_map["image_blur_score"] = "" if blur_score is None else f"{blur_score:.2f}"
    result_map["image_is_blurry"] = "" if blurry_flag is None else ("yes" if blurry_flag else "no")
    result_map["extracted_has_blank_values"] = "yes" if has_blank_values else "no"
    result_map["processed_at_utc"] = now_utc_iso()
    result_map["status"] = status
    result_map["error_message"] = error_message
    return [result_map[header] for header in PROCESSED_HEADERS]


def process_submission(
    submission: PendingSubmission,
    config: AppConfig,
    dedupe_state: DedupeState,
) -> list[str] | None:
    drive_file_id = submission.drive_file_id or ""
    fallback_submission_id = make_link_submission_id(submission.drive_link)
    submission_id = fallback_submission_id
    blur_score: float | None = None
    blurry_flag: bool | None = None

    try:
        if submission.drive_file_id is None:
            with dedupe_state.lock:
                if fallback_submission_id in dedupe_state.submission_ids:
                    return row_from_result(
                        submission_id=fallback_submission_id,
                        submission=submission,
                        drive_file_id="",
                        openai_model=config.openai_model,
                        status="duplicate_invalid_link",
                        error_message="This invalid Drive link was already recorded.",
                    )
                dedupe_state.submission_ids.add(fallback_submission_id)

            return row_from_result(
                submission_id=fallback_submission_id,
                submission=submission,
                drive_file_id="",
                openai_model=config.openai_model,
                status="invalid_drive_link",
                error_message="Could not extract a Google Drive file ID from the provided link.",
            )

        drive_client, openai_client = get_worker_clients(config)
        document = fetch_drive_document(drive_client, submission.drive_file_id)
        submission_id = make_submission_id(document.content)

        with dedupe_state.lock:
            if submission_id in dedupe_state.submission_ids:
                return row_from_result(
                    submission_id=submission_id,
                    submission=submission,
                    drive_file_id=drive_file_id,
                    openai_model=config.openai_model,
                    status="duplicate_file_content",
                    error_message="This file's content matches a voucher that was already processed.",
                    blur_score=blur_score,
                    blurry_flag=blurry_flag,
                )
            dedupe_state.submission_ids.add(submission_id)

        blur_score = compute_blur_score(document.content)
        blurry_flag = is_blurry_image(blur_score)

        extraction, usage = call_openai_extract(
            openai_client,
            config.openai_model,
            document,
            max_attempts=config.openai_max_attempts,
        )

        normalized_operation_number = normalize_operation_number(extraction.operation_number or "")
        if not normalized_operation_number:
            return row_from_result(
                submission_id=submission_id,
                submission=submission,
                drive_file_id=drive_file_id,
                openai_model=config.openai_model,
                status="blank_operation_number",
                error_message="Extraction succeeded but operation number is blank or unreadable.",
                extracted=extraction,
                usage=usage,
                blur_score=blur_score,
                blurry_flag=blurry_flag,
            )

        with dedupe_state.lock:
            if normalized_operation_number in dedupe_state.operation_numbers:
                return row_from_result(
                    submission_id=submission_id,
                    submission=submission,
                    drive_file_id=drive_file_id,
                    openai_model=config.openai_model,
                    status="duplicate_operation_number",
                    error_message=(
                        f"Operation number already processed: {extraction.operation_number}"
                    ),
                    extracted=extraction,
                    usage=usage,
                    blur_score=blur_score,
                    blurry_flag=blurry_flag,
                )
            dedupe_state.operation_numbers.add(normalized_operation_number)

        return row_from_result(
            submission_id=submission_id,
            submission=submission,
            drive_file_id=drive_file_id,
            openai_model=config.openai_model,
            status="ok",
            error_message="",
            extracted=extraction,
            usage=usage,
            blur_score=blur_score,
            blurry_flag=blurry_flag,
        )
    except Exception as exc:
        retry_count = exc.retry_count if isinstance(exc, ExtractionCallError) else 0
        logging.exception(
            "Failed processing raw row %s, file_id %s.",
            submission.raw_row_number,
            drive_file_id,
        )
        return row_from_result(
            submission_id=submission_id,
            submission=submission,
            drive_file_id=drive_file_id,
            openai_model=config.openai_model,
            status="processing_error",
            error_message=str(exc),
            usage=OpenAIUsage(retry_count=retry_count),
            blur_score=blur_score,
            blurry_flag=blurry_flag,
        )


def process_submissions_in_parallel(
    submissions: list[PendingSubmission],
    config: AppConfig,
    dedupe_state: DedupeState,
) -> list[list[str]]:
    completed_results: list[tuple[PendingSubmission, list[str]]] = []

    with ThreadPoolExecutor(max_workers=config.max_workers) as executor:
        futures = {
            executor.submit(process_submission, submission, config, dedupe_state): submission
            for submission in submissions
        }

        for future in as_completed(futures):
            submission = futures[future]
            row = future.result()
            if row is None:
                continue
            completed_results.append((submission, row))
            logging.info(
                "Completed raw row %s with status '%s'.",
                submission.raw_row_number,
                row[PROCESSED_HEADERS.index("status")],
            )

    completed_results.sort(key=lambda item: (int(item[0].raw_row_number), item[0].link_index))
    return [row for _, row in completed_results]


def sort_rows_for_append(rows: list[list[str]]) -> list[list[str]]:
    date_index = PROCESSED_HEADERS.index("extracted_date")
    time_index = PROCESSED_HEADERS.index("extracted_time")
    raw_row_index = PROCESSED_HEADERS.index("raw_row_number")

    def sort_key(row: list[str]) -> tuple[int, str, str, int]:
        extracted_date = str(row[date_index]).strip()
        extracted_time = str(row[time_index]).strip()
        has_complete_datetime = bool(extracted_date and extracted_time)
        raw_row_number = int(str(row[raw_row_index]).strip() or "0")

        return (
            0 if has_complete_datetime else 1,
            extracted_date if has_complete_datetime else "",
            extracted_time if has_complete_datetime else "",
            raw_row_number,
        )

    return sorted(rows, key=sort_key)


def count_rows_by_status(rows: list[list[str]]) -> dict[str, int]:
    status_index = PROCESSED_HEADERS.index("status")
    counts: dict[str, int] = {}

    for row in rows:
        status = str(row[status_index]).strip()
        counts[status] = counts.get(status, 0) + 1

    return counts


def count_rows_requiring_review(rows: list[list[str]]) -> int:
    status_index = PROCESSED_HEADERS.index("status")
    blurry_index = PROCESSED_HEADERS.index("image_is_blurry")
    blank_values_index = PROCESSED_HEADERS.index("extracted_has_blank_values")

    total = 0
    for row in rows:
        status = str(row[status_index]).strip()
        is_blurry = str(row[blurry_index]).strip().lower() == "yes"
        has_blank_values = str(row[blank_values_index]).strip().lower() == "yes"
        if status == "blank_operation_number" or is_blurry or has_blank_values:
            total += 1
    return total


def get_processed_worksheet(config: AppConfig) -> gspread.Worksheet:
    credentials = build_google_credentials(
        service_account_file=config.service_account_file,
        service_account_json=config.service_account_json,
    )
    sheets_client = get_sheets_client(credentials)
    spreadsheet = sheets_client.open_by_key(config.spreadsheet_id)
    return ensure_processed_sheet(spreadsheet, config.processed_sheet_name)


def get_drive_client(config: AppConfig) -> Any:
    credentials = build_google_credentials(
        service_account_file=config.service_account_file,
        service_account_json=config.service_account_json,
    )
    return build("drive", "v3", credentials=credentials)


def fetch_drive_preview(file_id: str) -> DrivePreview:
    configure_logging()
    config = load_config()
    drive_client = get_drive_client(config)
    document = fetch_drive_document(drive_client, file_id)
    return DrivePreview(mime_type=document.mime_type, content=document.content)


def update_manual_review(
    *,
    sheet_row_number: int,
    operation_number: str,
    amount: float | None,
    currency: str,
    date: str,
    time_value: str,
    phone_or_recipient: str,
    status: str,
    error_message: str,
) -> None:
    configure_logging()
    config = load_config()
    processed_ws = get_processed_worksheet(config)

    current_headers = processed_ws.row_values(1)
    row_values = processed_ws.row_values(sheet_row_number)
    row_map = normalize_processed_row_values(row_values)

    row_map["extracted_operation_number"] = operation_number.strip()
    row_map["extracted_amount"] = "" if amount is None else amount
    row_map["extracted_currency"] = currency.strip().upper()
    row_map["extracted_date"] = date.strip()
    row_map["extracted_time"] = time_value.strip()
    row_map["extracted_phone_or_recipient"] = phone_or_recipient.strip()
    updated_extraction = VoucherExtraction(
        operation_number=row_map["extracted_operation_number"] or None,
        amount=None if row_map["extracted_amount"] == "" else float(row_map["extracted_amount"]),
        currency=row_map["extracted_currency"] or None,
        date=row_map["extracted_date"] or None,
        time=row_map["extracted_time"] or None,
        phone_or_recipient=row_map["extracted_phone_or_recipient"] or None,
    )
    row_map["extracted_has_blank_values"] = "yes" if has_blank_extracted_values(updated_extraction) else "no"
    row_map["status"] = status.strip()
    row_map["error_message"] = error_message.strip()
    row_map["manually_reviewed"] = "yes"

    updated_row = [row_map.get(header, "") for header in PROCESSED_HEADERS]
    processed_ws.update(
        values=[updated_row],
        range_name=f"A{sheet_row_number}:{column_number_to_letter(len(PROCESSED_HEADERS))}{sheet_row_number}",
    )


def fetch_processed_dataset() -> ProcessedDatasetView:
    configure_logging()
    config = load_config()

    processed_ws = get_processed_worksheet(config)

    values = processed_ws.get_all_values()
    if len(values) <= 1:
        return ProcessedDatasetView(total_rows=0, rows=[])

    rows: list[dict[str, Any]] = []
    for sheet_row_number, row in enumerate(values[1:], start=2):
        row_map = normalize_processed_row_values(row)
        row_map["_sheet_row_number"] = sheet_row_number
        rows.append(row_map)

    return ProcessedDatasetView(total_rows=len(rows), rows=rows)


def run_update() -> UpdateSummary:
    configure_logging()
    config = load_config()

    credentials = build_google_credentials(
        service_account_file=config.service_account_file,
        service_account_json=config.service_account_json,
    )
    sheets_client = get_sheets_client(credentials)
    spreadsheet = sheets_client.open_by_key(config.spreadsheet_id)

    raw_ws = spreadsheet.worksheet(config.raw_sheet_name)
    processed_ws = ensure_processed_sheet(spreadsheet, config.processed_sheet_name)

    raw_rows = get_raw_rows(raw_ws)
    existing_ids, existing_drive_file_ids, existing_operation_numbers, dataset_size_before_update = (
        get_processed_sheet_indexes(processed_ws)
    )
    pending_submissions = build_pending_submissions(
        raw_rows,
        existing_ids,
        existing_drive_file_ids,
    )
    dedupe_state = DedupeState(
        submission_ids=set(existing_ids),
        operation_numbers=set(existing_operation_numbers),
        lock=threading.Lock(),
    )

    logging.info("Raw rows found: %s", len(raw_rows))
    logging.info("Already processed submissions: %s", len(existing_ids))
    logging.info("Already processed drive file IDs: %s", len(existing_drive_file_ids))
    logging.info("Already processed operation numbers: %s", len(existing_operation_numbers))
    logging.info("Pending submissions: %s", len(pending_submissions))
    logging.info("Using model '%s' with max_workers=%s.", config.openai_model, config.max_workers)

    if not pending_submissions:
        logging.info("No new submissions to process.")
        return UpdateSummary(
            dataset_size_before_update=dataset_size_before_update,
            total_submissions_to_analyze=len(pending_submissions),
            appended_rows=0,
            accepted_rows=0,
            blank_operation_number_rows=0,
            duplicate_operation_number_rows=0,
            duplicate_file_content_rows=0,
            invalid_link_rows=0,
            processing_error_rows=0,
            rows_requiring_review=0,
        )

    rows_to_append = process_submissions_in_parallel(
        pending_submissions,
        config,
        dedupe_state,
    )
    if not rows_to_append:
        logging.info("No rows remained after parallel dedupe filtering.")
        return UpdateSummary(
            dataset_size_before_update=dataset_size_before_update,
            total_submissions_to_analyze=len(pending_submissions),
            appended_rows=0,
            accepted_rows=0,
            blank_operation_number_rows=0,
            duplicate_operation_number_rows=0,
            duplicate_file_content_rows=0,
            invalid_link_rows=0,
            processing_error_rows=0,
            rows_requiring_review=0,
        )

    rows_to_append = sort_rows_for_append(rows_to_append)
    status_counts = count_rows_by_status(rows_to_append)
    processed_ws.append_rows(rows_to_append, value_input_option="RAW")
    logging.info(
        "Appended %s processed rows into '%s'.",
        len(rows_to_append),
        config.processed_sheet_name,
    )
    return UpdateSummary(
        dataset_size_before_update=dataset_size_before_update,
        total_submissions_to_analyze=len(pending_submissions),
        appended_rows=len(rows_to_append),
        accepted_rows=status_counts.get("ok", 0),
        blank_operation_number_rows=status_counts.get("blank_operation_number", 0),
        duplicate_operation_number_rows=status_counts.get("duplicate_operation_number", 0),
        duplicate_file_content_rows=status_counts.get("duplicate_file_content", 0),
        invalid_link_rows=(
            status_counts.get("invalid_drive_link", 0)
            + status_counts.get("duplicate_invalid_link", 0)
        ),
        processing_error_rows=status_counts.get("processing_error", 0),
        rows_requiring_review=count_rows_requiring_review(rows_to_append),
    )


def main() -> None:
    run_update()


if __name__ == "__main__":
    main()
