"""Process new Yape voucher submissions into a structured 'processed data' tab.

Simple, function-based implementation for easy maintenance.
"""

from __future__ import annotations

import hashlib
import io
import json
import logging
import os
import re
from datetime import datetime, timezone
from typing import Any

import gspread
from dotenv import load_dotenv
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from openai import OpenAI
from pydantic import BaseModel, ConfigDict, Field


REQUIRED_RAW_COLUMNS = ["timestamp", "comprobante yape", "email address"]
DEFAULT_OPENAI_MODEL = "gpt-5-mini"

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
    "processed_at_utc",
    "status",
    "error_message",
    "raw_openai_json",
]

DRIVE_ID_PATTERNS = [
    re.compile(r"/d/([a-zA-Z0-9_-]+)"),
    re.compile(r"[?&]id=([a-zA-Z0-9_-]+)"),
]


class ConfigError(Exception):
    """Raised when required configuration is missing."""


class VoucherExtraction(BaseModel):
    """Structured extraction schema returned by the model."""

    model_config = ConfigDict(extra="forbid")

    operation_number: str | None = Field(default=None)
    amount: str | None = Field(default=None)
    currency: str | None = Field(default=None)
    date: str | None = Field(default=None)
    time: str | None = Field(default=None)
    phone_or_recipient: str | None = Field(default=None)


def configure_logging() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")


def get_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise ConfigError(f"Missing required environment variable: {name}")
    return value


def get_env_optional(name: str, default: str) -> str:
    value = os.getenv(name, "").strip()
    return value or default


def load_config() -> dict[str, str]:
    load_dotenv()
    return {
        "openai_api_key": get_env("OPENAI_API_KEY"),
        "spreadsheet_id": get_env("SPREADSHEET_ID"),
        "raw_sheet_name": get_env("RAW_SHEET_NAME"),
        "processed_sheet_name": get_env("PROCESSED_SHEET_NAME"),
        "service_account_file": get_env("GOOGLE_SERVICE_ACCOUNT_FILE"),
        "openai_model": get_env_optional("OPENAI_MODEL", DEFAULT_OPENAI_MODEL),
    }


def get_google_clients(service_account_file: str) -> tuple[gspread.Client, Any]:
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive.readonly",
    ]
    creds = Credentials.from_service_account_file(service_account_file, scopes=scopes)
    sheets_client = gspread.authorize(creds)
    drive_client = build("drive", "v3", credentials=creds)
    return sheets_client, drive_client


def normalize_header(header: str) -> str:
    return header.strip().lower()


def get_raw_rows(raw_ws: gspread.Worksheet) -> list[dict[str, str]]:
    values = raw_ws.get_all_values()
    if not values:
        raise ValueError("Raw sheet is empty.")

    headers = values[0]
    norm_headers = [normalize_header(h) for h in headers]
    for required in REQUIRED_RAW_COLUMNS:
        if required not in norm_headers:
            raise ValueError(f"Raw sheet missing required column: {required}. Detected headers: {headers}")

    rows: list[dict[str, str]] = []
    for row_num, row in enumerate(values[1:], start=2):
        row_padded = row + [""] * (len(headers) - len(row))
        row_map = {normalize_header(headers[i]): row_padded[i].strip() for i in range(len(headers))}
        row_map["_raw_row_number"] = str(row_num)
        rows.append(row_map)

    return rows


def make_submission_id(timestamp: str, drive_link: str, email: str) -> str:
    raw = f"{timestamp}|{drive_link}|{email}".encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def ensure_processed_sheet(spreadsheet: gspread.Spreadsheet, sheet_name: str) -> gspread.Worksheet:
    try:
        ws = spreadsheet.worksheet(sheet_name)
    except gspread.WorksheetNotFound:
        logging.info("Processed sheet '%s' not found, creating it.", sheet_name)
        ws = spreadsheet.add_worksheet(title=sheet_name, rows=1000, cols=30)

    current_headers = ws.row_values(1)
    if current_headers != PROCESSED_HEADERS:
        ws.update(values=[PROCESSED_HEADERS], range_name="A1:Q1")

    return ws


def get_existing_submission_ids(processed_ws: gspread.Worksheet) -> set[str]:
    values = processed_ws.get_all_values()
    if len(values) <= 1:
        return set()

    header = values[0]
    try:
        idx = header.index("submission_id")
    except ValueError:
        return set()

    existing_ids: set[str] = set()
    for row in values[1:]:
        if idx < len(row) and row[idx].strip():
            existing_ids.add(row[idx].strip())

    return existing_ids


def extract_drive_file_id(drive_link: str) -> str:
    for pattern in DRIVE_ID_PATTERNS:
        match = pattern.search(drive_link)
        if match:
            return match.group(1)
    raise ValueError("Could not extract Google Drive file ID from link.")


def download_drive_file_bytes(drive_client: Any, file_id: str) -> bytes:
    request = drive_client.files().get_media(fileId=file_id)
    buffer = io.BytesIO()
    downloader = MediaIoBaseDownload(buffer, request)

    done = False
    while not done:
        _, done = downloader.next_chunk()

    buffer.seek(0)
    return buffer.read()


def build_prompt() -> str:
    return (
        "Extract data from this Yape voucher image. "
        "If a field is missing or unclear, return null."
    )


def call_openai_extract(openai_client: OpenAI, model: str, image_bytes: bytes) -> tuple[VoucherExtraction, str]:
    # Structured outputs via Responses API + Pydantic schema.
    response = openai_client.responses.parse(
        model=model,
        input=[
            {
                "role": "user",
                "content": [
                    {"type": "input_text", "text": build_prompt()},
                    {"type": "input_image", "image_bytes": image_bytes},
                ],
            }
        ],
        text_format=VoucherExtraction,
    )

    parsed = response.output_parsed
    if parsed is None:
        raise ValueError("Model returned no structured output.")

    if isinstance(parsed, VoucherExtraction):
        schema_result = parsed
    else:
        schema_result = VoucherExtraction.model_validate(parsed)

    raw_json = json.dumps(schema_result.model_dump(), ensure_ascii=False)
    return schema_result, raw_json


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def row_from_result(
    *,
    submission_id: str,
    raw_row_number: str,
    timestamp: str,
    email: str,
    drive_link: str,
    drive_file_id: str,
    openai_model: str,
    status: str,
    error_message: str,
    raw_openai_json: str,
    extracted: VoucherExtraction | None = None,
) -> list[str]:
    extracted = extracted or VoucherExtraction()

    result_map = {col: "" for col in PROCESSED_HEADERS}
    result_map["submission_id"] = submission_id
    result_map["raw_row_number"] = raw_row_number
    result_map["raw_timestamp"] = timestamp
    result_map["uploader_email"] = email
    result_map["voucher_drive_link"] = drive_link
    result_map["voucher_drive_file_id"] = drive_file_id
    result_map["extracted_operation_number"] = extracted.operation_number or ""
    result_map["extracted_amount"] = extracted.amount or ""
    result_map["extracted_currency"] = extracted.currency or ""
    result_map["extracted_date"] = extracted.date or ""
    result_map["extracted_time"] = extracted.time or ""
    result_map["extracted_phone_or_recipient"] = extracted.phone_or_recipient or ""
    result_map["openai_model"] = openai_model
    result_map["processed_at_utc"] = now_utc_iso()
    result_map["status"] = status
    result_map["error_message"] = error_message
    result_map["raw_openai_json"] = raw_openai_json

    return [result_map[h] for h in PROCESSED_HEADERS]


def process_one_row(
    *,
    row: dict[str, str],
    existing_ids: set[str],
    drive_client: Any,
    openai_client: OpenAI,
    openai_model: str,
) -> list[str] | None:
    timestamp = row.get("timestamp", "")
    drive_link = row.get("comprobante yape", "")
    email = row.get("email address", "")
    raw_row_number = row.get("_raw_row_number", "")

    submission_id = make_submission_id(timestamp, drive_link, email)
    if submission_id in existing_ids:
        return None

    try:
        file_id = extract_drive_file_id(drive_link)
        image_bytes = download_drive_file_bytes(drive_client, file_id)
        extracted, raw_openai_json = call_openai_extract(openai_client, openai_model, image_bytes)

        return row_from_result(
            submission_id=submission_id,
            raw_row_number=raw_row_number,
            timestamp=timestamp,
            email=email,
            drive_link=drive_link,
            drive_file_id=file_id,
            openai_model=openai_model,
            status="ok",
            error_message="",
            raw_openai_json=raw_openai_json,
            extracted=extracted,
        )
    except Exception as exc:
        file_id = ""
        try:
            file_id = extract_drive_file_id(drive_link)
        except Exception:
            pass

        return row_from_result(
            submission_id=submission_id,
            raw_row_number=raw_row_number,
            timestamp=timestamp,
            email=email,
            drive_link=drive_link,
            drive_file_id=file_id,
            openai_model=openai_model,
            status="error",
            error_message=str(exc),
            raw_openai_json="",
        )


def main() -> None:
    configure_logging()
    config = load_config()

    sheets_client, drive_client = get_google_clients(config["service_account_file"])
    spreadsheet = sheets_client.open_by_key(config["spreadsheet_id"])
    raw_ws = spreadsheet.worksheet(config["raw_sheet_name"])
    processed_ws = ensure_processed_sheet(spreadsheet, config["processed_sheet_name"])

    raw_rows = get_raw_rows(raw_ws)
    existing_ids = get_existing_submission_ids(processed_ws)
    openai_client = OpenAI(api_key=config["openai_api_key"])

    rows_to_append: list[list[str]] = []

    logging.info("Raw rows found: %s", len(raw_rows))
    logging.info("Already processed IDs: %s", len(existing_ids))

    for row in raw_rows:
        result_row = process_one_row(
            row=row,
            existing_ids=existing_ids,
            drive_client=drive_client,
            openai_client=openai_client,
            openai_model=config["openai_model"],
        )
        if result_row is not None:
            rows_to_append.append(result_row)
            existing_ids.add(result_row[0])

    if rows_to_append:
        processed_ws.append_rows(rows_to_append, value_input_option="RAW")
        logging.info("Appended %s new rows into '%s'.", len(rows_to_append), config["processed_sheet_name"])
    else:
        logging.info("No new submissions to process.")


if __name__ == "__main__":
    main()
