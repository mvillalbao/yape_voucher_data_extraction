from __future__ import annotations

from pathlib import Path
import sys

import cv2
import numpy as np


# Edita estos valores para probar rapido.
IMAGE_PATH = r"C:\Users\matia\Documents\Github\yape_voucher_data_extraction\sample.jpg"
BLUR_THRESHOLD = 200.0
BLUR_CENTER_CROP_SPECS = [
    (0.80, 0.80),
    (0.65, 0.65),
    (0.50, 0.50),
]
BLUR_OVERLAP_SECTOR_CROP_SPECS = [
    ("top_left", 0.65, 0.65),
    ("top_right", 0.65, 0.65),
    ("bottom_left", 0.65, 0.65),
    ("bottom_right", 0.65, 0.65),
]


def load_image(path: Path) -> np.ndarray:
    image = cv2.imread(str(path))
    if image is None:
        raise FileNotFoundError(f"No se pudo abrir la imagen: {path}")
    return image


def center_crop(image: np.ndarray, width_ratio: float, height_ratio: float) -> np.ndarray:
    height, width = image.shape[:2]
    crop_width = max(1, int(width * width_ratio))
    crop_height = max(1, int(height * height_ratio))

    x1 = max(0, (width - crop_width) // 2)
    y1 = max(0, (height - crop_height) // 2)
    x2 = min(width, x1 + crop_width)
    y2 = min(height, y1 + crop_height)

    return image[y1:y2, x1:x2]


def overlap_sector_crop(image: np.ndarray, sector: str, width_ratio: float, height_ratio: float) -> np.ndarray:
    height, width = image.shape[:2]
    crop_width = max(1, int(width * width_ratio))
    crop_height = max(1, int(height * height_ratio))

    if sector == "top_left":
        x1, y1 = 0, 0
    elif sector == "top_right":
        x1, y1 = max(0, width - crop_width), 0
    elif sector == "bottom_left":
        x1, y1 = 0, max(0, height - crop_height)
    elif sector == "bottom_right":
        x1, y1 = max(0, width - crop_width), max(0, height - crop_height)
    else:
        raise ValueError(f"Sector no soportado: {sector}")

    x2 = min(width, x1 + crop_width)
    y2 = min(height, y1 + crop_height)
    return image[y1:y2, x1:x2]


def variance_of_laplacian(image: np.ndarray) -> float:
    gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
    return float(cv2.Laplacian(gray, cv2.CV_64F).var())


def analyze_image(path: Path) -> tuple[float, bool, np.ndarray]:
    image = load_image(path)
    scores: list[float] = []

    for width_ratio, height_ratio in BLUR_CENTER_CROP_SPECS:
        cropped = center_crop(image, width_ratio=width_ratio, height_ratio=height_ratio)
        scores.append(variance_of_laplacian(cropped))

    for sector, width_ratio, height_ratio in BLUR_OVERLAP_SECTOR_CROP_SPECS:
        cropped = overlap_sector_crop(
            image,
            sector=sector,
            width_ratio=width_ratio,
            height_ratio=height_ratio,
        )
        scores.append(variance_of_laplacian(cropped))

    representative_crop = center_crop(image, width_ratio=0.65, height_ratio=0.65)
    score = float(np.median(np.array(scores, dtype=np.float64)))
    is_flagged = score < BLUR_THRESHOLD
    return score, is_flagged, representative_crop


def build_cropped_output_path(path: Path) -> Path:
    return path.with_name(f"{path.stem}_cropped{path.suffix}")


def main() -> None:
    path = Path(sys.argv[1]) if len(sys.argv) > 1 else Path(IMAGE_PATH)
    score, is_flagged, cropped = analyze_image(path)
    cropped_output_path = build_cropped_output_path(path)

    saved = cv2.imwrite(str(cropped_output_path), cropped)
    if not saved:
        raise RuntimeError(f"No se pudo guardar el recorte en: {cropped_output_path}")

    print(f"image_path: {path}")
    print(f"crop_shape: {cropped.shape[1]}x{cropped.shape[0]}")
    print(f"cropped_image_path: {cropped_output_path}")
    print(f"blur_threshold: {BLUR_THRESHOLD:.2f}")
    print(f"variance_of_laplacian: {score:.2f}")
    print(f"imagen_borrosa_flag: {'yes' if is_flagged else 'no'}")


if __name__ == "__main__":
    main()
