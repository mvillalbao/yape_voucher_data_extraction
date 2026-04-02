from __future__ import annotations

from pathlib import Path
import sys

import cv2
import numpy as np


# Edita estos valores para probar rapido.
IMAGE_PATH = r"C:\Users\matia\Documents\Github\yape_voucher_data_extraction\sample.jpg"
BLUR_THRESHOLD = 200.0
BLUR_INNER_CROP_LEFT_RATIO = 0.10
BLUR_INNER_CROP_RIGHT_RATIO = 0.10
BLUR_INNER_CROP_TOP_RATIO = 0.08
BLUR_INNER_CROP_BOTTOM_RATIO = 0.08
BLUR_GRID_ROWS = 6
BLUR_GRID_COLS = 8
BLUR_WINDOW_WIDTH_RATIO = 0.55
BLUR_WINDOW_HEIGHT_RATIO = 0.55


def load_image(path: Path) -> np.ndarray:
    image = cv2.imread(str(path))
    if image is None:
        raise FileNotFoundError(f"No se pudo abrir la imagen: {path}")
    return image


def crop_image_borders(image: np.ndarray) -> np.ndarray:
    height, width = image.shape[:2]
    x1 = min(width - 1, max(0, int(width * BLUR_INNER_CROP_LEFT_RATIO)))
    x2 = max(x1 + 1, min(width, int(width * (1.0 - BLUR_INNER_CROP_RIGHT_RATIO))))
    y1 = min(height - 1, max(0, int(height * BLUR_INNER_CROP_TOP_RATIO)))
    y2 = max(y1 + 1, min(height, int(height * (1.0 - BLUR_INNER_CROP_BOTTOM_RATIO))))
    return image[y1:y2, x1:x2]


def generate_overlapping_grid_crops(image: np.ndarray) -> list[np.ndarray]:
    height, width = image.shape[:2]
    window_width = max(1, int(width * BLUR_WINDOW_WIDTH_RATIO))
    window_height = max(1, int(height * BLUR_WINDOW_HEIGHT_RATIO))

    max_x = max(0, width - window_width)
    max_y = max(0, height - window_height)

    x_positions = [int(round(value)) for value in np.linspace(0, max_x, num=max(BLUR_GRID_COLS, 1))]
    y_positions = [int(round(value)) for value in np.linspace(0, max_y, num=max(BLUR_GRID_ROWS, 1))]

    crops: list[np.ndarray] = []
    for y1 in y_positions:
        for x1 in x_positions:
            x2 = min(width, x1 + window_width)
            y2 = min(height, y1 + window_height)
            crops.append(image[y1:y2, x1:x2])
    return crops


def variance_of_laplacian(image: np.ndarray) -> float:
    gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
    return float(cv2.Laplacian(gray, cv2.CV_64F).var())


def analyze_image(path: Path) -> tuple[float, bool, np.ndarray]:
    image = load_image(path)
    representative_crop = crop_image_borders(image)
    scores = [variance_of_laplacian(cropped) for cropped in generate_overlapping_grid_crops(representative_crop)]
    score = float(np.median(np.array(scores, dtype=np.float64)))
    is_flagged = score < BLUR_THRESHOLD
    return score, is_flagged, representative_crop


def build_cropped_output_path(path: Path) -> Path:
    return path.with_name(f"{path.stem}_cropped{path.suffix}")


def build_windows_output_dir(path: Path) -> Path:
    return path.parent / f"{path.stem}_windows"


def main() -> None:
    path = Path(sys.argv[1]) if len(sys.argv) > 1 else Path(IMAGE_PATH)
    score, is_flagged, cropped = analyze_image(path)
    cropped_output_path = build_cropped_output_path(path)
    windows_output_dir = build_windows_output_dir(path)

    saved = cv2.imwrite(str(cropped_output_path), cropped)
    if not saved:
        raise RuntimeError(f"No se pudo guardar el recorte en: {cropped_output_path}")

    windows_output_dir.mkdir(parents=True, exist_ok=True)
    window_crops = generate_overlapping_grid_crops(cropped)
    for index, window_crop in enumerate(window_crops, start=1):
        window_path = windows_output_dir / f"{path.stem}_window_{index:02d}{path.suffix}"
        saved_window = cv2.imwrite(str(window_path), window_crop)
        if not saved_window:
            raise RuntimeError(f"No se pudo guardar la ventana en: {window_path}")

    print(f"image_path: {path}")
    print(f"crop_shape: {cropped.shape[1]}x{cropped.shape[0]}")
    print(f"cropped_image_path: {cropped_output_path}")
    print(f"windows_output_dir: {windows_output_dir}")
    print(f"windows_saved: {len(window_crops)}")
    print(f"blur_threshold: {BLUR_THRESHOLD:.2f}")
    print(f"variance_of_laplacian: {score:.2f}")
    print(f"imagen_borrosa_flag: {'yes' if is_flagged else 'no'}")


if __name__ == "__main__":
    main()
