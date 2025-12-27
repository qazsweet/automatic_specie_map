#!/usr/bin/env python3
"""
Read an image and generate an edge map using scikit-image.

Example:
  python3 edge_detect.py --input "test/maps/dpi200_Ancient_Antwren.png"
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
from skimage import color, feature, io, util
from skimage.transform import probabilistic_hough_line


def detect_edges(
    image: np.ndarray,
    *,
    sigma: float = 1.5,
    low_threshold: float | None = None,
    high_threshold: float | None = None,
) -> np.ndarray:
    """
    Returns a boolean edge map computed with Canny.
    """
    if image.ndim == 3:
        # Common cases: RGB (H,W,3) or RGBA (H,W,4).
        if image.shape[-1] == 4:
            image = color.rgba2rgb(image)
        gray = color.rgb2gray(image)
    else:
        gray = util.img_as_float(image)

    edges = feature.canny(
        gray,
        sigma=sigma,
        low_threshold=low_threshold,
        high_threshold=high_threshold,
    )
    return edges


def extract_lines_from_edges(
    edges: np.ndarray,
    *,
    threshold: int = 10,
    line_length: int = 50,
    line_gap: int = 10,
) -> list[tuple[tuple[int, int], tuple[int, int]]]:
    """
    Extract straight line segments from a boolean edge map.

    Uses a probabilistic Hough transform and returns segments as:
      [((x0, y0), (x1, y1)), ...]
    """
    if edges.dtype != bool:
        edges = edges.astype(bool)

    lines = probabilistic_hough_line(
        edges,
        threshold=threshold,
        line_length=line_length,
        line_gap=line_gap,
    )
    # skimage returns ((x0, y0), (x1, y1)) already, but keep it typed/explicit.
    return [((int(x0), int(y0)), (int(x1), int(y1))) for (x0, y0), (x1, y1) in lines]


def main() -> int:
    parser = argparse.ArgumentParser(description="Edge detection with scikit-image (Canny).")
    parser.add_argument(
        "--input",
        required=True,
        help="Path to an input image (png/jpg/...).",
    )
    parser.add_argument(
        "--output",
        default=None,
        help="Path to write the edge image (default: <input_stem>_edges.png next to input).",
    )
    parser.add_argument("--sigma", type=float, default=1.5, help="Gaussian smoothing sigma for Canny.")
    parser.add_argument("--low-threshold", type=float, default=None, help="Canny low threshold (0..1).")
    parser.add_argument("--high-threshold", type=float, default=None, help="Canny high threshold (0..1).")
    parser.add_argument(
        "--lines-json",
        default=None,
        help="Optional path to write detected line segments as JSON.",
    )
    parser.add_argument("--hough-threshold", type=int, default=10, help="Hough accumulator threshold.")
    parser.add_argument("--hough-line-length", type=int, default=50, help="Minimum accepted line length (px).")
    parser.add_argument("--hough-line-gap", type=int, default=10, help="Maximum gap between pixels (px).")
    args = parser.parse_args()

    input_path = Path(args.input)
    if not input_path.exists():
        raise FileNotFoundError(f"Input image not found: {input_path}")

    output_path = Path(args.output) if args.output else input_path.with_name(f"{input_path.stem}_edges.png")

    image = io.imread(str(input_path))
    edges_bool = detect_edges(
        image,
        sigma=args.sigma,
        low_threshold=args.low_threshold,
        high_threshold=args.high_threshold,
    )

    if args.lines_json:
        lines = extract_lines_from_edges(
            edges_bool,
            threshold=args.hough_threshold,
            line_length=args.hough_line_length,
            line_gap=args.hough_line_gap,
        )
        lines_payload = [
            {"x0": x0, "y0": y0, "x1": x1, "y1": y1} for ((x0, y0), (x1, y1)) in lines
        ]
        lines_path = Path(args.lines_json)
        lines_path.write_text(json.dumps(lines_payload, indent=2) + "\n", encoding="utf-8")
        print(f"Wrote lines JSON to: {lines_path} ({len(lines_payload)} segments)")

    # Save as a visible 8-bit image (0 or 255).
    edges_u8 = (edges_bool.astype(np.uint8) * 255)
    io.imsave(str(output_path), edges_u8)

    print(f"Wrote edges to: {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

