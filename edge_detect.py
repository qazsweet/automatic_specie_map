#!/usr/bin/env python3
"""
Read an image and generate an edge map using scikit-image.

Example:
  python3 edge_detect.py --input "test/maps/dpi200_Ancient_Antwren.png"
"""

from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
from skimage import color, feature, io, util


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

    # Save as a visible 8-bit image (0 or 255).
    edges_u8 = (edges_bool.astype(np.uint8) * 255)
    io.imsave(str(output_path), edges_u8)

    print(f"Wrote edges to: {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

