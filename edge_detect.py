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
from matplotlib import pyplot as plt
from matplotlib.patches import Circle, Rectangle
from skimage import color, feature, io, util
from skimage.draw import line as draw_line
from skimage.measure import label, regionprops
from skimage.morphology import closing, dilation, disk, remove_small_objects
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


def _merge_overlapping_boxes(
    boxes: list[tuple[int, int, int, int]],
    *,
    iou_threshold: float = 0.3,
) -> list[tuple[int, int, int, int]]:
    """
    Greedily merge boxes whose IoU exceeds iou_threshold.

    Boxes are (min_row, min_col, max_row, max_col).
    """

    def iou(a: tuple[int, int, int, int], b: tuple[int, int, int, int]) -> float:
        ar0, ac0, ar1, ac1 = a
        br0, bc0, br1, bc1 = b
        ir0, ic0 = max(ar0, br0), max(ac0, bc0)
        ir1, ic1 = min(ar1, br1), min(ac1, bc1)
        ih, iw = max(0, ir1 - ir0), max(0, ic1 - ic0)
        inter = ih * iw
        if inter == 0:
            return 0.0
        area_a = max(0, ar1 - ar0) * max(0, ac1 - ac0)
        area_b = max(0, br1 - br0) * max(0, bc1 - bc0)
        union = area_a + area_b - inter
        return float(inter) / float(union) if union else 0.0

    def union(a: tuple[int, int, int, int], b: tuple[int, int, int, int]) -> tuple[int, int, int, int]:
        ar0, ac0, ar1, ac1 = a
        br0, bc0, br1, bc1 = b
        return (min(ar0, br0), min(ac0, bc0), max(ar1, br1), max(ac1, bc1))

    merged = list(boxes)
    changed = True
    while changed:
        changed = False
        out: list[tuple[int, int, int, int]] = []
        while merged:
            current = merged.pop()
            did_merge = False
            for idx, other in enumerate(merged):
                if iou(current, other) >= iou_threshold:
                    merged[idx] = union(current, other)
                    did_merge = True
                    changed = True
                    break
            if not did_merge:
                out.append(current)
        merged = out
    return merged


def extract_boxes_from_edges(
    edges: np.ndarray,
    *,
    dilate_radius: int = 1,
    closing_radius: int = 3,
    min_area: int = 200,
    merge_overlaps: bool = True,
    merge_iou_threshold: float = 0.3,
) -> list[tuple[int, int, int, int]]:
    """
    Extract bounding boxes from an edge map.

    This is a pragmatic approach:
    - dilate edges to connect gaps
    - close to form thicker, more continuous contours
    - remove tiny components
    - connected-components -> bounding boxes

    Returns boxes as (min_row, min_col, max_row, max_col).
    """
    if edges.dtype != bool:
        edges = edges.astype(bool)

    mask = edges
    if dilate_radius > 0:
        mask = dilation(mask, disk(dilate_radius))
    if closing_radius > 0:
        mask = closing(mask, disk(closing_radius))

    if min_area > 0:
        # skimage>=0.26 deprecates min_size; use max_size to keep only "large enough" components.
        # Note: max_size removes objects smaller than or equal to its value.
        mask = remove_small_objects(mask, max_size=int(min_area))

    labeled = label(mask)
    boxes: list[tuple[int, int, int, int]] = []
    for r in regionprops(labeled):
        min_row, min_col, max_row, max_col = r.bbox
        boxes.append((int(min_row), int(min_col), int(max_row), int(max_col)))

    if merge_overlaps and boxes:
        boxes = _merge_overlapping_boxes(boxes, iou_threshold=merge_iou_threshold)

    # stable order: top-to-bottom then left-to-right
    boxes.sort(key=lambda b: (b[0], b[1], b[2], b[3]))
    return boxes


def extract_boxes_from_lines(
    lines: list[tuple[tuple[int, int], tuple[int, int]]],
    image_shape: tuple[int, int],
    **kwargs,
) -> list[tuple[int, int, int, int]]:
    """
    Rasterize line segments into a mask, then call extract_boxes_from_edges().
    """
    h, w = int(image_shape[0]), int(image_shape[1])
    mask = np.zeros((h, w), dtype=bool)
    for (x0, y0), (x1, y1) in lines:
        rr, cc = draw_line(int(y0), int(x0), int(y1), int(x1))
        rr = np.clip(rr, 0, h - 1)
        cc = np.clip(cc, 0, w - 1)
        mask[rr, cc] = True
    return extract_boxes_from_edges(mask, **kwargs)


def box_centers(boxes: list[tuple[int, int, int, int]]) -> list[tuple[float, float]]:
    """
    Compute (cx, cy) centers for boxes.

    Boxes are (min_row, min_col, max_row, max_col).
    Returned centers are in image coordinates where x=col, y=row.
    """
    centers: list[tuple[float, float]] = []
    for min_row, min_col, max_row, max_col in boxes:
        cx = (min_col + max_col) / 2.0
        cy = (min_row + max_row) / 2.0
        centers.append((cx, cy))
    return centers


def plot_image_with_boxes_and_centers(
    image: np.ndarray,
    boxes: list[tuple[int, int, int, int]],
    *,
    circle_radius: float = 5.0,
    box_color: str = "lime",
    center_color: str = "red",
    linewidth: float = 2.0,
    save_path: str | Path | None = None,
    show: bool = False,
) -> None:
    """
    Plot the image, draw each box, and draw a circle at each box center.
    """
    # Normalize image for display (handle grayscale/RGBA)
    disp = image
    if disp.ndim == 3 and disp.shape[-1] == 4:
        disp = color.rgba2rgb(disp)

    fig, ax = plt.subplots(figsize=(8, 8), dpi=150)
    ax.imshow(disp, cmap="gray" if disp.ndim == 2 else None)
    ax.set_axis_off()

    for (min_row, min_col, max_row, max_col) in boxes:
        w = max_col - min_col
        h = max_row - min_row
        ax.add_patch(
            Rectangle(
                (min_col, min_row),
                w,
                h,
                fill=False,
                edgecolor=box_color,
                linewidth=linewidth,
            )
        )
        cx = (min_col + max_col) / 2.0
        cy = (min_row + max_row) / 2.0
        ax.add_patch(
            Circle(
                (cx, cy),
                radius=circle_radius,
                fill=False,
                edgecolor=center_color,
                linewidth=linewidth,
            )
        )

    fig.tight_layout(pad=0)
    if save_path is not None:
        fig.savefig(str(save_path), bbox_inches="tight", pad_inches=0)
    if show:
        plt.show()
    plt.close(fig)


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
    parser.add_argument(
        "--boxes-json",
        default=None,
        help="Optional path to write extracted bounding boxes as JSON.",
    )
    parser.add_argument(
        "--boxes-from",
        choices=("edges", "lines"),
        default="edges",
        help="Whether to extract boxes from the edge map or from detected lines.",
    )
    parser.add_argument("--box-dilate-radius", type=int, default=1, help="Box extraction: dilation radius (px).")
    parser.add_argument("--box-closing-radius", type=int, default=3, help="Box extraction: closing radius (px).")
    parser.add_argument("--box-min-area", type=int, default=200, help="Box extraction: minimum component area (px).")
    parser.add_argument(
        "--box-merge-iou",
        type=float,
        default=0.3,
        help="Box extraction: IoU threshold for merging overlaps.",
    )
    parser.add_argument(
        "--plot-output",
        default=None,
        help="Optional path to save a plot of the image with boxes + center circles.",
    )
    parser.add_argument(
        "--plot-show",
        action="store_true",
        help="Show the matplotlib window (may not work in headless environments).",
    )
    parser.add_argument("--center-circle-radius", type=float, default=5.0, help="Center circle radius (px).")
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

    lines: list[tuple[tuple[int, int], tuple[int, int]]] | None = None
    if args.lines_json or (args.boxes_json and args.boxes_from == "lines"):
        lines = extract_lines_from_edges(
            edges_bool,
            threshold=args.hough_threshold,
            line_length=args.hough_line_length,
            line_gap=args.hough_line_gap,
        )
        if args.lines_json:
            lines_payload = [
                {"x0": x0, "y0": y0, "x1": x1, "y1": y1} for ((x0, y0), (x1, y1)) in lines
            ]
            lines_path = Path(args.lines_json)
            lines_path.write_text(json.dumps(lines_payload, indent=2) + "\n", encoding="utf-8")
            print(f"Wrote lines JSON to: {lines_path} ({len(lines_payload)} segments)")

    if args.boxes_json:
        if args.boxes_from == "lines":
            if lines is None:
                lines = []
            boxes = extract_boxes_from_lines(
                lines,
                edges_bool.shape[:2],
                dilate_radius=args.box_dilate_radius,
                closing_radius=args.box_closing_radius,
                min_area=args.box_min_area,
                merge_overlaps=True,
                merge_iou_threshold=args.box_merge_iou,
            )
        else:
            boxes = extract_boxes_from_edges(
                edges_bool,
                dilate_radius=args.box_dilate_radius,
                closing_radius=args.box_closing_radius,
                min_area=args.box_min_area,
                merge_overlaps=True,
                merge_iou_threshold=args.box_merge_iou,
            )
        boxes_payload = [
            {
                "x": int(min_col),
                "y": int(min_row),
                "w": int(max_col - min_col),
                "h": int(max_row - min_row),
                "min_row": int(min_row),
                "min_col": int(min_col),
                "max_row": int(max_row),
                "max_col": int(max_col),
            }
            for (min_row, min_col, max_row, max_col) in boxes
        ]
        boxes_path = Path(args.boxes_json)
        boxes_path.write_text(json.dumps(boxes_payload, indent=2) + "\n", encoding="utf-8")
        print(f"Wrote boxes JSON to: {boxes_path} ({len(boxes_payload)} boxes)")
    else:
        boxes = []

    if args.plot_output or args.plot_show:
        plot_path = Path(args.plot_output) if args.plot_output else None
        plot_image_with_boxes_and_centers(
            image,
            boxes,
            circle_radius=args.center_circle_radius,
            save_path=plot_path,
            show=args.plot_show,
        )
        if plot_path is not None:
            print(f"Wrote plot to: {plot_path}")

    # Save as a visible 8-bit image (0 or 255).
    edges_u8 = (edges_bool.astype(np.uint8) * 255)
    io.imsave(str(output_path), edges_u8)

    print(f"Wrote edges to: {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

