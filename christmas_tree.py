#!/usr/bin/env python3
"""
在终端打印一棵 ASCII 圣诞树（可选彩色、装饰、随机种子）。

示例：
  python3 christmas_tree.py
  python3 christmas_tree.py --height 18 --ornaments 0.08 --color --seed 42
"""

from __future__ import annotations

import argparse
import random
import sys


ANSI_RESET = "\x1b[0m"
ANSI_GREEN = "\x1b[32m"
ANSI_BROWN = "\x1b[33m"
ANSI_RED = "\x1b[31m"
ANSI_YELLOW = "\x1b[33;1m"
ANSI_BLUE = "\x1b[34m"
ANSI_MAGENTA = "\x1b[35m"


def _colorize(text: str, color: str, enable: bool) -> str:
    if not enable:
        return text
    return f"{color}{text}{ANSI_RESET}"


def render_tree(
    *,
    height: int,
    trunk_height: int,
    trunk_width: int,
    ornaments_rate: float,
    color: bool,
    rng: random.Random,
) -> str:
    if height < 3:
        raise ValueError("height 必须 >= 3")
    if trunk_height < 1:
        raise ValueError("trunk_height 必须 >= 1")
    if trunk_width < 1 or trunk_width % 2 == 0:
        raise ValueError("trunk_width 必须为奇数且 >= 1")
    if not (0.0 <= ornaments_rate <= 1.0):
        raise ValueError("ornaments 必须在 [0, 1] 范围内")

    # 树冠最大宽度（奇数）
    max_width = 2 * height - 1
    mid = max_width // 2

    ornament_palette = [ANSI_RED, ANSI_YELLOW, ANSI_BLUE, ANSI_MAGENTA]

    lines: list[str] = []

    # 顶部星星（用 + 避免与第一层树冠重复）
    star = _colorize("+", ANSI_YELLOW, color)
    lines.append(" " * mid + star)

    # 树冠
    for i in range(1, height + 1):
        width = 2 * i - 1
        left_pad = (max_width - width) // 2

        row_chars: list[str] = []
        for _ in range(width):
            # 边缘用叶子，内部随机点缀装饰
            if rng.random() < ornaments_rate:
                c = _colorize("o", rng.choice(ornament_palette), color)
            else:
                c = _colorize("*", ANSI_GREEN, color)
            row_chars.append(c)

        lines.append(" " * left_pad + "".join(row_chars))

    # 树干
    trunk_left_pad = (max_width - trunk_width) // 2
    trunk_unit = _colorize("|" * trunk_width, ANSI_BROWN, color)
    for _ in range(trunk_height):
        lines.append(" " * trunk_left_pad + trunk_unit)

    # 底座
    base = _colorize("=" * max_width, ANSI_BROWN, color)
    lines.append(base)

    return "\n".join(lines)


def parse_args(argv: list[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="打印一棵 ASCII 圣诞树")
    p.add_argument("--height", type=int, default=12, help="树冠高度（默认 12）")
    p.add_argument("--trunk-height", type=int, default=3, help="树干高度（默认 3）")
    p.add_argument("--trunk-width", type=int, default=3, help="树干宽度（奇数，默认 3）")
    p.add_argument(
        "--ornaments",
        type=float,
        default=0.06,
        help="装饰出现概率 [0,1]（默认 0.06）",
    )
    p.add_argument("--seed", type=int, default=None, help="随机种子（默认不固定）")
    p.add_argument(
        "--color",
        action="store_true",
        help="启用 ANSI 颜色（默认关闭）",
    )
    return p.parse_args(argv)


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    rng = random.Random(args.seed)
    try:
        out = render_tree(
            height=args.height,
            trunk_height=args.trunk_height,
            trunk_width=args.trunk_width,
            ornaments_rate=args.ornaments,
            color=args.color and sys.stdout.isatty(),
            rng=rng,
        )
    except ValueError as e:
        print(f"参数错误：{e}", file=sys.stderr)
        return 2

    print(out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))

