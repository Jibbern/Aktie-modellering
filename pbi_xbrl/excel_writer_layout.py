"""Pure layout estimation helpers for workbook writer surfaces."""
from __future__ import annotations

import math
import re
from typing import Any


def estimate_wrapped_line_count(
    text: Any,
    col_chars: float,
    min_lines: int = 1,
    max_lines: int = 6,
) -> float:
    txt = "" if text is None else str(text)
    txt = txt.replace("\r\n", "\n").replace("\r", "\n")
    paragraphs = [re.sub(r"[ \t\f\v]+", " ", part).strip() for part in txt.split("\n")]
    effective_chars_per_line = max(18, int(float(col_chars or 0) * 1.02) - 4)
    estimated_lines = 0.0
    seen_nonempty = 0
    for para in paragraphs:
        if not para:
            if seen_nonempty > 0:
                estimated_lines += 0.35
            continue
        if seen_nonempty > 0:
            estimated_lines += 0.35 if len(para) <= max(14, int(effective_chars_per_line * 0.35)) else 0.55
        raw_lines = max(1, math.ceil(len(para) / float(effective_chars_per_line)))
        weighted_lines = float(raw_lines)
        remainder = len(para) % effective_chars_per_line
        if raw_lines >= 2 and remainder and remainder <= max(10, int(effective_chars_per_line * 0.18)):
            weighted_lines = max(1.0, weighted_lines - 0.30)
        estimated_lines += weighted_lines
        seen_nonempty += 1
    return min(max(estimated_lines or 1.0, float(min_lines)), float(max_lines))


def estimate_wrapped_row_height(
    text: Any,
    col_chars: float,
    base_height: float,
    line_height: float,
    min_lines: int = 1,
    max_lines: int = 6,
) -> float:
    estimated_lines = estimate_wrapped_line_count(text, col_chars, min_lines=min_lines, max_lines=max_lines)
    if estimated_lines <= 1.15:
        row_h = float(base_height)
    elif estimated_lines <= 2.15:
        row_h = float(base_height) + float(line_height)
    elif estimated_lines <= 3.15:
        row_h = float(base_height) + (2.0 * float(line_height))
    else:
        extra_lines = max(0.0, estimated_lines - 3.15)
        row_h = float(base_height) + (2.0 * float(line_height)) + (
            math.ceil(extra_lines) * max(1.0, float(line_height) - 2.0)
        )
    max_height = float(base_height) + (max(0, int(max_lines) - 1) * float(line_height))
    row_h = min(row_h, max_height)
    return round(row_h * 2.0) / 2.0
