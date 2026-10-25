# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen
"""Logical output layout shared by join validation and native lowering."""

from ._expr import _identifier


def join_layout(left, right, left_on, right_on, how, coalesce, suffix):
    if how in ("semi", "anti"):
        return [(name, "left", name, None) for name in left]
    left_keys, right_keys = dict(zip(left_on, right_on)), set(right_on)
    result = []
    for name in left:
        if coalesce and how == "right" and name in left_keys:
            continue
        side = "coalesce" if coalesce and how == "full" and name in left_keys else "left"
        result.append((name, side, name, left_keys.get(name)))
    occupied = {field[0] for field in result}
    left_outputs = frozenset(occupied)
    for name in right:
        if coalesce and how != "right" and name in right_keys:
            continue
        output = name + suffix if name in left_outputs else name
        _identifier(output)
        if output in occupied:
            raise ValueError(f"duplicate join output column {output!r}")
        occupied.add(output)
        result.append((output, "right", name, None))
    return result
