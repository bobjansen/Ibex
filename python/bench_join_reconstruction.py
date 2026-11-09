# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen
"""Diagnostic stages for bench_join_gap --reconstruction.

Each stage reuses prepared inputs; medians are not additive. The cached-left
route is a counterfactual upper bound, not an implementation: it excludes the
cost of producing the selection and constructing the left frame.
"""
import statistics
import time

import numpy as np
import polars as pl
from polars.testing import assert_frame_equal
from ibex_polars import _engine
from bench_category_isolation import stages


def measure(raw, expected, output, categories, left_rows, iterations):
    if left_rows is None:
        return {'applicable': False}
    row, source = left_rows
    column = raw[row]
    def row_array():
        return column.chunk(0) if column.num_chunks == 1 else column.combine_chunks()
    rows = row_array()
    numbers = rows.to_numpy(zero_copy_only=False) if rows.null_count == 0 else None
    ordered = numbers is not None and len(numbers) > 0 and (
        len(numbers) == 1 or (numbers[1:] > numbers[:-1]).all())
    identity = bool(ordered and len(numbers) == source.height and numbers[0] == 0
                    and numbers[-1] == source.height - 1)
    def mask():
        keep = np.zeros(source.height, dtype=bool)
        keep[numbers] = True
        return pl.Series(keep)
    left = _engine._left_rows(rows, source)
    left_count = list(raw.column_names).index(row) + 1
    right_names = list(expected)[source.width:]
    native = raw.select(list(range(left_count, raw.num_columns))).rename_columns(right_names)
    rest = {name: expected[name] for name in right_names}
    right_output = dict(zip(right_names, (output[n] for n in raw.column_names[left_count:])))
    def right():
        return _engine._to_polars(native, rest, right_output, categories)
    right_frame = right()
    def assemble():
        return pl.DataFrame([*left.get_columns(), *right_frame.get_columns()])
    def cached_left():
        return pl.DataFrame([*left.get_columns(), *right().get_columns()])
    def complete():
        return _engine._to_polars(raw, expected, output, categories, left_rows)
    # Order-sensitive checks catch misalignment, not just matching row sets.
    reference = complete()
    assert_frame_equal(assemble(), reference)
    assert_frame_equal(cached_left(), reference)
    del reference
    calls = {'complete': complete, 'cached_left_upper_bound': cached_left,
             'row_array': row_array, 'left_total': lambda: _engine._left_rows(row_array(), source),
             'right_total': right, 'assembly': assemble}
    if numbers is not None:
        calls['numpy_view'] = lambda: rows.to_numpy(zero_copy_only=False)
        calls['order_check'] = lambda: (numbers[1:] > numbers[:-1]).all()
    if ordered and not identity:
        keep = mask()
        calls['mask_build'] = mask
        calls['left_filter'] = lambda: source.filter(keep)
    info = {}
    for name in right_names:
        data = native.select([name])
        dtype, native_type = rest[name], right_output[name]
        calls['right_column:' + name] = lambda data=data, name=name, dtype=dtype, native_type=native_type: (
            _engine._to_polars(data, {name: dtype}, {name: native_type}, categories))
        info[name] = {'expected': str(dtype), 'arrow': str(native[name].type),
                      'chunks': native[name].num_chunks, 'nulls': native[name].null_count}
        if dtype.base_type() == pl.Categorical:
            known = _engine._known_codes(native[name], dtype, name, categories)
            info[name]['known_codes'] = known is not None
            if known is not None:
                info[name]['id_mapping'] = 'offset' if known[1].offset is not None else 'lookup'
                info[name]['dictionary_entries'] = len(known[1].ids)
                for stage, call in stages(known[0], known[1], dtype, name).items():
                    calls['category:' + name + ':' + stage] = call
                calls['category:' + name + ':known_codes'] = lambda name=name, dtype=dtype: (
                    _engine._known_codes(native[name], dtype, name, categories))
    for call in calls.values():
        call(); call()
    samples = {key: [] for key in calls}
    previous = None
    for iteration in range(iterations):
        order = list(calls) if iteration % 2 == 0 else list(reversed(calls))
        for key in order:
            start = time.perf_counter_ns()
            current = calls[key]()
            elapsed = time.perf_counter_ns() - start
            samples[key].append(elapsed / 1e6)
            previous = current
    from ab_queries import wilcoxon_p
    before, after = samples['complete'], samples['cached_left_upper_bound']
    return {'applicable': True, 'notes': __doc__, 'source_rows': source.height,
            'row_chunks': column.num_chunks, 'row_nulls': rows.null_count,
            'ordered_unique': bool(ordered), 'identity': identity, 'right_columns': info,
            'samples_ms': samples, 'median_ms': {k: statistics.median(v) for k, v in samples.items()},
            'cached_left_over_complete': statistics.median([a/b for a, b in zip(after, before)]),
            'cached_left_p': wilcoxon_p([a-b for a, b in zip(after, before)])}
