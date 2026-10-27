# SPDX-License-Identifier: AGPL-3.0-only
# Copyright (C) 2026 Bob Jansen
"""Categorical columns that keep their Polars category ids across Ibex.

A resident Polars Categorical column reaches Ibex as one dictionary chunk whose
entries are the column's distinct labels, and each entry remembers the physical
id Polars gave its label. A result column that still carries that dictionary
maps its codes back to those ids with a gather. Only other dictionaries have to
be interned label by label, which costs tens of nanoseconds per distinct label.
"""

from dataclasses import dataclass

import numpy as np
import polars as pl
import pyarrow as pa


@dataclass(frozen=True)
class Known:
    """A dictionary handed to Ibex, and the Polars id of each of its entries."""

    dictionary: pa.Array
    ids: np.ndarray
    dtype: pl.DataType


def eligible(series):
    """Whether `unify` can represent the column.

    Narrower physical types keep the label-based path, as does a column with no
    labels: it has nothing to map and its dictionary would be empty.
    """
    return (series.dtype.base_type() == pl.Categorical and series.to_physical().dtype == pl.UInt32
            and series.null_count() < len(series))


def unify(series):
    """The column as one int32/utf8 dictionary chunk, and what its entries map to."""
    physical = series.to_physical().rechunk().to_arrow()
    if isinstance(physical, pa.ChunkedArray):
        physical = physical.combine_chunks()
    labels = series.drop_nulls().unique()
    ids = labels.to_physical().to_numpy()
    # The native importer reads 32-bit offsets, as it does for any utf8 column.
    dictionary = labels.cast(pl.String).to_arrow().cast(pa.string())
    codes_of = np.zeros(int(ids.max()) + 1, dtype=np.int32)
    codes_of[ids] = np.arange(len(ids), dtype=np.int32)
    nulls = physical.is_null().to_numpy(zero_copy_only=False) if physical.null_count else None
    codes = codes_of[physical.fill_null(0).to_numpy(zero_copy_only=False)]
    array = pa.DictionaryArray.from_arrays(pa.array(codes, mask=nulls), dictionary)
    return array, Known(dictionary, ids, series.dtype)


def physical(chunk, categories, dtype):
    """Polars ids of a result chunk, or None unless its dictionary is a known one.

    Lengths are compared first so that only a plausible dictionary is read.
    """
    dictionary = chunk.dictionary
    for known in categories:
        if known.dtype == dtype and len(known.dictionary) == len(dictionary) \
                and known.dictionary.equals(dictionary):
            break
    else:
        return None
    indices = chunk.indices
    nulls = indices.is_null().to_numpy(zero_copy_only=False) if indices.null_count else None
    if nulls is not None:
        indices = indices.fill_null(0)
    ids = known.ids[indices.to_numpy(zero_copy_only=False)]
    return pl.from_arrow(pa.array(ids, pa.uint32(), mask=nulls))
