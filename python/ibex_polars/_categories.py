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
    """The column as one int32/utf8 dictionary chunk, and what its entries map to.

    One pass over the physical ids finds which labels occur, in id order.
    """
    ids_column = series.to_physical().to_arrow()
    if isinstance(ids_column, pa.ChunkedArray):
        ids_column = ids_column.combine_chunks()
    nulls = None
    if ids_column.null_count:
        nulls = ids_column.is_null().to_numpy(zero_copy_only=False)
        ids_column = ids_column.fill_null(0)
    values = ids_column.to_numpy(zero_copy_only=False)
    top = int(values.max())
    if top >= 2**31:
        raise NotImplementedError("categorical ids exceed the native Int32 code range")
    present = np.zeros(top + 1, dtype=bool)
    if nulls is None:
        present[values] = True
    else:
        present[values[~nulls]] = True
    ids = np.flatnonzero(present).astype(np.uint32)
    labels = pl.Series(ids, dtype=pl.UInt32).cat.to(series.dtype).cast(pl.String)
    # The native importer reads 32-bit offsets, as it does for any utf8 column.
    dictionary = labels.to_arrow().cast(pa.string())
    if len(ids) == top - int(ids[0]) + 1:
        # Ids a fresh cast assigns are consecutive: the code is the offset.
        codes = values.view(np.int32) - np.int32(ids[0])
    else:
        codes_of = np.zeros(top + 1, dtype=np.int32)
        codes_of[ids] = np.arange(len(ids), dtype=np.int32)
        codes = codes_of[values]
    if nulls is not None:
        codes[nulls] = 0
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
