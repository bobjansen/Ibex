// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#pragma once

#include <ibex/core/column.hpp>
#include <ibex/runtime/interpreter.hpp>
#include <ibex/runtime/operator.hpp>

#include <cstddef>
#include <optional>
#include <utility>

namespace ibex::runtime {

/// Move a streamed chunk through the established table evaluator boundary.
/// Transport identity is deliberately not table metadata and is restored by
/// the caller that owns the one-input/one-output mapping contract.
[[nodiscard]] auto chunk_to_table(Chunk chunk) -> Table;
[[nodiscard]] auto table_to_chunk(Table table) -> Chunk;

/// Append `src`'s validity for `src_rows` rows onto `dst`, which currently
/// describes `dst_rows` rows.
///
/// Concatenating chunks cannot leave this implicit. A validity bitmap is not
/// carried by the column, so appending values alone leaves a bitmap describing
/// only the FIRST chunk while the column grows past it — and every row beyond
/// it then reads as null. A chunk with no bitmap is all-valid, so a column that
/// only gains nulls in a later chunk still needs one backfilled for the rows
/// already appended.
///
/// Latent until `IBEX_CHUNK_ROWS` existed: production never emitted a second
/// chunk, so no concat ever ran with one. It surfaced as an `order` over a
/// null-bearing column disagreeing with itself between the serial and parallel
/// gather, both of which were faithfully reading a bitmap that had run out.
inline void append_validity(std::optional<ValidityBitmap>& dst, std::size_t dst_rows,
                            const std::optional<ValidityBitmap>& src, std::size_t src_rows) {
    if (!dst.has_value() && !src.has_value()) {
        return;  // both all-valid — the common case needs no bitmap at all
    }
    if (!dst.has_value()) {
        ValidityBitmap filled;
        filled.reserve(dst_rows + src_rows);
        for (std::size_t r = 0; r < dst_rows; ++r) {
            filled.push_back(true);
        }
        dst = std::move(filled);
    }
    dst->reserve(dst_rows + src_rows);
    for (std::size_t r = 0; r < src_rows; ++r) {
        dst->push_back(src.has_value() ? (*src)[r] : true);
    }
}

}  // namespace ibex::runtime
