// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#pragma once

#include <ibex/core/column.hpp>
#include <ibex/runtime/interpreter.hpp>
#include <ibex/runtime/operator.hpp>

#include <cstddef>
#include <cstdint>
#include <optional>
#include <utility>

namespace ibex::runtime {

/// Move a streamed chunk through the established table evaluator boundary.
/// Transport identity is deliberately not table metadata and is restored by
/// the caller that owns the one-input/one-output mapping contract.
[[nodiscard]] auto chunk_to_table(Chunk chunk) -> Table;
[[nodiscard]] auto table_to_chunk(Table table) -> Chunk;

/// Morsel identity survives every one-input/one-output parallel-map operator.
/// It is intentionally separate from Table metadata: sequence/row offset are
/// executor transport state, never user-visible table properties.
struct ChunkIdentity {
    std::uint64_t sequence = 0;
    std::size_t row_offset = 0;
};

[[nodiscard]] inline auto chunk_identity_of(const Chunk& chunk) -> ChunkIdentity {
    return ChunkIdentity{.sequence = chunk.sequence, .row_offset = chunk.row_offset};
}

[[nodiscard]] inline auto table_to_chunk(Table table, ChunkIdentity identity) -> Chunk {
    auto chunk = table_to_chunk(std::move(table));
    chunk.sequence = identity.sequence;
    chunk.row_offset = identity.row_offset;
    return chunk;
}

/// Preserves schema for operators that skip zero-row results.
///
/// A stream carries its schema in its chunks, so an operator that emits no chunk
/// emits no schema either: the result materializes as a table with no columns at
/// all, and anything downstream that names a column — a join looking for its key,
/// a filter for the value it compares — fails with "unknown column" on what is
/// really just an empty input.
///
/// Callers offer empty results as Tables; this helper retains the first one with
/// columns, together with its chunk identity. At end of stream, release() converts
/// it back to a zero-row chunk only if emitted() has never been called. The Table
/// is storage for the empty columns and their metadata; no filtering happens here.
class SchemaCarrier {
   public:
    /// Offer a zero-row result as the schema of last resort.
    void hold(Table&& empty, ChunkIdentity identity = {}) {
        if (!held_.has_value() && !empty.columns.empty()) {
            held_ = Held{.table = std::move(empty), .identity = identity};
        }
    }
    /// Whether a schema is already held, for a caller whose empty Table costs
    /// something to build and who would otherwise build one per chunk.
    [[nodiscard]] auto holding() const noexcept -> bool { return held_.has_value(); }
    void emitted() { emitted_ = true; }
    /// The held chunk — once, and only if nothing else was ever emitted.
    [[nodiscard]] auto release() -> std::optional<Chunk> {
        if (emitted_ || !held_.has_value()) {
            return std::nullopt;
        }
        emitted_ = true;
        return table_to_chunk(std::move(held_->table), held_->identity);
    }

   private:
    struct Held {
        Table table;
        ChunkIdentity identity;
    };
    std::optional<Held> held_;
    bool emitted_ = false;
};

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
