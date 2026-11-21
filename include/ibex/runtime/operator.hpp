// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#pragma once

#include <ibex/core/column.hpp>
#include <ibex/ir/node.hpp>
#include <ibex/runtime/interpreter.hpp>
#include <ibex/runtime/interrupt.hpp>
#include <ibex/runtime/table_properties.hpp>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <memory>
#include <optional>
#include <string>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>

namespace ibex::runtime {

/// A horizontal slice of a table flowing through an operator pipeline.
///
/// Carries the same `ColumnEntry` shape as `Table` — columns with
/// optional per-row validity bitmaps. Table-level metadata (`ordering`,
/// `time_index`) is only meaningful on a fully materialized table, so
/// it is not part of `Chunk` by default. During the chunked-execution
/// migration, `TableSourceOperator` wraps a pre-built `Table` as a
/// single chunk and stashes that table's metadata on the chunk so
/// `MaterializeOperator` can restore it on the receiving side. Sources
/// that are natively chunked leave the metadata fields empty.
struct Chunk {
    std::vector<ColumnEntry> columns;

    /// Order-sensitive metadata. Private because assembling it field by field is
    /// what produced the bugs `TableProperties` exists to prevent: an operator
    /// that copies `time_index` through and forgets that its rows moved has
    /// written something false, and nothing in a plain struct stops it. Read it
    /// here; replace it wholesale through `set_properties`.
    [[nodiscard]] auto properties() const noexcept -> const TableProperties& { return props_; }
    [[nodiscard]] auto ordering() const noexcept
        -> const std::optional<std::vector<ir::OrderKey>>& {
        return props_.ordering();
    }
    [[nodiscard]] auto time_index() const noexcept -> const std::optional<std::string>& {
        return props_.time_index();
    }
    [[nodiscard]] auto grouped_by() const noexcept -> const std::vector<std::string>& {
        return props_.grouped_by();
    }

    /// The single mutator. Applies the TimeFrame invariant on the way in, so a
    /// caller cannot land a half-stated claim.
    void set_properties(const TableProperties& props) { props_ = props.normalized(); }

   private:
    TableProperties props_;

   public:
    /// Logical row count for a column-less chunk (e.g. from `Table(n)`); see
    /// the matching field on `Table`. Only consulted by `rows()` when `columns`
    /// is empty.
    std::optional<std::size_t> logical_rows;

    /// Position of this chunk in its source's emission order. When a chunked
    /// source partitions one immutable table into row ranges (see
    /// `PartitionedTableSource` / `TableRangeMorsel`), workers may produce those
    /// ranges out of order; `sequence` lets an ordered merger reassemble the
    /// original row order. Zero for the single-source and chunk-at-a-time
    /// paths that do not partition. Chunk-preserving operators must copy it
    /// through unchanged (including empty schema-carrier chunks).
    std::uint64_t sequence = 0;

    /// Absolute starting row of this chunk within its source table, i.e. the
    /// index in the original (un-partitioned) table of this chunk's first row.
    /// Zero for non-partitioned sources. Like `sequence`, it must survive
    /// chunk-preserving operators so range-aware kernels can address the shared
    /// input by absolute index.
    std::size_t row_offset = 0;

    void add_column(std::string name, ColumnValue column,
                    std::optional<ValidityBitmap> validity = std::nullopt) {
        columns.push_back(ColumnEntry{
            .name = std::move(name),
            .column = std::make_shared<ColumnValue>(std::move(column)),
            .validity = std::move(validity),
        });
    }

    void replace_column(std::size_t pos, ColumnValue column) {
        columns.at(pos).column = std::make_shared<ColumnValue>(std::move(column));
    }

    void replace_column(std::size_t pos, ColumnValue column,
                        std::optional<ValidityBitmap> validity) {
        auto& entry = columns.at(pos);
        entry.column = std::make_shared<ColumnValue>(std::move(column));
        entry.validity = std::move(validity);
    }

    [[nodiscard]] auto mutable_column(std::size_t pos) -> ColumnValue& {
        auto& column = columns.at(pos).column;
        if (column.use_count() != 1) {
            column = std::make_shared<ColumnValue>(*column);
        }
        return *column;
    }

    [[nodiscard]] auto rows() const noexcept -> std::size_t {
        if (columns.empty()) {
            return logical_rows.value_or(0);
        }
        return column_size(*columns.front().column);
    }
};

/// Pull-based operator interface. `next()` returns the operator's next
/// chunk, or `std::nullopt` when the stream is exhausted. Errors
/// propagate as `std::unexpected`.
class Operator {
   public:
    Operator() = default;
    Operator(const Operator&) = delete;
    Operator(Operator&&) = delete;
    auto operator=(const Operator&) -> Operator& = delete;
    auto operator=(Operator&&) -> Operator& = delete;
    virtual ~Operator() = default;

    [[nodiscard]] virtual auto next() -> std::expected<std::optional<Chunk>, std::string> = 0;
};

using OperatorPtr = std::unique_ptr<Operator>;

/// Source operator that wraps an already-materialized `Table` and emits
/// it as a single chunk. Used as an adapter during the chunked-execution
/// migration: any operator that still produces a full `Table` can be
/// wrapped in a `TableSourceOperator` to plug into the chunk pipeline.
class TableSourceOperator final : public Operator {
   public:
    explicit TableSourceOperator(Table table) : table_(std::move(table)) {}

    [[nodiscard]] auto next() -> std::expected<std::optional<Chunk>, std::string> override {
        if (emitted_) {
            return std::optional<Chunk>{};
        }
        emitted_ = true;
        Chunk chunk;
        chunk.columns = std::move(table_.columns);
        chunk.set_properties(table_.properties());
        // logical_rows is only meaningful for a column-less frame.
        if (chunk.columns.empty()) {
            chunk.logical_rows = table_.logical_rows;
        }
        return std::optional<Chunk>{std::move(chunk)};
    }

   private:
    Table table_;
    bool emitted_ = false;
};

/// Sink that drains a child operator into a `Table`. Chunks are
/// consumed one at a time: the first chunk's columns are moved into
/// the result, and every subsequent chunk is appended and released
/// before the next is pulled, so peak memory is bounded by
/// `result + 1 chunk` rather than the full chunk list.
///
/// Concat assumes all chunks agree on schema (column count, names, and
/// variant alternatives) and that any `Column<Categorical>` values across
/// chunks share the same backing dictionary — which is the contract the
/// chunked csv source provides. Per-chunk validity bitmaps are concatenated
/// across chunks: a missing bitmap on any chunk means "every row valid", so
/// a nullable column that carries validity on only some chunks is widened to
/// a full-length bitmap on demand.
///
/// Append a chunk column's optional validity onto the result column's optional
/// validity. `prev_rows` is the result column's length before this chunk and
/// `src_rows` the chunk column's length. A `nullopt` bitmap means every row is
/// valid; the result stays `nullopt` (zero overhead) until some chunk actually
/// carries nulls, at which point the previously-accumulated rows are backfilled
/// as valid.
inline void materialize_append_validity(std::optional<ValidityBitmap>& dst_valid,
                                        std::size_t prev_rows,
                                        const std::optional<ValidityBitmap>& src_valid,
                                        std::size_t src_rows) {
    if (!dst_valid.has_value() && !src_valid.has_value()) {
        return;  // both all-valid: keep the column bitmap-free.
    }
    if (!dst_valid.has_value()) {
        // The result was implicitly all-valid so far; materialize that prefix.
        dst_valid.emplace(prev_rows, true);
    }
    // Word-wise either way. A push_back(operator[]) per row was 5% of a 1-core
    // grouped update over a nullable 1e7-row table, and an exact reserve() per
    // chunk reallocated the whole bitmap each time.
    if (src_valid.has_value()) {
        dst_valid->append(*src_valid, src_rows);
    } else {
        dst_valid->resize(prev_rows + src_rows, true);
    }
}

/// Append every value of `src_value` onto `dst_value`. Both must already hold
/// the same variant alternative; callers check that and report a schema
/// mismatch, which is a better error than this could give.
///
/// This is the merge concat that every multi-chunk path bottoms out in, and it
/// is not a small cost. Appending a `push_back` per row is what it used to do,
/// which for a flat numeric column is a bounds check and a size update per
/// element instead of one `memmove`: on PDS-H q21, gluing a 2.9M-row
/// intermediate back together row by row cost 33ms — the whole of that query's
/// regression once its source began arriving in six chunks rather than one.
/// With one chunk there is no concat at all, so nothing paid for this until
/// streaming existed.
///
/// Growth is left to the underlying vector, which grows geometrically.
/// Reserving exactly `size + chunk` before each append is the trap in the other
/// direction: it makes the concat quadratic in the chunk count, invisible at
/// one chunk and dominant at a few hundred.
inline void append_column_values(ColumnValue& dst_value, const ColumnValue& src_value) {
    std::visit(
        [&](auto& dst) {
            using Col = std::decay_t<decltype(dst)>;
            const auto& src = std::get<Col>(src_value);
            if constexpr (std::is_same_v<Col, Column<Categorical>>) {
                // Codes are copied as-is rather than re-interned, which is only
                // sound because the chunks share one dictionary — the contract
                // every caller of this relies on (`MaterializeOperator` unifies
                // a chunk that breaks it; see `CategoricalDictionaryUnifier`).
                const auto& codes = src.codes();
                dst.append_codes(codes.begin(), codes.end());
            } else if constexpr (std::is_same_v<Col, Column<std::string>>) {
                // One block of bytes, and the offsets shifted by a constant.
                dst.append(src);
            } else if constexpr (std::is_same_v<Col, Column<bool>>) {
                // Packed many rows per word, so shifted a word at a time.
                dst.append(src);
            } else if constexpr (std::is_same_v<Col, Column<std::int64_t>>) {
                dst.append_integer_values(src);
            } else {
                (void)dst.insert(dst.end(), src.begin(), src.end());
            }
        },
        dst_value);
}

/// Puts the Categorical columns of one chunk stream onto one dictionary per
/// column, owned by this object.
///
/// The chunk contract is that every chunk of a stream shares one dictionary,
/// so codes can be copied verbatim. Sources inside the runtime honor it (the
/// streamed scan remaps each unit at emission), but an extern source is free to
/// build every chunk over its own dictionary -- an Arrow stream does, batch by
/// batch -- and verbatim codes would then read the wrong labels, or past the
/// end of the dictionary.
///
/// Dictionaries only grow, so the translation for the last source dictionary
/// seen is kept and extended by its new entries: a source that shares one
/// growing dictionary pays for each entry once, plus one pass over the codes.
class CategoricalDictionaryUnifier {
   public:
    using code_type = Column<Categorical>::code_type;

    /// Whether `local` already speaks column `column`'s unified dictionary.
    [[nodiscard]] auto owns(std::size_t column, const Column<Categorical>& local) const noexcept
        -> bool {
        if (column >= states_.size()) {
            return false;
        }
        const std::optional<State>& state = states_[column];
        return state.has_value() &&
               state->shared.dictionary_identity() == local.dictionary_identity();
    }

    /// The translation for `local`'s dictionary: `remap[local code]` is the
    /// unified code. Only entries not translated before are interned -- one
    /// lookup per dictionary entry, never per row. The reference is valid
    /// until the next call for this column.
    [[nodiscard]] auto remap(std::size_t column, const Column<Categorical>& local)
        -> const std::vector<code_type>& {
        State& state = state_for(column);
        if (state.source.get() != local.dictionary_identity()) {
            // Held, not just compared: a freed dictionary's address can be
            // reused by the next chunk's.
            state.source = local.dictionary_ptr();
            state.remap.clear();
        }
        const std::vector<std::string>& dictionary = *state.source;
        if (state.remap.size() < dictionary.size()) {
            // Intern the new entries, then read back the code each landed on.
            // `clear()` drops those scratch codes and keeps the dictionary.
            const std::size_t first = state.remap.size();
            for (std::size_t entry = first; entry < dictionary.size(); ++entry) {
                state.shared.push_back(dictionary[entry]);
            }
            for (std::size_t entry = first; entry < dictionary.size(); ++entry) {
                state.remap.push_back(state.shared.code_at(entry - first));
            }
            state.shared.clear();
        }
        return state.remap;
    }

    /// A column with no rows over column `column`'s unified dictionary.
    [[nodiscard]] auto shared(std::size_t column) -> Column<Categorical> {
        return state_for(column).shared.with_codes();
    }

    /// `local`'s rows over column `column`'s unified dictionary; see `gather`.
    [[nodiscard]] auto translate(std::size_t column, const Column<Categorical>& local,
                                 const std::optional<ValidityBitmap>& validity)
        -> std::expected<Column<Categorical>, std::string> {
        const std::vector<code_type>& table = remap(column, local);
        return gather(table, state_for(column).shared, local, validity);
    }

    /// `local`'s rows over `shared`'s dictionary, where `table[local code]` is
    /// the shared code (a `remap` result, possibly kept from an earlier call).
    /// A null row (per `validity`) becomes code 0: its payload is unspecified
    /// and must not be read. A valid row whose code is not in its own
    /// dictionary is an error.
    [[nodiscard]] static auto gather(const std::vector<code_type>& table,
                                     const Column<Categorical>& shared,
                                     const Column<Categorical>& local,
                                     const std::optional<ValidityBitmap>& validity)
        -> std::expected<Column<Categorical>, std::string> {
        const auto entries = static_cast<std::uint32_t>(table.size());
        // Out-of-range codes read entry 0 (or this zero, for an empty
        // dictionary) and are reported after the loop, which keeps the loop a
        // plain gather.
        const code_type empty = 0;
        const code_type* lookup = table.empty() ? &empty : table.data();
        const std::size_t rows = local.size();
        const code_type* codes = local.codes_data();
        Column<Categorical>::codes_storage out(rows);
        bool out_of_range = false;
        if (!validity.has_value()) {
            for (std::size_t row = 0; row < rows; ++row) {
                const auto code = static_cast<std::uint32_t>(codes[row]);
                out_of_range |= code >= entries;
                out[row] = lookup[code < entries ? code : 0U];
            }
        } else {
            for (std::size_t row = 0; row < rows; ++row) {
                const bool valid = (*validity)[row];
                const auto code = valid ? static_cast<std::uint32_t>(codes[row]) : 0U;
                out_of_range |= valid && code >= entries;
                out[row] = valid ? lookup[code < entries ? code : 0U] : 0;
            }
        }
        if (out_of_range) {
            for (std::size_t row = 0; row < rows; ++row) {
                const auto code = static_cast<std::uint32_t>(codes[row]);
                if ((!validity.has_value() || (*validity)[row]) && code >= entries) {
                    return std::unexpected("Categorical code " + std::to_string(codes[row]) +
                                           " is outside its dictionary of " +
                                           std::to_string(entries) + " entries");
                }
            }
        }
        Column<Categorical> translated = shared.with_codes(std::move(out));
        translated.set_meta(local.meta());
        return translated;
    }

    /// Translate every Categorical column of `columns` that does not already
    /// speak its unified dictionary. Column `i` is unified with column `i` of
    /// every earlier call.
    [[nodiscard]] auto unify(std::vector<ColumnEntry>& columns)
        -> std::expected<void, std::string> {
        for (std::size_t i = 0; i < columns.size(); ++i) {
            auto& entry = columns[i];
            const auto* local = std::get_if<Column<Categorical>>(entry.column.get());
            if (local == nullptr || owns(i, *local)) {
                continue;
            }
            auto translated = translate(i, *local, entry.validity);
            if (!translated.has_value()) {
                return std::unexpected("column '" + entry.name + "': " + translated.error());
            }
            entry.column = std::make_shared<ColumnValue>(std::move(*translated));
        }
        return {};
    }

   private:
    struct State {
        Column<Categorical> shared;                        ///< no rows; owns the unified dictionary
        std::shared_ptr<std::vector<std::string>> source;  ///< last dictionary translated
        std::vector<code_type> remap;                      ///< `remap[source code]` = unified code
    };
    auto state_for(std::size_t column) -> State& {
        if (states_.size() <= column) {
            states_.resize(column + 1);
        }
        std::optional<State>& state = states_[column];
        if (!state.has_value()) {
            state.emplace();
        }
        return *state;
    }

    std::vector<std::optional<State>> states_;
};

class MaterializeOperator {
   public:
    explicit MaterializeOperator(OperatorPtr child) : child_(std::move(child)) {}

    [[nodiscard]] auto run() -> std::expected<Table, std::string> {
        Table result;

        auto first_res = child_->next();
        if (!first_res.has_value()) {
            return std::unexpected(std::move(first_res.error()));
        }
        if (!first_res.value().has_value()) {
            return result;
        }

        Chunk first = std::move(*first_res.value());
        result.columns = std::move(first.columns);
        for (std::size_t i = 0; i < result.columns.size(); ++i) {
            result.index[result.columns[i].name] = i;
        }
        result.set_properties(first.properties());
        // logical_rows is only meaningful for a column-less frame.
        if (result.columns.empty()) {
            result.logical_rows = first.logical_rows;
        }

        const std::size_t n_cols = result.columns.size();

        // Reserved capacity per output column, grown geometrically below.
        //
        // Reserving exactly `size + chunk` per chunk is what a growing vector
        // must never do: `reserve` allocates precisely what it is asked for, so
        // every chunk reallocates and copies the whole accumulated column, and
        // the concat becomes quadratic in the chunk count. That is invisible
        // with one chunk and dominates everything at a few hundred (a 20M-row
        // pipeline at a 64k grain spent 93% of its runtime in `memmove`).
        std::vector<std::size_t> reserved(n_cols, 0);
        for (std::size_t i = 0; i < n_cols; ++i) {
            reserved[i] = column_size(*result.columns[i].column);
        }

        while (true) {
            // Per-chunk interruption boundary for streamed pipelines.
            if (interrupt_requested()) {
                return std::unexpected(interrupt_message());
            }
            auto chunk_res = child_->next();
            if (!chunk_res.has_value()) {
                return std::unexpected(std::move(chunk_res.error()));
            }
            if (!chunk_res.value().has_value()) {
                break;
            }
            Chunk chunk = std::move(*chunk_res.value());

            if (chunk.columns.size() != n_cols) {
                return std::unexpected("MaterializeOperator: chunk schema mismatch (column count)");
            }

            if (n_cols == 0) {
                // Column-less frames (e.g. `Table(n)` row scaffolds) carry their
                // row count in `logical_rows`, not in any column; accumulate it
                // across chunks so multi-chunk zero-column output reports the
                // total rather than only the first chunk's count.
                result.logical_rows =
                    result.logical_rows.value_or(0) + chunk.logical_rows.value_or(0);
                continue;
            }

            for (std::size_t i = 0; i < n_cols; ++i) {
                if (chunk.columns[i].name != result.columns[i].name) {
                    return std::unexpected(
                        "MaterializeOperator: chunk schema mismatch (column name)");
                }
                if (chunk.columns[i].column->index() != result.columns[i].column->index()) {
                    return std::unexpected(
                        "MaterializeOperator: chunk schema mismatch (column type)");
                }
            }

            // Every result column has the same length before this chunk lands;
            // capture it once for validity backfill. (`n_cols == 0` returned
            // above, so there is always a column 0 here.)
            const std::size_t prev_rows = column_size(*result.columns[0].column);
            for (std::size_t i = 0; i < n_cols; ++i) {
                const std::size_t src_rows = column_size(*chunk.columns[i].column);
                auto& dst_col = result.mutable_column(i);
                // A chunk over another dictionary than the rows so far: move
                // those rows onto a dictionary this sink owns (once per column),
                // then translate the chunk onto it. Free while they share one.
                if (const auto* src_cat =
                        std::get_if<Column<Categorical>>(chunk.columns[i].column.get())) {
                    const auto& dst_cat = std::get<Column<Categorical>>(dst_col);
                    if (dst_cat.dictionary_identity() != src_cat->dictionary_identity()) {
                        if (!dictionaries_.owns(i, dst_cat)) {
                            auto moved =
                                dictionaries_.translate(i, dst_cat, result.columns[i].validity);
                            if (!moved.has_value()) {
                                return std::unexpected("MaterializeOperator: " + moved.error());
                            }
                            dst_col = std::move(*moved);
                        }
                        auto translated =
                            dictionaries_.translate(i, *src_cat, chunk.columns[i].validity);
                        if (!translated.has_value()) {
                            return std::unexpected("MaterializeOperator: " + translated.error());
                        }
                        chunk.columns[i].column =
                            std::make_shared<ColumnValue>(std::move(*translated));
                    }
                }
                const std::size_t needed = prev_rows + src_rows;
                const bool grow = needed > reserved[i];
                if (grow) {
                    reserved[i] = std::max(needed, reserved[i] * 2);
                }
                if (grow) {
                    std::visit([&](auto& dst) { dst.reserve(reserved[i]); }, dst_col);
                }
                append_column_values(dst_col, *chunk.columns[i].column);
                materialize_append_validity(result.columns[i].validity, prev_rows,
                                            chunk.columns[i].validity, src_rows);
            }
            // `chunk` goes out of scope here, releasing its memory before
            // the next `child_->next()` call.
        }

        return result;
    }

   private:
    OperatorPtr child_;
    CategoricalDictionaryUnifier dictionaries_;
};

/// Forwards `child`'s chunks with every Categorical column on one dictionary
/// per column (`CategoricalDictionaryUnifier`). Wraps a source the runtime did
/// not build, so the operators above it may copy codes verbatim.
class CategoricalUnifyingOperator final : public Operator {
   public:
    explicit CategoricalUnifyingOperator(OperatorPtr child) : child_(std::move(child)) {}

    [[nodiscard]] auto next() -> std::expected<std::optional<Chunk>, std::string> override {
        auto pulled = child_->next();
        if (!pulled.has_value() || !pulled->has_value()) {
            return pulled;
        }
        if (auto unified = dictionaries_.unify((*pulled)->columns); !unified.has_value()) {
            return std::unexpected(std::move(unified.error()));
        }
        return pulled;
    }

   private:
    OperatorPtr child_;
    CategoricalDictionaryUnifier dictionaries_;
};

}  // namespace ibex::runtime
