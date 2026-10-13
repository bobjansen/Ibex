// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#pragma once

#include <ibex/runtime/interpreter.hpp>

#include <algorithm>
#include <array>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <limits>
#include <optional>
#include <robin_hood.h>
#include <string>
#include <string_view>
#include <type_traits>
#include <vector>

namespace ibex::runtime {

struct PackedKeyEncoder {
    // MSVC has no __uint128_t. This is only a packed identity key, so an array
    // of words is both portable and avoids pulling a compiler-specific integer
    // type into the packed key path.
    template <std::size_t Words>
    struct PackedWords {
        std::array<std::uint64_t, Words> w{};

        [[nodiscard]] friend auto operator==(const PackedWords&, const PackedWords&)
            -> bool = default;
    };
    template <std::size_t Words>
    struct PackedWordsHash {
        auto operator()(const PackedWords<Words>& value) const noexcept -> std::size_t {
            std::uint64_t acc = 0;
            for (const auto word : value.w) {
                acc ^= word + 0x9e3779b97f4a7c15ULL + (acc << 6U) + (acc >> 2U);
            }
            return static_cast<std::size_t>(acc);
        }
    };
    using Packed128 = PackedWords<2>;
    using Packed256 = PackedWords<4>;

    /// OR `cell` into the packed key at bit offset `shift`. A cell never spans
    /// more than two words because no cell is wider than 64 bits.
    template <std::size_t Words>
    static void splice(PackedWords<Words>& key, std::uint64_t cell, unsigned shift) {
        const unsigned word = shift / 64U;
        const unsigned off = shift % 64U;
        key.w[word] |= cell << off;
        // `cell >> 64` is UB, so the carry into the next word is only taken when
        // the cell actually straddles the boundary.
        if (off != 0 && word + 1 < Words) {
            key.w[word + 1] |= cell >> (64U - off);
        }
    }

    /// The exact inverse of `splice`/the single-word shift in `pack_row`:
    /// recover the `width_bits`-wide cell that was spliced in at bit offset
    /// `shift`. Only ever needed to decode an ALREADY-PACKED key back into its
    /// per-column values (fast-path migration); packing itself never reads a
    /// cell back out, so this has no hot-path cost.
    template <typename Packed>
    [[nodiscard]] static auto extract_cell(const Packed& key, unsigned shift, unsigned width_bits)
        -> std::uint64_t {
        const std::uint64_t mask =
            width_bits >= 64U ? ~std::uint64_t{0} : ((std::uint64_t{1} << width_bits) - 1);
        if constexpr (std::is_same_v<Packed, std::uint64_t>) {
            return (key >> shift) & mask;
        } else {
            const unsigned word = shift / 64U;
            const unsigned off = shift % 64U;
            std::uint64_t value = key.w[word] >> off;
            if (off != 0 && word + 1 < key.w.size()) {
                value |= key.w[word + 1] << (64U - off);
            }
            return value & mask;
        }
    }

    /// One fixed-width integral key column, resolved to its raw storage and the
    /// bit offset it occupies in the packed key.
    struct PackCol {
        enum class Kind : std::uint8_t { Int64, Date, Ts, Bool, Cat } kind{Kind::Int64};
        const std::int64_t* i64 = nullptr;
        const Date* date = nullptr;
        const Timestamp* ts = nullptr;
        const Column<bool>* boolean = nullptr;
        const Column<Categorical>* cat = nullptr;
        const std::uint32_t* remap = nullptr;  ///< local code -> operator-global id
        unsigned shift = 0;  ///< bit offset of this column's cell in the packed key
        /// Subtracted (wrapping) from an Int64/Ts/Date value to form its cell.
        /// Zero unless cells are sized from the data, in which case it is the
        /// bottom of the column's window (see `size_cells_from_data`).
        std::uint64_t offset = 0;
    };
    struct PackedPlan {
        std::vector<PackCol> cols;
        unsigned width = 0;  ///< total packed width in bytes
    };

    /// Per-key-column interning state for Categorical columns.
    ///
    /// A categorical code is only meaningful against ITS OWN chunk's dictionary,
    /// so packing the raw code would merge two different values that happen to
    /// share a code in different chunks. Resolving each dictionary entry to an
    /// operator-global id fixes that, and costs one lookup per DICTIONARY ENTRY
    /// per chunk rather than one per row: the row loop then reads `remap[code]`,
    /// a single array index with no hashing and no allocation at all.
    struct CatIntern {
        /// Views point into `arena`, whose deque never invalidates references.
        robin_hood::unordered_flat_map<std::string_view, std::uint32_t> ids;
        std::deque<std::string> arena;
        std::vector<std::uint32_t> remap;  ///< rebuilt per chunk, indexed by local code
    };

    /// A key is packable iff every column reduces to a fixed-width INTEGRAL cell
    /// whose byte equality equals value equality, with no nulls, and the columns
    /// together fit in 32 bytes.
    ///
    /// Doubles are excluded (-0.0/NaN break byte equality). Strings are excluded
    /// because interning one per row would cost the hash lookup this path exists
    /// to avoid. Categoricals ARE included: their dictionary is interned once per
    /// chunk into operator-global ids (see `CatIntern`), which is what makes a
    /// code comparable across chunks.
    auto build_packed_key(const std::vector<const ColumnEntry*>& entries)
        -> std::optional<PackedPlan> {
        layout_miss_ = false;
        for (const auto* entry : entries) {
            if (entry->validity.has_value()) {
                return std::nullopt;
            }
        }
        if (size_cells_from_data) {
            return build_ranged_layout(entries);
        }
        return build_packed_layout(entries);
    }

    /// The shape half of `build_packed_key`: every column is a packable kind
    /// and none carries nulls. Touches no state, so it can probe a chunk
    /// without fixing a data-sized layout from it.
    [[nodiscard]] static auto packable_shape(const std::vector<const ColumnEntry*>& entries)
        -> bool {
        return std::ranges::all_of(entries, [](const ColumnEntry* entry) {
            const ColumnValue& column = *entry->column;
            return !entry->validity.has_value() &&
                   (std::holds_alternative<Column<std::int64_t>>(column) ||
                    std::holds_alternative<Column<Date>>(column) ||
                    std::holds_alternative<Column<Timestamp>>(column) ||
                    std::holds_alternative<Column<bool>>(column) ||
                    std::holds_alternative<Column<Categorical>>(column));
        });
    }

    /// Size each cell from the values it holds instead of from its type.
    ///
    /// With type widths an Int64 key costs 64 bits whatever it holds, so the
    /// h2o q10 key -- three Categoricals and three Int64s -- needed 288 bits,
    /// missed Packed256, and fell to the generic path (550 ns/row, ~400 B per
    /// group). Sized from the data its cells need 7+7+17+7+7+17 = 62 bits.
    ///
    /// The packed words of earlier chunks live on in the caller's maps, so the
    /// layout is fixed by the FIRST chunk and never changes. Each column gets
    /// a window: the bits it needed, plus an even share of whatever the chosen
    /// word has left over, centred on what was seen. A later chunk whose
    /// values fall outside a window cannot be packed under that layout; the
    /// build then fails with `layout_miss()` set, and the caller must leave
    /// the packed path for good (the aggregate migrates to its generic path).
    ///
    /// Opt-in: only a caller that can migrate may set it.
    bool size_cells_from_data = false;

    /// True when the last `build_packed_key` failed because the data did not
    /// fit the fixed data-sized layout (or, on the first chunk, needed more
    /// than 256 bits) -- as opposed to the shape not being packable at all.
    [[nodiscard]] auto layout_miss() const -> bool { return layout_miss_; }

    /// Same as `build_packed_key`, minus the "no column may carry nulls" check.
    ///
    /// Used to recover a stable fast path's (kind, shift) layout when a LATER
    /// chunk's nulls are exactly what disqualifies `build_packed_key` -- the
    /// layout itself does not depend on nullability, only on each column's
    /// type and position, which stay fixed for the life of the query once the
    /// packed path has been selected. The migration path calls this to learn
    /// how to decode the packed keys a prior, null-free chunk already built.
    auto build_packed_layout(const std::vector<const ColumnEntry*>& entries)
        -> std::optional<PackedPlan> {
        // Size the interning state ONCE, before any of it is pointed at.
        //
        // `intern_categorical` hands back `remap.data()`, and `PackCol` holds
        // that pointer for the rest of the chunk. Growing `cat_interns_` while
        // those pointers are live reallocates the vector, and `CatIntern` holds
        // a robin_hood map whose move constructor is not noexcept — so
        // `move_if_noexcept` COPIES, `remap` gets a fresh buffer, and column
        // 0's pointer is left dangling the moment column 1 is interned.
        //
        // The symptom was a second categorical key column silently reading
        // freed memory: PDS-H q7's `by { supp_nation, cust_nation, l_year }`
        // emitted 8 groups instead of 4, the first chunk's four separated from
        // the rest, because only the first chunk paid a reallocation. It needed
        // multi-chunk input to show at all (`IBEX_CHUNK_ROWS`).
        if (cat_interns_.size() < entries.size()) {
            cat_interns_.resize(entries.size());
        }
        PackedPlan plan;
        plan.cols.reserve(entries.size());
        unsigned bytes = 0;
        for (std::size_t k = 0; k < entries.size(); ++k) {
            const auto& entry = *entries[k];
            PackCol col;
            col.shift = bytes * 8;
            const ColumnValue& column = *entry.column;
            if (const auto* c_int = std::get_if<Column<std::int64_t>>(&column)) {
                col.kind = PackCol::Kind::Int64;
                col.i64 = c_int->data();
                bytes += 8;
            } else if (const auto* c_date = std::get_if<Column<Date>>(&column)) {
                col.kind = PackCol::Kind::Date;
                col.date = c_date->data();
                bytes += 4;
            } else if (const auto* c_ts = std::get_if<Column<Timestamp>>(&column)) {
                col.kind = PackCol::Kind::Ts;
                col.ts = c_ts->data();
                bytes += 8;
            } else if (const auto* c_bool = std::get_if<Column<bool>>(&column)) {
                col.kind = PackCol::Kind::Bool;
                col.boolean = c_bool;
                bytes += 1;
            } else if (const auto* c_cat = std::get_if<Column<Categorical>>(&column)) {
                col.remap = intern_categorical(k, *c_cat);
                // An empty dictionary would leave the row loop indexing a remap
                // that has no entry for any code. Declining here keeps the hot
                // loop free of a per-row range check.
                if (col.remap == nullptr) {
                    return std::nullopt;
                }
                col.kind = PackCol::Kind::Cat;
                col.cat = c_cat;
                bytes += 4;
            } else {
                return std::nullopt;
            }
            if (bytes > sizeof(Packed256)) {
                return std::nullopt;
            }
            plan.cols.push_back(col);
        }
        plan.width = bytes;
        return plan;
    }

    /// One column's fixed cell: its width and the bottom of its value window.
    struct CellLayout {
        unsigned bits = 0;
        std::int64_t lo = 0;  ///< window bottom; ids for Cat, unused for Bool
    };

    /// What one chunk holds in a column, in the terms its cell is sized by.
    struct Observed {
        std::int64_t min = 0;
        std::int64_t max = 0;  ///< for Cat: highest global id assigned so far
    };

    [[nodiscard]] static constexpr auto cell_cap(PackCol::Kind kind) -> unsigned {
        switch (kind) {
            case PackCol::Kind::Int64:
            case PackCol::Kind::Ts:
                return 64U;
            case PackCol::Kind::Date:
            case PackCol::Kind::Cat:
                return 32U;
            case PackCol::Kind::Bool:
                return 1U;
        }
        return 0U;
    }

    template <typename T, typename Get>
    [[nodiscard]] static auto observe_range(const T* data, std::size_t n, Get get) -> Observed {
        if (n == 0) {
            return {};
        }
        std::int64_t lo = get(data[0]);
        std::int64_t hi = lo;
        for (std::size_t i = 1; i < n; ++i) {
            const std::int64_t v = get(data[i]);
            lo = std::min(lo, v);
            hi = std::max(hi, v);
        }
        return {.min = lo, .max = hi};
    }

    /// Smallest window of `bits` bits that holds `[seen.min, seen.max]`,
    /// centred on it and kept inside the kind's own value range so the
    /// fit check can compare signed values directly.
    [[nodiscard]] static auto place_window(PackCol::Kind kind, unsigned bits, const Observed& seen)
        -> std::int64_t {
        if (kind == PackCol::Kind::Cat || kind == PackCol::Kind::Bool || bits >= 64U) {
            return kind == PackCol::Kind::Cat || kind == PackCol::Kind::Bool ? 0 : seen.min;
        }
        const bool is_date = kind == PackCol::Kind::Date;
        const auto floor = is_date ? std::int64_t{std::numeric_limits<std::int32_t>::min()}
                                   : std::numeric_limits<std::int64_t>::min();
        const auto ceil = is_date ? std::int64_t{std::numeric_limits<std::int32_t>::max()}
                                  : std::numeric_limits<std::int64_t>::max();
        const std::uint64_t mask = (std::uint64_t{1} << bits) - 1;
        const std::uint64_t span =
            static_cast<std::uint64_t>(seen.max) - static_cast<std::uint64_t>(seen.min);
        const std::uint64_t below_room =
            static_cast<std::uint64_t>(seen.min) - static_cast<std::uint64_t>(floor);
        auto lo = static_cast<std::int64_t>(static_cast<std::uint64_t>(seen.min) -
                                            std::min((mask - span) / 2, below_room));
        if (static_cast<std::uint64_t>(ceil) - static_cast<std::uint64_t>(lo) < mask) {
            lo = static_cast<std::int64_t>(static_cast<std::uint64_t>(ceil) - mask);
        }
        return lo;
    }

    [[nodiscard]] static auto fits(PackCol::Kind kind, const CellLayout& cell, const Observed& seen)
        -> bool {
        if (kind == PackCol::Kind::Bool || cell.bits >= 64U) {
            return true;
        }
        const std::uint64_t mask = (std::uint64_t{1} << cell.bits) - 1;
        if (kind == PackCol::Kind::Cat) {
            return static_cast<std::uint64_t>(seen.max) <= mask;
        }
        // `place_window` keeps lo + mask inside the kind's range, so this
        // cannot overflow.
        const auto hi = static_cast<std::int64_t>(static_cast<std::uint64_t>(cell.lo) + mask);
        return seen.min >= cell.lo && seen.max <= hi;
    }

    /// Fix `layout_` from the first chunk's observations, or fail when even
    /// the tightest cells need more than 256 bits.
    auto fix_layout(const std::vector<PackCol>& cols, const std::vector<Observed>& seen) -> bool {
        std::vector<CellLayout> layout(cols.size());
        unsigned total = 0;
        for (std::size_t k = 0; k < cols.size(); ++k) {
            const auto kind = cols[k].kind;
            const std::uint64_t span = kind == PackCol::Kind::Cat
                                           ? static_cast<std::uint64_t>(seen[k].max)
                                           : static_cast<std::uint64_t>(seen[k].max) -
                                                 static_cast<std::uint64_t>(seen[k].min);
            layout[k].bits =
                kind == PackCol::Kind::Bool ? 1U : static_cast<unsigned>(std::bit_width(span));
            total += layout[k].bits;
        }
        unsigned word_bits = 0;
        for (const unsigned candidate : {64U, 128U, 256U}) {
            if (total <= candidate) {
                word_bits = candidate;
                break;
            }
        }
        if (word_bits == 0) {
            return false;
        }
        // Every bit the word has spare is headroom for later chunks, so hand
        // it out one at a time, round-robin, until each cell is at its cap.
        unsigned spare = word_bits - total;
        for (bool grew = true; spare != 0 && grew;) {
            grew = false;
            for (std::size_t k = 0; k < cols.size() && spare != 0; ++k) {
                if (layout[k].bits < cell_cap(cols[k].kind)) {
                    ++layout[k].bits;
                    --spare;
                    grew = true;
                }
            }
        }
        for (std::size_t k = 0; k < cols.size(); ++k) {
            layout[k].lo = place_window(cols[k].kind, layout[k].bits, seen[k]);
        }
        layout_ = std::move(layout);
        layout_bytes_ = word_bits / 8U;
        return true;
    }

    auto build_ranged_layout(const std::vector<const ColumnEntry*>& entries)
        -> std::optional<PackedPlan> {
        // Sized once before any remap pointer is taken; see `build_packed_layout`.
        if (cat_interns_.size() < entries.size()) {
            cat_interns_.resize(entries.size());
        }
        PackedPlan plan;
        plan.cols.resize(entries.size());
        std::vector<Observed> seen(entries.size());
        for (std::size_t k = 0; k < entries.size(); ++k) {
            PackCol& col = plan.cols[k];
            const ColumnValue& column = *entries[k]->column;
            if (const auto* c_int = std::get_if<Column<std::int64_t>>(&column)) {
                col.kind = PackCol::Kind::Int64;
                col.i64 = c_int->data();
                seen[k] = observe_range(col.i64, c_int->size(), [](std::int64_t v) { return v; });
            } else if (const auto* c_date = std::get_if<Column<Date>>(&column)) {
                col.kind = PackCol::Kind::Date;
                col.date = c_date->data();
                seen[k] = observe_range(col.date, c_date->size(),
                                        [](Date v) { return std::int64_t{v.days}; });
            } else if (const auto* c_ts = std::get_if<Column<Timestamp>>(&column)) {
                col.kind = PackCol::Kind::Ts;
                col.ts = c_ts->data();
                seen[k] = observe_range(col.ts, c_ts->size(), [](Timestamp v) { return v.nanos; });
            } else if (const auto* c_bool = std::get_if<Column<bool>>(&column)) {
                col.kind = PackCol::Kind::Bool;
                col.boolean = c_bool;
            } else if (const auto* c_cat = std::get_if<Column<Categorical>>(&column)) {
                col.kind = PackCol::Kind::Cat;
                col.cat = c_cat;
                col.remap = intern_categorical(k, *c_cat);
                if (col.remap == nullptr) {
                    layout_miss_ = true;
                    return std::nullopt;
                }
                // Ids are assigned densely from 0, so the count so far bounds
                // every id this chunk can produce -- no row scan needed.
                seen[k].max = static_cast<std::int64_t>(cat_interns_[k].ids.size()) - 1;
            } else {
                return std::nullopt;
            }
        }
        if (layout_.empty()) {
            if (!fix_layout(plan.cols, seen)) {
                layout_miss_ = true;
                return std::nullopt;
            }
        } else {
            for (std::size_t k = 0; k < plan.cols.size(); ++k) {
                if (!fits(plan.cols[k].kind, layout_[k], seen[k])) {
                    layout_miss_ = true;
                    return std::nullopt;
                }
            }
        }
        unsigned shift = 0;
        for (std::size_t k = 0; k < plan.cols.size(); ++k) {
            plan.cols[k].shift = shift;
            plan.cols[k].offset = static_cast<std::uint64_t>(layout_[k].lo);
            shift += layout_[k].bits;
        }
        plan.width = layout_bytes_;
        return plan;
    }

    /// Resolve chunk-local codes of key column `k` to operator-global ids,
    /// returning the remap, or nullptr when the dictionary is empty. Runs once
    /// per chunk per categorical key column.
    auto intern_categorical(std::size_t k, const Column<Categorical>& cat) -> const std::uint32_t* {
        // Never grows `cat_interns_` — `build_packed_key` sized it before any
        // caller took a pointer into it, and growing here would dangle those.
        auto& state = cat_interns_[k];
        const auto& dict = cat.dictionary();
        const std::size_t dict_size = dict.size();
        if (dict_size == 0) {
            return nullptr;
        }
        state.remap.resize(dict_size);
        for (std::size_t code = 0; code < dict_size; ++code) {
            const std::string_view value = dict[code];
            if (const auto it = state.ids.find(value); it != state.ids.end()) {
                state.remap[code] = it->second;
                continue;
            }
            const auto id = static_cast<std::uint32_t>(state.ids.size());
            // The view must outlive the chunk's dictionary, so the map keys are
            // views into this deque rather than into the column.
            state.arena.emplace_back(value);
            state.ids.emplace(std::string_view{state.arena.back()}, id);
            state.remap[code] = id;
        }
        return state.remap.data();
    }

    /// Pack one row's key. Cheap enough — a handful of array reads and shifts,
    /// no hashing and no branch on width — that callers which need the key
    /// twice recompute it rather than materialize a buffer.
    template <typename Packed>
    [[nodiscard]] static auto pack_row(const std::vector<PackCol>& cols, std::size_t row)
        -> Packed {
        Packed key{};
        for (const auto& col : cols) {
            std::uint64_t cell = 0;
            switch (col.kind) {
                case PackCol::Kind::Int64:
                    cell = static_cast<std::uint64_t>(col.i64[row]) - col.offset;
                    break;
                case PackCol::Kind::Date:
                    // Wraps in 32 bits, so a zero offset is the plain code.
                    cell =
                        static_cast<std::uint32_t>(static_cast<std::uint32_t>(col.date[row].days) -
                                                   static_cast<std::uint32_t>(col.offset));
                    break;
                case PackCol::Kind::Ts:
                    cell = static_cast<std::uint64_t>(col.ts[row].nanos) - col.offset;
                    break;
                case PackCol::Kind::Bool:
                    cell = (*col.boolean)[row] ? 1U : 0U;
                    break;
                case PackCol::Kind::Cat:
                    cell = col.remap[static_cast<std::size_t>(col.cat->code_at(row))];
                    break;
            }
            if constexpr (std::is_same_v<Packed, std::uint64_t>) {
                key |= cell << col.shift;
            } else {
                splice(key, cell, col.shift);
            }
        }
        return key;
    }

    /// Materialize this chunk's packed key for every row in `[begin, end)`.
    template <typename Packed>
    static void build_keys(const std::vector<PackCol>& cols, std::size_t begin, std::size_t end,
                           Packed* out) {
        for (std::size_t row = begin; row < end; ++row) {
            out[row] = pack_row<Packed>(cols, row);
        }
    }

    /// Interning state, indexed by key column position (see `CatIntern`).
    std::vector<CatIntern> cat_interns_;
    /// The data-sized layout, fixed by the first chunk; empty until then.
    std::vector<CellLayout> layout_;
    unsigned layout_bytes_ = 0;
    bool layout_miss_ = false;
};

}  // namespace ibex::runtime
