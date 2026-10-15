// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#pragma once

#include <ibex/core/compiler.hpp>
#include <ibex/runtime/interpreter.hpp>

#include <algorithm>
#include <array>
#include <atomic>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <limits>
#include <memory>
#include <optional>
#include <robin_hood.h>
#include <string>
#include <string_view>
#include <type_traits>
#include <vector>

#include "string_arena_internal.hpp"

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
        enum class Kind : std::uint8_t { Int64, Date, Ts, Bool, Cat, Str } kind{Kind::Int64};
        const std::int64_t* i64 = nullptr;
        const Date* date = nullptr;
        const Timestamp* ts = nullptr;
        const Column<bool>* boolean = nullptr;
        const Column<Categorical>* cat = nullptr;
        const std::uint32_t* remap = nullptr;  ///< local code -> operator-global id
        /// Set while the column's dictionary is the adopted id space
        /// (`CatIntern::base`): `remap` is then the identity, and the code is
        /// the cell. Reading the remap anyway cost a cache miss per row once
        /// it outgrew the cache -- h2o q10's 5M-level id3 is a 20 MB remap,
        /// ~300 ms of a ~950 ms query at one core.
        bool cat_identity = false;
        const std::uint32_t* codes = nullptr;  ///< Str: each row's interned id
        /// Set when the column may hold nulls; a null row packs as `null_cell`,
        /// a value no non-null row of the column can take.
        const ValidityBitmap* valid = nullptr;
        std::uint64_t null_cell = 0;
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
    ///
    /// Every chunk of an in-memory table shares ONE dictionary, though, and
    /// then the codes already are operator-global ids. Interning h2o's 5M-level
    /// id3 anyway was ~650 ms of serial map inserts per query. So the first
    /// dictionary is adopted as the id space (`base`, remap the identity), and
    /// the map is only built once a chunk arrives with a different one.
    struct CatIntern {
        /// Views point into `arena`, whose deque never invalidates references.
        robin_hood::unordered_flat_map<std::string_view, std::uint32_t> ids;
        std::deque<std::string> arena;
        std::vector<std::uint32_t> remap;  ///< rebuilt per chunk, indexed by local code
        /// The adopted dictionary, while every chunk so far has used it; null
        /// once `ids` holds the id space instead.
        std::shared_ptr<std::vector<std::string>> base;
        /// Ids assigned so far; every id is below it.
        std::size_t count = 0;
    };

    /// Per-key-column interning state for String columns.
    ///
    /// A String has no fixed-width cell, so it is interned PER ROW into an id
    /// first -- one hash probe per row, which is what kept strings out of this
    /// path. Against the generic path it still wins by far: at h2o q10's
    /// shape (a 5M-level String among six keys) the generic key cost ~750 ns
    /// per row, where interning is one probe that threads like the
    /// single-string discovery: rows are hash-partitioned, and each partition
    /// owns a map that persists across chunks. A partition hands out its own
    /// ids, so a row's id is `local * parts + partition` -- unique without
    /// any cross-partition numbering, and dense enough that 5M strings take
    /// 23 bits.
    struct StrIntern {
        struct Part {
            /// Views into `arena`.
            robin_hood::unordered_flat_map<std::string_view, std::uint32_t> ids;
            StringArena arena;
        };
        std::vector<Part> parts;  ///< a power of two, fixed by the first chunk
        unsigned part_bits = 0;
        /// The chunk column `codes` was built from, so the layout can refuse a
        /// column nobody interned.
        const Column<std::string>* column = nullptr;
        ::ibex::detail::NoInitVector<std::uint32_t> codes;  ///< per row of `column`

        /// Every id handed out so far is below this.
        [[nodiscard]] auto id_bound() const -> std::size_t {
            std::size_t most = 0;
            for (const auto& part : parts) {
                most = std::max(most, part.ids.size());
            }
            return most << part_bits;
        }
    };

    /// A key is packable iff every column reduces to a fixed-width INTEGRAL cell
    /// whose byte equality equals value equality, and the columns together fit
    /// in 32 bytes. Nulls only when cells are sized from the data: a nullable
    /// column then keeps its cell's top value for null (see `CellLayout`).
    ///
    /// Doubles are excluded (-0.0/NaN break byte equality). Categoricals ARE
    /// included: their dictionary is interned once per chunk into
    /// operator-global ids (see `CatIntern`), which is what makes a code
    /// comparable across chunks. Strings are included only when cells are
    /// sized from the data, and only after the caller has run
    /// `intern_strings` on the chunk (see `StrIntern`).
    ///
    /// Sizing cells from the data scans every Int64/Date/Timestamp key column
    /// for its range; `run` and `tasks` are as for `intern_strings`, and split
    /// that scan (at h2o q10's 1e7 rows, three Int64 keys were ~15 ms on one
    /// core). The one-argument form scans on the calling thread.
    auto build_packed_key(const std::vector<const ColumnEntry*>& entries)
        -> std::optional<PackedPlan> {
        return build_packed_key(entries, 1, [](std::size_t tasks, const auto& body) {
            for (std::size_t t = 0; t < tasks; ++t) {
                body(t);
            }
        });
    }
    template <typename Run>
    auto build_packed_key(const std::vector<const ColumnEntry*>& entries, std::size_t tasks,
                          const Run& run) -> std::optional<PackedPlan> {
        layout_miss_ = false;
        if (size_cells_from_data) {
            return build_ranged_layout(entries, tasks, run);
        }
        for (const auto* entry : entries) {
            if (entry->validity.has_value()) {
                return std::nullopt;
            }
        }
        return build_packed_layout(entries);
    }

    /// The shape half of `build_packed_key`: every column is a packable kind.
    /// Admits String and nullable columns, so it is for a caller that sizes
    /// cells from the data and runs `intern_strings`. Touches no state, so it can probe a chunk
    /// without fixing a data-sized layout from it.
    [[nodiscard]] static auto packable_shape(const std::vector<const ColumnEntry*>& entries)
        -> bool {
        return std::ranges::all_of(entries, [](const ColumnEntry* entry) {
            const ColumnValue& column = *entry->column;
            return (std::holds_alternative<Column<std::int64_t>>(column) ||
                    std::holds_alternative<Column<Date>>(column) ||
                    std::holds_alternative<Column<Timestamp>>(column) ||
                    std::holds_alternative<Column<bool>>(column) ||
                    std::holds_alternative<Column<Categorical>>(column) ||
                    std::holds_alternative<Column<std::string>>(column));
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
                col.cat_identity = cat_interns_[k].base != nullptr;
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
    ///
    /// A column the first chunk saw nullable keeps the cell's top value,
    /// all ones, for null: its values must stay below it, so it costs at most
    /// one more bit, and it never grows to 64 bits, where no value is spare.
    /// h2o's 5%-NA q10 has a validity bitmap on every key column; without
    /// this it ran the generic path at 4.9x Polars.
    struct CellLayout {
        unsigned bits = 0;
        std::int64_t lo = 0;  ///< window bottom; ids for Cat, unused for Bool
        bool nullable = false;
    };

    /// What one chunk holds in a column, in the terms its cell is sized by.
    struct Observed {
        std::int64_t min = 0;
        std::int64_t max = 0;  ///< for Cat: highest global id assigned so far
    };

    /// Cat and Str cells hold an id counted up from 0, not a value.
    [[nodiscard]] static constexpr auto is_id(PackCol::Kind kind) -> bool {
        return kind == PackCol::Kind::Cat || kind == PackCol::Kind::Str;
    }

    [[nodiscard]] static constexpr auto cell_cap(PackCol::Kind kind, bool nullable) -> unsigned {
        switch (kind) {
            case PackCol::Kind::Int64:
            case PackCol::Kind::Ts:
                return nullable ? 63U : 64U;
            case PackCol::Kind::Date:
            case PackCol::Kind::Cat:
            case PackCol::Kind::Str:
                return 32U;
            case PackCol::Kind::Bool:
                return nullable ? 2U : 1U;
        }
        return 0U;
    }

    /// The largest cell a value may take: the cell's top value is null's when
    /// the column is nullable.
    [[nodiscard]] static auto value_room(unsigned bits, bool nullable) -> std::uint64_t {
        const std::uint64_t mask = bits >= 64U ? ~std::uint64_t{0} : (std::uint64_t{1} << bits) - 1;
        return nullable ? mask - 1 : mask;
    }

    /// The range of the non-null values; {0, 0} when there are none. A null
    /// row's payload is whatever the column holds there, so it must not
    /// widen the window.
    /// Split over up to `tasks` ranges of `run` (see `build_packed_key`).
    template <typename T, typename Get, typename Run>
    [[nodiscard]] static auto observe_range(const T* data, std::size_t n, Get get,
                                            const ValidityBitmap* valid, std::size_t tasks,
                                            const Run& run) -> Observed {
        constexpr std::size_t kMinRangeRows = std::size_t{1} << 16;
        const std::size_t ranges = std::clamp<std::size_t>(n / kMinRangeRows, 1, tasks);
        const std::size_t grain = (n + ranges - 1) / ranges;
        std::vector<std::int64_t> los(ranges, std::numeric_limits<std::int64_t>::max());
        std::vector<std::int64_t> his(ranges, std::numeric_limits<std::int64_t>::min());
        run(ranges, [&](std::size_t r) {
            const std::size_t begin = std::min(n, r * grain);
            const std::size_t end = std::min(n, begin + grain);
            std::int64_t lo = los[r];
            std::int64_t hi = his[r];
            if (valid == nullptr) {
                for (std::size_t i = begin; i < end; ++i) {
                    const std::int64_t v = get(data[i]);
                    lo = std::min(lo, v);
                    hi = std::max(hi, v);
                }
            } else {
                for (std::size_t i = begin; i < end; ++i) {
                    if ((*valid)[i]) {
                        const std::int64_t v = get(data[i]);
                        lo = std::min(lo, v);
                        hi = std::max(hi, v);
                    }
                }
            }
            los[r] = lo;
            his[r] = hi;
        });
        const std::int64_t lo = *std::ranges::min_element(los);
        const std::int64_t hi = *std::ranges::max_element(his);
        return lo > hi ? Observed{} : Observed{.min = lo, .max = hi};
    }

    /// Smallest window of `bits` bits that holds `[seen.min, seen.max]`,
    /// centred on it and kept inside the kind's own value range so the
    /// fit check can compare signed values directly.
    [[nodiscard]] static auto place_window(PackCol::Kind kind, unsigned bits, bool nullable,
                                           const Observed& seen) -> std::int64_t {
        if (is_id(kind) || kind == PackCol::Kind::Bool || bits >= 64U) {
            return is_id(kind) || kind == PackCol::Kind::Bool ? 0 : seen.min;
        }
        const bool is_date = kind == PackCol::Kind::Date;
        const auto floor = is_date ? std::int64_t{std::numeric_limits<std::int32_t>::min()}
                                   : std::numeric_limits<std::int64_t>::min();
        const auto ceil = is_date ? std::int64_t{std::numeric_limits<std::int32_t>::max()}
                                  : std::numeric_limits<std::int64_t>::max();
        const std::uint64_t mask = value_room(bits, nullable);
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
        const std::uint64_t mask = value_room(cell.bits, cell.nullable);
        if (is_id(kind)) {
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
            const bool nullable = cols[k].valid != nullptr;
            const std::uint64_t span = is_id(kind) ? static_cast<std::uint64_t>(seen[k].max)
                                                   : static_cast<std::uint64_t>(seen[k].max) -
                                                         static_cast<std::uint64_t>(seen[k].min);
            if (nullable && span == std::numeric_limits<std::uint64_t>::max()) {
                return false;  // every value taken: none left for null
            }
            layout[k].nullable = nullable;
            layout[k].bits =
                kind == PackCol::Kind::Bool
                    ? cell_cap(kind, nullable)
                    : static_cast<unsigned>(std::bit_width(nullable ? span + 1 : span));
            if (layout[k].bits > cell_cap(kind, nullable)) {
                return false;
            }
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
                if (layout[k].bits < cell_cap(cols[k].kind, layout[k].nullable)) {
                    ++layout[k].bits;
                    --spare;
                    grew = true;
                }
            }
        }
        for (std::size_t k = 0; k < cols.size(); ++k) {
            layout[k].lo = place_window(cols[k].kind, layout[k].bits, layout[k].nullable, seen[k]);
        }
        layout_ = std::move(layout);
        layout_bytes_ = word_bits / 8U;
        return true;
    }

    template <typename Run>
    auto build_ranged_layout(const std::vector<const ColumnEntry*>& entries, std::size_t tasks,
                             const Run& run) -> std::optional<PackedPlan> {
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
            const ValidityBitmap* valid =
                entries[k]->validity.has_value() ? &*entries[k]->validity : nullptr;
            col.valid = valid;
            if (const auto* c_int = std::get_if<Column<std::int64_t>>(&column)) {
                col.kind = PackCol::Kind::Int64;
                col.i64 = c_int->data();
                seen[k] = observe_range(
                    col.i64, c_int->size(), [](std::int64_t v) { return v; }, valid, tasks, run);
            } else if (const auto* c_date = std::get_if<Column<Date>>(&column)) {
                col.kind = PackCol::Kind::Date;
                col.date = c_date->data();
                seen[k] = observe_range(
                    col.date, c_date->size(), [](Date v) { return std::int64_t{v.days}; }, valid,
                    tasks, run);
            } else if (const auto* c_ts = std::get_if<Column<Timestamp>>(&column)) {
                col.kind = PackCol::Kind::Ts;
                col.ts = c_ts->data();
                seen[k] = observe_range(
                    col.ts, c_ts->size(), [](Timestamp v) { return v.nanos; }, valid, tasks, run);
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
                col.cat_identity = cat_interns_[k].base != nullptr;
                // Ids are assigned densely from 0, so the count so far bounds
                // every id this chunk can produce -- no row scan needed.
                seen[k].max = static_cast<std::int64_t>(cat_interns_[k].count) - 1;
            } else if (const auto* c_str = std::get_if<Column<std::string>>(&column)) {
                // Its ids come from `intern_strings`, which the caller ran on
                // this very column; anything else is a caller that cannot
                // intern, and the generic path is the safe answer.
                // An id past 32 bits was truncated in `codes`.
                if (k >= str_interns_.size() || str_interns_[k].column != c_str ||
                    str_interns_[k].id_bound() > (std::size_t{1} << 32U)) {
                    layout_miss_ = true;
                    return std::nullopt;
                }
                col.kind = PackCol::Kind::Str;
                col.codes = str_interns_[k].codes.data();
                seen[k].max = static_cast<std::int64_t>(str_interns_[k].id_bound()) - 1;
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
                // A column the first chunk saw without nulls has no null cell.
                if (!fits(plan.cols[k].kind, layout_[k], seen[k]) ||
                    (plan.cols[k].valid != nullptr && !layout_[k].nullable)) {
                    layout_miss_ = true;
                    return std::nullopt;
                }
            }
        }
        unsigned shift = 0;
        for (std::size_t k = 0; k < plan.cols.size(); ++k) {
            plan.cols[k].shift = shift;
            plan.cols[k].offset = static_cast<std::uint64_t>(layout_[k].lo);
            plan.cols[k].null_cell = value_room(layout_[k].bits, false);
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
        const auto& dict_ptr = cat.dictionary_ptr();
        if (state.count == 0 && state.base == nullptr) {
            state.base = dict_ptr;
        }
        if (state.base != nullptr) {
            if (dict_ptr == state.base) {
                // A dictionary only ever grows at its end, so the codes it
                // held before keep their meaning.
                const std::size_t had = state.remap.size();
                if (dict_size > had) {
                    state.remap.resize(dict_size);
                    for (std::size_t code = had; code < dict_size; ++code) {
                        state.remap[code] = static_cast<std::uint32_t>(code);
                    }
                }
                state.count = std::max(state.count, dict_size);
                return state.remap.data();
            }
            // A second dictionary: from here on ids come from the map, so seed
            // it with the adopted one's ids, which are its codes. Copied into
            // the arena, since a growing dictionary moves its strings.
            const auto& base = *state.base;
            state.ids.reserve(state.count);
            for (std::size_t code = 0; code < state.count; ++code) {
                state.arena.emplace_back(base[code]);
                state.ids.emplace(std::string_view{state.arena.back()},
                                  static_cast<std::uint32_t>(code));
            }
            state.base.reset();
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
        state.count = state.ids.size();
        return state.remap.data();
    }

    /// Intern every String key column of this chunk into `str_interns_`, for
    /// `build_packed_key` to pack. `run(n, body)` calls `body(i)` for each
    /// `i < n`, across workers if it can; `tasks` is how many it can use at
    /// once, and `parts` the partition count to fix on a first call (a power
    /// of two, at most 256; 1 for a caller that will not fan out).
    template <typename Run>
    void intern_strings(const std::vector<const ColumnEntry*>& entries, std::size_t parts,
                        std::size_t tasks, const Run& run) {
        if (str_interns_.size() < entries.size()) {
            str_interns_.resize(entries.size());
        }
        for (std::size_t k = 0; k < entries.size(); ++k) {
            const auto* column = std::get_if<Column<std::string>>(&*entries[k]->column);
            // A nullable column is interned whole: a null row's payload gets
            // an id like any string, and the packer never reads it.
            if (column == nullptr) {
                continue;
            }
            auto& state = str_interns_[k];
            if (state.parts.empty()) {
                state.part_bits =
                    static_cast<unsigned>(std::bit_width(std::max<std::size_t>(parts, 1)) - 1);
                state.parts.resize(std::size_t{1} << state.part_bits);
            }
            intern_column(state, *column, tasks, run);
            state.column = column;
        }
    }

    template <typename Run>
    static void intern_column(StrIntern& state, const Column<std::string>& column,
                              std::size_t tasks, const Run& run) {
        const std::size_t rows = column.size();
        const char* chars = column.chars_data();
        const std::uint32_t* offsets = column.offsets_data();
        const auto view_at = [&](std::size_t row) {
            return std::string_view{chars + offsets[row], offsets[row + 1] - offsets[row]};
        };
        state.codes.resize(rows);
        std::uint32_t* codes = state.codes.data();
        const unsigned bits = state.part_bits;
        const std::size_t part_count = state.parts.size();
        // High bits pick the partition: the map indexes by the low ones.
        const auto part_of = [bits](std::size_t hash) -> std::size_t {
            return bits == 0 ? 0 : static_cast<std::size_t>(std::uint64_t{hash} >> (64U - bits));
        };
        // A partition's probe of `view`, as the row's code.
        const auto probe = [&](std::size_t p, std::string_view view) -> std::uint32_t {
            auto& part = state.parts[p];
            auto it = part.ids.find(view);
            std::uint32_t local = 0;
            if (it == part.ids.end()) {
                local = static_cast<std::uint32_t>(part.ids.size());
                part.ids.emplace(part.arena.intern(view), local);
            } else {
                local = it->second;
            }
            return static_cast<std::uint32_t>((std::size_t{local} << bits) | p);
        };
        const robin_hood::hash<std::string_view> hasher;
        if (tasks < 2 || part_count < 2 || rows < kParallelInternRows) {
            for (std::size_t row = 0; row < rows; ++row) {
                const auto view = view_at(row);
                codes[row] = probe(part_of(hasher(view)), view);
            }
            return;
        }

        // Pass 1: each row's partition, and a histogram per row range. Ranges
        // are contiguous, so a partition's rows stay in row order below.
        const std::size_t ranges = tasks;
        const std::size_t grain = (rows + ranges - 1) / ranges;
        ::ibex::detail::NoInitVector<std::uint8_t> part_of_row(rows);
        std::vector<std::size_t> counts(ranges * part_count, 0);
        run(ranges, [&](std::size_t r) {
            const std::size_t end = std::min(rows, (r + 1) * grain);
            std::size_t* range_counts = counts.data() + (r * part_count);
            for (std::size_t row = r * grain; row < end; ++row) {
                const auto p = static_cast<std::uint8_t>(part_of(hasher(view_at(row))));
                part_of_row[row] = p;
                ++range_counts[p];
            }
        });
        std::vector<std::size_t> part_begin(part_count + 1, 0);
        {
            std::size_t running = 0;
            for (std::size_t p = 0; p < part_count; ++p) {
                part_begin[p] = running;
                for (std::size_t r = 0; r < ranges; ++r) {
                    const std::size_t n = counts[(r * part_count) + p];
                    counts[(r * part_count) + p] = running;
                    running += n;
                }
            }
            part_begin[part_count] = running;
        }
        // Pass 2: scatter each row, with its view, into its partition's slice.
        // The view travels with the row so the probe below reads it
        // sequentially instead of missing on the offsets first.
        ::ibex::detail::NoInitVector<std::uint32_t> scattered_rows(rows);
        ::ibex::detail::NoInitVector<std::string_view> scattered_views(rows);
        run(ranges, [&](std::size_t r) {
            const std::size_t end = std::min(rows, (r + 1) * grain);
            std::size_t* cursor = counts.data() + (r * part_count);
            for (std::size_t row = r * grain; row < end; ++row) {
                const std::size_t at = cursor[part_of_row[row]]++;
                scattered_rows[at] = static_cast<std::uint32_t>(row);
                scattered_views[at] = view_at(row);
            }
        });
        // Pass 3: each task takes whole partitions, so a map has one writer.
        std::atomic<std::size_t> next{0};
        run(std::min(tasks, part_count), [&](std::size_t) {
            for (std::size_t p = next.fetch_add(1, std::memory_order_relaxed); p < part_count;
                 p = next.fetch_add(1, std::memory_order_relaxed)) {
                const std::size_t begin = part_begin[p];
                const std::size_t end = part_begin[p + 1];
                // Size the map once a sample shows most rows are new keys, so
                // it does not rehash its way up (see `try_discover_partitioned`).
                constexpr std::size_t kSample = 1024;
                const std::size_t held = state.parts[p].ids.size();
                for (std::size_t i = begin; i < end; ++i) {
                    if (i == begin + kSample && end - i > 4 * kSample) {
                        const std::size_t fresh = state.parts[p].ids.size() - held;
                        if (fresh * 2 > kSample) {
                            state.parts[p].ids.reserve(state.parts[p].ids.size() +
                                                       ((end - i) * fresh / kSample));
                        }
                    }
                    constexpr std::size_t kAhead = 16;
                    if (i + kAhead < end) {
                        ::ibex::detail::prefetch(scattered_views[i + kAhead].data());
                    }
                    codes[scattered_rows[i]] = probe(p, scattered_views[i]);
                }
            }
        });
    }

    /// Below this many rows a chunk is interned on the calling thread.
    static constexpr std::size_t kParallelInternRows = std::size_t{1} << 16;

    /// Pack one row's key. Cheap enough — a handful of array reads and shifts,
    /// no hashing and no branch on width — that callers which need the key
    /// twice recompute it rather than materialize a buffer.
    template <typename Packed>
    [[nodiscard]] static auto pack_row(const std::vector<PackCol>& cols, std::size_t row)
        -> Packed {
        Packed key{};
        for (const auto& col : cols) {
            std::uint64_t cell = 0;
            // Checked first: a null row's payload is not a value to read (a
            // Categorical null's code need not index the remap).
            if (col.valid != nullptr && !(*col.valid)[row]) {
                cell = col.null_cell;
            } else {
                switch (col.kind) {
                    case PackCol::Kind::Int64:
                        cell = static_cast<std::uint64_t>(col.i64[row]) - col.offset;
                        break;
                    case PackCol::Kind::Date:
                        // Wraps in 32 bits, so a zero offset is the plain code.
                        cell = static_cast<std::uint32_t>(
                            static_cast<std::uint32_t>(col.date[row].days) -
                            static_cast<std::uint32_t>(col.offset));
                        break;
                    case PackCol::Kind::Ts:
                        cell = static_cast<std::uint64_t>(col.ts[row].nanos) - col.offset;
                        break;
                    case PackCol::Kind::Bool:
                        cell = (*col.boolean)[row] ? 1U : 0U;
                        break;
                    case PackCol::Kind::Cat:
                        cell = cat_cell(col, row);
                        break;
                    case PackCol::Kind::Str:
                        cell = col.codes[row];
                        break;
                }
            }
            if constexpr (std::is_same_v<Packed, std::uint64_t>) {
                key |= cell << col.shift;
            } else {
                splice(key, cell, col.shift);
            }
        }
        return key;
    }

    /// A Categorical column's cell for `row`: its operator-global id.
    [[nodiscard]] static auto cat_cell(const PackCol& col, std::size_t row) -> std::uint64_t {
        const auto code = static_cast<std::size_t>(col.cat->code_at(row));
        return col.cat_identity ? code : col.remap[code];
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
    /// Likewise for String key columns (see `StrIntern`).
    std::vector<StrIntern> str_interns_;
    /// The data-sized layout, fixed by the first chunk; empty until then.
    std::vector<CellLayout> layout_;
    unsigned layout_bytes_ = 0;
    bool layout_miss_ = false;
};

}  // namespace ibex::runtime
