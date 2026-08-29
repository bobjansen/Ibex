// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#pragma once

#include <ibex/runtime/interpreter.hpp>

#include <array>
#include <cstddef>
#include <cstdint>
#include <deque>
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
    };
    struct PackedPlan {
        std::vector<PackCol> cols;
        unsigned width = 0;  ///< total packed width in bytes
    };

    /// Bit width of one packed cell, matching the byte counts
    /// `build_packed_layout` accumulates per `PackCol::Kind`.
    [[nodiscard]] static auto width_bits_of(PackCol::Kind kind) -> unsigned {
        switch (kind) {
            case PackCol::Kind::Int64:
            case PackCol::Kind::Ts:
                return 64U;
            case PackCol::Kind::Date:
            case PackCol::Kind::Cat:
                return 32U;
            case PackCol::Kind::Bool:
                return 8U;
        }
        return 0U;
    }

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
        for (const auto* entry : entries) {
            if (entry->validity.has_value()) {
                return std::nullopt;
            }
        }
        return build_packed_layout(entries);
    }

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
                    cell = static_cast<std::uint64_t>(col.i64[row]);
                    break;
                case PackCol::Kind::Date:
                    cell = static_cast<std::uint32_t>(col.date[row].days);
                    break;
                case PackCol::Kind::Ts:
                    cell = static_cast<std::uint64_t>(col.ts[row].nanos);
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
};

}  // namespace ibex::runtime
