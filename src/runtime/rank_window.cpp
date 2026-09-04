// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

// rank_window.cpp — window/rank column evaluation: `evaluate_rank_column`
// (rank, dense_rank, row_number, percent_rank, cume_dist, ntile, and the
// ordering/NA/tie policies) plus the scalar comparison it rests on. Pure
// Table → Column computation with no operator or plan dispatch. Split out of
// runtime_entry.cpp (formerly chunked.cpp); declared in interpreter_internal.hpp.

#include <ibex/core/column.hpp>
#include <ibex/core/time.hpp>
#include <ibex/format.hpp>
#include <ibex/ir/column_name_map.hpp>
#include <ibex/ir/expr_predicates.hpp>
#include <ibex/ir/join_output.hpp>
#include <ibex/ir/node.hpp>
#include <ibex/ir/schema.hpp>
#include <ibex/runtime/extern_registry.hpp>
#include <ibex/runtime/interpreter.hpp>
#include <ibex/runtime/interrupt.hpp>
#include <ibex/runtime/lazy_table.hpp>
#include <ibex/runtime/morsel.hpp>
#include <ibex/runtime/operator.hpp>
#include <ibex/runtime/pipeline.hpp>
#include <ibex/runtime/table_properties.hpp>
#include <ibex/runtime/worker_pool.hpp>

#include <algorithm>
#include <atomic>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <expected>
#include <functional>
#include <limits>
#include <memory>
#include <numeric>
#include <optional>
#include <pdqsort.h>
#include <robin_hood.h>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>

#if defined(__AVX2__) || defined(__BMI2__)
#include <immintrin.h>
#endif

#include "aggregate_chunked_internal.hpp"
#include "interpreter_internal.hpp"
#include "runtime_internal.hpp"

namespace ibex::runtime {

auto compare_scalar_for_order(const ScalarValue& lhs, const ScalarValue& rhs) -> int {
    return std::visit(
        [](const auto& l, const auto& r) -> int {
            using L = std::decay_t<decltype(l)>;
            using R = std::decay_t<decltype(r)>;
            if constexpr (std::is_same_v<L, R>) {
                if (l < r) {
                    return -1;
                }
                if (r < l) {
                    return 1;
                }
                return 0;
            } else {
                invariant_violation("compare_scalar_for_order: mismatched scalar types");
            }
        },
        lhs, rhs);
}

auto evaluate_rank_column(const Table& input, const ir::RankExpr& rank,
                          const std::vector<ir::ColumnRef>& group_by, const ExecutionContext& exec)
    -> std::expected<ComputedColumn, std::string> {
    const std::size_t rows = input.rows();
    auto order_keys = ordering_keys_for_table(input, rank.order_keys);
    if (order_keys.empty()) {
        return std::unexpected("rank(): expected at least one order key");
    }

    struct ResolvedKey {
        const ColumnEntry* entry = nullptr;
        bool ascending = true;
    };
    std::vector<ResolvedKey> resolved_keys;
    resolved_keys.reserve(order_keys.size());
    for (const auto& key : order_keys) {
        const auto* entry = input.find_entry(key.name);
        if (entry == nullptr) {
            return std::unexpected("rank(): order column not found: " + key.name +
                                   " (available: " + format_columns(input) + ")");
        }
        resolved_keys.push_back(ResolvedKey{.entry = entry, .ascending = key.ascending});
    }

    std::vector<const ColumnEntry*> group_entries;
    group_entries.reserve(group_by.size());
    for (const auto& key : group_by) {
        const auto* entry = input.find_entry(key.name);
        if (entry == nullptr) {
            return std::unexpected("rank(): group column not found: " + key.name +
                                   " (available: " + format_columns(input) + ")");
        }
        group_entries.push_back(entry);
    }

    // Pre-flatten every group/order key into a typed array so the hot sort
    // comparator does plain vector indexing instead of per-comparison variant
    // dispatch. Crucially, string keys are flattened to string_view (views into
    // the column's storage) rather than the std::string that scalar_at_for_order
    // allocates on every access — without this, sorting 4M rows by a string key
    // performs hundreds of millions of heap allocations.
    constexpr std::uint64_t kSignFlip = std::uint64_t{1} << 63U;
    enum class FlatKind : std::uint8_t { I64, F64, Str };
    struct FlatCol {
        FlatKind kind = FlatKind::I64;
        std::vector<std::uint64_t> u64;  // Int / Date.days / Timestamp.nanos / bool, sign-flipped
        std::vector<double> f64;
        std::vector<std::string_view> str;  // views into original column storage
        const ValidityBitmap* validity = nullptr;
        bool ascending = true;
    };

    auto flatten = [&](const ColumnEntry* entry, bool ascending) -> FlatCol {
        FlatCol fc;
        fc.ascending = ascending;
        if (entry->validity.has_value()) {
            fc.validity = &*entry->validity;
        }
        std::visit(
            [&](const auto& col) {
                using ColT = std::decay_t<decltype(col)>;
                if constexpr (std::is_same_v<ColT, Column<std::int64_t>>) {
                    fc.kind = FlatKind::I64;
                    fc.u64.reserve(rows);
                    for (auto v : col)
                        fc.u64.push_back(static_cast<std::uint64_t>(v) ^ kSignFlip);
                } else if constexpr (std::is_same_v<ColT, Column<double>>) {
                    fc.kind = FlatKind::F64;
                    fc.f64.assign(col.begin(), col.end());
                } else if constexpr (std::is_same_v<ColT, Column<Date>>) {
                    fc.kind = FlatKind::I64;
                    fc.u64.reserve(rows);
                    for (const auto& d : col)
                        fc.u64.push_back(static_cast<std::uint64_t>(d.days) ^ kSignFlip);
                } else if constexpr (std::is_same_v<ColT, Column<Timestamp>>) {
                    fc.kind = FlatKind::I64;
                    fc.u64.reserve(rows);
                    for (const auto& ts : col)
                        fc.u64.push_back(static_cast<std::uint64_t>(ts.nanos) ^ kSignFlip);
                } else if constexpr (std::is_same_v<ColT, Column<bool>>) {
                    fc.kind = FlatKind::I64;
                    fc.u64.reserve(rows);
                    for (std::size_t i = 0; i < rows; ++i)
                        fc.u64.push_back(static_cast<std::uint64_t>(col[i] ? 1 : 0) ^ kSignFlip);
                } else {
                    // Column<std::string> or categorical: view, no allocation.
                    fc.kind = FlatKind::Str;
                    fc.str.reserve(rows);
                    for (std::size_t i = 0; i < rows; ++i)
                        fc.str.push_back(col[i]);
                }
            },
            *entry->column);
        return fc;
    };

    // Built on demand. The radix fast path below resolves a Categorical or
    // numeric group key without ever comparing group values, and flattening a
    // string group key eagerly costs a 128MB array of string_views at 8M rows
    // — built only to be thrown away. Only the comparison-sort fallback and
    // the single-string-key group id loop actually read it.
    std::vector<FlatCol> group_flat;
    auto ensure_group_flat = [&] {
        if (!group_flat.empty() || group_entries.empty()) {
            return;
        }
        group_flat.reserve(group_entries.size());
        for (const auto* entry : group_entries) {
            group_flat.push_back(flatten(entry, /*ascending=*/true));
        }
    };

    std::vector<FlatCol> order_flat;
    order_flat.reserve(resolved_keys.size());
    for (const auto& key : resolved_keys)
        order_flat.push_back(flatten(key.entry, key.ascending));

    auto flat_is_null = [](const FlatCol& fc, std::size_t row) -> bool {
        return fc.validity != nullptr && !(*fc.validity)[row];
    };
    // Three-way compare of a single flat key; sign-flipped u64 compares as signed,
    // string_view as lexicographic — both match compare_scalar_for_order.
    auto flat_cmp = [](const FlatCol& fc, std::size_t lhs, std::size_t rhs) -> int {
        switch (fc.kind) {
            case FlatKind::I64: {
                auto l = fc.u64[lhs];
                auto r = fc.u64[rhs];
                return (l > r) - (l < r);
            }
            case FlatKind::F64: {
                auto l = fc.f64[lhs];
                auto r = fc.f64[rhs];
                return (l > r) - (l < r);
            }
            case FlatKind::Str: {
                const auto& l = fc.str[lhs];
                const auto& r = fc.str[rhs];
                return (l > r) - (l < r);
            }
        }
        return 0;
    };

    auto is_null_row_for_keys = [&](std::size_t row) -> bool {
        return std::ranges::any_of(order_flat,
                                   [&](const FlatCol& fc) { return flat_is_null(fc, row); });
    };

    auto same_group = [&](std::size_t lhs, std::size_t rhs) -> bool {
        return std::ranges::all_of(group_flat, [&](const FlatCol& fc) {
            const bool ln = flat_is_null(fc, lhs);
            const bool rn = flat_is_null(fc, rhs);
            if (ln != rn) {
                return false;
            }
            if (ln) {
                return true;
            }
            return flat_cmp(fc, lhs, rhs) == 0;
        });
    };

    auto equal_rank_keys = [&](std::size_t lhs, std::size_t rhs) -> bool {
        const bool lhs_null = is_null_row_for_keys(lhs);
        const bool rhs_null = is_null_row_for_keys(rhs);
        if (lhs_null || rhs_null) {
            return lhs_null == rhs_null;
        }
        return std::ranges::all_of(order_flat,
                                   [&](const FlatCol& fc) { return flat_cmp(fc, lhs, rhs) == 0; });
    };

    std::vector<std::size_t> idx;

    // Populated by the radix fast path when group_entries is non-empty: group g's
    // rows are idx[radix_group_starts[g]..radix_group_starts[g+1]). Used by the
    // rank sweep to avoid O(n) same_group calls.
    std::vector<std::size_t> radix_group_starts;

    // Fast path: a single non-null numeric order key with non-null group keys.
    // Radix-argsort by the order value (no O(n log n) string/comparison sort),
    // then a stable counting-sort by hashed group id makes each group contiguous
    // while preserving the within-group order from the radix pass. Falls back to
    // the comparison sort below for string order keys, multiple order keys, or
    // any nullable key (where na_option / null-group semantics need the general
    // path). This is the hot path for `rank(x) by g` over large frames.
    const bool radix_order = order_flat.size() == 1 && order_flat[0].kind != FlatKind::Str &&
                             order_flat[0].validity == nullptr &&
                             std::ranges::all_of(group_entries, [](const ColumnEntry* e) {
                                 return !e->validity.has_value();
                             });
    if (radix_order) {
        const FlatCol& ok = order_flat[0];
        std::vector<std::uint64_t> codes;
        if (ok.kind == FlatKind::F64) {
            codes.resize(rows);
            for (std::size_t i = 0; i < rows; ++i)
                codes[i] = double_to_sortable_u64(ok.f64[i]);
        } else {
            codes = ok.u64;  // already sign-flipped to order-preserving u64
        }
        // Invert the order-preserving codes for a descending key so an ascending
        // radix sort yields descending order.
        if (!ok.ascending) {
            for (auto& c : codes)
                c = ~c;
        }
        if (group_entries.empty()) {
            // Ungrouped: one run, so there is nothing to bucket and the whole
            // table is the slice.
            auto sort_result = radix_sort_u64_asc(std::move(codes), rows);
            idx.resize(rows);
            std::visit(
                [&](const auto& sorted) {
                    for (std::size_t i = 0; i < rows; ++i)
                        idx[i] = sorted[i];
                },
                sort_result);
        } else {
            // Assign group IDs using the already-flattened group_flat arrays (string_view,
            // no per-row allocation) instead of calling scalar_from_column (which
            // heap-allocates std::string for string columns on every row).
            std::vector<std::uint32_t> group_id(rows);
            std::uint32_t ngroups = 0;
            const Column<Categorical>* cat_group =
                group_entries.size() == 1
                    ? std::get_if<Column<Categorical>>(group_entries[0]->column.get())
                    : nullptr;
            if (cat_group != nullptr) {
                // A Categorical group key carries a dense code per row, so
                // resolve each CODE to a group id once instead of hashing the
                // dictionary string it stands for on every row. `flatten` gives
                // group keys string_views because order keys need lexicographic
                // comparison; grouping only needs equality, which the codes
                // already answer.
                //
                // Still resolved through the dictionary text rather than using
                // the code directly: nothing forbids a dictionary from carrying
                // the same string under two codes (a remap can produce one), and
                // treating those as different groups would split a group in two.
                constexpr std::uint32_t kUnset = std::numeric_limits<std::uint32_t>::max();
                const auto& dict = cat_group->dictionary();
                std::vector<std::uint32_t> code_gid(dict.size(), kUnset);
                robin_hood::unordered_flat_map<std::string_view, std::uint32_t> by_text;
                for (std::size_t r = 0; r < rows; ++r) {
                    const auto code = static_cast<std::size_t>(cat_group->code_at(r));
                    std::uint32_t gid = code_gid[code];
                    if (gid == kUnset) {
                        // Lazily, in row order, so group ids stay in order of
                        // first appearance exactly as the hashing loop assigns
                        // them.
                        auto [it, inserted] =
                            by_text.emplace(std::string_view{dict[code]}, ngroups);
                        if (inserted) {
                            ++ngroups;
                        }
                        gid = it->second;
                        code_gid[code] = gid;
                    }
                    group_id[r] = gid;
                }
            } else if (ensure_group_flat();
                       group_flat.size() == 1 && group_flat[0].kind == FlatKind::Str) {
                // Single string group key: hash string_views directly.
                robin_hood::unordered_flat_map<std::string_view, std::uint32_t> group_index;
                const auto& sv = group_flat[0].str;
                for (std::size_t r = 0; r < rows; ++r) {
                    auto [it, inserted] = group_index.emplace(sv[r], ngroups);
                    if (inserted)
                        ++ngroups;
                    group_id[r] = it->second;
                }
            } else {
                // General: build a flat key from each group_flat column without going
                // through ScalarValue. I64/F64 columns use their numeric values directly;
                // string columns still hash as string_view (the Key uses std::string only
                // for the fallback path which doesn't reach here).
                robin_hood::unordered_flat_map<Key, std::uint32_t, KeyHash, KeyEq> group_index;
                for (std::size_t r = 0; r < rows; ++r) {
                    Key key;
                    key.values.reserve(group_entries.size());
                    for (const auto* entry : group_entries)
                        push_key_value(key, *entry, r);
                    auto [it, inserted] = group_index.emplace(std::move(key), ngroups);
                    if (inserted)
                        ++ngroups;
                    group_id[r] = it->second;
                }
            }
            // Bucket rows by group, then sort each group's run where it sits.
            //
            // The obvious structure — sort all `rows` globally, then stable
            // counting-sort the result by group — walks a 64MB permutation
            // twice and cannot be split. Bucketing first makes each run
            // cache-resident and independent, so the runs sort concurrently
            // and the counting pass over the sorted order disappears. Measured
            // on a standalone harness at 8M rows, prices-shaped keys:
            //
            //   groups     global+count   bucket+per-group   ...threaded
            //        1          322ms            251ms            247ms
            //      252          346ms            150ms             50ms
            //   100000          344ms            190ms            100ms
            //
            // The key travels with the row so each run holds its own keys
            // contiguously; rows enter a run in ascending row order and the
            // slice sort is stable, so ties break by row exactly as the global
            // stable sort broke them.
            std::vector<std::size_t> cnt(static_cast<std::size_t>(ngroups) + 1, 0);
            for (std::size_t r = 0; r < rows; ++r)
                ++cnt[static_cast<std::size_t>(group_id[r]) + 1];
            for (std::size_t g = 0; g < ngroups; ++g)
                cnt[g + 1] += cnt[g];
            radix_group_starts =
                cnt;  // group g spans [radix_group_starts[g], radix_group_starts[g+1])

            idx.resize(rows);
            std::vector<std::uint64_t> run_keys(rows);
            {
                std::vector<std::size_t> cursor(cnt.begin(), cnt.end() - 1);
                for (std::size_t r = 0; r < rows; ++r) {
                    const std::size_t at = cursor[group_id[r]]++;
                    idx[at] = r;
                    run_keys[at] = codes[r];
                }
            }

            const std::size_t sort_workers =
                ngroups >= 2 ? group_barrier_worker_count(exec, rows) : 0;
            if (sort_workers >= 2) {
                std::atomic<std::size_t> next{0};
                auto batch = process_worker_pool().submit(sort_workers, [&](std::size_t) noexcept {
                    RadixSliceScratch scratch;
                    while (true) {
                        const std::size_t g = next.fetch_add(1, std::memory_order_relaxed);
                        if (g >= ngroups) {
                            return;
                        }
                        const std::size_t lo = radix_group_starts[g];
                        sort_key_index_slice(run_keys.data() + lo, idx.data() + lo,
                                             radix_group_starts[g + 1] - lo, scratch);
                    }
                });
                batch.wait();
            } else {
                RadixSliceScratch scratch;
                for (std::size_t g = 0; g < ngroups; ++g) {
                    const std::size_t lo = radix_group_starts[g];
                    sort_key_index_slice(run_keys.data() + lo, idx.data() + lo,
                                         radix_group_starts[g + 1] - lo, scratch);
                }
            }
        }
    } else {
        ensure_group_flat();
        idx.resize(rows);
        // std::ranges::iota is C++23 (P2440) and libc++ does not implement it;
        // the macOS CI build fails on it. Keep the iterator-pair form.
        // NOLINTNEXTLINE(modernize-use-ranges)
        std::iota(idx.begin(), idx.end(), std::size_t{0});
        // pdqsort is unstable, but the comparator's `lhs < rhs` tiebreak makes the
        // order total, so the result matches a stable sort.
        pdqsort(idx.begin(), idx.end(), [&](std::size_t lhs, std::size_t rhs) {
            // Order groups first (nulls sort first, ascending by value) so that rows
            // of the same group are contiguous for the sweep below.
            for (const auto& fc : group_flat) {
                const bool ln = flat_is_null(fc, lhs);
                const bool rn = flat_is_null(fc, rhs);
                if (ln != rn) {
                    return ln;  // null sorts first
                }
                if (ln) {
                    continue;
                }
                const int cmp = flat_cmp(fc, lhs, rhs);
                if (cmp != 0) {
                    return cmp < 0;
                }
            }
            // Within a group, order by the rank keys (honouring na_option).
            const bool lhs_null = is_null_row_for_keys(lhs);
            const bool rhs_null = is_null_row_for_keys(rhs);
            if (lhs_null || rhs_null) {
                if (lhs_null != rhs_null) {
                    if (rank.na_option == ir::RankNaOption::Top) {
                        return lhs_null;
                    }
                    // `Bottom` and `Keep` both put nulls after the values.
                    // `Keep` overwrites their ranks with null further down, so
                    // where they sit does not show — but it has to be SOMEWHERE
                    // consistent. Ordering them by row index instead made the
                    // comparator intransitive: with rows 7, null, 3 it said
                    // 7 < null and null < 3 and 3 < 7, and a sort given a
                    // comparator like that produces an arbitrary permutation,
                    // which is how every rank came out in row order.
                    //
                    // Putting them last also keeps the ordinals right: nulls
                    // consume rank positions as they are walked, so they must
                    // be walked after every value.
                    return !lhs_null;
                }
                return lhs < rhs;
            }
            for (const auto& fc : order_flat) {
                const int cmp = flat_cmp(fc, lhs, rhs);
                if (cmp != 0) {
                    return fc.ascending ? (cmp < 0) : (cmp > 0);
                }
            }
            return lhs < rhs;
        });
    }

    // The tie scan below is the hot loop of the whole function: one call per
    // row. `equal_rank_keys` answers it in full generality — an any_of for
    // nulls, then an all_of of three-way compares each dispatching on the
    // key's kind — none of which a single non-null numeric key needs. That is
    // exactly the shape the radix path above requires, so when it ran the
    // comparison is one array read per side.
    const FlatCol* solo_key = radix_order ? order_flat.data() : nullptr;
    auto same_rank_keys = [&](std::size_t lhs, std::size_t rhs) -> bool {
        if (solo_key != nullptr) {
            return solo_key->kind == FlatKind::F64 ? solo_key->f64[lhs] == solo_key->f64[rhs]
                                                   : solo_key->u64[lhs] == solo_key->u64[rhs];
        }
        return equal_rank_keys(lhs, rhs);
    };

    std::vector<double> rank_values(rows, 0.0);
    // Only `na_option = keep` ever writes this, and only a null key makes it
    // write. Allocating it regardless cost a bitmap per call for every rank
    // that has no nulls to keep.
    ValidityBitmap validity;
    if (rank.na_option == ir::RankNaOption::Keep) {
        validity.resize(rows, true);
    }

    // When the radix fast path ran, group boundaries are already known from the
    // counting sort. Iterate the groups directly: walk radix_group_starts as a
    // cursor so each group_end lookup is O(1) with no scanning. The pdqsort
    // fallback leaves radix_group_starts empty and uses the same_group per-row
    // scan (needed for nulls / string order keys / multi-key cases).
    std::size_t gs_cursor = 0;  // index into radix_group_starts for the fast path

    // One group's ranks. Groups share no rows, and every write below is to
    // `rank_values[idx[k]]` — a row this group owns — so groups are
    // independent and the loop over them can be split.
    auto rank_group = [&](std::size_t pos, std::size_t group_end) {
        std::size_t dense_rank = 1;
        std::size_t ordinal = 1;
        std::size_t i = pos;
        while (i < group_end) {
            std::size_t tie_end = i + 1;
            while (tie_end < group_end && same_rank_keys(idx[i], idx[tie_end])) {
                ++tie_end;
            }

            // The radix path admits no nullable order key, so there is nothing
            // to test when it ran.
            const bool null_tie = solo_key == nullptr && is_null_row_for_keys(idx[i]);
            double assigned = 0.0;
            if (null_tie && rank.na_option == ir::RankNaOption::Keep) {
                for (std::size_t k = i; k < tie_end; ++k) {
                    validity.set(idx[k], false);
                }
            } else {
                switch (rank.method) {
                    case ir::RankMethod::Average: {
                        const auto first_rank = static_cast<double>(ordinal);
                        const auto last_rank = static_cast<double>(ordinal + (tie_end - i) - 1);
                        assigned = (first_rank + last_rank) / 2.0;
                        break;
                    }
                    case ir::RankMethod::Min:
                    case ir::RankMethod::Dense:
                        assigned = static_cast<double>(
                            rank.method == ir::RankMethod::Dense ? dense_rank : ordinal);
                        break;
                    case ir::RankMethod::Max:
                        assigned = static_cast<double>(ordinal + (tie_end - i) - 1);
                        break;
                    case ir::RankMethod::First:
                        break;
                }
                if (rank.method == ir::RankMethod::First) {
                    for (std::size_t k = i; k < tie_end; ++k) {
                        auto value = static_cast<double>(ordinal + (k - i));
                        rank_values[idx[k]] =
                            rank.pct ? value / static_cast<double>(group_end - pos) : value;
                    }
                } else {
                    if (rank.pct) {
                        assigned /= static_cast<double>(group_end - pos);
                    }
                    for (std::size_t k = i; k < tie_end; ++k) {
                        rank_values[idx[k]] = assigned;
                    }
                }
            }

            ordinal += (tie_end - i);
            if (!null_tie || rank.na_option != ir::RankNaOption::Keep) {
                ++dense_rank;
            }
            i = tie_end;
        }
    };

    // Split the groups when the radix path ran. Two conditions make that safe
    // and are exactly what the path already guarantees: the group boundaries
    // are known up front (no serial scan to find them), and there is no
    // nullable order key — so nothing writes the shared validity bitmap, whose
    // neighbouring bits share a word and could not be written concurrently.
    // The result does not depend on the split: each group's ranks are computed
    // from its own rows and scattered to their own row positions.
    const std::size_t group_count = radix_group_starts.empty() ? 0 : radix_group_starts.size() - 1;
    const std::size_t sweep_workers =
        (solo_key != nullptr && group_count >= 2) ? group_barrier_worker_count(exec, rows) : 0;
    if (sweep_workers >= 2) {
        std::atomic<std::size_t> cursor{0};
        auto batch = process_worker_pool().submit(sweep_workers, [&](std::size_t) noexcept {
            while (true) {
                const std::size_t g = cursor.fetch_add(1, std::memory_order_relaxed);
                if (g >= group_count) {
                    return;
                }
                rank_group(radix_group_starts[g], radix_group_starts[g + 1]);
            }
        });
        batch.wait();
    } else {
        std::size_t pos = 0;
        while (pos < rows) {
            std::size_t group_end = 0;
            if (!radix_group_starts.empty()) {
                ++gs_cursor;  // advance past the current group's start
                group_end = radix_group_starts[gs_cursor];
            } else {
                group_end = pos + 1;
                while (group_end < rows && same_group(idx[pos], idx[group_end]))
                    ++group_end;
            }
            rank_group(pos, group_end);
            pos = group_end;
        }
    }

    const bool integral = !rank.pct && rank.method != ir::RankMethod::Average;
    if (integral) {
        Column<std::int64_t> out;
        out.resize_for_overwrite(rows);
        std::int64_t* dst = out.data();
        for (std::size_t r = 0; r < rows; ++r) {
            dst[r] = static_cast<std::int64_t>(rank_values[r]);
        }
        ComputedColumn result{.column = std::move(out), .validity = std::nullopt};
        if (rank.na_option == ir::RankNaOption::Keep) {
            result.validity = std::move(validity);
        }
        return result;
    }

    Column<double> out;
    out.resize_for_overwrite(rows);
    std::memcpy(out.data(), rank_values.data(), rows * sizeof(double));
    ComputedColumn result{.column = std::move(out), .validity = std::nullopt};
    if (rank.na_option == ir::RankNaOption::Keep) {
        result.validity = std::move(validity);
    }
    return result;
}

}  // namespace ibex::runtime
