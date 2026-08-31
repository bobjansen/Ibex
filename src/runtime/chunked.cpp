// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

// chunked.cpp — residual streaming operators, rank evaluation, extern-call
// execution, and concrete factories used by the physical and pipeline
// executors. Planning, migrated-plan dispatch, and generic pipeline execution
// live in their respective translation units. Split out of interpreter.cpp;
// shared declarations live in interpreter_internal.hpp.

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
#include <array>
#include <atomic>
#include <chrono>
#include <cmath>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <deque>
#include <exception>
#include <expected>
#include <functional>
#include <limits>
#include <memory>
#include <mutex>
#include <new>
#include <numeric>
#include <optional>
#include <pdqsort.h>
#include <ratio>
#include <robin_hood.h>
#include <span>
#include <string>
#include <string_view>
#include <thread>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>

#include "physical_plan.hpp"

#if defined(__AVX2__) || defined(__BMI2__)
#include <immintrin.h>
#endif

#include "aggregate_chunked_internal.hpp"
#include "chunk_conversion_internal.hpp"
#include "execution_profile_internal.hpp"
#include "interpreter_internal.hpp"
#include "join_chunked_internal.hpp"
#include "join_internal.hpp"
#include "kernel_filter.hpp"
#include "kernel_types.hpp"
#include "kernel_update.hpp"
#include "model_internal.hpp"
#include "packed_key_encoder_internal.hpp"
#include "physical_executor_internal.hpp"
#include "pipeline_executor_internal.hpp"
#include "reshape_internal.hpp"
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

auto materialize_operator(OperatorPtr op) -> std::expected<Table, std::string> {
    MaterializeOperator sink{std::move(op)};
    return sink.run();
}

auto is_streamable_inner_join(const ir::JoinNode& join) -> bool {
    // `nulls equal` goes to the materialized join, which implements the policy.
    // The streaming operators hash and probe on their own and would each need
    // the same null tagging; sending the opt-in case to the one implementation
    // that has it keeps a single definition of the semantics.
    return join.kind() == ir::JoinKind::Inner && !join.predicate().has_value() &&
           join.keys().size() == 1 && join.null_match() == ir::NullMatch::Never &&
           !join.expect().asserts_anything() && join.take() == ir::MatchSelection::All;
}

/// Two-fixed-width-int-key inner join (plans/parallelism-overview.md's
/// "stream multi-key joins" item; TPC-H q09's `lineitem` join is the
/// motivating case). Same structural gate as `is_streamable_inner_join`
/// plus a static schema check -- both keys, on both sides, must be provably
/// `Int64` -- so an ineligible pair (a string key, an unascribed/Unknown
/// schema) falls through to the materialized-call fallback (`interpret_node`'s
/// `Join` branch) exactly as it does today, never into a code path that could
/// fail at runtime.
/// `ChunkedInnerJoinOperator::initialize_pair` re-checks the actual runtime
/// column type regardless -- this is a routing optimization, not the sole
/// guarantee of correctness.
/// Whether every aggregation in `agg` can be computed incrementally, and so
/// streamed rather than materialized.
///
/// Named and shared for the same reason the join gates were: the physical
/// planner has to relay this rather than restate it. Reimplementing a
/// multi-clause eligibility test in the planner is what made it wrong about
/// two-key joins within an hour of being written.
auto aggregate_is_streamable(const ir::AggregateNode& agg) -> bool {
    return std::ranges::all_of(agg.aggregations(), [](const ir::AggSpec& spec) {
        switch (spec.func) {
            case ir::AggFunc::Count:
            case ir::AggFunc::Sum:
            case ir::AggFunc::Min:
            case ir::AggFunc::Max:
            case ir::AggFunc::Mean:
            case ir::AggFunc::Stddev:
            case ir::AggFunc::Skew:
            case ir::AggFunc::Kurtosis:
            case ir::AggFunc::First:
            case ir::AggFunc::Last:
                // First/Last: the operators themselves gate by column type
                // (numeric, string, categorical stream; Date/Timestamp fall to
                // the hash operator's error path -- unreachable in practice
                // since aggregation on those types is rejected upstream of the
                // chunked path entirely, same as every other agg func).
                return true;
            default:
                // Median/Quantile need all values; Ewma is row-order coupled --
                // these stay on the materializing path.
                return false;
        }
    });
}


auto is_streamable_pair_int_join(const ir::JoinNode& join) -> bool {
    if (join.kind() != ir::JoinKind::Inner || join.predicate().has_value() ||
        join.keys().size() != 2 || join.null_match() != ir::NullMatch::Never ||
        join.expect().asserts_anything() || join.take() != ir::MatchSelection::All) {
        return false;
    }
    const ir::SchemaInfo left_schema = ir::infer_schema(*join.children()[0]);
    const ir::SchemaInfo right_schema = ir::infer_schema(*join.children()[1]);
    if (!left_schema.is_known() || !right_schema.is_known()) {
        return false;
    }
    // NOLINTNEXTLINE(readability-use-anyofallof)
    for (const ir::JoinKey& key : join.keys()) {
        const ir::SchemaField* lf = left_schema.find(key.left);
        const ir::SchemaField* rf = right_schema.find(key.right);
        if (lf == nullptr || rf == nullptr || !lf->type.has_value() || !rf->type.has_value() ||
            *lf->type != ir::ColumnType::Int64 || *rf->type != ir::ColumnType::Int64) {
            return false;
        }
    }
    return true;
}

auto inner_join_table(const Table& left, const Table& right, const std::vector<ir::JoinKey>& keys,
                      const ir::JoinSuffixPolicy& suffix,
                      const std::vector<ir::OrderKey>& pending_order, const ExecutionContext& exec)
    -> std::expected<Table, std::string> {
    // I4 convergence: keep the materialized signature used by interpret_node,
    // but run the same hash join that build_operator selects for this exact
    // semantic subset. Table sources copy only their column handles.
    auto source = make_table_source(left);
    return materialize_operator(make_chunked_inner_join_operator(
        std::move(source), right, &keys, exec, suffix, &pending_order,
        physical_executor_detail::resolved_join_parallelism(exec)));
}

namespace {

// The single choke point for "build this subtree, then immediately drain it
// to a whole Table" — every call site in this file that needs a materialized
// side (a join's build/probe side, an update's input, an aggregate's fused
// join operand, ...) with no downstream consumer to overlap the build with
// routes through here, rather than each hand-rolling its own
// `build_operator` + `materialize_operator` pair. One place to reason about
// this pattern instead of N independently-drifting copies.
auto materialize_row_local(const ir::Node& node, const TableRegistry& registry,
                           const ScalarRegistry* scalars, const ExternRegistry* externs,
                           const ExecutionContext& exec, ModelResult* model_out)
    -> std::expected<Table, std::string> {
    auto op = build_operator(node, registry, scalars, externs, exec, model_out);
    if (!op.has_value()) {
        return std::unexpected(std::move(op.error()));
    }
    return materialize_operator(std::move(op.value()));
}

// The relational inputs of a materialized-call fallback node -- the subtrees
// `build_materialized_fallback` may build ahead through the physical path. For
// most kinds these are the direct children. Two shapes carry a child that is
// *not* an independent relational input and must not be built standalone:
//   - `Window`'s child is an `update` clause; only `interpret_node`'s Window
//     case may evaluate it (it needs the window duration). The real input is
//     the update's own child.
//   - `Stream`'s child is a per-buffer transform template over `__stream_input__`
//     and has no meaning outside the stream loop.
// A kind not listed here (or one whose children are template/expression nodes)
// gets no pre-build: `interpret_node` evaluates it whole, which is the prior
// behaviour.
auto fallback_relational_inputs(const ir::Node& node) -> std::vector<const ir::Node*> {
    std::vector<const ir::Node*> inputs;
    switch (node.kind()) {
        case ir::NodeKind::Melt:
        case ir::NodeKind::Dcast:
        case ir::NodeKind::Columns:
        case ir::NodeKind::Cov:
        case ir::NodeKind::Corr:
        case ir::NodeKind::Transpose:
        case ir::NodeKind::Matmul:
        case ir::NodeKind::Resample:
        case ir::NodeKind::Model:
        case ir::NodeKind::AsTimeframe:
        case ir::NodeKind::Update:
        case ir::NodeKind::Join:
            inputs.reserve(node.children().size());
            for (const auto& child : node.children()) {
                inputs.push_back(child.get());
            }
            break;
        case ir::NodeKind::Window:
            if (!node.children().empty() && !node.children().front()->children().empty()) {
                inputs.push_back(node.children().front()->children().front().get());
            }
            break;
        default:
            break;
    }
    return inputs;
}

// The materialized-call fallback for every node kind `plan_physical` does not
// migrate (reshape, stats, window, non-row-local update, materializing join,
// matmul, model, ...). `interpret_node` owns the per-kind semantics; this only
// makes sure the breaker's *inputs* still go through the physical path: each
// relational input is built and drained via `build_operator` (fused parallel
// scan, projection pushdown, streaming join), then `interpret_node` runs over
// the node with those inputs handed back pre-built through
// `pre_materialized_children` -- so a `Filter`/`Project` feeding the breaker is
// not re-evaluated whole-table and serial. `interpret_node` still recurses for
// anything deeper, and for any kind `fallback_relational_inputs` leaves empty.
auto build_materialized_fallback(const ir::Node& node, const TableRegistry& registry,
                                 const ScalarRegistry* scalars, const ExternRegistry* externs,
                                 const ExecutionContext& exec, ModelResult* model_out)
    -> std::expected<OperatorPtr, std::string> {
    const std::vector<const ir::Node*> inputs = fallback_relational_inputs(node);

    std::vector<Table> built;
    built.reserve(inputs.size());
    for (const ir::Node* input : inputs) {
        auto table = materialize_row_local(*input, registry, scalars, externs, exec, model_out);
        if (!table.has_value()) {
            return std::unexpected(std::move(table.error()));
        }
        built.push_back(std::move(table.value()));
    }
    // `built` is not resized past this point, so the addresses stay valid for
    // the `interpret_node` call below.
    std::vector<std::pair<const ir::Node*, const Table*>> handback;
    handback.reserve(inputs.size());
    for (std::size_t i = 0; i < inputs.size(); ++i) {
        handback.emplace_back(inputs[i], &built[i]);
    }
    ExecutionContext local = exec;
    local.pre_materialized_children = &handback;

    auto table = interpret_node(node, registry, scalars, externs, local, model_out);
    if (!table.has_value()) {
        return std::unexpected(std::move(table.error()));
    }
    return make_table_source(std::move(table.value()));
}

template <typename Fn>

auto build_unary_materializing_operator(const ir::Node& child_node, const TableRegistry& registry,
                                        const ScalarRegistry* scalars,
                                        const ExternRegistry* externs, const ExecutionContext& exec,
                                        ModelResult* model_out, Fn fn)
    -> std::expected<OperatorPtr, std::string> {
    auto materialized =
        materialize_row_local(child_node, registry, scalars, externs, exec, model_out);
    if (!materialized.has_value()) {
        return std::unexpected(std::move(materialized.error()));
    }
    auto result = fn(std::move(materialized.value()));
    if (!result.has_value()) {
        return std::unexpected(std::move(result.error()));
    }
    return make_table_source(std::move(result.value()));
}

}  // namespace

auto make_join_probe_operator(OperatorPtr source, std::optional<Table> materialized_source,
                              JoinProbeFactory probe) -> std::expected<OperatorPtr, std::string> {
    const ExecutionContext* exec = probe.execution_context();
    if (materialized_source.has_value() && exec != nullptr) {
        if (const std::size_t workers =
                pipeline_executor_detail::probe_morsel_workers(*materialized_source, *exec);
            workers >= 2) {
            return pipeline_executor_detail::build_probe_morsel_pipeline(
                std::move(*materialized_source), probe, workers, *exec);
        }
    }
    OperatorPtr probe_source = materialized_source.has_value()
                                   ? make_table_source(std::move(*materialized_source))
                                   : std::move(source);
    if (probe_source == nullptr) {
        return std::unexpected("join probe has no probe-side source");
    }
    return probe.attach_move(std::move(probe_source));
}

namespace {

// A materializing binary breaker (non-streamable join, matmul) now resolves in
// `interpret_node`, which drains both sides whole-table and serially.
// Overlapping the two materializations on a raw std::thread was tried twice
// (once unbudgeted, once under a since-removed helper-thread budget) and
// regressed the PDS-H suite both times (q09 +57% / +47.5%): the cost is
// structural -- both sides are already-expensive full materializations
// contending for the same cores/bandwidth -- not a concurrency count a budget
// could bound. A future attempt needs a cost-aware gate (skip when both sides
// are large), and belongs wherever that breaker is lifted onto the physical plan.

auto eval_extern_args(const std::vector<ir::Expr>& exprs, const ScalarRegistry* scalars,
                      const ExternRegistry* externs) -> std::expected<ExternArgs, std::string> {
    ExternArgs args;
    args.reserve(exprs.size());
    for (const auto& arg : exprs) {
        auto val = eval_expr(arg, Table{}, 0, scalars, externs);
        if (!val.has_value()) {
            return std::unexpected(std::move(val.error()));
        }
        // Externs take null-free ScalarValue arguments (see the null-arm plan).
        auto scalar = scalar_from_expr(val.value());
        if (!scalar.has_value()) {
            return std::unexpected("null argument in extern function call");
        }
        args.push_back(std::move(*scalar));
    }
    return args;
}

}  // namespace

auto invoke_extern_call(const ir::ExternCallNode& ec, const ScalarRegistry* scalars,
                        const ExternRegistry* externs) -> std::expected<ExternValue, std::string> {
    if (externs == nullptr) {
        return std::unexpected("extern call with no registry: " + ec.callee());
    }
    const auto* fn = externs->find(ec.callee());
    if (fn == nullptr) {
        return std::unexpected("unknown extern function: " + ec.callee());
    }
    if (fn->first_arg_is_table) {
        return std::unexpected("extern function requires a table input: " + ec.callee());
    }
    auto args = eval_extern_args(ec.args(), scalars, externs);
    if (!args.has_value()) {
        return std::unexpected(std::move(args.error()));
    }
    if (fn->kind == ExternReturnKind::Table && fn->chunked_table_func) {
        auto source = fn->chunked_table_func(args.value());
        if (source.has_value()) {
            auto materialized = materialize_operator(std::move(source.value()));
            if (!materialized.has_value()) {
                return std::unexpected(std::move(materialized.error()));
            }
            return ExternValue{std::move(materialized.value())};
        }
    }
    auto result = fn->func(args.value());
    if (!result.has_value()) {
        return std::unexpected(std::move(result.error()));
    }
    return result;
}

auto execute_program_preamble(const std::vector<ir::NodePtr>& preamble,
                              const ScalarRegistry* scalars, const ExternRegistry* externs)
    -> std::expected<void, std::string> {
    for (const auto& node : preamble) {
        if (node->kind() != ir::NodeKind::ExternCall) {
            return std::unexpected("program preamble only supports extern calls");
        }
        const auto& ec = ir::node_cast<ir::ExternCallNode>(*node);
        auto result = invoke_extern_call(ec, scalars, externs);
        if (!result.has_value()) {
            return std::unexpected(std::move(result.error()));
        }
    }
    return {};
}

/// Planner seam: returns a pull-based operator that, when drained,
/// produces the logical result of `node`. Chunked operators exist
/// today for node kinds that are safe and useful to stream; any other
/// node kind falls back to the full-table `interpret_node` path and
/// is wrapped in a `TableSourceOperator` so downstream chunked
/// operators see a uniform pull-based interface.
// Order-delay past Filter/Project/Rename, and Head/Tail pushdown past
// Project/Rename, are handled by the IR canonicalize pass
// (src/ir/canonicalize.cpp). IR arrives here in canonical form, so
// build_operator only needs one branch per NodeKind and the shapes it
// matches are the post-canonicalization shapes (e.g. Project(Filter(x))
// for the fused operator, not Project(Filter(Order(x)))).

auto morsel_grain(const ExecutionContext& exec, std::size_t rows) -> std::size_t {
    if (exec.parallel_grain != 0) {
        return exec.parallel_grain;  // explicit override, used as given
    }
    // Aim for this many morsels per worker, so one slow morsel cannot strand
    // the others. Below ~2 per thread the sweep shows real imbalance loss.
    constexpr std::size_t kMorselsPerThread = 4;
    // The measured good band was 16k-256k; clamp inside it. The upper bound is
    // what stops the formula from choosing a grain worse than the old constant
    // on a large input — see the declaration.
    constexpr std::size_t kMinGrain = 4096;
    constexpr std::size_t kMaxGrain = 65536;

    const std::size_t pool_size = process_worker_pool().size();
    const std::size_t budget = exec.compute_budget();
    const std::size_t threads = std::max<std::size_t>(std::min(budget, pool_size), 1);
    return std::clamp(rows / (threads * kMorselsPerThread), kMinGrain, kMaxGrain);
}

auto process_pipeline_stats() -> ParallelPipelineStats* {
    // File-local, like the worker pool and the query lease: a bundled plugin
    // statically links runtime code, so an inline header variable would give
    // each plugin its own counter (the RTLD_LOCAL trap).
    //
    // The reporter is a separate static whose destructor runs at exit. It holds
    // no reference to the counters it prints beyond the function-local statics
    // above it, which outlive it by declaration order.
    static const bool enabled = std::getenv("IBEX_PARALLEL_STATS") != nullptr;
    static ParallelPipelineStats stats;
    // Function-local exit reporter, instantiated once below; never copied or moved.
    // NOLINTNEXTLINE(cppcoreguidelines-special-member-functions)
    struct Reporter {
        ~Reporter() {
            if (!enabled) {
                return;
            }
            // Exit-time diagnostic: a failed stderr write must not turn into a
            // `std::terminate` from a throwing destructor.
            try {
                ibex::formatting::print(
                    stderr,
                    "pipeline stats: parallel={} serial={} morsels={} "
                    "pipelined_scans={} pipelined_stages={} range_heads={} two_phase={} "
                    "parallel_fields={} parallel_direct_numeric_fields={} parallel_probes={} "
                    "parallel_hash_builds={} parallel_aggregate_partitions={} "
                    "parallel_aggregate_finalizes={} "
                    "grouped_lifted_group_state={} chunk_direct_updates={}\n",
                    stats.parallel_pipelines.load(), stats.serial_pipelines.load(),
                    stats.morsels.load(), stats.pipelined_scans.load(),
                    stats.pipelined_stages.load(), stats.range_heads.load(),
                    stats.two_phase_filters.load(), stats.parallel_fields.load(),
                    stats.parallel_direct_numeric_fields.load(), stats.parallel_probes.load(),
                    stats.parallel_hash_builds.load(), stats.parallel_aggregate_partitions.load(),
                    stats.parallel_aggregate_finalizes.load(),
                    stats.grouped_lifted_group_state.load(), stats.chunk_direct_updates.load());
            } catch (...) {  // NOLINT(bugprone-empty-catch)
            }
        }
    };
    static const Reporter reporter;
    return enabled ? &stats : nullptr;
}

void configure_parallel_from_env(ExecutionContext& exec) {
    // The other two execution switches, applied the same way and for the same
    // reason: one authority per setting. Both were `getenv` at their use sites
    // until the seams that share them started to outnumber the settings —
    // `stream_scans` is read at three build seams that must agree, and
    // `parallel_join_probe` at three probe gates. An unset variable leaves the
    // caller's choice alone, so a context built by hand still means "ignore the
    // environment".
    if (const auto want = stream_scans_from_env(); want.has_value()) {
        exec.stream_scans = *want;
    }
    if (const auto want = parallel_join_probe_from_env(); want.has_value()) {
        exec.parallel_join_probe = *want;
    }
    // Pin the COMPUTE budget to the core count. Every compute gate sizes itself
    // from `parallel_threads`, falling back to the pool size when it is 0 — and
    // the pool is now sized for decode, which wants more threads than cores
    // (`decode_oversubscribe`). Without this, growing the pool would
    // oversubscribe the compute paths too, and those measurably do not want it
    // (q01 +4.6%, q17 +3.2%). With a multiplier of 1 this is the same number the
    // fallback produced, so the default configuration is unchanged.
    if (exec.parallel_threads == 0) {
        exec.parallel_threads = compute_thread_count();
    }
    if (exec.parallel_stats == nullptr) {
        exec.parallel_stats = process_pipeline_stats();
    }
    if (exec.execution_profile == nullptr && execution_profile_requested()) {
        // The budget occupancy is measured against. Read from the context or
        // the environment rather than `process_worker_pool().size()`, so that
        // asking for a profile never constructs a pool a serial query would
        // otherwise never have built.
        const std::size_t budget = exec.compute_budget();
        exec.execution_profile = std::make_shared<ExecutionProfileState>(budget);
    }
    if (const std::size_t grain = morsel_rows_from_env(); grain > 0) {
        exec.parallel_grain = grain;
        // An explicit grain is an explicit request to partition at that size,
        // so it also lowers the serial threshold — otherwise the default
        // threshold would silently override the knob it was asked to honor.
        exec.parallel_min_rows = std::min(exec.parallel_min_rows, grain);
    }
    // `parallel_threads` is set at the top of this function, not here: it is
    // the COMPUTE budget and must track the core count, because the pool it
    // used to default to is now sized for decode instead.
}
namespace physical_executor_detail {
/// Build a join the plan migrated: `HashBuild` on one side, `HashProbe` on the
/// other. The family-owned join executor implements both phases and exposes a
/// narrow construction boundary here. The kernel-pipeline plan's Phase 4 item
/// 1.
///
/// The three branches are the ones that used to sit in `build_operator_impl`'s
/// per-kind switch. Construction lives with the plan, the decisions are the
/// same ones `plan_join` relayed, and execution is now owned by the join
/// family. HashBuild and HashProbe are explicit structural nodes connected by
/// a typed runtime-orientation edge; an eligible probe can also become a step
/// inside a map pipeline.

namespace {

/// Both of a streaming join's fan-out phases, resolved for this query. The
/// capability halves are `physical::join_hash_build_parallelism` /
/// `join_probe_parallelism` (floor + worker ceiling); the resolved half needs
/// the `ExecutionContext` and the pool size, both in hand at build time. One
/// definition, shared by every join construction site, the way `distinct_table`
/// and `build_physical_distinct` share the dedup policy.
auto resolve_join_parallelism(physical::JoinParallelism par, const ExecutionContext& exec)
    -> physical::JoinParallelism {
    // The pool is sized for decode and spawns its threads on first touch, so a
    // serial query must not construct it just to learn it is serial.
    const std::size_t pool_size = exec.can_fan_out() ? process_worker_pool().size() : 0;
    physical::resolve_breaker_parallelism(par.build, exec, pool_size);
    physical::resolve_breaker_parallelism(par.probe, exec, pool_size);
    return par;
}

/// Resolve the policies carried by the explicit HashBuild and HashProbe nodes.
/// Taking copies is intentional: resolution is execution-context state and the
/// data-only physical plan remains reusable and inspectable.
auto resolved_join_parallelism(const physical::StreamingJoinNodes& nodes,
                               const ExecutionContext& exec) -> physical::JoinParallelism {
    return resolve_join_parallelism(
        {.build = nodes.build.parallelism, .probe = nodes.probe.parallelism}, exec);
}

/// As `resolved_join_parallelism`, for the hash aggregate. The capability half
/// (floors, worker ceiling, strategy) comes from the four aggregate node
/// policy factories; the resolved half needs the
/// `ExecutionContext` and pool size, both in hand here at build time. One
/// definition, shared by every aggregate construction site.
auto resolved_aggregate_parallelism(const physical::HashAggregateNodes& nodes,
                                    const ExecutionContext& exec)
    -> std::expected<physical::AggregateParallelism, std::string> {
    physical::AggregateParallelism par{
        .discovery = nodes.discovery.parallelism,
        .accumulation = nodes.accumulation.parallelism,
        .final_ordering = nodes.final_ordering.parallelism,
        .emission = nodes.emission.parallelism,
    };
    const std::size_t pool_size = exec.can_fan_out() ? process_worker_pool().size() : 0;
    physical::resolve_breaker_parallelism(par.discovery, exec, pool_size);
    physical::resolve_breaker_parallelism(par.accumulation, exec, pool_size);
    physical::resolve_breaker_parallelism(par.final_ordering, exec, pool_size);
    physical::resolve_breaker_parallelism(par.emission, exec, pool_size);
    return par;
}

}  // namespace

/// Compatibility construction sites that do not own a physical Plan still use
/// the same policy factories. Migrated joins take the `StreamingJoinNodes`
/// overload above instead. Exported via `physical_executor_internal.hpp` for
/// the pipeline unit's probe-fusion path.
auto resolved_join_parallelism(const ExecutionContext& exec) -> physical::JoinParallelism {
    return resolve_join_parallelism({.build = physical::join_hash_build_parallelism(),
                                     .probe = physical::join_probe_parallelism()},
                                    exec);
}

auto build_physical_join(const physical::Plan& plan, const ir::Node& node,
                         const TableRegistry& registry, const ScalarRegistry* scalars,
                         const ExternRegistry* externs, const ExecutionContext& exec,
                         ModelResult* model_out) -> std::expected<OperatorPtr, std::string> {
    const auto& join = ir::node_cast<ir::JoinNode>(node);
    const physical::JoinPlan& jp = plan.join;
    if (jp.branch == physical::JoinBranch::SemiAnti) {
        const bool stage_probe = pipeline_executor_detail::has_multi_unit_deferred_scan(
            *join.children()[0], registry, exec);
        // Multiple producers: tried the same overlap the inner-join site
        // once had here too, twice. First attempt (unbudgeted): q04
        // regressed +19%. Second attempt, under a since-removed
        // helper-thread budget: q04 and q18 both STILL regressed, and in
        // both cases the overlap is entered exactly once per query (verified with a temporary
        // entry-count trace) -- there is no recursive pile-up here for a
        // budget to bound, so the budget was never going to help. The
        // cost is inherent to overlapping this specific pair of sides,
        // not to how many raw threads accumulate. Reverted a second
        // time; see plans/parallelism-overview.md's "generalize
        // multiple producers" section before trying again here.
        auto left_op =
            build_operator(*join.children()[0], registry, scalars, externs, exec, model_out);
        if (!left_op.has_value()) {
            return std::unexpected(std::move(left_op.error()));
        }
        auto right =
            materialize_row_local(*join.children()[1], registry, scalars, externs, exec, model_out);
        if (!right.has_value()) {
            return std::unexpected(std::move(right.error()));
        }
        return pipeline_executor_detail::make_pipelined_stage_if(
            make_chunked_semi_anti_join_operator(std::move(left_op.value()),
                                                 std::move(right.value()), join.kind(),
                                                 &join.keys(), &exec),
            stage_probe, exec, execution_profile_entry(exec.execution_profile, node));
    }
    if (!plan.streaming_join.has_value()) {
        return std::unexpected("physical join: streaming plan has no HashBuild/HashProbe nodes");
    }
    const physical::StreamingJoinNodes& nodes = *plan.streaming_join;
    if (auto edge_error = physical::validate_streaming_join_edge(nodes)) {
        return std::unexpected(std::move(*edge_error));
    }
    // `nulls equal` goes to the materialized join, which implements the
    // policy. These streaming operators hash and probe on their own and
    // would each need the same null tagging; sending the opt-in case to the
    // one implementation that has it keeps a single definition of the
    // semantics -- and leaves this hot path bit-for-bit unchanged for every
    // join that does not ask for it.
    if (jp.branch == physical::JoinBranch::SingleKeyInner) {
        const bool stage_probe = pipeline_executor_detail::has_multi_unit_deferred_scan(
            *join.children()[0], registry, exec);
        // A deferred probe scan must not be interpreted here — the join
        // publishes build-side bounds into its filter slot first, then
        // interprets the right subtree itself (resolve_deferred_probe).
        const auto probe = deferred_probe_scan_of(*join.children()[1], exec);

        // Multiple producers (plans/parallelism-overview.md): the left
        // build and the right materialize were overlapped on a raw
        // std::thread here for a time (q10 ~-3% in-suite), but every
        // widening of the idea measured worse and was reverted, and the
        // site was removed ahead of the kernel-pipeline restructure —
        // branch concurrency needs a cost-aware gate, not a thread-count
        // one. Left builds first, then the right materializes.
        auto left_op =
            build_operator(*join.children()[0], registry, scalars, externs, exec, model_out);
        if (!left_op.has_value()) {
            return std::unexpected(std::move(left_op.error()));
        }
        if (probe.scan != nullptr) {
            auto built = make_scheduled_deferred_inner_join_operator(
                std::move(left_op.value()), join.children()[1].get(), &registry, scalars, externs,
                exec, &join.keys(), probe.scan, *probe.name, join.suffix(), &join.pending_order(),
                resolved_join_parallelism(nodes, exec), nodes.columns);
            if (!built.has_value()) {
                return std::unexpected(std::move(built.error()));
            }
            return pipeline_executor_detail::make_pipelined_stage_if(
                std::move(*built), stage_probe, exec,
                execution_profile_entry(exec.execution_profile, node));
        }
        auto right =
            materialize_row_local(*join.children()[1], registry, scalars, externs, exec, model_out);
        if (!right.has_value()) {
            return std::unexpected(std::move(right.error()));
        }
        auto built = make_scheduled_chunked_inner_join_operator(
            std::move(left_op.value()), std::move(right.value()), &join.keys(), exec, join.suffix(),
            &join.pending_order(), resolved_join_parallelism(nodes, exec), nodes.columns);
        if (!built.has_value()) {
            return std::unexpected(std::move(built.error()));
        }
        return pipeline_executor_detail::make_pipelined_stage_if(
            std::move(*built), stage_probe, exec,
            execution_profile_entry(exec.execution_profile, node));
    }
    // Streaming two-Int64-key inner join (plans/parallelism-overview.md's
    // "stream multi-key joins" item): same shape as the single-key
    // streamable path just above, minus the multiple-producers-overlap
    // machinery -- this builds the hash index on the smaller of the two
    // sides and streams/scans the other through
    // `ChunkedInnerJoinOperator`'s pair-key path, replacing
    // `join_table_impl`'s whole-table hash join for exactly this shape.
    // A deferred-probe right side (e.g. TPC-H q09's lineitem) is honored
    // exactly like the single-key branch above; see
    // `ChunkedInnerJoinOperator::resolve_deferred_probe_pair` for the
    // one-component filter this POC pushes into the scan.
    if (jp.branch == physical::JoinBranch::PairIntInner) {
        const bool stage_probe = pipeline_executor_detail::has_multi_unit_deferred_scan(
            *join.children()[0], registry, exec);
        const auto probe = deferred_probe_scan_of(*join.children()[1], exec);
        auto left_op =
            build_operator(*join.children()[0], registry, scalars, externs, exec, model_out);
        if (!left_op.has_value()) {
            return std::unexpected(std::move(left_op.error()));
        }
        if (probe.scan != nullptr) {
            auto built = make_scheduled_deferred_inner_join_operator(
                std::move(left_op.value()), join.children()[1].get(), &registry, scalars, externs,
                exec, &join.keys(), probe.scan, *probe.name, join.suffix(), &join.pending_order(),
                resolved_join_parallelism(nodes, exec), nodes.columns);
            if (!built.has_value()) {
                return std::unexpected(std::move(built.error()));
            }
            return pipeline_executor_detail::make_pipelined_stage_if(
                std::move(*built), stage_probe, exec,
                execution_profile_entry(exec.execution_profile, node));
        }
        auto right =
            materialize_row_local(*join.children()[1], registry, scalars, externs, exec, model_out);
        if (!right.has_value()) {
            return std::unexpected(std::move(right.error()));
        }
        auto built = make_scheduled_chunked_inner_join_operator(
            std::move(left_op.value()), std::move(right.value()), &join.keys(), exec, join.suffix(),
            &join.pending_order(), resolved_join_parallelism(nodes, exec), nodes.columns);
        if (!built.has_value()) {
            return std::unexpected(std::move(built.error()));
        }
        return pipeline_executor_detail::make_pipelined_stage_if(
            std::move(*built), stage_probe, exec,
            execution_profile_entry(exec.execution_profile, node));
    }

    return std::unexpected("physical join: plan named no streaming branch");
}

/// Build an aggregate the plan migrated: the streaming operator, or the
/// Join+Aggregate fusion. Phase 4 item 2.
///
/// Moved from `build_operator_impl`'s per-kind switch rather than rewritten.
/// `MaterializeAll` never arrives here -- it is not migrated, still falls back,
/// and is still counted in the backlog, which is what keeps that number honest.
auto build_physical_aggregate(const physical::Plan& plan, const ir::Node& node,
                              const TableRegistry& registry, const ScalarRegistry* scalars,
                              const ExternRegistry* externs, const ExecutionContext& exec,
                              ModelResult* model_out) -> std::expected<OperatorPtr, std::string> {
    const auto& agg = ir::node_cast<ir::AggregateNode>(node);
    if (agg.children().empty()) {
        return std::unexpected("aggregate node missing child");
    }
    // The plan decides; this reads its decision. `plan_aggregate` relays the
    // same two predicates this code used to call, so the routing is
    // identical by construction.
    const physical::AggregatePlan& ap = plan.aggregate;
    if (ap.strategy == physical::AggregateStrategy::FusedLeftJoinCount) {
        // Two logical nodes, one physical step: the join named here is
        // consumed, never handed to `build_operator`, so it is neither
        // planned nor counted separately. The skip is a consequence of who
        // calls whom -- the same way a fused `MapStep` partner is skipped
        // by the plan's walk simply never descending to it.
        const auto& join = ir::node_cast<ir::JoinNode>(*ap.fused_join);
        const std::string& counted_column = ap.counted_column;
        auto left =
            materialize_row_local(*join.children()[0], registry, scalars, externs, exec, model_out);
        if (!left.has_value()) {
            return std::unexpected(std::move(left.error()));
        }
        auto right =
            materialize_row_local(*join.children()[1], registry, scalars, externs, exec, model_out);
        if (!right.has_value()) {
            return std::unexpected(std::move(right.error()));
        }
        if (auto fused = left_join_count_table(join, agg, *left, *right, counted_column, &exec);
            fused.has_value()) {
            return make_table_source(std::move(*fused));
        }
        auto joined = join_table_impl(*left, *right, join.kind(), join.keys(), nullptr, scalars,
                                      compute_mask, join.suffix(), join.pending_order(),
                                      join.null_match(), join.expect(), join.take(), &exec);
        if (!joined.has_value()) {
            return std::unexpected(std::move(joined.error()));
        }
        auto result = aggregate_table(*joined, agg.group_by(), agg.aggregations(), &exec);
        if (!result.has_value()) {
            return std::unexpected(std::move(result.error()));
        }
        return make_table_source(std::move(*result));
    }
    if (ap.strategy == physical::AggregateStrategy::StreamingSorted) {
        if (!plan.hash_aggregate.has_value()) {
            return std::unexpected(
                "physical aggregate: adaptive strategy has no hash-fallback phase chain");
        }
        if (auto edge_error = physical::validate_hash_aggregate_edges(*plan.hash_aggregate)) {
            return std::unexpected(std::move(*edge_error));
        }
        if (agg.children().empty() ||
            plan.hash_aggregate->discovery.source != agg.children().front().get()) {
            return std::unexpected(
                "physical aggregate: Discovery input does not match the aggregate child");
        }
        auto parallelism = resolved_aggregate_parallelism(*plan.hash_aggregate, exec);
        if (!parallelism.has_value()) {
            return std::unexpected(std::move(parallelism.error()));
        }
        auto child_op =
            build_operator(*agg.children().front(), registry, scalars, externs, exec, model_out);
        if (!child_op.has_value()) {
            return std::unexpected(std::move(child_op.error()));
        }
        // The sorted operator streams group-at-a-time when the child's
        // chunks arrive sorted on the group keys, and otherwise replays the
        // first chunk into the hash aggregate phase operator — so it is safe
        // to route the whole streamable subset here.
        // Aggregates are often the terminal breaker and hash aggregation
        // emits only after consuming all input. Scheduling one in its own
        // stage in that shape buys no overlap and only creates a thread.
        // A join below it is staged instead: its probe stream can fill the
        // aggregate while it keeps pulling the next probe chunk.
        // Resolve the hash fallback's four structural-node policies here, where
        // the ExecutionContext is in hand, and hand them down. The operator
        // retains only data-dependent gates such as actual row counts and
        // strategy-specific usefulness thresholds.
        return make_chunked_aggregate_operator(std::move(child_op.value()), &agg.group_by(),
                                               &agg.aggregations(), exec, *parallelism, ap.columns);
    }

    return std::unexpected("physical aggregate: plan named no executable strategy");
}

/// Build a `Tail` breaker. Moved verbatim from the per-kind switch: `Tail`
/// needs every row before it can keep the last N, so it materializes the child
/// and calls `tail_table` rather than streaming. Same single-operator shape as
/// Head -- no `Plan` to consult.
auto build_physical_tail(const ir::Node& node, const TableRegistry& registry,
                         const ScalarRegistry* scalars, const ExternRegistry* externs,
                         const ExecutionContext& exec, ModelResult* model_out)
    -> std::expected<OperatorPtr, std::string> {
    const auto& tail = ir::node_cast<ir::TailNode>(node);
    if (tail.children().empty()) {
        return std::unexpected("tail node missing child");
    }
    auto count = evaluate_row_count_expr_impl(tail.count_expr(), scalars, externs);
    if (!count.has_value()) {
        return std::unexpected(count.error());
    }
    return build_unary_materializing_operator(
        *tail.children().front(), registry, scalars, externs, exec, model_out,
        [&](const Table& input) { return tail_table(input, *count, tail.group_by()); });
}

}  // namespace physical_executor_detail

namespace {

auto build_operator_impl(const ir::Node& node, const TableRegistry& registry,
                         const ScalarRegistry* scalars, const ExternRegistry* externs,
                         const ExecutionContext& exec, ModelResult* model_out)
    -> std::expected<OperatorPtr, std::string> {
    // Physical-plan seam (plans/kernel-pipeline-execution-plan.md). One plan
    // per node, and it describes the whole map chain or migrated breaker. The
    // executor below is also callable with an already-built plan, which makes
    // plan-edge mutation tests exercise the same consumer production uses.
    const physical::Plan plan = physical::plan_physical(node, registry, externs);
    if (plan.migrated) {
        return build_migrated_physical_operator(plan, node, registry, scalars, externs, exec,
                                                model_out);
    }
    // Counted in every mode. It used to fire only when the query could not fan
    // out, so at two cores or more the backlog read as empty -- a migration
    // counter that reports nothing on the configuration everything is measured
    // on. The seam visits each node once, so there is nothing to double count;
    // the test asserts the two modes agree.
    physical::note_materialized_call(plan.reason, node.kind());

    // A deferred lazy scan can be streamed instead of materialized. Everything
    // else — a registered table, a source with no unit decomposition — falls
    // through to the whole-table path at the bottom of this function, so
    // declining here costs nothing but the whole-table behaviour.
    if (node.kind() == ir::NodeKind::Scan && exec.stream_scans) {
        const auto& scan = ir::node_cast<ir::ScanNode>(node);
        if (!registry.contains(scan.source_name())) {
            // A null filter slot is what distinguishes a scan registered for
            // streaming from a deferred *probe* scan. A probe's decode belongs
            // to the join above it, which publishes build-side key bounds into
            // that slot first; streaming it here would decode it before those
            // bounds exist. See `deferred_probe_scan_of`, which draws the same
            // line from the other side.
            if (const auto* deferred = exec.deferred_scan(scan.source_name());
                deferred != nullptr && deferred->filter == nullptr) {
                auto units = deferred_scan_units(*deferred);
                if (units.size() > 1) {
                    if (exec.can_fan_out() &&
                        pipeline_executor_detail::scan_pipeline_worker_count(units.size()) > 0) {
                        return pipeline_executor_detail::build_pipelined_scan(
                            {}, false, *deferred, std::move(units), scalars, externs, exec);
                    }
                    return pipeline_executor_detail::make_deferred_scan_source(
                        *deferred, std::move(units), exec);
                }
            }
        }
    }

    if (node.kind() == ir::NodeKind::Filter) {
        const auto& filter = ir::node_cast<ir::FilterNode>(node);
        if (filter.children().empty()) {
            return std::unexpected("filter node missing child");
        }
        auto child_op =
            build_operator(*filter.children().front(), registry, scalars, externs, exec, model_out);
        if (!child_op.has_value()) {
            return std::unexpected(std::move(child_op.error()));
        }
        return pipeline_executor_detail::build_row_local_map_operator(
            node, std::move(child_op.value()), scalars, externs, exec, false);
    }

    if (node.kind() == ir::NodeKind::Project) {
        const auto& project = ir::node_cast<ir::ProjectNode>(node);
        if (project.children().empty()) {
            return std::unexpected("project node missing child");
        }
        auto child_op = build_operator(*project.children().front(), registry, scalars, externs,
                                       exec, model_out);
        if (!child_op.has_value()) {
            return std::unexpected(std::move(child_op.error()));
        }
        return pipeline_executor_detail::build_row_local_map_operator(
            node, std::move(child_op.value()), scalars, externs, exec, false);
    }

    // No FilterHead / FilterTail branch: fused Head(Filter(x)) / Tail(Filter(x))
    // (canonicalize R7/R8) is a migrated plan built by
    // `build_physical_filter_head_tail` at the seam above.

    if (node.kind() == ir::NodeKind::Rename) {
        const auto& rename = ir::node_cast<ir::RenameNode>(node);
        if (rename.children().empty()) {
            return std::unexpected("rename node missing child");
        }
        auto child_op =
            build_operator(*rename.children().front(), registry, scalars, externs, exec, model_out);
        if (!child_op.has_value()) {
            return std::unexpected(std::move(child_op.error()));
        }
        return pipeline_executor_detail::build_row_local_map_operator(
            node, std::move(child_op.value()), scalars, externs, exec, false);
    }

    if (node.kind() == ir::NodeKind::ExternCall && externs != nullptr) {
        const auto& ec = ir::node_cast<ir::ExternCallNode>(node);
        const auto* fn = externs->find(ec.callee());
        if (fn != nullptr && fn->chunked_table_func) {
            ExternArgs args;
            args.reserve(ec.args().size());
            bool args_ok = true;
            for (const auto& arg : ec.args()) {
                auto val = eval_expr(arg, Table{}, 0, scalars, externs);
                auto scalar = val.has_value() ? scalar_from_expr(val.value()) : std::nullopt;
                if (!scalar.has_value()) {
                    args_ok = false;
                    break;
                }
                args.push_back(std::move(*scalar));
            }
            if (args_ok) {
                auto op = fn->chunked_table_func(args);
                if (op.has_value()) {
                    return std::move(op.value());
                }
            }
        }
    }

    // No Distinct branch: every Distinct is a migrated plan, built by
    // `build_physical_distinct` at the seam above.

    // No Order branch: every Order is a migrated plan, built by
    // `build_physical_order` at the seam above. Unlike the join and the
    // aggregate there is no eligibility gate to relay -- one operator handles
    // every Order -- so the plan has nothing to decide and says only that this
    // node is an ordering breaker.

    // No Aggregate branch: `MaterializeAll` (Median, Quantile, Ewma) falls
    // through to the whole-table path below exactly as it always did, and every
    // other aggregate is a migrated plan built by `build_physical_aggregate` at
    // the seam above.

    // No TopK / Head / Tail branch: each is a migrated plan built at the seam
    // above (`build_physical_topk` / `build_physical_head` / `build_physical_tail`).
    // TopK stays a serial bounded-heap select (O(n log k)); Tail materializes
    // and calls `tail_table`; the plan just records that they are breakers.

    // Every other node kind is a materialized-call fallback: not migrated by
    // `plan_physical`, counted just above, and executed by `interpret_node`
    // rather than a per-kind branch. `build_materialized_fallback` still builds
    // the breaker's direct children through the physical path (fused parallel
    // scan, streaming join), so the switch's 15 hand-synced branches collapse
    // to one without regressing a filtered/projected input. Construct / Stream /
    // Program (preamble), Model (`model_out`), and the reshape / stat / window /
    // update / matmul / materializing-join kinds all resolve there.
    // `physical_fallbacks_for(kind)` buckets the backlog so a kind can later be
    // lifted to a migrated breaker-over-pipeline the way Join / Aggregate /
    // Order were. Scan is handled as a source by the caller.
    return build_materialized_fallback(node, registry, scalars, externs, exec, model_out);
}

}  // namespace

auto build_operator(const ir::Node& node, const TableRegistry& registry,
                    const ScalarRegistry* scalars, const ExternRegistry* externs,
                    const ExecutionContext& exec, ModelResult* model_out)
    -> std::expected<OperatorPtr, std::string> {
    if (exec.execution_profile == nullptr) {
        return build_operator_impl(node, registry, scalars, externs, exec, model_out);
    }
    auto* entry = execution_profile_entry(exec.execution_profile, node);
    std::expected<OperatorPtr, std::string> result;
    {
        const ExecutionProfileScope scope(entry, ProfilePhase::Build);
        result = build_operator_impl(node, registry, scalars, externs, exec, model_out);
    }
    if (!result.has_value()) {
        return result;
    }
    return profile_operator(std::move(result.value()), exec.execution_profile, node);
}

}  // namespace ibex::runtime
