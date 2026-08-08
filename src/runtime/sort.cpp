// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

// sort.cpp — ordering and row-selection: LSD radix sort machinery,
// order_table (single- and multi-key, pre-sorted fast path), and grouped
// head/tail selection.
// Split out of interpreter.cpp; shared declarations live in interpreter_internal.hpp.

#include <ibex/core/column.hpp>
#include <ibex/core/time.hpp>
#include <ibex/ir/node.hpp>
#include <ibex/runtime/interpreter.hpp>
#include <ibex/runtime/worker_pool.hpp>

#include <algorithm>
#include <array>
#include <atomic>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <expected>
#include <limits>
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

#include "interpreter_internal.hpp"
#include "runtime_internal.hpp"

namespace ibex::runtime {

// LSD radix sort over pre-sign-flipped uint64 keys.
// Idx is the index type: uint32_t for tables ≤ UINT32_MAX rows, uint64_t otherwise.
// Keys must already be sign-flipped (int64 XOR 1<<63) so unsigned order == signed order.
// All 8 byte histograms are built in a single pass; passes where every element
// shares the same byte value are skipped (common for clustered timestamps).
// Stable LSD radix sort of the index array `idx` by `src_keys` (parallel arrays;
// idx is the payload carried alongside each key). `idx` is sorted in place and
// must already hold a valid permutation of [0, rows) — passing iota gives a sort
// from scratch, passing an existing order makes this a stable re-sort by a new
// key (the building block for multi-key LSD). Keys are consumed.
namespace {

template <typename Idx>

void radix_sort_by_key(std::vector<std::uint64_t> src_keys, std::vector<Idx>& idx,
                       std::size_t rows) {
    // Build all 8 byte-histograms in one sequential scan.
    std::array<std::array<std::size_t, 256>, 8> hists{};
    for (std::size_t i = 0; i < rows; ++i) {
        auto k = src_keys[i];
        for (std::size_t p = 0; p < 8; ++p)
            ++hists[p][(k >> (p * 8U)) & 0xFFU];
    }

    std::vector<std::uint64_t> dst_keys(rows);
    std::vector<Idx> dst_idx(rows);
    // Ping-pong between the caller's idx buffer and dst_idx; src_* point at the
    // buffer currently holding live data.
    std::vector<std::uint64_t>* src_k = &src_keys;
    std::vector<std::uint64_t>* dst_k = &dst_keys;
    std::vector<Idx>* src_i = &idx;
    std::vector<Idx>* dst_i = &dst_idx;

    std::array<std::size_t, 256> cnt;  //  NOLINT(cppcoreguidelines-pro-type-member-init)
    for (std::size_t pass = 0; pass < 8; ++pass) {
        const auto& h = hists[pass];
        // Skip pass if all elements have the same byte value.
        std::size_t non_zero = 0;
        for (auto c : h)
            if (c)
                ++non_zero;
        if (non_zero <= 1)
            continue;

        auto shift = pass * 8U;
        // Convert histogram to exclusive prefix-sum write positions.
        std::size_t total = 0;
        for (std::size_t b = 0; b < 256; ++b) {
            cnt[b] = total;
            total += h[b];
        }
        // Stable scatter: sequential reads, random writes.
        // Prefetch the destination cache line a few elements ahead.
        for (std::size_t i = 0; i < rows; ++i) {
#if defined(__GNUC__) || defined(__clang__)
            constexpr std::size_t kPrefetchDist = 8;
            if (i + kPrefetchDist < rows) {
                const std::size_t pb = ((*src_k)[i + kPrefetchDist] >> shift) & 0xFFU;
                __builtin_prefetch(&(*dst_k)[cnt[pb]], 1, 1);
                __builtin_prefetch(&(*dst_i)[cnt[pb]], 1, 1);
            }
#endif
            const std::size_t bucket = ((*src_k)[i] >> shift) & 0xFFU;
            (*dst_k)[cnt[bucket]] = (*src_k)[i];
            (*dst_i)[cnt[bucket]] = (*src_i)[i];
            ++cnt[bucket];
        }
        std::swap(src_k, dst_k);
        std::swap(src_i, dst_i);
    }
    // Ensure the sorted permutation ends up in the caller's idx buffer.
    if (src_i != &idx)
        idx = std::move(*src_i);
}

}  // namespace

namespace {

template <typename Idx>

auto radix_sort_impl(std::vector<std::uint64_t> src_keys, std::size_t rows) -> std::vector<Idx> {
    std::vector<Idx> idx(rows);
    std::iota(idx.begin(), idx.end(), Idx{0});
    radix_sort_by_key(std::move(src_keys), idx, rows);
    return idx;
}

}  // namespace

/// Workers for a barrier operator whose unit of work is a GROUP — rank's sweep
/// and per-group sorts, the collect-aggregate reduce. Sized on ROW COUNT, so
/// the split, and therefore nothing about the answer, depends on the pool.
/// More workers than groups is pointless; each caller checks its own group
/// count, which this cannot see.
auto group_barrier_worker_count(const ExecutionContext& exec, std::size_t rows) -> std::size_t {
    if (on_worker_pool_thread() || !exec.parallel || rows < exec.parallel_min_rows) {
        return 0;
    }
    const std::size_t pool_size = process_worker_pool().size();
    const std::size_t budget = exec.parallel_threads == 0 ? pool_size : exec.parallel_threads;
    const std::size_t workers = std::min(budget, pool_size);
    return workers < 2 ? 0 : workers;
}

// Per-group sorting for the grouped rank path. The whole-table entry points
// above sort every row at once; this sorts one group's run where it already
// sits, so a caller holding rows bucketed by group can sort the buckets
// independently — and concurrently, since the runs are disjoint.
void sort_key_index_slice(std::uint64_t* keys, std::size_t* idx, std::size_t n,
                          RadixSliceScratch& scratch) {
    if (n < 2) {
        return;
    }
    // A radix pass builds a 256-bucket histogram whatever the run's length, so
    // a short run costs 2048 counter updates to order a handful of elements.
    // Measured: at 8 rows per group, radix per group ran 5.5x SLOWER than the
    // whole-table sort it replaces; with this fallback it is 3x faster.
    constexpr std::size_t kSmallRun = 64;
    if (n <= kSmallRun) {
        auto& pairs = scratch.pairs;
        pairs.resize(n);
        for (std::size_t i = 0; i < n; ++i) {
            pairs[i] = {keys[i], idx[i]};
        }
        std::ranges::stable_sort(
            pairs, [](const auto& lhs, const auto& rhs) { return lhs.first < rhs.first; });
        for (std::size_t i = 0; i < n; ++i) {
            keys[i] = pairs[i].first;
            idx[i] = pairs[i].second;
        }
        return;
    }

    std::array<std::array<std::size_t, 256>, 8> hists{};
    for (std::size_t i = 0; i < n; ++i) {
        const auto k = keys[i];
        for (std::size_t p = 0; p < 8; ++p) {
            ++hists[p][(k >> (p * 8U)) & 0xFFU];
        }
    }
    scratch.keys.resize(n);
    scratch.idx.resize(n);
    std::uint64_t* src_k = keys;
    std::uint64_t* dst_k = scratch.keys.data();
    std::size_t* src_i = idx;
    std::size_t* dst_i = scratch.idx.data();
    std::array<std::size_t, 256> cnt;  // NOLINT(cppcoreguidelines-pro-type-member-init)
    for (std::size_t pass = 0; pass < 8; ++pass) {
        const auto& h = hists[pass];
        std::size_t non_zero = 0;
        for (const auto c : h) {
            if (c != 0) {
                ++non_zero;
            }
        }
        if (non_zero <= 1) {
            continue;  // every element shares this byte
        }
        const auto shift = pass * 8U;
        std::size_t total = 0;
        for (std::size_t b = 0; b < 256; ++b) {
            cnt[b] = total;
            total += h[b];
        }
        for (std::size_t i = 0; i < n; ++i) {
            const std::size_t bucket = (src_k[i] >> shift) & 0xFFU;
            dst_k[cnt[bucket]] = src_k[i];
            dst_i[cnt[bucket]] = src_i[i];
            ++cnt[bucket];
        }
        std::swap(src_k, dst_k);
        std::swap(src_i, dst_i);
    }
    if (src_i != idx) {
        std::copy_n(src_i, n, idx);
        std::copy_n(src_k, n, keys);
    }
}

// Dispatch to 32-bit indices for tables that fit, 64-bit otherwise.
using SortIdx = std::variant<std::vector<std::uint32_t>, std::vector<std::uint64_t>>;
auto radix_sort_u64_asc(std::vector<std::uint64_t> keys, std::size_t rows) -> SortIdx {
    if (rows <= std::numeric_limits<std::uint32_t>::max())
        return radix_sort_impl<std::uint32_t>(std::move(keys), rows);
    return radix_sort_impl<std::uint64_t>(std::move(keys), rows);
}

// Stable multi-key sort by LSD radix: `codes[k]` holds one order-preserving u64
// per row for sort key k (key 0 most significant). Sorts least- to most-
// significant key, each pass stable, so the result equals a stable comparison
// sort on the same keys with ties broken by original row order. Each key is
// gathered into the current index order first so the radix scatter reads
// sequentially.
namespace {

template <typename Idx>

auto lsd_multi_radix(const std::vector<std::vector<std::uint64_t>>& codes, std::size_t rows)
    -> std::vector<Idx> {
    std::vector<Idx> idx(rows);
    std::iota(idx.begin(), idx.end(), Idx{0});
    for (std::size_t k = codes.size(); k-- > 0;) {
        const auto& code = codes[k];
        std::vector<std::uint64_t> gathered(rows);
        for (std::size_t i = 0; i < rows; ++i)
            gathered[i] = code[idx[i]];
        radix_sort_by_key(std::move(gathered), idx, rows);
    }
    return idx;
}

}  // namespace

namespace {

// Sort `input` by an already-resolved key list. The TimeFrame ordering policy
// (and any implicit time-index tiebreaker) is decided by the public
// `order_table` wrapper below before this runs.
/// Gather a sort permutation across worker threads.
///
/// This is the sort's second half: the radix produces a permutation, and then
/// every column is rewritten through it. That rewrite is pure data movement —
/// output row `i` reads input row `idx[i]` — so it splits perfectly, and after
/// the key work was cut down it became the largest remaining serial block in a
/// sorted query.
///
/// Work is split by (column x row range) rather than by column alone: a column
/// count is a poor divisor (a 4-column table cannot use 8 threads) and column
/// widths differ. **Range boundaries are aligned to 64 rows**, which is what
/// makes `Column<bool>` and validity bitmaps safe to write concurrently — their
/// words then belong to exactly one range. Output rows are contiguous, so that
/// alignment is available here; a scattered scatter cannot buy it.
///
/// A string column is one indivisible task: its offsets are cumulative, so a
/// partial range has no meaning without a prefix sum over ranges.
template <typename Idx>
auto gather_rows_parallel(const Table& input, const std::vector<Idx>& idx,
                          const std::vector<ir::OrderKey>* ordering, const ExecutionContext& exec)
    -> Table {
    const std::size_t rows = idx.size();
    const std::size_t n_cols = input.columns.size();

    const std::size_t pool_size = process_worker_pool().size();
    const std::size_t budget = exec.parallel_threads == 0 ? pool_size : exec.parallel_threads;
    const std::size_t threads = std::min(budget, pool_size);
    const bool worth_it =
        exec.parallel && !on_worker_pool_thread() && threads >= 2 && n_cols != 0 &&
        rows >= exec.parallel_min_rows &&
        (exec.parallel_min_cells == 0 || rows * n_cols >= exec.parallel_min_cells);
    if (!worth_it) {
        return gather_rows(input, idx, ordering);
    }

    // Allocate every output column first: the column vector must not be
    // resized once workers hold pointers into it.
    Table output;
    output.columns.reserve(n_cols);
    for (const auto& entry : input.columns) {
        output.add_column(entry.name, make_gather_column(*entry.column, rows));
        if (entry.validity.has_value()) {
            output.columns.back().validity = ValidityBitmap(rows, false);
        }
    }

    struct Task {
        std::size_t column;
        std::size_t lo;
        std::size_t hi;
    };
    std::vector<Task> tasks;
    constexpr std::size_t kAlign = 64;
    // Enough tasks that a slow one cannot strand the rest, rounded up to whole
    // 64-row words so no two tasks share a bit-packed word.
    std::size_t span = (rows + (threads * 4) - 1) / (threads * 4);
    span = ((span + kAlign - 1) / kAlign) * kAlign;
    span = std::max(span, kAlign);
    for (std::size_t c = 0; c < n_cols; ++c) {
        if (std::holds_alternative<Column<std::string>>(*input.columns[c].column)) {
            tasks.push_back({.column = c, .lo = 0, .hi = rows});  // indivisible
            continue;
        }
        for (std::size_t lo = 0; lo < rows; lo += span) {
            tasks.push_back({.column = c, .lo = lo, .hi = std::min(lo + span, rows)});
        }
    }

    std::atomic<std::size_t> cursor{0};
    {
        auto batch = process_worker_pool().submit(threads, [&](std::size_t) noexcept {
            while (true) {
                const std::size_t t = cursor.fetch_add(1, std::memory_order_relaxed);
                if (t >= tasks.size()) {
                    return;
                }
                const auto& task = tasks[t];
                const auto& src = input.columns[task.column];
                gather_range_into(*output.columns[task.column].column, *src.column, idx, task.lo,
                                  task.hi);
                if (src.validity.has_value()) {
                    gather_validity_range(*output.columns[task.column].validity, *src.validity, idx,
                                          task.lo, task.hi);
                }
            }
        });
        batch.wait();
    }

    // Finalisation must match the serial gather exactly, `else` branch included
    // — a dropped ordering here would be invisible in the values and wrong in
    // the metadata.
    output.set_properties(ordering != nullptr ? input.properties().with_ordering(*ordering)
                                              : input.properties());
    return output;
}

/// True when `name`'s values never decrease down the column.
///
/// Two callers: skipping a sort whose key is already ordered, and deciding
/// whether a TimeFrame's implicit time tiebreaker has anything left to do.
/// Conservative — a column carrying nulls, or of a type not handled here,
/// answers false.
[[nodiscard]] auto column_is_non_decreasing(const Table& input, const std::string& name) -> bool {
    const auto* entry = input.find_entry(name);
    if (entry == nullptr || entry->validity.has_value()) {
        return false;
    }
    const auto* column = input.find(name);
    if (column == nullptr) {
        return false;
    }
    const std::size_t rows = input.rows();
    bool sorted = false;
    std::visit(
        [&](const auto& col) {
            using ColT = std::decay_t<decltype(col)>;
            auto scan = [&](auto get) {
                sorted = true;
                for (std::size_t i = 1; i < rows; ++i) {
                    if (get(col, i) < get(col, i - 1)) {
                        sorted = false;
                        return;
                    }
                }
            };
            if constexpr (std::is_same_v<ColT, Column<Timestamp>>) {
                scan([](const auto& c, std::size_t i) { return c[i].nanos; });
            } else if constexpr (std::is_same_v<ColT, Column<std::int64_t>>) {
                scan([](const auto& c, std::size_t i) { return c[i]; });
            } else if constexpr (std::is_same_v<ColT, Column<Date>>) {
                scan([](const auto& c, std::size_t i) { return c[i].days; });
            }
        },
        *column);
    return sorted;
}

auto order_table_resolved(const Table& input, const std::vector<ir::OrderKey>& resolved_keys,
                          const ExecutionContext& exec) -> std::expected<Table, std::string> {
    std::size_t rows = input.rows();
    if (rows <= 1 || input.columns.empty()) {
        Table output = input;
        output.set_properties(input.properties().with_ordering(resolved_keys));
        return output;
    }

    // The input may already carry a claim that it is in this order, in which
    // case there is nothing to prove by looking at the data at all. This is
    // what makes an upstream `order` — or a join that emitted its left rows in
    // order — pay for the sort once instead of once per consumer, and unlike
    // the data scan below it covers multi-key, descending and string orderings.
    if (input.properties().satisfies(resolved_keys)) {
        Table output = input;
        output.set_properties(input.properties().with_ordering(resolved_keys));
        return output;
    }

    // Fast pre-sorted check for single ascending Timestamp/Date/Int key — avoids building
    // the 8 MB flat_keys[0].u64 vector when the input is already sorted (common TimeFrame case).
    if (resolved_keys.size() == 1 && resolved_keys[0].ascending &&
        !input.find_entry(resolved_keys[0].name)->validity.has_value()) {
        const auto* column = input.find(resolved_keys[0].name);
        if (column != nullptr) {
            bool already_sorted = false;
            std::visit(
                [&](const auto& col) {
                    using ColT = std::decay_t<decltype(col)>;
                    if constexpr (std::is_same_v<ColT, Column<Timestamp>>) {
                        already_sorted = true;
                        for (std::size_t i = 1; i < rows; ++i) {
                            if (col[i].nanos < col[i - 1].nanos) {
                                already_sorted = false;
                                break;
                            }
                        }
                    } else if constexpr (std::is_same_v<ColT, Column<std::int64_t>>) {
                        already_sorted = true;
                        for (std::size_t i = 1; i < rows; ++i) {
                            if (col[i] < col[i - 1]) {
                                already_sorted = false;
                                break;
                            }
                        }
                    } else if constexpr (std::is_same_v<ColT, Column<Date>>) {
                        already_sorted = true;
                        for (std::size_t i = 1; i < rows; ++i) {
                            if (col[i].days < col[i - 1].days) {
                                already_sorted = false;
                                break;
                            }
                        }
                    }
                },
                *column);
            if (already_sorted) {
                Table output = input;
                output.set_properties(input.properties().with_ordering(resolved_keys));
                return output;
            }
        }
    }

    // Pre-extract each sort key into a flat typed array so the hot comparator
    // loop does plain vector indexing rather than per-comparison variant dispatch.
    // I64 keys are sign-flipped to uint64 at extraction time so that unsigned
    // comparison is equivalent to signed comparison — this lets radix_sort_u64_asc
    // consume the vector directly without an extra copy.
    constexpr std::uint64_t kSignFlip = std::uint64_t{1} << 63;
    enum class FlatKind : std::uint8_t { I64, F64, Str };
    struct FlatKey {
        FlatKind kind = FlatKind::I64;
        std::vector<std::uint64_t> u64;  // Int / Date.days / Timestamp.nanos, sign-flipped
        std::vector<double> f64;
        std::vector<std::string_view> str;  // views into original column storage
        bool ascending = true;
        const ValidityBitmap* validity = nullptr;  // null when the key has no nulls
        /// Number of distinct values when this key was built as a DENSE RANK in
        /// [0, cardinality), or 0 when the key is an arbitrary integer. Only a
        /// dense rank can be counting-sorted, because the bucket array is
        /// indexed by the key itself.
        std::size_t cardinality = 0;

        [[nodiscard]] auto is_null(std::size_t row) const noexcept -> bool {
            return validity != nullptr && !(*validity)[row];
        }
    };

    // A null key sorts last, and stays last under `desc` — the null position does
    // not flip with the direction. That cannot be folded into the flat key (a
    // sentinel max value would migrate to the front on a descending sort), so a
    // null-bearing key skips every radix/pre-sorted fast path below and takes the
    // comparator, which ranks null-ness ahead of value.
    bool has_null_keys = false;
    for (const auto& key : resolved_keys) {
        const auto* entry = input.find_entry(key.name);
        if (entry != nullptr && entry->validity.has_value()) {
            has_null_keys = true;
            break;
        }
    }

    std::vector<FlatKey> flat_keys;
    flat_keys.reserve(resolved_keys.size());
    for (const auto& key : resolved_keys) {
        const auto* column = input.find(key.name);
        if (column == nullptr) {
            return std::unexpected("order column not found: " + key.name +
                                   " (available: " + format_columns(input) + ")");
        }
        const auto* entry = input.find_entry(key.name);
        FlatKey fk;
        fk.ascending = key.ascending;
        fk.validity = entry != nullptr && entry->validity.has_value() ? &*entry->validity : nullptr;
        std::visit(
            [&](const auto& col) {
                using ColT = std::decay_t<decltype(col)>;
                if constexpr (std::is_same_v<ColT, Column<std::int64_t>>) {
                    fk.kind = FlatKind::I64;
                    fk.u64.reserve(rows);
                    for (auto v : col)
                        fk.u64.push_back(static_cast<std::uint64_t>(v) ^ kSignFlip);
                } else if constexpr (std::is_same_v<ColT, Column<double>>) {
                    fk.kind = FlatKind::F64;
                    fk.f64.assign(col.begin(), col.end());
                } else if constexpr (std::is_same_v<ColT, Column<Date>>) {
                    fk.kind = FlatKind::I64;
                    fk.u64.reserve(rows);
                    for (const auto& d : col)
                        fk.u64.push_back(static_cast<std::uint64_t>(d.days) ^ kSignFlip);
                } else if constexpr (std::is_same_v<ColT, Column<Timestamp>>) {
                    fk.kind = FlatKind::I64;
                    fk.u64.reserve(rows);
                    for (const auto& ts : col)
                        fk.u64.push_back(static_cast<std::uint64_t>(ts.nanos) ^ kSignFlip);
                } else if constexpr (std::is_same_v<ColT, Column<bool>>) {
                    fk.kind = FlatKind::I64;
                    fk.u64.reserve(rows);
                    for (std::size_t i = 0; i < rows; ++i)
                        fk.u64.push_back(static_cast<std::uint64_t>(col[i] ? 1 : 0) ^ kSignFlip);
                } else if constexpr (std::is_same_v<ColT, Column<std::string>>) {
                    fk.kind = FlatKind::Str;
                    fk.str.reserve(rows);
                    for (std::size_t i = 0; i < rows; ++i)
                        fk.str.push_back(col[i]);
                } else {
                    // Categorical: rank the DICTIONARY by value, then map each
                    // row's code through that ranking, so this becomes an
                    // ordinary integer key and takes the radix paths above.
                    //
                    // The obvious alternative — flatten to one string_view per
                    // row and let `ordinal_encode` discover the distinct values
                    // — hashes every row to rebuild a dictionary the column is
                    // already carrying. Sorting 5M rows by a 3-value symbol
                    // spent ~13% of the whole query doing exactly that.
                    const auto& dict = col.dictionary();
                    std::vector<std::uint32_t> order(dict.size());
                    std::iota(order.begin(), order.end(), 0U);
                    std::ranges::sort(
                        order, [&](std::uint32_t a, std::uint32_t b) { return dict[a] < dict[b]; });
                    // Equal strings must share a rank. Two dictionary entries
                    // can hold the same value (dictionaries are per row group
                    // upstream), and giving those distinct ranks would order
                    // equal values as if they differed.
                    std::vector<std::uint64_t> rank(dict.size());
                    std::uint64_t next = 0;
                    for (std::size_t r = 0; r < order.size(); ++r) {
                        if (r > 0 && dict[order[r]] != dict[order[r - 1]]) {
                            ++next;
                        }
                        rank[order[r]] = next;
                    }
                    fk.kind = FlatKind::I64;
                    fk.cardinality = dict.empty() ? 0 : static_cast<std::size_t>(next) + 1;
                    fk.u64.reserve(rows);
                    for (std::size_t i = 0; i < rows; ++i) {
                        const auto code = col.code_at(i);
                        // A null row's code carries no meaning: nulls are ranked
                        // by `is_null` in the comparator, and a null-bearing key
                        // never reaches a radix path at all.
                        const std::uint64_t value =
                            (code >= 0 && static_cast<std::size_t>(code) < rank.size())
                                ? rank[static_cast<std::size_t>(code)]
                                : 0;
                        fk.u64.push_back(value ^ kSignFlip);
                    }
                }
            },
            *column);
        flat_keys.push_back(std::move(fk));
    }

    // Fast path: a single dense-ranked key with few distinct values — counting
    // sort. One increment and one write per row over a bucket array that fits in
    // L1, against the general radix's EIGHT byte-histograms per row.
    //
    // The radix already skips the seven passes whose byte never varies, so it
    // does a single scatter — but it still builds all eight histograms to
    // discover that, and on a 5M-row sort by a 3-value symbol that histogram
    // pass alone was the largest single cost in the whole query.
    //
    // Stability comes free: rows are appended to their bucket in input order.
    if (!has_null_keys && flat_keys.size() == 1 && flat_keys[0].kind == FlatKind::I64 &&
        flat_keys[0].cardinality != 0) {
        constexpr std::size_t kCountingSortCap = 1U << 12;
        const std::size_t buckets = flat_keys[0].cardinality;
        if (buckets <= kCountingSortCap) {
            const auto& keys = flat_keys[0].u64;
            std::vector<std::size_t> position(buckets + 1, 0);
            for (std::size_t i = 0; i < rows; ++i) {
                ++position[(keys[i] ^ kSignFlip) + 1];
            }
            if (flat_keys[0].ascending) {
                for (std::size_t b = 1; b <= buckets; ++b) {
                    position[b] += position[b - 1];
                }
            } else {
                // Descending: buckets are laid out high-to-low, but rows still
                // enter each bucket in input order, so equal keys stay stable.
                std::vector<std::size_t> counts(position.begin() + 1, position.end());
                std::size_t total = 0;
                for (std::size_t b = buckets; b-- > 0;) {
                    position[b] = total;
                    total += counts[b];
                }
            }
            auto build = [&]<typename Idx>() -> SortIdx {
                std::vector<Idx> idx(rows);
                for (std::size_t i = 0; i < rows; ++i) {
                    idx[position[keys[i] ^ kSignFlip]++] = static_cast<Idx>(i);
                }
                return SortIdx{std::move(idx)};
            };
            auto sort_result = rows <= std::numeric_limits<std::uint32_t>::max()
                                   ? build.template operator()<std::uint32_t>()
                                   : build.template operator()<std::uint64_t>();
            return std::visit(
                [&]<typename Idx>(
                    const std::vector<Idx>& idx) -> std::expected<Table, std::string> {
                    return gather_rows_parallel(input, idx, &resolved_keys, exec);
                },
                sort_result);
        }
    }

    // Fast path: single ascending I64 key — radix sort (pre-sorted case already handled above).
    if (!has_null_keys && flat_keys.size() == 1 && flat_keys[0].kind == FlatKind::I64 &&
        flat_keys[0].ascending) {
        auto sort_result = radix_sort_u64_asc(std::move(flat_keys[0].u64), rows);
        return std::visit(
            [&]<typename Idx>(const std::vector<Idx>& idx) -> std::expected<Table, std::string> {
                return gather_rows_parallel(input, idx, &resolved_keys, exec);
            },
            sort_result);
    }

    // Fast path: single ascending F64 key — map each double to an order-preserving
    // uint64 and radix sort, avoiding the comparison-based stable_sort.
    if (flat_keys.size() == 1 && flat_keys[0].kind == FlatKind::F64 && flat_keys[0].ascending) {
        std::vector<std::uint64_t> radix_keys(rows);
        const auto& f = flat_keys[0].f64;
        for (std::size_t i = 0; i < rows; ++i)
            radix_keys[i] = double_to_sortable_u64(f[i]);
        auto sort_result = radix_sort_u64_asc(std::move(radix_keys), rows);
        return std::visit(
            [&]<typename Idx>(const std::vector<Idx>& idx) -> std::expected<Table, std::string> {
                return gather_rows_parallel(input, idx, &resolved_keys, exec);
            },
            sort_result);
    }

    // Ordinal-encode a string column to order-preserving u64 codes: dedup via
    // hash (O(rows)), sort the distinct values, map each row to its sorted rank.
    // Codes preserve string order, so radix on them == lexicographic sort.
    // Returns nullopt once the distinct count exceeds `cap` (bailing immediately,
    // so a high-cardinality reject is cheap) — the caller then prefers a
    // comparison sort, where the distinct-sort would cost as much as sorting all
    // rows anyway.
    auto ordinal_encode = [rows](const std::vector<std::string_view>& vals,
                                 std::size_t cap) -> std::optional<std::vector<std::uint64_t>> {
        robin_hood::unordered_map<std::string_view, std::uint64_t> code_of;
        std::vector<std::string_view> distinct;
        for (auto sv : vals) {
            if (code_of.emplace(sv, 0).second) {
                distinct.push_back(sv);
                if (distinct.size() > cap)
                    return std::nullopt;
            }
        }
        std::ranges::sort(distinct);
        for (std::uint64_t r = 0; r < distinct.size(); ++r)
            code_of[distinct[r]] = r;
        std::vector<std::uint64_t> code(rows);
        for (std::size_t i = 0; i < rows; ++i)
            code[i] = code_of[vals[i]];
        return code;
    };

    auto radix_gather =
        [&](std::vector<std::vector<std::uint64_t>>& codes) -> std::expected<Table, std::string> {
        if (rows <= std::numeric_limits<std::uint32_t>::max()) {
            auto idx = lsd_multi_radix<std::uint32_t>(codes, rows);
            return gather_rows_parallel(input, idx, &resolved_keys, exec);
        }
        auto idx = lsd_multi_radix<std::uint64_t>(codes, rows);
        return gather_rows_parallel(input, idx, &resolved_keys, exec);
    };

    // Invert order-preserving codes for a descending key so an ascending radix
    // yields descending order (equal codes stay equal → still stable).
    auto apply_descending = [](std::vector<std::uint64_t>& code, bool ascending) {
        if (!ascending)
            for (auto& c : code)
                c = ~c;
    };

    // Radix path for multi-key sorts and single descending numeric keys. Map each
    // key to an order-preserving u64 (strings via ordinal encoding, unconditional
    // here since the alternative for multi-key is itself a slow comparison sort)
    // and LSD-radix from the least- to the most-significant key. Single ascending
    // numeric keys already returned via the fast paths above.
    const bool use_radix_multi =
        !has_null_keys &&
        (flat_keys.size() >= 2 || (flat_keys.size() == 1 && flat_keys[0].kind != FlatKind::Str));
    if (use_radix_multi) {
        std::vector<std::vector<std::uint64_t>> codes;
        codes.reserve(flat_keys.size());
        for (auto& fk : flat_keys) {
            std::vector<std::uint64_t> code;
            switch (fk.kind) {
                case FlatKind::I64:
                    code = std::move(fk.u64);  // already sign-flipped to order-preserving u64
                    break;
                case FlatKind::F64:
                    code.resize(rows);
                    for (std::size_t i = 0; i < rows; ++i)
                        code[i] = double_to_sortable_u64(fk.f64[i]);
                    break;
                case FlatKind::Str:
                    code =
                        std::move(*ordinal_encode(fk.str, std::numeric_limits<std::size_t>::max()));
                    break;
            }
            apply_descending(code, fk.ascending);
            codes.push_back(std::move(code));
        }
        return radix_gather(codes);
    }

    // Single lone string key: ordinal-encode + radix when the column is low
    // cardinality (categorical/dictionary-like, the common case), where the
    // distinct-sort is far cheaper than sorting every row. High-cardinality
    // columns exceed the cap and fall through to the comparison sort below.
    if (!has_null_keys && flat_keys.size() == 1 && flat_keys[0].kind == FlatKind::Str) {
        constexpr std::size_t kOrdinalCap = std::size_t{1} << 16;
        if (auto code = ordinal_encode(flat_keys[0].str, kOrdinalCap)) {
            apply_descending(*code, flat_keys[0].ascending);
            std::vector<std::vector<std::uint64_t>> codes;
            codes.push_back(std::move(*code));
            return radix_gather(codes);
        }
    }

    // General path: a lone high-cardinality string key — comparison-based sort.
    // pdqsort is unstable, but the comparator's `lhs < rhs` tiebreak makes the
    // order total (no real ties), so the result matches a stable sort.
    auto compare_row = [&](std::size_t lhs, std::size_t rhs) -> bool {
        for (const auto& fk : flat_keys) {
            // Null-ness outranks value and ignores `ascending`: a non-null always
            // precedes a null, so nulls land last on asc and desc alike.
            const bool lhs_null = fk.is_null(lhs);
            const bool rhs_null = fk.is_null(rhs);
            if (lhs_null != rhs_null) {
                return rhs_null;
            }
            if (lhs_null) {
                continue;  // both null on this key — tie, fall through to the next
            }
            switch (fk.kind) {
                case FlatKind::I64: {
                    auto l = fk.u64[lhs];
                    auto r = fk.u64[rhs];
                    if (l != r)
                        return fk.ascending ? (l < r) : (l > r);
                    break;
                }
                case FlatKind::F64: {
                    auto l = fk.f64[lhs];
                    auto r = fk.f64[rhs];
                    if (l != r)
                        return fk.ascending ? (l < r) : (l > r);
                    break;
                }
                case FlatKind::Str: {
                    auto l = fk.str[lhs];
                    auto r = fk.str[rhs];
                    if (l != r)
                        return fk.ascending ? (l < r) : (l > r);
                    break;
                }
            }
        }
        return lhs < rhs;
    };
    std::vector<std::size_t> idx(rows);
    std::iota(idx.begin(), idx.end(), std::size_t{0});
    pdqsort(idx.begin(), idx.end(), compare_row);
    return gather_rows_parallel(input, idx, &resolved_keys, exec);
}

}  // namespace

auto permute_table_rows(const Table& input, const std::vector<std::size_t>& perm,
                        std::vector<ir::OrderKey> ordering, const ExecutionContext& exec) -> Table {
    Table output = gather_rows_parallel(input, perm, nullptr, exec);
    output.set_properties(input.properties().with_ordering(std::move(ordering)));
    return output;
}

auto order_table(const Table& input, const std::vector<ir::OrderKey>& keys,
                 const ExecutionContext& exec) -> std::expected<Table, std::string> {
    auto resolved_keys = ordering_keys_for_table(input, keys);
    // A TimeFrame is time-sorted by construction. Ordering it purely by the time
    // index (ascending) preserves that. Ordering by any other key reshuffles the
    // rows — permitted, e.g. sorting resampled OHLC bars by symbol — but the time
    // index is appended as an implicit final tiebreaker so each leading-key group
    // stays time-ascending (keeping grouped rolling/window correct) and the
    // TimeFrame designation is kept. normalize_time_index (inside gather_rows)
    // resets `ordering` to time-only, so the true multi-key order is restored
    // here after the sort.
    bool relaxed_timeframe = false;
    std::vector<ir::OrderKey> ordering_out;  // empty = same as the keys we sort by
    if (input.time_index().has_value()) {
        const bool time_only =
            keys.size() == 1 && keys[0].name == *input.time_index() && keys[0].ascending;
        if (!time_only) {
            if (keys.empty()) {
                return std::unexpected("order on TimeFrame must be by time index ascending");
            }
            relaxed_timeframe = true;
            if (std::ranges::none_of(resolved_keys, [&](const ir::OrderKey& k) {
                    return k.name == *input.time_index();
                })) {
                const ir::OrderKey time_key{.name = *input.time_index(), .ascending = true};
                // The resulting order IS (leading keys..., time) either way — that
                // is what the metadata below records.
                ordering_out = resolved_keys;
                ordering_out.push_back(time_key);
                // But actually SORTING by the time index is only necessary when
                // the input is not already time-ascending. Every path in
                // `order_table_resolved` is stable, so a stable sort by the
                // leading keys already leaves each group in its original — and
                // therefore time-ascending — order.
                //
                // A TimeFrame is time-sorted by construction, so this nearly
                // always holds; it is verified rather than assumed because
                // getting it wrong would silently misorder rows within a group.
                // Skipping it turns `order symbol` on a TimeFrame from a two-key
                // radix — including a full 64-bit pass over every timestamp —
                // into a single-key sort, which was half the cost of the sort.
                if (!column_is_non_decreasing(input, *input.time_index())) {
                    resolved_keys.push_back(time_key);
                }
            }
        }
    }
    auto result = order_table_resolved(input, resolved_keys, exec);
    if (result.has_value() && relaxed_timeframe) {
        result->set_properties(input.properties().with_ordering(
            ordering_out.empty() ? std::move(resolved_keys) : std::move(ordering_out)));
    }
    return result;
}

auto head_table(const Table& input, std::size_t count, const std::vector<ir::ColumnRef>& group_by)
    -> std::expected<Table, std::string> {
    if (count == 0) {
        Table output;
        for (const auto& entry : input.columns) {
            output.add_column(entry.name, make_empty_like(*entry.column));
        }
        output.set_properties(input.properties());
        return output;
    }

    const std::size_t rows = input.rows();
    if (rows <= count && group_by.empty()) {
        Table output = input;
        normalize_time_index(output);
        return output;
    }

    if (group_by.empty()) {
        std::vector<std::size_t> idx(std::min(rows, count));
        std::iota(idx.begin(), idx.end(), std::size_t{0});
        return gather_rows(input, idx);
    }

    robin_hood::unordered_flat_map<Key, std::size_t, KeyHash, KeyEq> seen_counts;
    seen_counts.reserve(rows);
    std::vector<std::size_t> idx;
    idx.reserve(
        std::min(rows, count * std::max<std::size_t>(1, rows / std::max<std::size_t>(1, count))));

    if (group_by.size() > kMaxKeyColumns) {
        return std::unexpected("head: at most " + std::to_string(kMaxKeyColumns) +
                               " group-by columns");
    }
    for (std::size_t row = 0; row < rows; ++row) {
        Key key;
        key.values.reserve(group_by.size());
        for (const auto& ref : group_by) {
            const auto* entry = input.find_entry(ref.name);
            if (entry == nullptr) {
                return std::unexpected("head group-by column not found: " + ref.name +
                                       " (available: " + format_columns(input) + ")");
            }
            // Through the shared builder, so the null bit travels with the
            // value: a null cell holds its type's zero, and pushing the raw
            // scalar merges a null key into the zero group.
            push_key_value(key, *entry, row);
        }
        auto& seen = seen_counts[key];
        if (seen >= count) {
            continue;
        }
        ++seen;
        idx.push_back(row);
    }

    return gather_rows(input, idx);
}

auto tail_table(const Table& input, std::size_t count, const std::vector<ir::ColumnRef>& group_by)
    -> std::expected<Table, std::string> {
    if (count == 0) {
        Table output;
        for (const auto& entry : input.columns) {
            output.add_column(entry.name, make_empty_like(*entry.column));
        }
        output.set_properties(input.properties());
        return output;
    }

    const std::size_t rows = input.rows();
    if (rows <= count && group_by.empty()) {
        Table output = input;
        normalize_time_index(output);
        return output;
    }

    if (group_by.empty()) {
        const std::size_t keep = std::min(rows, count);
        std::vector<std::size_t> idx(keep);
        const std::size_t start = rows - keep;
        std::iota(idx.begin(), idx.end(), start);
        return gather_rows(input, idx);
    }

    robin_hood::unordered_flat_map<Key, std::vector<std::size_t>, KeyHash, KeyEq> groups;
    groups.reserve(rows);
    std::vector<Key> order;
    order.reserve(rows);

    if (group_by.size() > kMaxKeyColumns) {
        return std::unexpected("tail: at most " + std::to_string(kMaxKeyColumns) +
                               " group-by columns");
    }
    for (std::size_t row = 0; row < rows; ++row) {
        Key key;
        key.values.reserve(group_by.size());
        for (const auto& ref : group_by) {
            const auto* entry = input.find_entry(ref.name);
            if (entry == nullptr) {
                return std::unexpected("tail group-by column not found: " + ref.name +
                                       " (available: " + format_columns(input) + ")");
            }
            push_key_value(key, *entry, row);
        }
        auto [it, inserted] = groups.try_emplace(key);
        if (inserted) {
            order.push_back(key);
        }
        it->second.push_back(row);
    }

    std::vector<std::size_t> idx;
    idx.reserve(rows);
    for (const auto& key : order) {
        const auto& group_rows = groups.find(key)->second;
        const std::size_t keep = std::min(group_rows.size(), count);
        const std::size_t start = group_rows.size() - keep;
        idx.insert(idx.end(), group_rows.begin() + static_cast<std::ptrdiff_t>(start),
                   group_rows.end());
    }

    return gather_rows(input, idx);
}

}  // namespace ibex::runtime
