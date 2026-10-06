// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

// sort.cpp — ordering and row-selection: LSD radix sort machinery,
// order_table (single- and multi-key, pre-sorted fast path), and grouped
// head/tail selection.
// Split out of interpreter.cpp; shared declarations live in interpreter_internal.hpp.

#include <ibex/core/column.hpp>
#include <ibex/core/decimal.hpp>
#include <ibex/core/time.hpp>
#include <ibex/ir/node.hpp>
#include <ibex/runtime/interpreter.hpp>
#include <ibex/runtime/worker_pool.hpp>

#include <algorithm>
#include <array>
#include <atomic>
#include <bit>
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

void radix_sort_by_key_serial(std::vector<std::uint64_t> src_keys, std::vector<Idx>& idx,
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

/// Below this many rows the MSD split's 65536-bucket histogram costs more
/// than the LSD passes it saves.
constexpr std::size_t kParallelRadixMinRows = std::size_t{1} << 16;

/// Run `body(t, begin, end)` over `workers` fixed, contiguous chunks of
/// `[0, rows)`, chunk `t` on worker `t`; inline when `workers` is 1. A radix
/// pass needs the same chunks when it counts as when it scatters, which a
/// dynamically handed-out range cannot promise.
template <typename Body>
void for_sort_chunks(std::size_t workers, std::size_t rows, const Body& body) {
    if (workers < 2) {
        body(std::size_t{0}, std::size_t{0}, rows);
        return;
    }
    auto batch = process_worker_pool().submit(
        workers, [&](std::size_t t) { body(t, rows * t / workers, rows * (t + 1) / workers); });
    batch.wait();
}

/// `idx[i] = i` for every row, across `workers`.
template <typename Idx>
void fill_identity(std::vector<Idx>& idx, std::size_t workers) {
    for_sort_chunks(workers, idx.size(), [&](std::size_t, std::size_t begin, std::size_t end) {
        // NOLINTNEXTLINE(modernize-use-ranges): Apple libc++ does not provide ranges::iota.
        std::iota(idx.begin() + static_cast<std::ptrdiff_t>(begin),
                  idx.begin() + static_cast<std::ptrdiff_t>(end), static_cast<Idx>(begin));
    });
}

/// Stably sort one bucket of an MSD split, already in place in `keys`/`idx`,
/// by its low `bits` key bits (the bits above are equal across the bucket).
///
/// A short bucket is insertion-sorted and a cache-sized one takes LSD byte
/// passes, skipping the bytes it does not vary in. A larger one is split
/// again on its top bits and each part sorted the same way, so no bucket is
/// ever scattered whole more than once per level: a skewed key (a Float64's
/// exponent puts most rows in a few top-level buckets) costs a level, not a
/// pass over a large bucket per byte. The split is stable, so the sort is.
template <typename Idx>
void sort_msd_bucket(std::uint64_t* keys, Idx* idx, std::size_t n, unsigned bits,
                     std::vector<std::uint64_t>& scratch_keys, std::vector<Idx>& scratch_idx) {
    if (n < 2 || bits == 0) {
        return;
    }
    constexpr std::size_t kInsertionMax = 32;
    if (n <= kInsertionMax) {
        for (std::size_t i = 1; i < n; ++i) {
            const std::uint64_t k = keys[i];
            const Idx v = idx[i];
            std::size_t j = i;
            while (j > 0 && keys[j - 1] > k) {
                keys[j] = keys[j - 1];
                idx[j] = idx[j - 1];
                --j;
            }
            keys[j] = k;
            idx[j] = v;
        }
        return;
    }
    scratch_keys.resize(std::max(scratch_keys.size(), n));
    scratch_idx.resize(std::max(scratch_idx.size(), n));

    constexpr std::size_t kLsdMax = std::size_t{1} << 12;
    if (n > kLsdMax && bits > 8) {
        constexpr unsigned kSplitBits = 8;
        const unsigned rest = bits - kSplitBits;
        std::array<std::size_t, 257> start{};
        for (std::size_t i = 0; i < n; ++i) {
            ++start[((keys[i] >> rest) & 0xFFU) + 1];
        }
        for (std::size_t d = 1; d <= 256; ++d) {
            start[d] += start[d - 1];
        }
        std::array<std::size_t, 256> next;  // NOLINT(cppcoreguidelines-pro-type-member-init)
        std::copy_n(start.begin(), 256, next.begin());
        for (std::size_t i = 0; i < n; ++i) {
            const std::size_t at = next[(keys[i] >> rest) & 0xFFU]++;
            scratch_keys[at] = keys[i];
            scratch_idx[at] = idx[i];
        }
        std::copy_n(scratch_keys.data(), n, keys);
        std::copy_n(scratch_idx.data(), n, idx);
        for (std::size_t d = 0; d < 256; ++d) {
            sort_msd_bucket(keys + start[d], idx + start[d], start[d + 1] - start[d], rest,
                            scratch_keys, scratch_idx);
        }
        return;
    }

    const unsigned passes = (bits + 7U) / 8U;
    std::array<std::array<std::size_t, 256>, 8> hists{};
    for (std::size_t i = 0; i < n; ++i) {
        const auto k = keys[i];
        for (unsigned p = 0; p < passes; ++p) {
            ++hists[p][(k >> (p * 8U)) & 0xFFU];
        }
    }
    std::uint64_t* src_k = keys;
    std::uint64_t* dst_k = scratch_keys.data();
    Idx* src_i = idx;
    Idx* dst_i = scratch_idx.data();
    std::array<std::size_t, 256> cnt;  // NOLINT(cppcoreguidelines-pro-type-member-init)
    for (unsigned pass = 0; pass < passes; ++pass) {
        const auto& h = hists[pass];
        std::size_t non_zero = 0;
        for (const auto c : h) {
            non_zero += c != 0 ? 1U : 0U;
        }
        if (non_zero <= 1) {
            continue;
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

/// Stable radix sort of `idx` by `src_keys` that touches main memory about
/// twice instead of once per varying key byte.
///
/// The LSD sort scatters the whole array once per byte, and on a random
/// Float64 key all eight bytes vary: eight passes over memory, which is
/// bandwidth, not cores, and so did not get faster with more workers. Here
/// one scatter on the top 16 bits that vary splits the rows into up to 65536
/// buckets, each small enough to sort in cache, and the buckets are then
/// sorted independently.
///
/// The scatter is split into `workers` fixed, contiguous chunks: each chunk
/// takes the slots of bucket `b` after every earlier chunk's, so equal digits
/// keep their input order, and each bucket's sort is stable. A heavily skewed
/// key (most rows in one bucket) still sorts correctly, with that bucket on a
/// single worker.
template <typename Idx>
void radix_sort_by_key_msd(std::vector<std::uint64_t> src_keys, std::vector<Idx>& idx,
                           std::size_t rows, std::size_t workers) {
    constexpr unsigned kDigitBits = 16;
    const auto run_chunks = [&](const auto& body) { for_sort_chunks(workers, rows, body); };

    // The bits that vary: every bit above the highest one is shared by all rows.
    std::vector<std::uint64_t> chunk_diff(workers, 0);
    const std::uint64_t first = src_keys[0];
    run_chunks([&](std::size_t t, std::size_t begin, std::size_t end) {
        std::uint64_t diff = 0;
        for (std::size_t i = begin; i < end; ++i) {
            diff |= src_keys[i] ^ first;
        }
        chunk_diff[t] = diff;
    });
    std::uint64_t diff = 0;
    for (const auto d : chunk_diff) {
        diff |= d;
    }
    if (diff == 0) {
        return;  // every key equal: the input order is the stable order
    }
    const auto varying = static_cast<unsigned>(std::bit_width(diff));
    const unsigned digit_bits = std::min(kDigitBits, varying);
    const unsigned low_bits = varying - digit_bits;
    const std::uint64_t digit_mask = (std::uint64_t{1} << digit_bits) - 1;
    const auto digit = [&](std::uint64_t k) {
        return static_cast<std::size_t>((k >> low_bits) & digit_mask);
    };
    const std::size_t buckets = std::size_t{1} << digit_bits;

    // Counts per chunk, then bucket-major, chunk-minor offsets.
    std::vector<std::size_t> pos(workers * buckets, 0);
    run_chunks([&](std::size_t t, std::size_t begin, std::size_t end) {
        std::size_t* counts = pos.data() + (t * buckets);
        for (std::size_t i = begin; i < end; ++i) {
            ++counts[digit(src_keys[i])];
        }
    });
    std::vector<std::size_t> bucket_begin(buckets + 1, 0);
    std::size_t total = 0;
    for (std::size_t b = 0; b < buckets; ++b) {
        bucket_begin[b] = total;
        for (std::size_t t = 0; t < workers; ++t) {
            const std::size_t c = pos[(t * buckets) + b];
            pos[(t * buckets) + b] = total;
            total += c;
        }
    }
    bucket_begin[buckets] = total;

    ::ibex::detail::NoInitVector<std::uint64_t> keys;
    keys.resize(rows);
    ::ibex::detail::NoInitVector<Idx> out;
    out.resize(rows);
    run_chunks([&](std::size_t t, std::size_t begin, std::size_t end) {
        std::size_t* next = pos.data() + (t * buckets);
        for (std::size_t i = begin; i < end; ++i) {
            const std::size_t at = next[digit(src_keys[i])]++;
            keys[at] = src_keys[i];
            out[at] = idx[i];
        }
    });
    src_keys = {};

    // Sort the buckets, in tasks of adjacent buckets holding about the same
    // number of rows: a skewed key packs most rows into a few neighbouring
    // buckets, and tasks of a fixed bucket count left one worker with them.
    // Each worker keeps its scratch across its tasks.
    const std::size_t task_rows = std::max<std::size_t>(1, rows / (workers * 16));
    std::vector<std::size_t> task_begin{0};
    for (std::size_t b = 0; b < buckets; ++b) {
        if (bucket_begin[b + 1] - bucket_begin[task_begin.back()] >= task_rows) {
            task_begin.push_back(b + 1);
        }
    }
    if (task_begin.back() != buckets) {
        task_begin.push_back(buckets);
    }
    const std::size_t tasks = task_begin.size() - 1;
    std::atomic<std::size_t> cursor{0};
    const auto sort_tasks = [&](std::size_t) {
        std::vector<std::uint64_t> scratch_keys;
        std::vector<Idx> scratch_idx;
        for (std::size_t task = cursor.fetch_add(1, std::memory_order_relaxed); task < tasks;
             task = cursor.fetch_add(1, std::memory_order_relaxed)) {
            for (std::size_t b = task_begin[task]; b < task_begin[task + 1]; ++b) {
                const std::size_t lo = bucket_begin[b];
                sort_msd_bucket(keys.data() + lo, out.data() + lo, bucket_begin[b + 1] - lo,
                                low_bits, scratch_keys, scratch_idx);
            }
        }
    };
    if (workers < 2) {
        sort_tasks(0);
    } else {
        auto batch = process_worker_pool().submit(workers, sort_tasks);
        batch.wait();
    }
    run_chunks([&](std::size_t, std::size_t begin, std::size_t end) {
        std::copy(out.data() + begin, out.data() + end, idx.data() + begin);
    });
}

/// Stable radix sort of `idx` by `src_keys`. A small input takes the plain
/// LSD sort; a large one the cache-friendly MSD split, across `workers`.
template <typename Idx>
void radix_sort_by_key(std::vector<std::uint64_t> src_keys, std::vector<Idx>& idx, std::size_t rows,
                       std::size_t workers) {
    if (rows < kParallelRadixMinRows) {
        radix_sort_by_key_serial(std::move(src_keys), idx, rows);
        return;
    }
    radix_sort_by_key_msd(std::move(src_keys), idx, rows, std::max<std::size_t>(1, workers));
}

}  // namespace

namespace {

template <typename Idx>

auto radix_sort_impl(std::vector<std::uint64_t> src_keys, std::size_t rows, std::size_t workers)
    -> std::vector<Idx> {
    std::vector<Idx> idx(rows);
    fill_identity(idx, workers);
    radix_sort_by_key(std::move(src_keys), idx, rows, workers);
    return idx;
}

}  // namespace

/// Workers for a barrier operator whose unit of work is a GROUP — rank's sweep
/// and per-group sorts, the collect-aggregate reduce. Sized on ROW COUNT, so
/// the split, and therefore nothing about the answer, depends on the pool.
/// More workers than groups is pointless; each caller checks its own group
/// count, which this cannot see.
auto group_barrier_worker_count(const ExecutionContext& exec, std::size_t rows) -> std::size_t {
    if (on_worker_pool_thread() || !exec.can_fan_out() || rows < exec.parallel_min_rows) {
        return 0;
    }
    const std::size_t pool_size = process_worker_pool().size();
    const std::size_t budget = exec.compute_budget();
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
auto radix_sort_u64_asc(std::vector<std::uint64_t> keys, std::size_t rows, std::size_t workers)
    -> SortIdx {
    if (rows <= std::numeric_limits<std::uint32_t>::max())
        return radix_sort_impl<std::uint32_t>(std::move(keys), rows, workers);
    return radix_sort_impl<std::uint64_t>(std::move(keys), rows, workers);
}

// Stable multi-key sort by LSD radix: `codes[k]` holds one order-preserving u64
// per row for sort key k (key 0 most significant). Sorts least- to most-
// significant key, each pass stable, so the result equals a stable comparison
// sort on the same keys with ties broken by original row order. Each key is
// gathered into the current index order first so the radix scatter reads
// sequentially.
namespace {

template <typename Idx>

auto lsd_multi_radix(const std::vector<std::vector<std::uint64_t>>& codes, std::size_t rows,
                     const ExecutionContext& exec, std::size_t workers) -> std::vector<Idx> {
    std::vector<Idx> idx(rows);
    fill_identity(idx, workers);
    for (std::size_t k = codes.size(); k-- > 0;) {
        const auto& code = codes[k];
        std::vector<std::uint64_t> gathered(rows);
        for_row_ranges(&exec, rows, [&](std::size_t begin, std::size_t end) {
            for (std::size_t i = begin; i < end; ++i)
                gathered[i] = code[static_cast<std::size_t>(idx[i])];  // Index is below rows.
        });
        radix_sort_by_key(std::move(gathered), idx, rows, workers);
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
    const std::size_t budget = exec.compute_budget();
    const std::size_t threads = std::min(budget, pool_size);
    const bool worth_it =
        exec.can_fan_out() && !on_worker_pool_thread() && threads >= 2 && n_cols != 0 &&
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
        auto batch = process_worker_pool().submit(threads, [&](std::size_t) {
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
    // Workers for the radix passes; 1 means serial. Sized on rows, so the
    // permutation, which is stable, cannot depend on it.
    const std::size_t sort_workers =
        std::max<std::size_t>(1, group_barrier_worker_count(exec, rows));
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
        if (const auto* decimal_col = std::get_if<Column<Decimal>>(column)) {
            const Decimal* values = decimal_col->data();
            const bool fits64 = std::all_of(values, values + rows, [](const Decimal& value) {
                return value.units >= Int128{INT64_MIN} && value.units <= Int128{INT64_MAX};
            });
            if (fits64) {
                fk.kind = FlatKind::I64;
                fk.u64 = decimal_order_keys(*decimal_col, rows);
                flat_keys.push_back(std::move(fk));
            } else {
                // Encode the signed 128-bit unit count as two order-preserving
                // u64 keys. Splitting after flipping the sign bit preserves
                // full signed order; the existing stable multi-key radix sorts
                // the low half first and the high half second. Dense ordinal
                // ranking sorted all rows once before sorting them again.
                constexpr Int128 kWord = Int128{1} << 64;
                FlatKey high;
                high.kind = FlatKind::I64;
                high.ascending = key.ascending;
                high.validity = fk.validity;
                high.u64.reserve(rows);
                fk.u64.reserve(rows);
                for (const Decimal& value : *decimal_col) {
                    Int128 upper = value.units / kWord;
                    const Int128 lower = value.units % kWord;
                    if (lower < 0) {
                        --upper;
                    }
                    high.u64.push_back(static_cast<std::uint64_t>(upper) ^ kSignFlip);
                    fk.u64.push_back(static_cast<std::uint64_t>(value.units));
                }
                // Both halves must be complemented for descending order.
                flat_keys.push_back(std::move(high));
                flat_keys.push_back(std::move(fk));
            }
            continue;
        }
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
                } else if constexpr (std::is_same_v<ColT, Column<Decimal>>) {
                    // Units are exact within a column (one scale), so their
                    // order is the value order; see decimal_order_keys for
                    // how a >64-bit unit keeps that exact.
                    fk.kind = FlatKind::I64;
                    fk.u64 = decimal_order_keys(col, rows);
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
                    // NOLINTNEXTLINE(modernize-use-ranges): Because Apple libc++
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
                ++position[static_cast<std::size_t>(keys[i] ^ kSignFlip) + 1];
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
                    idx[position[static_cast<std::size_t>(keys[i] ^ kSignFlip)]++] =
                        static_cast<Idx>(i);
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
        auto sort_result = radix_sort_u64_asc(std::move(flat_keys[0].u64), rows, sort_workers);
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
        for_row_ranges(&exec, rows, [&](std::size_t begin, std::size_t end) {
            for (std::size_t i = begin; i < end; ++i)
                radix_keys[i] = double_to_sortable_u64(f[i]);
        });
        auto sort_result = radix_sort_u64_asc(std::move(radix_keys), rows, sort_workers);
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
        for (std::size_t r = 0; r < distinct.size(); ++r)
            code_of[distinct[r]] = r;
        std::vector<std::uint64_t> code(rows);
        for (std::size_t i = 0; i < rows; ++i)
            code[i] = code_of[vals[i]];
        return code;
    };

    auto radix_gather =
        [&](std::vector<std::vector<std::uint64_t>>& codes) -> std::expected<Table, std::string> {
        if (rows <= std::numeric_limits<std::uint32_t>::max()) {
            auto idx = lsd_multi_radix<std::uint32_t>(codes, rows, exec, sort_workers);
            return gather_rows_parallel(input, idx, &resolved_keys, exec);
        }
        auto idx = lsd_multi_radix<std::uint64_t>(codes, rows, exec, sort_workers);
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
                    for_row_ranges(&exec, rows, [&](std::size_t begin, std::size_t end) {
                        for (std::size_t i = begin; i < end; ++i)
                            code[i] = double_to_sortable_u64(fk.f64[i]);
                    });
                    break;
                case FlatKind::Str: {
                    // Uncapped: the encode only bails when the distinct count
                    // EXCEEDS the cap, which a size_t count cannot do at
                    // SIZE_MAX. Multi-key has no comparison fallback worth
                    // taking, so the encoding is unconditional here; the guard
                    // states that invariant rather than assuming it.
                    auto encoded = ordinal_encode(fk.str, std::numeric_limits<std::size_t>::max());
                    if (!encoded.has_value()) {
                        return std::unexpected(
                            "sort: ordinal encoding of string key exceeded "
                            "the distinct-value limit");
                    }
                    code = std::move(*encoded);
                    break;
                }
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
    // NOLINTNEXTLINE(modernize-use-ranges): Apple libc++ does not provide ranges::iota.
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
        // NOLINTNEXTLINE(modernize-use-ranges): Apple libc++ does not provide ranges::iota.
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
        // NOLINTNEXTLINE(modernize-use-ranges): Apple libc++ does not provide ranges::iota.
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
