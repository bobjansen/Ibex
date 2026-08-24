// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#pragma once

#include <ibex/runtime/interpreter.hpp>
#include <ibex/runtime/table_properties.hpp>
#include <ibex/runtime/worker_pool.hpp>

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <cstring>
#include <functional>
#include <optional>
#include <span>
#include <stdexcept>
#include <string>
#include <string_view>
#include <type_traits>
#include <vector>

namespace ibex::runtime {

enum class ExprType : std::uint8_t {
    Int,
    Double,
    Bool,
    String,
    Categorical,
    Date,
    Timestamp,
};

struct StringViewHash {
    using is_transparent = void;
    auto operator()(std::string_view sv) const noexcept -> std::size_t {
        return std::hash<std::string_view>{}(sv);
    }
};

struct StringViewEq {
    using is_transparent = void;
    auto operator()(std::string_view a, std::string_view b) const noexcept -> bool {
        return a == b;
    }
};

struct Mask {
    /// One byte per row, and every filter kernel writes all of them — so the
    /// buffer is left uninitialized rather than filled first. The fill is a
    /// whole extra pass over the mask against a predicate that is often a
    /// single compare, which measured 5-23% on the filter suite. Any new
    /// producer must write every slot: an unwritten byte is garbage, and
    /// garbage is usually truthy, so it selects a row it should not.
    ibex::detail::NoInitVector<uint8_t> value;
    std::optional<std::vector<uint8_t>> valid;  // nullopt = all rows valid

    /// Adopt `v` as this mask's 3VL validity. `off` is the source offset — the
    /// mask itself is always dense, so row `i` reads `(*v)[off + i]`.
    // NOLINTNEXTLINE(bugprone-easily-swappable-parameters) — source offset, then row count
    void apply_validity(const ValidityBitmap* v, std::size_t off, std::size_t n) {
        if (v == nullptr) {
            return;
        }
        valid.emplace(n, uint8_t{1});
        for (std::size_t i = 0; i < n; ++i) {
            (*valid)[i] = static_cast<uint8_t>((*v)[off + i]);
        }
    }
};

[[nodiscard]] auto is_simple_identifier(std::string_view name) -> bool;
[[nodiscard]] auto format_columns(const Table& table) -> std::string;
/// Re-establish the TimeFrame ordering invariant: a table with a time index and
/// no grouping is ordered by that index ascending. A group-major table (any
/// `by` upstream, recorded in `grouped_by`) is left alone -- its ordering is the
/// operator's to state. Callers must therefore set `grouped_by` BEFORE calling
/// this, which `apply_table_properties` does by construction.
auto normalize_time_index(Table& table) -> void;

/// Extract the order-sensitive metadata from a materialized table, so it can be
/// fed to `TableProperties::derive`.
[[nodiscard]] auto table_properties_of(const Table& table) -> TableProperties;

/// Write derived `props` onto `table` (setting or clearing `ordering` /
/// `time_index`) and re-establish the TimeFrame invariant via
/// `normalize_time_index`. The single place metadata lands on a table, so the
/// serial operators and the merger apply it identically — an operator that
/// assigns `table.ordering()` / `table.time_index()` / `table.grouped_by()` directly
/// is bypassing every rule in `TableProperties` and will drift from them.
auto apply_table_properties(Table& table, const TableProperties& props) -> void;
[[nodiscard]] auto int64_to_date_checked(std::int64_t value) -> Date;
[[nodiscard]] auto scalar_from_column(const ColumnValue& column, std::size_t row) -> ScalarValue;
[[nodiscard]] auto column_kind(const ColumnValue& column) -> ExprType;

inline auto append_value(ColumnValue& out, const ColumnValue& src, std::size_t index) -> void {
    std::visit(
        [&](auto& dst_col) {
            using ColType = std::decay_t<decltype(dst_col)>;
            const auto* src_col = std::get_if<ColType>(&src);
            if (src_col == nullptr) {
                throw std::runtime_error("append_value: source/destination column type mismatch");
            }
            if constexpr (std::is_same_v<ColType, Column<Categorical>>) {
                dst_col.push_code(src_col->code_at(index));
            } else {
                dst_col.push_back((*src_col)[index]);
            }
        },
        out);
}

/// Run `body(begin, end)` over `n` rows, across workers when that is worth it.
///
/// The unit of work is a row range and every range writes a disjoint slice of
/// an already-sized output, so there is no merge and the result is identical to
/// running the ranges in order — which is what lets a gather be threaded without
/// changing a single byte of its answer.
/// The row floor is `exec.parallel_min_rows` — the same knob the rest of the
/// engine gates on, rather than a private constant a test could not reach.
///
/// **Range boundaries are aligned to 64 rows.** That is what makes a bit-packed
/// destination — `Column<bool>`, a validity bitmap — safe to write from several
/// ranges at once: each 64-row word then belongs to exactly one range. The
/// alignment is the same rule `gather_range_into` documents and the sort's
/// (column x range) tasks obey, and it costs only a rounding of the grain. The
/// alternative this replaced was leaving bit-packed columns serial, which
/// answered the same question differently in the same engine.
template <typename Body>
void for_row_ranges(const ExecutionContext* exec, std::size_t n, Body&& body) {
    constexpr std::size_t kMaxRanges = 64;
    constexpr std::size_t kAlign = 64;
    std::size_t ranges = 1;
    if (exec != nullptr && exec->parallel && n >= exec->parallel_min_rows &&
        !on_worker_pool_thread()) {
        const std::size_t min_rows = std::max<std::size_t>(exec->parallel_min_rows, 1);
        auto& pool = process_worker_pool();
        const std::size_t budget = exec->compute_budget();
        ranges = std::clamp<std::size_t>(n / min_rows, 1, kMaxRanges);
        ranges = std::min({ranges, budget, pool.size()});
    }
    if (ranges < 2) {
        body(std::size_t{0}, n);
        return;
    }
    // Round the grain up to a whole number of 64-row words. Rounding UP (rather
    // than down) keeps the range count at or below `ranges`, so no range is left
    // unvisited by the fixed-size batch below.
    const std::size_t grain = (((n + ranges - 1) / ranges) + kAlign - 1) / kAlign * kAlign;
    std::atomic<std::size_t> cursor{0};
    auto batch = process_worker_pool().submit(ranges, [&](std::size_t) {
        while (true) {
            const std::size_t r = cursor.fetch_add(1, std::memory_order_relaxed);
            const std::size_t begin = r * grain;
            if (begin >= n) {
                return;
            }
            body(begin, std::min(n, begin + grain));
        }
    });
    batch.wait();
}

// Abort with a diagnostic on a broken internal invariant (defined in
// interpreter.cpp). Declared here rather than in interpreter_internal.hpp
// because the gather kernel below needs it and this is the lower layer.
[[noreturn]] void invariant_violation(std::string_view detail);

/// Allocate an output column of `rows` rows shaped like `src`, ready to be
/// filled by `gather_range_into`.
///
/// A `Column<std::string>` cannot be sized without first totalling its bytes,
/// so it comes back empty and is built whole by the gather itself.
inline auto make_gather_column(const ColumnValue& src, std::size_t rows) -> ColumnValue {
    return with_meta_of(
        std::visit(
            [&](const auto& col) -> ColumnValue {
                using ColT = std::decay_t<decltype(col)>;
                if constexpr (std::is_same_v<ColT, Column<Categorical>>) {
                    // Shares the source dictionary; only the codes are gathered.
                    return Column<Categorical>(col.dictionary_ptr(), col.index_ptr(),
                                               std::vector<Column<Categorical>::code_type>(rows));
                } else if constexpr (std::is_same_v<ColT, Column<std::string>>) {
                    return ColT{};
                } else if constexpr (!std::is_same_v<ColT, Column<bool>> &&
                                     requires(ColT c) { c.resize_for_overwrite(rows); }) {
                    // `gather_range_into` writes every slot in
                    // [0, rows), so value-initializing first is
                    // a whole extra pass over the output.
                    // Date/Timestamp carry default member
                    // initializers and so are not trivially
                    // default constructible; they keep resize().
                    //
                    // Column<bool> is excluded deliberately: its
                    // last word extends past `rows`, and those
                    // tail bits are never written by a gather.
                    // Leaving them indeterminate would make a
                    // whole-word read of the bitmap depend on
                    // uninitialized memory.
                    ColT dst;
                    dst.resize_for_overwrite(rows);
                    return dst;
                } else {
                    ColT dst;
                    dst.resize(rows);
                    return dst;
                }
            },
            src),
        src);
}

/// Copy output rows `[lo, hi)` from `src` through `idx` into an already-sized
/// `dst`.
///
/// **This is the one gather kernel.** Every path that rewrites a column through
/// an index array goes through it — the serial `gather_rows`, the sort's
/// parallel (column x range) tasks, and `gather_column`'s row ranges — so the
/// rules below are stated once and cannot be answered three different ways.
/// (The two-phase filter is the deliberate exception: it writes into a
/// caller-presized output at a prefix-summed offset, which is a different
/// operation and is what lets it split strings this cannot. See
/// `filter_gather_is_thread_safe`.)
///
/// **Concurrency:** output rows are contiguous, so two ranges write disjoint
/// memory for every column that stores at least one addressable unit per row.
/// `Column<bool>` and validity bitmaps pack 64 rows per word, so a caller
/// splitting one column across threads must align its range boundaries to 64 —
/// which is cheap here precisely because the ranges are contiguous, unlike a
/// scattered scatter. Aligning is strictly better than the obvious alternative
/// of leaving bit-packed columns serial: it costs a rounding of the grain and
/// buys the same parallelism every other type gets.
///
/// A string column has no partial form: its flat offsets are cumulative, so it
/// is built whole and `[lo, hi)` must be the entire column. A caller with
/// several columns to gather still parallelizes across them by treating a
/// string as one indivisible task.
///
/// `idx` is a span so a caller holding a raw pointer and a count can call this
/// without materializing a vector; `std::vector<Idx>` converts implicitly.
template <typename Idx>
void gather_range_into(ColumnValue& dst_v, const ColumnValue& src_v, std::span<const Idx> idx,
                       std::size_t lo, std::size_t hi) {
    std::visit(
        [&](const auto& src) {
            using ColT = std::decay_t<decltype(src)>;
            if constexpr (std::is_same_v<ColT, Column<std::string>>) {
                if (lo != 0 || hi != idx.size()) {
                    invariant_violation("gather_rows: a string column has no partial-range form");
                }
                std::size_t total_chars = 0;
                const auto* src_off = src.offsets_data();
                const auto* src_char = src.chars_data();
                for (std::size_t pos = 0; pos < hi; ++pos) {
                    auto si = static_cast<std::size_t>(idx[pos]);
                    total_chars += src_off[si + 1] - src_off[si];
                }
                ColT dst;
                dst.resize_for_gather(hi, total_chars);
                auto* dst_off = dst.offsets_data();
                auto* dst_char = dst.chars_data();
                dst_off[0] = 0;
                std::uint32_t cur = 0;
                for (std::size_t pos = 0; pos < hi; ++pos) {
                    auto si = static_cast<std::size_t>(idx[pos]);
                    std::uint32_t len = src_off[si + 1] - src_off[si];
                    std::memcpy(dst_char + cur, src_char + src_off[si], len);
                    cur += len;
                    dst_off[pos + 1] = cur;
                }
                dst_v = std::move(dst);
            } else {
                auto* dst = std::get_if<ColT>(&dst_v);
                if (dst == nullptr) {
                    invariant_violation("gather_rows: source/destination column type mismatch");
                }
                if constexpr (std::is_same_v<ColT, Column<Categorical>>) {
                    auto* dp = dst->codes_data();
                    const auto* sp = src.codes_data();
                    for (std::size_t pos = lo; pos < hi; ++pos) {
                        dp[pos] = sp[static_cast<std::size_t>(idx[pos])];
                    }
                } else if constexpr (std::is_same_v<ColT, Column<bool>>) {
                    for (std::size_t pos = lo; pos < hi; ++pos) {
                        dst->set(pos, src[static_cast<std::size_t>(idx[pos])]);
                    }
                } else {
                    auto* dp = dst->data();
                    const auto* sp = src.data();
                    for (std::size_t pos = lo; pos < hi; ++pos) {
                        dp[pos] = sp[static_cast<std::size_t>(idx[pos])];
                    }
                }
            }
        },
        src_v);
}

/// Vector overload. Deduction does not convert `std::vector<Idx>` to
/// `std::span<const Idx>`, so the common caller gets a forwarder rather than an
/// explicit template argument at every call site.
template <typename Idx>
void gather_range_into(ColumnValue& dst_v, const ColumnValue& src_v, const std::vector<Idx>& idx,
                       std::size_t lo, std::size_t hi) {
    gather_range_into<Idx>(dst_v, src_v, std::span<const Idx>{idx}, lo, hi);
}

/// Copy output rows `[lo, hi)` of a validity bitmap. Same 64-row alignment rule
/// as `gather_range_into`.
template <typename Idx>
void gather_validity_range(ValidityBitmap& dst, const ValidityBitmap& src, std::span<const Idx> idx,
                           std::size_t lo, std::size_t hi) {
    auto* dst_words = dst.words_data();
    const auto* src_bytes = src.buffer_data();
    const std::size_t src_offset = src.buffer_offset();
    for (std::size_t pos = lo; pos < hi; ++pos) {
        const std::size_t source_bit = src_offset + static_cast<std::size_t>(idx[pos]);
        const bool valid = ((src_bytes[source_bit / 8] >> (source_bit % 8)) & 0x01U) != 0U;
        const auto mask = ValidityBitmap::word_type{1} << (pos % 64);
        if (valid) {
            dst_words[pos / 64] |= mask;
        } else {
            dst_words[pos / 64] &= ~mask;
        }
    }
}

/// Vector overload, for the same deduction reason as `gather_range_into`'s.
template <typename Idx>
void gather_validity_range(ValidityBitmap& dst, const ValidityBitmap& src,
                           const std::vector<Idx>& idx, std::size_t lo, std::size_t hi) {
    gather_validity_range<Idx>(dst, src, std::span<const Idx>{idx}, lo, hi);
}

/// Bulk-gather rows from `src` into a new column using the index array.
/// One std::visit per column, not per row — much faster for large gathers.
///
/// This is `gather_rows`' single-column form and shares its kernel:
/// `make_gather_column` sizes the output and `gather_range_into` fills it, so
/// the per-type concurrency rules are stated once, in that kernel, instead of
/// being answered differently here. `exec` splits the fill across 64-row-aligned
/// row ranges, which covers every type but `std::string` — a string's flat
/// offsets are cumulative, so a range cannot know where to write without a
/// prefix pass, and it is gathered whole in one call.
///
/// `exec` is REQUIRED rather than defaulted. It was optional, and half the call
/// sites then gathered serially by omission rather than by decision — nothing in
/// the signature said which was intended. Pass `nullptr` to mean serial, and say
/// why.
[[nodiscard]] inline auto gather_column(const ColumnValue& src, const std::size_t* indices,
                                        std::size_t n, const ExecutionContext* exec)
    -> ColumnValue {
    const std::span<const std::size_t> idx{indices, n};
    ColumnValue out = make_gather_column(src, n);
    if (std::holds_alternative<Column<std::string>>(src)) {
        gather_range_into(out, src, idx, 0, n);  // indivisible: whole column or nothing
        return out;
    }
    for_row_ranges(exec, n, [&](std::size_t begin, std::size_t end) {
        gather_range_into(out, src, idx, begin, end);
    });
    return out;
}

/// Bulk-gather rows from `src`, treating sentinel values (kNull = SIZE_MAX) as null positions
/// that receive default values. Returns the new column plus a validity bitmap if any nulls exist.
// n is the array length, kNull the sentinel value; distinct roles, not realistically
// confusable at the handful of call sites.
// NOLINTBEGIN(bugprone-easily-swappable-parameters)
[[nodiscard]] inline auto gather_column_with_nulls(const ColumnValue& src,
                                                   const std::size_t* indices, std::size_t n,
                                                   std::size_t kNull, const ExecutionContext* exec)
    -> std::pair<ColumnValue, std::optional<ValidityBitmap>> {
    // NOLINTEND(bugprone-easily-swappable-parameters)
    auto gathered = std::visit(
        [&](const auto& col) -> std::pair<ColumnValue, std::optional<ValidityBitmap>> {
            using ColT = std::decay_t<decltype(col)>;
            bool has_null = false;
            for (std::size_t i = 0; i < n && !has_null; ++i)
                has_null = (indices[i] == kNull);
            if (!has_null) {
                // The no-sentinel fast path is a plain gather, so it gets the
                // shared kernel and the caller's parallelism. The sentinel path
                // below stays serial: it branches per row and writes a validity
                // bit alongside each value, which is a different kernel.
                return {gather_column(src, indices, n, exec), std::nullopt};
            }
            ValidityBitmap bm(n, true);
            if constexpr (std::is_same_v<ColT, Column<Categorical>>) {
                std::vector<Column<Categorical>::code_type> codes(n);
                const auto* sp = col.codes_data();
                for (std::size_t i = 0; i < n; ++i) {
                    if (indices[i] != kNull) {
                        codes[i] = sp[indices[i]];
                    } else {
                        codes[i] = 0;
                        bm.set(i, false);
                    }
                }
                return {
                    Column<Categorical>(col.dictionary_ptr(), col.index_ptr(), std::move(codes)),
                    std::move(bm)};
            } else if constexpr (std::is_same_v<ColT, Column<std::string>>) {
                const auto* src_off = col.offsets_data();
                const char* src_char = col.chars_data();
                std::size_t total_chars = 0;
                for (std::size_t i = 0; i < n; ++i) {
                    if (indices[i] != kNull)
                        total_chars += src_off[indices[i] + 1] - src_off[indices[i]];
                }
                ColT dst;
                dst.resize_for_gather(n, total_chars);
                auto* dst_off = dst.offsets_data();
                char* dst_char = dst.chars_data();
                dst_off[0] = 0;
                std::uint32_t cur = 0;
                for (std::size_t i = 0; i < n; ++i) {
                    if (indices[i] != kNull) {
                        std::uint32_t len = src_off[indices[i] + 1] - src_off[indices[i]];
                        ::memcpy(dst_char + cur, src_char + src_off[indices[i]], len);
                        cur += len;
                    } else {
                        bm.set(i, false);
                    }
                    dst_off[i + 1] = cur;
                }
                return {std::move(dst), std::move(bm)};
            } else if constexpr (std::is_same_v<ColT, Column<bool>>) {
                ColT dst;
                dst.resize(n, false);
                for (std::size_t i = 0; i < n; ++i) {
                    if (indices[i] != kNull) {
                        dst.set(i, col[indices[i]]);
                    } else {
                        bm.set(i, false);
                    }
                }
                return {std::move(dst), std::move(bm)};
            } else {
                ColT dst;
                dst.resize(n);
                auto* dp = dst.data();
                const auto* sp = col.data();
                for (std::size_t i = 0; i < n; ++i) {
                    if (indices[i] != kNull) {
                        dp[i] = sp[indices[i]];
                    } else {
                        dp[i] = {};
                        bm.set(i, false);
                    }
                }
                return {std::move(dst), std::move(bm)};
            }
        },
        src);
    // The values keep their meaning through a gather; only the rows changed.
    gathered.first = with_meta_of(std::move(gathered.first), src);
    return gathered;
}

/// A gathered column and the validity bitmap that came with it.
using GatheredColumn = std::pair<ColumnValue, std::optional<ValidityBitmap>>;

/// One column in a multi-column gather.
struct ColumnGatherJob {
    const ColumnValue* column = nullptr;
    const ValidityBitmap* validity = nullptr;  ///< null when the source has none
    const std::size_t* idx = nullptr;          ///< output row -> source row
    /// Set when the CALLER knows this column cannot be written by range — a
    /// join index carrying `kNull` sentinels is the case that needs it, because
    /// the sentinel branch writes a validity bit beside each value and so is a
    /// different kernel. A string column is detected here and needs no flag.
    bool indivisible = false;
};

/// Gather several columns in ONE worker batch, as (column x range) tasks.
///
/// This is the shape `gather_rows_parallel` (sort.cpp) uses, made available to
/// the operators that gather a whole output at once. The alternative — calling
/// `gather_column` in a loop over columns and letting each call fan out its own
/// rows — submits and waits a batch PER COLUMN, and measurement says the
/// barrier eats the parallel gather it buys (the two inner-join benchmarks came
/// out +1.8% / +5.0% that way: inside the noise floor, so not a regression, but
/// no win either on an output that clears every gate).
///
/// Two kinds of task share the list:
///   * a (column, range) task, whose ranges are 64-ALIGNED so a bit-packed
///     destination (`Column<bool>`, a validity bitmap) never has two ranges in
///     one word;
///   * a whole-column task, for a string (cumulative offsets have no partial
///     form) or for a job the caller marked `indivisible`. Those still run
///     CONCURRENTLY WITH the other columns, which is the point of batching.
///
/// `gather_whole(j)` produces job `j`'s column in full. It is used for the
/// indivisible tasks and for the serial fallback, so a caller with extra
/// per-column semantics (the join's sentinel handling) keeps them in one place.
/// It takes the INDEX rather than the job so a caller can reach its own
/// parallel array of per-column state.
///
/// The admission test consults `parallel_min_cells` as well as
/// `parallel_min_rows`: the cost scales with output WIDTH as much as with rows,
/// which is the same reason the pipeline and the sort's gather both test it.
template <typename GatherWhole>
[[nodiscard]] auto gather_columns_batched(std::span<const ColumnGatherJob> jobs, std::size_t total,
                                          const ExecutionContext* exec, GatherWhole&& gather_whole)
    -> std::vector<GatheredColumn> {
    std::vector<GatheredColumn> out(jobs.size());

    const std::size_t pool_size = process_worker_pool().size();
    // `exec` is checked for null by `worth_it` below, so the budget must be
    // null-safe rather than assume it: the value is unused when there is no
    // context, but reading it is not.
    const std::size_t budget = exec != nullptr ? exec->compute_budget() : pool_size;
    const std::size_t threads = std::min(budget, pool_size);
    const bool worth_it =
        exec != nullptr && exec->parallel && !on_worker_pool_thread() && threads >= 2 &&
        !jobs.empty() && total >= exec->parallel_min_rows &&
        (exec->parallel_min_cells == 0 || total * jobs.size() >= exec->parallel_min_cells);

    if (!worth_it) {
        // Serial by decision: the fan-out choice is made here, once, so the
        // per-column body must not make it again.
        for (std::size_t j = 0; j < jobs.size(); ++j) {
            out[j] = gather_whole(j);
        }
        return out;
    }

    struct Task {
        std::size_t job = 0;
        std::size_t lo = 0;
        std::size_t hi = 0;
        bool indivisible = false;
    };

    // Enough tasks that one slow column cannot strand the rest, rounded up to
    // whole 64-row words so no two tasks share a bit-packed word.
    constexpr std::size_t kAlign = 64;
    std::size_t span = (total + (threads * 4) - 1) / (threads * 4);
    span = ((span + kAlign - 1) / kAlign) * kAlign;
    span = std::max(span, kAlign);

    std::vector<Task> tasks;
    tasks.reserve(jobs.size() * ((total / span) + 1));
    for (std::size_t j = 0; j < jobs.size(); ++j) {
        const auto& job = jobs[j];
        if (job.indivisible || std::holds_alternative<Column<std::string>>(*job.column)) {
            tasks.push_back({.job = j, .lo = 0, .hi = total, .indivisible = true});
            continue;
        }
        // Allocate before the fan-out: the destinations must not be reshaped
        // once workers hold references into `out`.
        out[j].first = make_gather_column(*job.column, total);
        if (job.validity != nullptr) {
            out[j].second = ValidityBitmap(total, false);
        }
        for (std::size_t lo = 0; lo < total; lo += span) {
            tasks.push_back(
                {.job = j, .lo = lo, .hi = std::min(lo + span, total), .indivisible = false});
        }
    }
    if (tasks.empty()) {
        return out;
    }

    std::atomic<std::size_t> cursor{0};
    auto batch = process_worker_pool().submit(std::min(threads, tasks.size()), [&](std::size_t) {
        while (true) {
            const std::size_t t = cursor.fetch_add(1, std::memory_order_relaxed);
            if (t >= tasks.size()) {
                return;
            }
            const auto& task = tasks[t];
            const auto& job = jobs[task.job];
            if (task.indivisible) {
                out[task.job] = gather_whole(task.job);
                continue;
            }
            const std::span<const std::size_t> idx{job.idx, total};
            gather_range_into(out[task.job].first, *job.column, idx, task.lo, task.hi);
            if (job.validity != nullptr) {
                gather_validity_range(*out[task.job].second, *job.validity, idx, task.lo, task.hi);
            }
        }
    });
    batch.wait();
    return out;
}

/// Append a default (zero / empty) value to a type-erased column.
inline auto append_default(ColumnValue& col) -> void {
    std::visit(
        [](auto& c) {
            using ColType = std::decay_t<decltype(c)>;
            if constexpr (std::is_same_v<ColType, Column<std::int64_t>>) {
                c.push_back(std::int64_t{0});
            } else if constexpr (std::is_same_v<ColType, Column<double>>) {
                c.push_back(0.0);
            } else if constexpr (std::is_same_v<ColType, Column<std::string>>) {
                c.push_back(std::string_view{});
            } else if constexpr (std::is_same_v<ColType, Column<Categorical>>) {
                c.push_code(0);
            } else {
                c.push_back({});
            }
        },
        col);
}

/// Append `count` default (zero / empty) values to a type-erased column.
inline auto append_defaults(ColumnValue& col, std::size_t count) -> void {
    std::visit(
        [count](auto& c) {
            using ColType = std::decay_t<decltype(c)>;
            if constexpr (std::is_same_v<ColType, Column<std::int64_t>>) {
                (void)c.insert(c.end(), count, std::int64_t{0});
            } else if constexpr (std::is_same_v<ColType, Column<double>>) {
                (void)c.insert(c.end(), count, 0.0);
            } else if constexpr (std::is_same_v<ColType, Column<std::string>>) {
                for (std::size_t i = 0; i < count; ++i)
                    c.push_back(std::string_view{});
            } else if constexpr (std::is_same_v<ColType, Column<Categorical>>) {
                for (std::size_t i = 0; i < count; ++i)
                    c.push_code(0);
            } else {
                for (std::size_t i = 0; i < count; ++i)
                    c.push_back({});
            }
        },
        col);
}

[[nodiscard]] inline auto make_empty_like(const ColumnValue& src) -> ColumnValue {
    return with_meta_of(std::visit(
                            [](const auto& col) -> ColumnValue {
                                using ColType = std::decay_t<decltype(col)>;
                                if constexpr (std::is_same_v<ColType, Column<Categorical>>) {
                                    return Column<Categorical>{
                                        col.dictionary_ptr(), col.index_ptr(), {}};
                                }
                                return ColType{};
                            },
                            src),
                        src);
}

}  // namespace ibex::runtime
