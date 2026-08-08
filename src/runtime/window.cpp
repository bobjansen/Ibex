// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

// window.cpp — time-window operators: rolling aggregates over duration
// windows (apply_rolling_func) and timeframe resampling (resample_table).
// Split out of interpreter.cpp; shared declarations live in interpreter_internal.hpp.

#include <ibex/core/column.hpp>
#include <ibex/core/time.hpp>
#include <ibex/ir/node.hpp>
#include <ibex/runtime/interpreter.hpp>

#include <algorithm>
#include <bit>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <expected>
#include <iterator>
#include <limits>
#include <optional>
#include <set>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>

#if defined(__AVX2__) || defined(__BMI2__)
#include <immintrin.h>
#endif

#include "interpreter_internal.hpp"

namespace ibex::runtime {

namespace {

// Find the first row index lo in [0, row] where time[lo] > time[row] - duration.
// The trailing window is half-open, (t - duration, t], so that a duration means
// the same thing here as it does to `resample`: on a regular grid of spacing s,
// a k*s window holds k rows, not k+1. The `aligned` variant is closed on the
// left instead — see win_lo — because a row sitting exactly on a bucket boundary
// belongs to that bucket, matching resample's [start, start + duration).
// The time index column must be Timestamp or Date and sorted ascending.
auto window_lo(const ColumnValue& time_col, std::size_t row, ir::Duration duration) -> std::size_t {
    if (const auto* ts_col = std::get_if<Column<Timestamp>>(&time_col)) {
        std::int64_t threshold = (*ts_col)[row].nanos - duration.count();
        std::size_t lo = 0;
        std::size_t hi = row;
        while (lo < hi) {
            const std::size_t mid = lo + ((hi - lo) / 2);
            if ((*ts_col)[mid].nanos <= threshold) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        return lo;
    }
    // Date column: convert duration (nanoseconds) to days
    const auto& date_col = std::get<Column<Date>>(time_col);
    static constexpr std::int64_t kNsPerDay = 86'400'000'000'000LL;
    auto duration_days = static_cast<std::int32_t>(duration.count() / kNsPerDay);
    std::int32_t threshold = date_col[row].days - duration_days;
    std::size_t lo = 0;
    std::size_t hi = row;
    while (lo < hi) {
        const std::size_t mid = lo + ((hi - lo) / 2);
        if (date_col[mid].days <= threshold) {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    return lo;
}

/// A deque specialised for indices. Its power-of-two capacity turns wraparound
/// into a mask, and its contiguous storage avoids the node allocations a
/// `std::deque` of candidates would pay.
///
/// The mask is cached rather than derived from `buf_.size()` on each use. That
/// matters because the rolling extrema loop below touches `back()`,
/// `push_back()` and `pop_front()` per row, and `buf_.size()` is a pointer
/// subtraction the compiler has to redo after any write it cannot prove leaves
/// the vector alone.
///
/// NOT measurably faster than deriving the mask each time, on any shape tried:
/// Int64 and Double, ascending and descending input, widths 256 and 1024, up to
/// 10M rows. An interleaved run once showed 1.32x for Int64, but building each
/// side TWICE put two builds of identical source 66ms and 61ms apart — the same
/// spread as the change itself, so that reading was code layout, not the mask.
/// Kept for parity with the standalone index_ring_deque library and because it
/// is strictly less work, not for a speedup.
///
/// Removing the per-row `isnan` check entirely (the obvious next suspect) made
/// the Double case slightly SLOWER, so it is not the cost either. Whatever
/// dominates this loop, it is not the deque arithmetic.
class IndexRingDeque {
   public:
    explicit IndexRingDeque(std::size_t initial_capacity = 0) {
        if (initial_capacity > 0) {
            auto cap = next_power_of_two(initial_capacity);
            if (!cap.has_value())
                throw std::length_error("rolling_min/max deque capacity overflow");
            buf_.resize(*cap);
            mask_ = buf_.size() - 1;
        }
    }

    [[nodiscard]] auto empty() const noexcept -> bool { return size_ == 0; }
    [[nodiscard]] auto size() const noexcept -> std::size_t { return size_; }
    [[nodiscard]] auto capacity() const noexcept -> std::size_t { return buf_.size(); }
    [[nodiscard]] auto front() const noexcept -> std::size_t { return buf_[head_]; }
    [[nodiscard]] auto back() const noexcept -> std::size_t {
        return buf_[(head_ + size_ - 1) & mask_];
    }

    // `head_` is deliberately left where it is when the deque empties: the ring
    // is valid from any head position, so resetting it would be a branch per
    // pop for no benefit.
    void pop_front() noexcept {
        head_ = (head_ + 1) & mask_;
        --size_;
    }

    void pop_back() noexcept { --size_; }

    void push_back(std::size_t value) {
        if (size_ == buf_.size())
            grow_and_linearize();
        buf_[(head_ + size_) & mask_] = value;
        ++size_;
    }

   private:
    [[nodiscard]] static constexpr auto next_power_of_two(std::size_t n)
        -> std::optional<std::size_t> {
        constexpr auto max_power = std::size_t{1} << (std::numeric_limits<std::size_t>::digits - 1);
        if (n > max_power)
            return std::nullopt;
        return std::bit_ceil(n);
    }

    void grow_and_linearize() {
        const std::size_t old_cap = buf_.size();
        if (old_cap == 0) {
            buf_.resize(16);
            mask_ = buf_.size() - 1;
            return;
        }
        if (old_cap > (std::numeric_limits<std::size_t>::max() / 2)) {
            throw std::length_error("rolling_min/max deque capacity overflow");
        }
        const std::size_t new_cap = old_cap * 2;
        std::vector<std::size_t> next(new_cap);
        const std::size_t first_count = std::min(size_, old_cap - head_);
        std::copy_n(buf_.begin() + static_cast<std::ptrdiff_t>(head_), first_count, next.begin());
        std::copy_n(buf_.begin(), size_ - first_count,
                    next.begin() + static_cast<std::ptrdiff_t>(first_count));
        buf_ = std::move(next);
        head_ = 0;
        mask_ = buf_.size() - 1;
    }

    std::vector<std::size_t> buf_;
    std::size_t head_ = 0;
    std::size_t size_ = 0;
    std::size_t mask_ = 0;
};

}  // namespace

// Compute a rolling aggregate column over a time-indexed window.
// The table must be a TimeFrame (time_index set, sorted ascending).
//
// Null/NaN semantics (three states per input element, matching polars/Arrow):
//   • NULL    (validity=false): a missing observation — skipped entirely; its
//     payload is undefined and never read. A window of only nulls yields null.
//   • NaN     (validity=true, value is NaN): a present-but-undefined value —
//     propagated, so any window overlapping it yields NaN. NaNs are tracked in a
//     separate counter rather than fed into the running accumulator, because a
//     later subtraction cannot undo a NaN (NaN−NaN=NaN) — so a NaN poisons only
//     the windows it actually overlaps and clears once it ages out.
//   • finite: fed into the accumulator as usual.
// When no input element is null the result carries no validity bitmap (nullopt)
// and the numbers are bit-identical to the null-unaware path.
auto rolling_window_spec(const ir::CallExpr& call, std::optional<ir::Duration> block_default)
    -> std::expected<WindowSpec, std::string> {
    // Lowering attaches the per-call window as a sentinel named arg (mirroring
    // rep's __array_len): __window_n for a row count, __window_ns for a duration
    // already normalised to nanoseconds. A per-call window overrides the block.
    for (const auto& na : call.named_args) {
        const bool is_n = na.name == "__window_n";
        const bool is_ns = na.name == "__window_ns";
        if (!is_n && !is_ns)
            continue;
        const auto* lit = na.value ? std::get_if<ir::Literal>(&na.value->node) : nullptr;
        const auto* iv = lit ? std::get_if<std::int64_t>(&lit->value) : nullptr;
        if (iv == nullptr) {
            return std::unexpected(call.callee + ": malformed window argument");
        }
        if (is_n) {
            if (*iv <= 0)
                return std::unexpected(call.callee + ": count window must be a positive integer");
            return WindowSpec{CountWindow{*iv}};
        }
        return WindowSpec{ir::Duration{*iv}};
    }
    if (block_default.has_value())
        return WindowSpec{*block_default};
    return std::unexpected(
        call.callee + ": requires a window clause or an explicit window argument, e.g. " +
        call.callee + "(col, 20) for 20 rows or " + call.callee + "(col, 5s) for a time window");
}

auto window_bound_column(const Table& table, ir::Duration duration, bool aligned, bool want_end)
    -> std::expected<ComputedColumn, std::string> {
    const char* fn = want_end ? "window_end" : "window_start";
    if (!table.time_index().has_value()) {
        return std::unexpected(std::string(fn) + ": requires a TimeFrame");
    }
    const std::int64_t dur = duration.count();
    if (dur <= 0) {
        return std::unexpected(std::string(fn) + ": window duration must be positive");
    }
    const std::size_t rows = table.rows();
    const auto* tcv = table.find(*table.time_index());

    // Nominal bound of the window containing time `t`, in `unit` steps. aligned:
    // start = floor(t/unit)*unit (grid boundary), end = start + unit. trailing:
    // start = t - unit, end = t (the row's own timestamp).
    auto bound = [&](std::int64_t t, std::int64_t unit) -> std::int64_t {
        if (aligned) {
            const std::int64_t start = window_bucket_index(t, unit) * unit;
            return want_end ? start + unit : start;
        }
        return want_end ? t : t - unit;
    };

    if (const auto* ts = std::get_if<Column<Timestamp>>(tcv)) {
        Column<Timestamp> out;
        out.resize(rows);
        for (std::size_t i = 0; i < rows; ++i) {
            out[i] = Timestamp{bound((*ts)[i].nanos, dur)};
        }
        return ComputedColumn{std::move(out), std::nullopt};
    }
    if (const auto* dt = std::get_if<Column<Date>>(tcv)) {
        // Date time index: the duration is expressed in days.
        static constexpr std::int64_t kNsPerDay = 86'400'000'000'000LL;
        const std::int64_t dur_days = dur / kNsPerDay;
        if (dur_days <= 0) {
            return std::unexpected(std::string(fn) +
                                   ": duration must be at least one day for a Date time index");
        }
        Column<Date> out;
        out.resize(rows);
        for (std::size_t i = 0; i < rows; ++i) {
            out[i] = Date{static_cast<std::int32_t>(bound((*dt)[i].days, dur_days))};
        }
        return ComputedColumn{std::move(out), std::nullopt};
    }
    return std::unexpected(std::string(fn) + ": time index must be a Timestamp or Date column");
}

// RESOLVED for `rolling_sum`, OPEN for `rolling_std` (2026-08-02).
//
// Both were ~20% slower than before Arrow buffer adoption with unchanged kernel
// source. The cause for sum was register allocation -- the running accumulator
// spilled to the stack inside the row loop -- driven by `Column<T>` growing an
// adopted-buffer mode (a12bf0f) and inflating this function. Instantiating the
// row loop per (window kind x has-nulls), giving it its own accumulator state
// instead of by-reference captures, and dropping the validity test and running
// count from the null-free case removed the spill: sum is now ~2x FASTER than
// pre-adoption, not merely recovered.
//
// `rolling_std` did NOT respond to the same treatment (neutral, still ~1.2x).
// Ruled out for it: register pressure (no spill dominates its profile), the
// division (no `vdivsd` in the top samples), and `sqrt`'s errno guard
// (replacing it changed nothing). Its profile concentrates on a `vucomisd`/`jb`
// pair in the output expression, which may be sampling skid -- the next step is
// a cycle-accurate profile, which this box cannot give (WSL2 has no hardware
// counters).
auto apply_rolling_func(const ir::CallExpr& call, const Table& table, WindowSpec spec, bool aligned)
    -> std::expected<ComputedColumn, std::string> {
    std::size_t rows = table.rows();

    // A count window spans the last `count_n` rows and needs no time index.
    // A duration window spans (t - dur, t] and requires a TimeFrame.
    const bool is_count = std::holds_alternative<CountWindow>(spec);
    const std::size_t count_n =
        is_count ? static_cast<std::size_t>(std::get<CountWindow>(spec).n) : 0;

    // Duration-window setup — skipped entirely for count windows. Maps the time
    // column to a contiguous int64 array and expresses the duration in the same
    // unit. Timestamp: nanoseconds (layout-compatible with int64, no copy).
    // Date: days (int32, widened into a temporary buffer).
    const ColumnValue* time_col_ptr = nullptr;
    const std::int64_t* time_vals = nullptr;
    std::vector<std::int64_t> time_vals_buf;  // only allocated for the Date path
    std::int64_t dur_val = 0;
    ir::Duration duration{0};
    if (!is_count) {
        if (!table.time_index().has_value()) {
            return std::unexpected(call.callee +
                                   ": duration window requires a TimeFrame — use a count window "
                                   "(e.g. " +
                                   call.callee + "(col, 20)) or as_timeframe()");
        }
        duration = std::get<ir::Duration>(spec);
        time_col_ptr = &*table.find(*table.time_index());
        if (const auto* ts_col = std::get_if<Column<Timestamp>>(time_col_ptr)) {
            // Timestamp is {int64_t nanos} — pointer-cast avoids an 8 MB copy.
            static_assert(sizeof(Timestamp) == sizeof(std::int64_t) &&
                          alignof(Timestamp) == alignof(std::int64_t));
            // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
            time_vals = reinterpret_cast<const std::int64_t*>(ts_col->data());
            dur_val = duration.count();
        } else {
            const auto& date_col = std::get<Column<Date>>(*time_col_ptr);
            time_vals_buf.resize(rows);
            for (std::size_t i = 0; i < rows; ++i)
                time_vals_buf[i] = date_col[i].days;
            time_vals = time_vals_buf.data();
            static constexpr std::int64_t kNsPerDay = 86'400'000'000'000LL;
            dur_val = duration.count() / kNsPerDay;
        }
    }

    // `aligned` duration window: reset on the epoch grid. bucket_start(i) is the
    // start of the fixed-grid bucket containing row i (floor toward -inf, so
    // negative timestamps bucket correctly), and the window is [bucket_start, t].
    const bool aligned_dur = aligned && !is_count;
    auto bucket_start = [&](std::size_t i) -> std::int64_t {
        return window_bucket_index(time_vals[i], dur_val) * dur_val;
    };

    // Spec-aware window bounds, shared by both rolling mechanisms.
    //
    // should_drop(lo, i): is row `lo` outside the window ending at row `i`?
    //   Both bounds are monotonic (the TimeFrame is sorted ascending), so the
    //   two-pointer `lo` only ever advances. For an aligned window `lo` is out
    //   once it predates the current bucket's start.
    auto should_drop = [&](std::size_t lo, std::size_t i) -> bool {
        if (is_count)
            return (i - lo) >= count_n;
        if (aligned_dur)
            return time_vals[lo] < bucket_start(i);
        // Trailing window is half-open on the left: (t - dur, t].
        return time_vals[lo] <= time_vals[i] - dur_val;
    };
    // win_lo(i): the first in-window row index for the window ending at `i`.
    auto win_lo = [&](std::size_t i) -> std::size_t {
        if (is_count)
            return i + 1 >= count_n ? i + 1 - count_n : 0;
        if (aligned_dur) {
            // First row at or after this bucket's start — binary search, O(log n),
            // matching the trailing `window_lo`.
            const std::int64_t start = bucket_start(i);
            std::size_t lo = 0;
            std::size_t hi = i;
            while (lo < hi) {
                const std::size_t mid = lo + ((hi - lo) / 2);
                if (time_vals[mid] < start) {
                    lo = mid + 1;
                } else {
                    hi = mid;
                }
            }
            return lo;
        }
        return window_lo(*time_col_ptr, i, duration);
    };

    if (call.callee == "rolling_count") {
        Column<std::int64_t> result;
        result.resize_for_overwrite(rows);
        // Write through a hoisted pointer, not `result[i]`. The mutable
        // `operator[]` calls `detach_external()` on every element — a check the
        // optimizer cannot hoist out, because the call it guards may change the
        // very state it tests. Taking `data()` once pays that check once, and
        // is what the other rolling kernels already do.
        auto* result_values = result.data();
        std::size_t lo = 0;
        for (std::size_t i = 0; i < rows; ++i) {
            while (lo < i && should_drop(lo, i))
                ++lo;
            result_values[i] = static_cast<std::int64_t>(i - lo + 1);
        }
        return ComputedColumn{.column = std::move(result), .validity = std::nullopt};
    }

    if (call.args.empty()) {
        return std::unexpected(call.callee + ": expected column argument");
    }
    const auto* col_ref = ir::as_column_ref(*call.args[0]);
    if (!col_ref) {
        return std::unexpected(call.callee + ": argument must be a column name");
    }
    const auto* src = table.find(col_ref->name);
    if (!src) {
        return std::unexpected(call.callee + ": unknown column '" + col_ref->name + "'");
    }
    // Source validity: nullptr means every element is valid (the common case).
    // valid_at(j) is false only for true NULLs — never read the value when false.
    const auto* src_entry = table.find_entry(col_ref->name);
    const ValidityBitmap* sv =
        (src_entry != nullptr && src_entry->validity.has_value()) ? &*src_entry->validity : nullptr;
    const auto* validity_bytes = sv == nullptr ? nullptr : sv->buffer_data();
    const std::size_t validity_offset = sv == nullptr ? 0 : sv->buffer_offset();
    auto valid_at = [validity_bytes, validity_offset](std::size_t j) noexcept -> bool {
        if (validity_bytes == nullptr) {
            return true;
        }
        const std::size_t bit = validity_offset + j;
        return ((validity_bytes[bit / 8] >> (bit % 8)) & 0x01U) != 0U;
    };

    if (call.callee == "rolling_mean") {
        return std::visit(
            [&](const auto& col) -> std::expected<ComputedColumn, std::string> {
                using T = std::decay_t<decltype(col)>::value_type;
                if constexpr (!std::is_same_v<T, std::int64_t> && !std::is_same_v<T, double>) {
                    return std::unexpected("rolling_mean: column must be numeric (Int or Float)");
                } else {
                    const auto* values = col.data();
                    Column<double> result;
                    result.resize_for_overwrite(rows);
                    auto* result_values = result.data();
                    if (sv == nullptr) {
                        double sum = 0.0;
                        std::size_t val_cnt = 0;
                        std::size_t nan_cnt = 0;
                        auto add = [&](std::size_t j) {
                            auto v = static_cast<double>(values[j]);
                            if (std::isnan(v)) {
                                ++nan_cnt;
                            } else {
                                sum += v;
                                ++val_cnt;
                            }
                        };
                        auto drop = [&](std::size_t j) {
                            auto v = static_cast<double>(values[j]);
                            if (std::isnan(v)) {
                                --nan_cnt;
                            } else {
                                sum -= v;
                                --val_cnt;
                            }
                        };
                        std::size_t lo = 0;
                        for (std::size_t i = 0; i < rows; ++i) {
                            add(i);
                            while (lo < i && should_drop(lo, i)) {
                                drop(lo);
                                ++lo;
                            }
                            result_values[i] = nan_cnt > 0
                                                   ? std::numeric_limits<double>::quiet_NaN()
                                                   : sum / static_cast<double>(val_cnt);
                        }
                        return ComputedColumn{.column = std::move(result),
                                              .validity = std::nullopt};
                    }

                    std::optional<ValidityBitmap> out_valid;  // built lazily on first null result
                    double sum = 0.0;
                    std::size_t val_cnt = 0;  // finite, non-null elements in the window
                    std::size_t nan_cnt = 0;  // valid-but-NaN elements in the window
                    auto add = [&](std::size_t j) {
                        if (!valid_at(j))
                            return;  // NULL: skip, payload undefined
                        auto v = static_cast<double>(values[j]);
                        if (std::isnan(v)) {
                            ++nan_cnt;
                        } else {
                            sum += v;
                            ++val_cnt;
                        }
                    };
                    auto drop = [&](std::size_t j) {
                        if (!valid_at(j))
                            return;
                        auto v = static_cast<double>(values[j]);
                        if (std::isnan(v)) {
                            --nan_cnt;
                        } else {
                            sum -= v;
                            --val_cnt;
                        }
                    };
                    std::size_t lo = 0;
                    for (std::size_t i = 0; i < rows; ++i) {
                        add(i);
                        while (lo < i && should_drop(lo, i)) {
                            drop(lo);
                            ++lo;
                        }
                        if (nan_cnt > 0) {
                            result_values[i] = std::numeric_limits<double>::quiet_NaN();
                        } else if (val_cnt > 0) {
                            result_values[i] = sum / static_cast<double>(val_cnt);
                        } else {
                            result_values[i] = 0.0;  // window of only nulls -> null
                            if (!out_valid)
                                out_valid.emplace(rows, true);
                            out_valid->set(i, false);
                        }
                    }
                    return ComputedColumn{.column = std::move(result),
                                          .validity = std::move(out_valid)};
                }
            },
            *src);
    }

    if (call.callee == "rolling_sum") {
        return std::visit(
            [&](const auto& col) -> std::expected<ComputedColumn, std::string> {
                using ColT = std::decay_t<decltype(col)>;
                using T = ColT::value_type;
                if constexpr (!std::is_same_v<T, std::int64_t> && !std::is_same_v<T, double>) {
                    return std::unexpected("rolling_sum: column must be numeric (Int or Float)");
                } else {
                    ColT result;
                    result.resize_for_overwrite(rows);
                    auto* result_values = result.data();
                    const auto* col_values = col.data();
                    std::optional<ValidityBitmap> out_valid;
                    // One instantiation per (window kind x has-nulls).
                    //
                    // The accumulator state lives INSIDE this function rather
                    // than being captured by reference from the enclosing
                    // scope, and the null-free instantiation drops both the
                    // validity test and the running `val_cnt`: with no nulls
                    // the window is never empty, so the count is not needed at
                    // all. Fewer values have to survive the row loop, which is
                    // what was spilling the running sum to the stack.
                    auto run_rows = [&](auto drop_pred, auto has_nulls_tag) {
                        constexpr bool kHasNulls = decltype(has_nulls_tag)::value;
                        T sum{};
                        std::size_t val_cnt = 0;  // non-null, non-NaN in window
                        std::size_t nan_cnt = 0;  // valid-but-NaN (Float only)
                        std::size_t lo = 0;
                        for (std::size_t i = 0; i < rows; ++i) {
                            if (!kHasNulls || valid_at(i)) {
                                const T v = col_values[i];
                                bool is_nan = false;
                                if constexpr (std::is_floating_point_v<T>) {
                                    is_nan = std::isnan(v);
                                }
                                if (is_nan) {
                                    ++nan_cnt;
                                } else {
                                    sum += v;
                                    if constexpr (kHasNulls) {
                                        ++val_cnt;
                                    }
                                }
                            }
                            while (lo < i && drop_pred(lo, i)) {
                                if (!kHasNulls || valid_at(lo)) {
                                    const T w = col_values[lo];
                                    bool is_nan = false;
                                    if constexpr (std::is_floating_point_v<T>) {
                                        is_nan = std::isnan(w);
                                    }
                                    if (is_nan) {
                                        --nan_cnt;
                                    } else {
                                        sum -= w;
                                        if constexpr (kHasNulls) {
                                            --val_cnt;
                                        }
                                    }
                                }
                                ++lo;
                            }
                            if (nan_cnt > 0) {
                                // Only reachable for Float columns (Int has no
                                // NaN) -- but every arm must still write, since
                                // the output is resized without initialisation.
                                if constexpr (std::is_floating_point_v<T>) {
                                    result_values[i] = std::numeric_limits<T>::quiet_NaN();
                                } else {
                                    result_values[i] = T{};
                                }
                            } else if constexpr (!kHasNulls) {
                                // `lo <= i` always, so the window holds at least
                                // row i: there is no empty-window case here.
                                result_values[i] = sum;
                            } else if (val_cnt > 0) {
                                result_values[i] = sum;
                            } else {
                                result_values[i] = T{};  // window of only nulls -> null
                                if (!out_valid) {
                                    out_valid.emplace(rows, true);
                                }
                                out_valid->set(i, false);
                            }
                        }
                    };
                    // The window kind is resolved here too, rather than calling
                    // the general `should_drop`, which branches on
                    // `is_count`/`aligned_dur` and keeps their captures live.
                    const bool has_nulls = sv != nullptr;
                    if (!is_count && !aligned_dur) {
                        const std::int64_t dur = dur_val;
                        const std::int64_t* times = time_vals;
                        auto pred = [times, dur](std::size_t l, std::size_t r) {
                            return times[l] <= times[r] - dur;
                        };
                        if (has_nulls) {
                            run_rows(pred, std::true_type{});
                        } else {
                            run_rows(pred, std::false_type{});
                        }
                    } else if (has_nulls) {
                        run_rows(should_drop, std::true_type{});
                    } else {
                        run_rows(should_drop, std::false_type{});
                    }
                    return ComputedColumn{std::move(result), std::move(out_valid)};
                }
            },
            *src);
    }

    if (call.callee == "rolling_median") {
        return std::visit(
            [&](const auto& col) -> std::expected<ComputedColumn, std::string> {
                using T = std::decay_t<decltype(col)>::value_type;
                if constexpr (!std::is_same_v<T, std::int64_t> && !std::is_same_v<T, double>) {
                    return std::unexpected("rolling_median: column must be numeric (Int or Float)");
                } else {
                    // Sliding-window median via two multisets — O(n log w).
                    //
                    // lo holds the lower half, hi the upper half.
                    // Invariants:
                    //   (1) lo.size() == hi.size()     (even total)
                    //    OR lo.size() == hi.size() + 1 (odd total)
                    //   (2) max(lo) <= min(hi)
                    //
                    // Median = max(lo) when sizes differ, else avg of both tops.
                    std::multiset<double> lo;  // lower half  — max is rbegin()
                    std::multiset<double> hi;  // upper half  — min is begin()

                    // Restore invariant (1) after a single insert or erase.
                    auto rebalance = [&] {
                        if (lo.size() > hi.size() + 1) {
                            hi.insert(*lo.rbegin());
                            lo.erase(std::prev(lo.end()));
                        } else if (hi.size() > lo.size()) {
                            lo.insert(*hi.begin());
                            hi.erase(hi.begin());
                        }
                    };

                    auto insert_val = [&](double x) {
                        // Preserves invariant (2): x goes to lo if it belongs
                        // in the lower half, hi otherwise.
                        if (lo.empty() || x <= *lo.rbegin())
                            lo.insert(x);
                        else
                            hi.insert(x);
                        rebalance();
                    };

                    auto remove_val = [&](double x) {
                        // Remove one copy from whichever half contains it.
                        auto it = lo.find(x);
                        if (it != lo.end())
                            lo.erase(it);
                        else
                            hi.erase(hi.find(x));
                        rebalance();
                    };

                    Column<double> result;
                    result.resize_for_overwrite(rows);
                    auto* result_values = result.data();
                    const auto* col_values = col.data();
                    std::optional<ValidityBitmap> out_valid;
                    std::size_t nan_cnt = 0;  // valid-but-NaN values in window
                    // Only finite, non-null values enter the multisets; NULLs are
                    // skipped and NaNs counted (NaN can't participate in an ordered
                    // structure and would corrupt the median).
                    auto add = [&](std::size_t j) {
                        if (!valid_at(j))
                            return;
                        auto v = static_cast<double>(col_values[j]);
                        if (std::isnan(v))
                            ++nan_cnt;
                        else
                            insert_val(v);
                    };
                    auto drop = [&](std::size_t j) {
                        if (!valid_at(j))
                            return;
                        auto v = static_cast<double>(col_values[j]);
                        if (std::isnan(v))
                            --nan_cnt;
                        else
                            remove_val(v);
                    };
                    std::size_t lo_ptr = 0;
                    for (std::size_t i = 0; i < rows; ++i) {
                        add(i);
                        while (lo_ptr < i && should_drop(lo_ptr, i)) {
                            drop(lo_ptr);
                            ++lo_ptr;
                        }
                        if (nan_cnt > 0) {
                            result_values[i] = std::numeric_limits<double>::quiet_NaN();
                        } else if (lo.empty() && hi.empty()) {
                            result_values[i] = 0.0;  // window of only nulls -> null
                            if (!out_valid)
                                out_valid.emplace(rows, true);
                            out_valid->set(i, false);
                        } else {
                            result_values[i] = (lo.size() > hi.size())
                                                   ? static_cast<double>(*lo.rbegin())
                                                   : (*lo.rbegin() + *hi.begin()) / 2.0;
                        }
                    }
                    return ComputedColumn{.column = std::move(result),
                                          .validity = std::move(out_valid)};
                }
            },
            *src);
    }

    if (call.callee == "rolling_std") {
        return std::visit(
            [&](const auto& col) -> std::expected<ComputedColumn, std::string> {
                using T = std::decay_t<decltype(col)>::value_type;
                if constexpr (!std::is_same_v<T, std::int64_t> && !std::is_same_v<T, double>) {
                    return std::unexpected("rolling_std: column must be numeric (Int or Float)");
                } else {
                    // O(n) sliding window. The TimeFrame is sorted ascending, so
                    // `lo` is monotonic: each row is added once on the right and
                    // dropped once on the left. We maintain running (mean, m2)
                    // with Welford add and its exact inverse for removal. Only
                    // finite, non-null values enter Welford; NULLs are skipped and
                    // NaNs counted separately (a NaN can't be inverse-Welford'd out,
                    // so keeping it out of the recurrence lets it clear on exit).
                    Column<double> result;
                    result.resize_for_overwrite(rows);
                    auto* result_values = result.data();
                    const auto* col_values = col.data();
                    std::optional<ValidityBitmap> out_valid;
                    // Specialised per (window kind x has-nulls), as in
                    // `rolling_sum`. Welford carries two doubles across the row
                    // loop, so keeping the validity test and its two captures
                    // out of the null-free instantiation matters more here, not
                    // less.
                    auto run_rows = [&](auto drop_pred, auto has_nulls_tag) {
                        constexpr bool kHasNulls = decltype(has_nulls_tag)::value;
                        double mean = 0.0;
                        double m2 = 0.0;
                        std::size_t cnt = 0;      // finite, non-null in window
                        std::size_t nan_cnt = 0;  // valid-but-NaN in window
                        std::size_t lo = 0;
                        for (std::size_t i = 0; i < rows; ++i) {
                            if (!kHasNulls || valid_at(i)) {
                                const auto x = static_cast<double>(col_values[i]);
                                if (std::isnan(x)) {
                                    ++nan_cnt;
                                } else {
                                    ++cnt;
                                    const double delta = x - mean;
                                    mean += delta / static_cast<double>(cnt);
                                    m2 += delta * (x - mean);
                                }
                            }
                            while (lo < i && drop_pred(lo, i)) {
                                if (!kHasNulls || valid_at(lo)) {
                                    const auto y = static_cast<double>(col_values[lo]);
                                    if (std::isnan(y)) {
                                        --nan_cnt;
                                    } else {
                                        const double mean_old = mean;
                                        --cnt;
                                        mean =
                                            cnt == 0
                                                ? 0.0
                                                : (((static_cast<double>(cnt) + 1.0) * mean_old) -
                                                   y) /
                                                      static_cast<double>(cnt);
                                        m2 -= (y - mean_old) * (y - mean);
                                    }
                                }
                                ++lo;
                            }
                            if (nan_cnt > 0) {
                                result_values[i] = std::numeric_limits<double>::quiet_NaN();
                            } else if (cnt == 0) {
                                result_values[i] = 0.0;  // window of only nulls -> null
                                if (!out_valid) {
                                    out_valid.emplace(rows, true);
                                }
                                out_valid->set(i, false);
                            } else {
                                // Clamp away tiny negative m2 from floating-point drift.
                                result_values[i] = cnt < 2
                                                       ? 0.0
                                                       : std::sqrt(std::max(0.0, m2) /
                                                                   static_cast<double>(cnt - 1));
                            }
                        }
                    };
                    const bool has_nulls = sv != nullptr;
                    if (!is_count && !aligned_dur) {
                        const std::int64_t dur = dur_val;
                        const std::int64_t* times = time_vals;
                        auto pred = [times, dur](std::size_t l, std::size_t r) {
                            return times[l] <= times[r] - dur;
                        };
                        if (has_nulls) {
                            run_rows(pred, std::true_type{});
                        } else {
                            run_rows(pred, std::false_type{});
                        }
                    } else if (has_nulls) {
                        run_rows(should_drop, std::true_type{});
                    } else {
                        run_rows(should_drop, std::false_type{});
                    }
                    return ComputedColumn{.column = std::move(result),
                                          .validity = std::move(out_valid)};
                }
            },
            *src);
    }

    if (call.callee == "rolling_ewma") {
        // Parse alpha from the second argument (a numeric literal).
        double alpha = 0.0;
        if (call.args.size() < 2) {
            return std::unexpected(
                "rolling_ewma: expected two arguments: rolling_ewma(col, alpha)");
        }
        if (const auto* lit = std::get_if<ir::Literal>(&call.args[1]->node)) {
            if (const auto* dv = std::get_if<double>(&lit->value)) {
                alpha = *dv;
            } else if (const auto* iv = std::get_if<std::int64_t>(&lit->value)) {
                alpha = static_cast<double>(*iv);
            } else {
                return std::unexpected("rolling_ewma: alpha must be a numeric literal");
            }
        } else {
            return std::unexpected("rolling_ewma: alpha must be a numeric literal");
        }
        return std::visit(
            [&](const auto& col) -> std::expected<ComputedColumn, std::string> {
                using T = std::decay_t<decltype(col)>::value_type;
                if constexpr (!std::is_same_v<T, std::int64_t> && !std::is_same_v<T, double>) {
                    return std::unexpected("rolling_ewma: column must be numeric (Int or Float)");
                } else {
                    // O(n) sliding window. The TimeFrame is sorted ascending, so
                    // `lo` is monotonic: each row enters once on the right and is
                    // dropped once on the left. The windowed EWMA restarts at each
                    // window's first element (the seed), which expands to
                    //   result_values[i] = alpha*R_i + (1-alpha)*beta^(i-lo)*col_values[lo]
                    // with R_i = sum_{j=lo..i} beta^(i-j)*col_values[j], maintained as
                    //   add right:  R = beta*R + col_values[i]
                    //   drop left:  R -= beta^(i-lo)*col_values[lo]
                    // reproducing the from-scratch O(n*w) recurrence in one pass.
                    // beta_pow caches beta^k (k bounded by the window width).
                    //
                    // Null handling: the weights are position-based, so a null
                    // can't be skipped without renumbering the whole window.
                    // Instead a null contributes 0 (a "no-return" tick) — its
                    // payload is never read. (NaN still propagates through the
                    // recurrence; genuine NaNs in an EWMA input are out of scope.)
                    const double beta = 1.0 - alpha;
                    const auto* col_values = col.data();
                    auto val = [&](std::size_t j) -> double {
                        return valid_at(j) ? static_cast<double>(col_values[j]) : 0.0;
                    };
                    Column<double> result;
                    result.resize_for_overwrite(rows);
                    auto* result_values = result.data();
                    std::vector<double> beta_pow{1.0};  // beta_pow[k] == beta^k
                    beta_pow.reserve(64);
                    auto bpow = [&](std::size_t k) -> double {
                        while (beta_pow.size() <= k)
                            beta_pow.push_back(beta_pow.back() * beta);
                        return beta_pow[k];
                    };
                    double r = 0.0;
                    std::size_t lo = 0;
                    for (std::size_t i = 0; i < rows; ++i) {
                        r = (beta * r) + val(i);  // add col_values[i] at weight 1
                        while (lo < i && should_drop(lo, i)) {
                            r -= bpow(i - lo) * val(lo);
                            ++lo;
                        }
                        result_values[i] = (alpha * r) + ((1.0 - alpha) * bpow(i - lo) * val(lo));
                    }
                    return ComputedColumn{.column = std::move(result), .validity = std::nullopt};
                }
            },
            *src);
    }

    if (call.callee == "rolling_quantile") {
        // Parse p from the second argument (a numeric literal).
        double p = 0.5;
        if (call.args.size() < 2) {
            return std::unexpected(
                "rolling_quantile: expected two arguments: rolling_quantile(col, p)");
        }
        if (const auto* lit = std::get_if<ir::Literal>(&call.args[1]->node)) {
            if (const auto* dv = std::get_if<double>(&lit->value)) {
                p = *dv;
            } else if (const auto* iv = std::get_if<std::int64_t>(&lit->value)) {
                p = static_cast<double>(*iv);
            } else {
                return std::unexpected("rolling_quantile: p must be a numeric literal");
            }
        } else {
            return std::unexpected("rolling_quantile: p must be a numeric literal");
        }
        return std::visit(
            [&](const auto& col) -> std::expected<ComputedColumn, std::string> {
                using T = std::decay_t<decltype(col)>::value_type;
                if constexpr (!std::is_same_v<T, std::int64_t> && !std::is_same_v<T, double>) {
                    return std::unexpected(
                        "rolling_quantile: column must be numeric (Int or Float)");
                } else {
                    Column<double> result;
                    result.resize_for_overwrite(rows);
                    auto* result_values = result.data();
                    const auto* col_values = col.data();
                    std::optional<ValidityBitmap> out_valid;
                    std::vector<double> window;
                    for (std::size_t i = 0; i < rows; ++i) {
                        const std::size_t lo = win_lo(i);
                        // Collect finite, non-null values; flag any valid NaN.
                        window.clear();
                        bool has_nan = false;
                        for (std::size_t j = lo; j <= i; ++j) {
                            if (!valid_at(j))
                                continue;
                            auto v = static_cast<double>(col_values[j]);
                            if (std::isnan(v))
                                has_nan = true;
                            else
                                window.push_back(v);
                        }
                        if (has_nan) {
                            result_values[i] = std::numeric_limits<double>::quiet_NaN();
                            continue;
                        }
                        if (window.empty()) {
                            result_values[i] = 0.0;  // window of only nulls -> null
                            if (!out_valid)
                                out_valid.emplace(rows, true);
                            out_valid->set(i, false);
                            continue;
                        }
                        std::ranges::sort(window);
                        const std::size_t n = window.size();
                        const double idx = p * static_cast<double>(n - 1);
                        auto idx_lo = static_cast<std::size_t>(idx);
                        const std::size_t idx_hi = idx_lo + 1 < n ? idx_lo + 1 : idx_lo;
                        const double frac = idx - static_cast<double>(idx_lo);
                        result_values[i] =
                            window[idx_lo] + (frac * (window[idx_hi] - window[idx_lo]));
                    }
                    return ComputedColumn{.column = std::move(result),
                                          .validity = std::move(out_valid)};
                }
            },
            *src);
    }

    if (call.callee == "rolling_skew") {
        return std::visit(
            [&](const auto& col) -> std::expected<ComputedColumn, std::string> {
                using T = std::decay_t<decltype(col)>::value_type;
                if constexpr (!std::is_same_v<T, std::int64_t> && !std::is_same_v<T, double>) {
                    return std::unexpected("rolling_skew: column must be numeric (Int or Float)");
                } else {
                    Column<double> result;
                    result.resize_for_overwrite(rows);
                    auto* result_values = result.data();
                    const auto* col_values = col.data();
                    std::optional<ValidityBitmap> out_valid;
                    std::vector<double> window;
                    for (std::size_t i = 0; i < rows; ++i) {
                        const std::size_t lo = win_lo(i);
                        window.clear();
                        bool has_nan = false;
                        for (std::size_t j = lo; j <= i; ++j) {
                            if (!valid_at(j))
                                continue;
                            auto v = static_cast<double>(col_values[j]);
                            if (std::isnan(v))
                                has_nan = true;
                            else
                                window.push_back(v);
                        }
                        if (has_nan) {
                            result_values[i] = std::numeric_limits<double>::quiet_NaN();
                            continue;
                        }
                        if (window.empty()) {
                            result_values[i] = 0.0;  // window of only nulls -> null
                            if (!out_valid)
                                out_valid.emplace(rows, true);
                            out_valid->set(i, false);
                            continue;
                        }
                        const std::size_t n = window.size();
                        if (n < 3) {
                            result_values[i] = 0.0;
                            continue;
                        }
                        double mean = 0.0;
                        for (const double v : window)
                            mean += v;
                        mean /= static_cast<double>(n);
                        double m2 = 0.0;
                        double m3 = 0.0;
                        for (const double v : window) {
                            const double d = v - mean;
                            m2 += d * d;
                            m3 += d * d * d;
                        }
                        if (m2 == 0.0) {
                            result_values[i] = 0.0;
                        } else {
                            auto dn = static_cast<double>(n);
                            result_values[i] =
                                (dn * std::sqrt(dn - 1.0) / (dn - 2.0)) * (m3 / std::pow(m2, 1.5));
                        }
                    }
                    return ComputedColumn{.column = std::move(result),
                                          .validity = std::move(out_valid)};
                }
            },
            *src);
    }

    if (call.callee == "rolling_kurtosis") {
        return std::visit(
            [&](const auto& col) -> std::expected<ComputedColumn, std::string> {
                using T = std::decay_t<decltype(col)>::value_type;
                if constexpr (!std::is_same_v<T, std::int64_t> && !std::is_same_v<T, double>) {
                    return std::unexpected(
                        "rolling_kurtosis: column must be numeric (Int or Float)");
                } else {
                    Column<double> result;
                    result.resize_for_overwrite(rows);
                    auto* result_values = result.data();
                    const auto* col_values = col.data();
                    std::optional<ValidityBitmap> out_valid;
                    std::vector<double> window;
                    for (std::size_t i = 0; i < rows; ++i) {
                        const std::size_t lo = win_lo(i);
                        window.clear();
                        bool has_nan = false;
                        for (std::size_t j = lo; j <= i; ++j) {
                            if (!valid_at(j))
                                continue;
                            auto v = static_cast<double>(col_values[j]);
                            if (std::isnan(v))
                                has_nan = true;
                            else
                                window.push_back(v);
                        }
                        if (has_nan) {
                            result_values[i] = std::numeric_limits<double>::quiet_NaN();
                            continue;
                        }
                        if (window.empty()) {
                            result_values[i] = 0.0;  // window of only nulls -> null
                            if (!out_valid)
                                out_valid.emplace(rows, true);
                            out_valid->set(i, false);
                            continue;
                        }
                        const std::size_t n = window.size();
                        if (n < 4) {
                            result_values[i] = 0.0;
                            continue;
                        }
                        double mean = 0.0;
                        for (const double v : window)
                            mean += v;
                        mean /= static_cast<double>(n);
                        double m2 = 0.0;
                        double m4 = 0.0;
                        for (const double v : window) {
                            const double d = v - mean;
                            const double d2 = d * d;
                            m2 += d2;
                            m4 += d2 * d2;
                        }
                        if (m2 == 0.0) {
                            result_values[i] = 0.0;
                        } else {
                            auto dn = static_cast<double>(n);
                            // Fisher excess kurtosis (unbiased, matches scipy/pandas):
                            result_values[i] =
                                (dn - 1.0) / ((dn - 2.0) * (dn - 3.0)) *
                                (((dn + 1.0) * dn * m4 / (m2 * m2)) - (3.0 * (dn - 1.0)));
                        }
                    }
                    return ComputedColumn{.column = std::move(result),
                                          .validity = std::move(out_valid)};
                }
            },
            *src);
    }

    // rolling_first / rolling_last — the value at the window's leading edge
    // (win_lo, the first in-window row) or trailing edge (i, the current row).
    // Null-ness of the picked row carries through. Works for any non-categorical
    // column type (OHLC's open/close are the motivating case).
    if (call.callee == "rolling_first" || call.callee == "rolling_last") {
        const bool is_last = call.callee == "rolling_last";
        return std::visit(
            [&](const auto& col) -> std::expected<ComputedColumn, std::string> {
                using ColT = std::decay_t<decltype(col)>;
                if constexpr (std::is_same_v<ColT, Column<Categorical>>) {
                    return std::unexpected(call.callee + ": categorical columns are not supported");
                } else {
                    ColT result;
                    std::optional<ValidityBitmap> out_valid;
                    if constexpr (!std::is_same_v<ColT, Column<std::string>>) {
                        result.resize(rows);
                    }
                    std::size_t lo = 0;
                    for (std::size_t i = 0; i < rows; ++i) {
                        while (lo < i && should_drop(lo, i))
                            ++lo;
                        const std::size_t pick = is_last ? i : lo;
                        if constexpr (std::is_same_v<ColT, Column<std::string>>) {
                            result.push_back(col[pick]);
                        } else if constexpr (std::is_same_v<ColT, Column<bool>>) {
                            result.set(i, col[pick]);
                        } else {
                            result[i] = col[pick];
                        }
                        if (!valid_at(pick)) {
                            if (!out_valid) {
                                out_valid.emplace(rows, true);
                            }
                            out_valid->set(i, false);
                        }
                    }
                    return ComputedColumn{std::move(result), std::move(out_valid)};
                }
            },
            *src);
    }

    // rolling_min / rolling_max — monotonic deque, O(n) amortised.
    const bool is_min = call.callee == "rolling_min";
    return std::visit(
        [&](const auto& col) -> std::expected<ComputedColumn, std::string> {
            using ColT = std::decay_t<decltype(col)>;
            if constexpr (std::is_same_v<ColT, Column<Categorical>> ||
                          std::is_same_v<ColT, Column<std::string>>) {
                return std::unexpected(call.callee + ": string columns not supported");
            } else {
                using T = ColT::value_type;
                ColT result;
                // Unlike the other rolling kernels this one also instantiates for
                // Date/Timestamp/bool, which have no uninitialised resize.
                if constexpr (requires { result.resize_for_overwrite(rows); }) {
                    result.resize_for_overwrite(rows);
                } else {
                    result.resize(rows);
                }
                std::optional<ValidityBitmap> out_valid;
                // Unlike the other rolling kernels this one also instantiates
                // for `Column<bool>`, which is bit-packed and has no dense
                // `data()`. The numeric instantiations still get the hoisted
                // pointer; bool keeps the per-element path it has to use.
                T* result_values = nullptr;
                if constexpr (is_dense_column_v<ColT>) {
                    result_values = result.data();
                }
                auto set_at = [&](std::size_t idx, T value) {
                    if constexpr (is_dense_column_v<ColT>) {
                        result_values[idx] = value;
                    } else {
                        result[idx] = value;
                    }
                };
                // Array-backed monotonic deque of candidate-extremum row indices.
                // It reuses expired front slots, so memory tracks the live window
                // width instead of total input rows.
                try {
                    // `count_n + 1`, not `count_n`: row `i` is pushed BEFORE the
                    // window is trimmed below, so the deque transiently holds
                    // the previous window's candidates plus one. Reserving only
                    // the window width guarantees one reallocation on a
                    // power-of-two width, and is the bound an unchecked push
                    // would silently overrun — see the regression test.
                    IndexRingDeque dq(is_count ? std::min(count_n, rows) + 1 : 0);
                    std::size_t nan_cnt = 0;  // valid-but-NaN elements currently in window
                    std::size_t lo = 0;
                    for (std::size_t i = 0; i < rows; ++i) {
                        if (valid_at(i)) {
                            // Mutated below inside the `if constexpr (is_floating_point_v<T>)`
                            // branch; the checker misses mutations that only occur there.
                            // NOLINTNEXTLINE(misc-const-correctness)
                            bool is_nan_i = false;
                            if constexpr (std::is_floating_point_v<T>) {
                                is_nan_i = std::isnan(col[i]);
                            }
                            if (is_nan_i) {
                                ++nan_cnt;
                            } else {
                                while (!dq.empty() && (is_min ? (col[dq.back()] >= col[i])
                                                              : (col[dq.back()] <= col[i])))
                                    dq.pop_back();
                                dq.push_back(i);
                            }
                        }
                        while (lo < i && should_drop(lo, i)) {
                            if (valid_at(lo)) {
                                if constexpr (std::is_floating_point_v<T>) {
                                    if (std::isnan(col[lo]))
                                        --nan_cnt;
                                }
                            }
                            if (!dq.empty() && dq.front() == lo)
                                dq.pop_front();
                            ++lo;
                        }
                        if (nan_cnt > 0) {
                            // See rolling_sum: unreachable for Int, but the arm
                            // must write since the output is resized without
                            // initialisation.
                            if constexpr (std::is_floating_point_v<T>) {
                                set_at(i, std::numeric_limits<T>::quiet_NaN());
                            } else {
                                set_at(i, T{});
                            }
                        } else if (!dq.empty()) {
                            set_at(i, col[dq.front()]);
                        } else {
                            set_at(i, T{});  // window of only nulls -> null
                            if (!out_valid)
                                out_valid.emplace(rows, true);
                            out_valid->set(i, false);
                        }
                    }
                } catch (const std::length_error& e) {
                    return std::unexpected(std::string(e.what()));
                }
                return ComputedColumn{std::move(result), std::move(out_valid)};
            }
        },
        *src);
}

/// Start of the `dur_ns` bucket containing `nanos`, cut on LOCAL boundaries in
/// `zone`.
///
/// A day is not 86400 seconds everywhere: the two DST days are 23 and 25 hours
/// long, and several zones sit at :30 or :45 offsets, so even an hourly grid
/// differs from the UTC one. Bucketing on the UTC grid over zoned data
/// therefore answers a different question than the one asked -- for nycflights13
/// it moves 11% of departures into a different calendar day.
///
/// Converting the local boundary back uses `choose::earliest`, which resolves
/// the case where local midnight does not exist (Brazil used to start DST at
/// midnight, so the day began at 01:00) to the transition instant -- the moment
/// the day actually started.
///
/// The caches are `thread_local` rather than captured state because the
/// resample paths are threaded. Offsets change twice a year and consecutive
/// rows share a bucket, so both hit on nearly every row of sorted input.
#if defined(IBEX_HAS_STD_CHRONO_TIME_ZONES)
[[nodiscard]] auto zoned_bucket_start(const std::chrono::time_zone* zone, std::int64_t nanos,
                                      std::int64_t dur_ns) -> std::int64_t {
    thread_local const std::chrono::time_zone* cached_zone = nullptr;
    thread_local std::int64_t cached_begin = 0;
    thread_local std::int64_t cached_end = 0;
    thread_local std::int64_t cached_offset = 0;

    if (zone != cached_zone || nanos < cached_begin || nanos >= cached_end) {
        const std::chrono::sys_time<std::chrono::nanoseconds> instant{
            std::chrono::nanoseconds{nanos}};
        const std::chrono::sys_info info = zone->get_info(instant);
        cached_zone = zone;
        cached_offset = std::chrono::duration_cast<std::chrono::nanoseconds>(info.offset).count();
        cached_begin = info.begin.time_since_epoch() <= std::chrono::nanoseconds::min()
                           ? std::numeric_limits<std::int64_t>::min()
                           : std::chrono::duration_cast<std::chrono::nanoseconds>(
                                 info.begin.time_since_epoch())
                                 .count();
        cached_end =
            info.end.time_since_epoch() >= std::chrono::nanoseconds::max()
                ? std::numeric_limits<std::int64_t>::max()
                : std::chrono::duration_cast<std::chrono::nanoseconds>(info.end.time_since_epoch())
                      .count();
    }

    const std::int64_t local = nanos + cached_offset;
    std::int64_t q = local / dur_ns;
    if (local < 0 && local % dur_ns != 0) {
        --q;
    }
    const std::chrono::local_time<std::chrono::nanoseconds> boundary{
        std::chrono::nanoseconds{q * dur_ns}};
    return zone->to_sys(boundary, std::chrono::choose::earliest).time_since_epoch().count();
}
#endif

auto resample_table_impl(const Table& input, ir::Duration bucket_dur,
                         const std::vector<ir::ColumnRef>& extra_group_by,
                         const std::vector<ir::AggSpec>& aggregations)
    -> std::expected<Table, std::string> {
    if (!input.time_index().has_value())
        return std::unexpected("resample requires a TimeFrame — use as_timeframe() first");

    const std::string& ts_name = *input.time_index();
    const auto* ts_cv = input.find(ts_name);
    if (ts_cv == nullptr)
        return std::unexpected("resample: time index column '" + ts_name + "' not found");
    const auto* ts_col = std::get_if<Column<Timestamp>>(ts_cv);
    if (ts_col == nullptr)
        return std::unexpected("resample: time index must be a Timestamp column");

    const std::int64_t dur_ns = bucket_dur.count();
    if (dur_ns <= 0)
        return std::unexpected("resample: duration must be positive");

    // A zoned time index buckets on LOCAL boundaries; an unzoned one is an
    // instant with no wall clock attached, so it keeps the UTC grid (SPEC 2.4).
#if defined(IBEX_HAS_STD_CHRONO_TIME_ZONES)
    const std::chrono::time_zone* zone = nullptr;
#else
    std::optional<std::string_view> zone;
#endif
    if (const auto& zone_id = ts_col->meta().zone; zone_id.has_value()) {
        const std::string& name = zone_name(*zone_id);
        if (!is_known_zone(name)) {
            return std::unexpected("resample: unknown time zone '" + name + "' on time index '" +
                                   ts_name + "'");
        }
#if defined(IBEX_HAS_STD_CHRONO_TIME_ZONES)
        zone = std::chrono::locate_zone(name);
#else
        zone = name;
#endif
    }

    const auto rows = input.rows();
    const auto bucket_of = [&](std::size_t i) -> std::int64_t {
        const std::int64_t nanos = (*ts_col)[i].nanos;
#if defined(IBEX_HAS_STD_CHRONO_TIME_ZONES)
        if (zone != nullptr) {
            return zoned_bucket_start(zone, nanos, dur_ns);
        }
#else
        if (zone.has_value()) {
            return local_bucket_start(*zone, nanos, dur_ns);
        }
#endif
        std::int64_t q = nanos / dur_ns;
        if (nanos < 0 && nanos % dur_ns != 0)
            --q;  // floor for negative timestamps
        return q * dur_ns;
    };

    // Fast vectorised path: bucket-only grouping with simple numeric reducers
    // over non-null Int/Float columns. The time index is sorted, so each bucket
    // is a contiguous slice and per-bucket first/last/min/max/sum/mean/count
    // reduce with tight (auto-vectorising) loops — far cheaper than the generic
    // row-wise aggregate. Falls through for extra group-by, complex aggregates
    // (median/stddev/...), nullable inputs, or non-numeric columns.
    auto simple_resample = [&] -> std::optional<std::expected<Table, std::string>> {
        if (!extra_group_by.empty() || rows == 0) {
            return std::nullopt;
        }
        for (const auto& agg : aggregations) {
            switch (agg.func) {
                case ir::AggFunc::Sum:
                case ir::AggFunc::Mean:
                case ir::AggFunc::Min:
                case ir::AggFunc::Max:
                case ir::AggFunc::Count:
                case ir::AggFunc::First:
                case ir::AggFunc::Last:
                    break;
                default:
                    return std::nullopt;  // complex aggregate -> generic path
            }
            if (agg.func == ir::AggFunc::Count) {
                continue;
            }
            const auto* entry = input.find_entry(agg.column.name);
            if (entry == nullptr || entry->validity.has_value()) {
                return std::nullopt;  // missing or nullable -> generic path
            }
            const ColumnValue& cv = *entry->column;
            if (!std::holds_alternative<Column<std::int64_t>>(cv) &&
                !std::holds_alternative<Column<double>>(cv)) {
                return std::nullopt;  // non-numeric -> generic path
            }
        }

        // Bucket boundaries: starts[g] is the first row of bucket g; the trailing
        // sentinel `rows` closes the last bucket.
        std::vector<std::size_t> starts;
        std::vector<std::int64_t> bvals;
        starts.reserve(1024);
        bvals.reserve(1024);
        std::int64_t prev = 0;
        for (std::size_t i = 0; i < rows; ++i) {
            std::int64_t b = bucket_of(i);
            if (i == 0 || b != prev) {
                starts.push_back(i);
                bvals.push_back(b);
                prev = b;
            }
        }
        const std::size_t ng = bvals.size();
        starts.push_back(rows);

        Table out;
        Column<Timestamp> ts_out;
        ts_out.reserve(ng);
        for (std::int64_t b : bvals)
            ts_out.push_back(Timestamp{b});
        // Bucket starts are instants in the same zone as the index they came from.
        ts_out.set_meta(ts_col->meta());
        out.add_column(ts_name, std::move(ts_out));

        for (const auto& agg : aggregations) {
            if (agg.func == ir::AggFunc::Count) {
                Column<std::int64_t> c;
                c.reserve(ng);
                for (std::size_t g = 0; g < ng; ++g)
                    c.push_back(static_cast<std::int64_t>(starts[g + 1] - starts[g]));
                out.add_column(agg.alias, std::move(c));
                continue;
            }
            const ColumnValue& cv = *input.find_entry(agg.column.name)->column;
            std::visit(
                [&](const auto& src) {
                    using T = std::decay_t<decltype(src)>::value_type;
                    if constexpr (std::is_same_v<T, std::int64_t> || std::is_same_v<T, double>) {
                        const bool to_double = (agg.func == ir::AggFunc::Mean);
                        if (to_double) {
                            Column<double> c;
                            c.reserve(ng);
                            for (std::size_t g = 0; g < ng; ++g) {
                                const std::size_t lo = starts[g];
                                const std::size_t hi = starts[g + 1];
                                double acc = 0.0;
                                for (std::size_t j = lo; j < hi; ++j)
                                    acc += static_cast<double>(src[j]);
                                c.push_back(acc / static_cast<double>(hi - lo));
                            }
                            out.add_column(agg.alias, std::move(c));
                        } else {
                            Column<T> c;
                            c.reserve(ng);
                            for (std::size_t g = 0; g < ng; ++g) {
                                const std::size_t lo = starts[g];
                                const std::size_t hi = starts[g + 1];
                                T v = src[lo];
                                switch (agg.func) {
                                    case ir::AggFunc::First:
                                        break;
                                    case ir::AggFunc::Last:
                                        v = src[hi - 1];
                                        break;
                                    case ir::AggFunc::Min:
                                        for (std::size_t j = lo + 1; j < hi; ++j)
                                            v = std::min(v, src[j]);
                                        break;
                                    case ir::AggFunc::Max:
                                        for (std::size_t j = lo + 1; j < hi; ++j)
                                            v = std::max(v, src[j]);
                                        break;
                                    case ir::AggFunc::Sum: {
                                        T s = T{};
                                        for (std::size_t j = lo; j < hi; ++j)
                                            s += src[j];
                                        v = s;
                                        break;
                                    }
                                    default:
                                        break;
                                }
                                c.push_back(v);
                            }
                            out.add_column(agg.alias, std::move(c));
                        }
                    }
                },
                cv);
        }
        // Resample establishes the time index, and its bars come out in time
        // order -- `time_frame` states both, so neither can be set without the
        // other.
        apply_table_properties(out, TableProperties::time_frame(ts_name));
        return std::expected<Table, std::string>{std::move(out)};
    };
    if (auto fast = simple_resample(); fast.has_value()) {
        return std::move(*fast);
    }

    // Grouped vectorised path: the same trick as `simple_resample`, extended to
    // ONE extra group key -- which is the shape every market-data resample has
    // (`by symbol`).
    //
    // The generic path below builds a 5M-element `_bucket` column, hashes every
    // row on the composite `(_bucket, symbol)`, and drives a `std::vector<
    // AggSlot>` whose element carries two ScalarValues and a std::vector<double>
    // for median -- allocated per group per aggregate whether or not the
    // aggregate is `max`. Measured at 5M rows / 100 symbols / ~100 ticks per
    // bar: 58.9ms, against 19.9ms for the ungrouped fast path on the same data,
    // with AggSlot move/copy/growth alone ~15% of profile.
    //
    // This exploits the two facts the generic path throws away:
    //   * the time index is sorted, so a bucket is a CONTIGUOUS row range -- no
    //     bucket column and no hashing of it; and
    //   * the group key has few distinct values, so a bucket's accumulators are
    //     a dense array indexed by a factorized code, not a hash table.
    //
    // NOTE this is density-sensitive in a way worth stating: it wins because a
    // bar holds many rows. On a degenerate feed (~1 row per bar) there is
    // nothing to vectorise over and this saves nothing -- see
    // benchmarking/window_ohlc/README.md on tick density.
    auto grouped_simple_resample = [&] -> std::optional<std::expected<Table, std::string>> {
        // One key only. Multi-key is not the common shape and a composite code
        // would reintroduce exactly the per-row hashing this exists to avoid.
        if (extra_group_by.size() != 1 || rows == 0) {
            return std::nullopt;
        }
        const auto* key_entry = input.find_entry(extra_group_by[0].name);
        if (key_entry == nullptr || key_entry->validity.has_value()) {
            return std::nullopt;  // missing or nullable key -> generic path
        }
        for (const auto& agg : aggregations) {
            switch (agg.func) {
                case ir::AggFunc::Sum:
                case ir::AggFunc::Mean:
                case ir::AggFunc::Min:
                case ir::AggFunc::Max:
                case ir::AggFunc::Count:
                case ir::AggFunc::First:
                case ir::AggFunc::Last:
                    break;
                default:
                    return std::nullopt;
            }
            if (agg.func == ir::AggFunc::Count) {
                continue;
            }
            const auto* entry = input.find_entry(agg.column.name);
            if (entry == nullptr || entry->validity.has_value()) {
                return std::nullopt;
            }
            const ColumnValue& cv = *entry->column;
            if (!std::holds_alternative<Column<std::int64_t>>(cv) &&
                !std::holds_alternative<Column<double>>(cv)) {
                return std::nullopt;
            }
        }

        // Factorize the key. A Categorical arrives pre-factorized; a string or
        // integer key costs one dictionary pass. Above `kMaxCodes` a dense
        // per-bucket accumulator array stops being cache-resident and hashing
        // is the right structure again -- that case is the standing
        // high-cardinality group-by gap, not this path's business.
        constexpr std::size_t kMaxCodes = 4096;
        std::vector<std::int32_t> codes(rows);
        std::size_t ncodes = 0;
        const ColumnValue& key_cv = *key_entry->column;
        std::vector<std::string> str_dict;   // for String reconstruction
        std::vector<std::int64_t> int_dict;  // for Int64 reconstruction
        const Column<Categorical>* cat = std::get_if<Column<Categorical>>(&key_cv);
        if (cat != nullptr) {
            ncodes = cat->dictionary().size();
            if (ncodes > kMaxCodes) {
                return std::nullopt;
            }
            for (std::size_t i = 0; i < rows; ++i) {
                codes[i] = cat->code_at(i);
            }
        } else if (const auto* s = std::get_if<Column<std::string>>(&key_cv)) {
            robin_hood::unordered_map<std::string, std::int32_t> idx;
            for (std::size_t i = 0; i < rows; ++i) {
                auto [it, inserted] = idx.emplace((*s)[i], static_cast<std::int32_t>(ncodes));
                if (inserted) {
                    str_dict.emplace_back((*s)[i]);  // string_view -> string: direct-init
                    if (++ncodes > kMaxCodes) {
                        return std::nullopt;
                    }
                }
                codes[i] = it->second;
            }
        } else if (const auto* n = std::get_if<Column<std::int64_t>>(&key_cv)) {
            robin_hood::unordered_map<std::int64_t, std::int32_t> idx;
            for (std::size_t i = 0; i < rows; ++i) {
                auto [it, inserted] = idx.emplace((*n)[i], static_cast<std::int32_t>(ncodes));
                if (inserted) {
                    int_dict.push_back((*n)[i]);
                    if (++ncodes > kMaxCodes) {
                        return std::nullopt;
                    }
                }
                codes[i] = it->second;
            }
        } else {
            return std::nullopt;  // key type this path cannot factorize
        }

        // Bucket boundaries: contiguous because the time index is sorted.
        std::vector<std::size_t> starts;
        std::vector<std::int64_t> bvals;
        starts.reserve(1024);
        bvals.reserve(1024);
        std::int64_t prev = 0;
        for (std::size_t i = 0; i < rows; ++i) {
            const std::int64_t b = bucket_of(i);
            if (i == 0 || b != prev) {
                starts.push_back(i);
                bvals.push_back(b);
                prev = b;
            }
        }
        const std::size_t nb = bvals.size();
        starts.push_back(rows);

        // One pass to lay out the output: which (bucket, code) pairs exist, in
        // (bucket asc, first appearance within the bucket) order, and which
        // output row each input row feeds. `first_row`/`last_row` make First
        // and Last pure gathers over the OUTPUT -- no pass over the input at
        // all -- which for an OHLC query removes two of the five scans.
        std::vector<std::uint32_t> row_out(rows);
        std::vector<std::int64_t> out_ts;
        std::vector<std::int32_t> out_code;
        std::vector<std::size_t> first_row;
        std::vector<std::size_t> last_row;
        std::vector<std::int64_t> out_count;
        std::vector<std::int32_t> slot_of(ncodes, -1);
        std::vector<std::int32_t> touched;
        touched.reserve(std::min<std::size_t>(ncodes, 256));
        for (std::size_t g = 0; g < nb; ++g) {
            const std::size_t lo = starts[g];
            const std::size_t hi = starts[g + 1];
            for (std::size_t i = lo; i < hi; ++i) {
                const std::int32_t c = codes[i];
                std::int32_t slot = slot_of[static_cast<std::size_t>(c)];
                if (slot < 0) {
                    slot = static_cast<std::int32_t>(out_ts.size());
                    slot_of[static_cast<std::size_t>(c)] = slot;
                    touched.push_back(c);
                    out_ts.push_back(bvals[g]);
                    out_code.push_back(c);
                    first_row.push_back(i);
                    last_row.push_back(i);
                    out_count.push_back(0);
                }
                row_out[i] = static_cast<std::uint32_t>(slot);
                last_row[static_cast<std::size_t>(slot)] = i;
                ++out_count[static_cast<std::size_t>(slot)];
            }
            for (const std::int32_t c : touched) {
                slot_of[static_cast<std::size_t>(c)] = -1;
            }
            touched.clear();
        }
        const std::size_t n_out = out_ts.size();

        Table out;
        Column<Timestamp> ts_out;
        ts_out.reserve(n_out);
        for (const std::int64_t b : out_ts) {
            ts_out.push_back(Timestamp{b});
        }
        // Bucket starts are instants in the same zone as the index they came from.
        ts_out.set_meta(ts_col->meta());
        out.add_column(ts_name, std::move(ts_out));

        // Rebuild the key column in its ORIGINAL type -- the generic path emits
        // the key as it found it, and a resample that silently changed a
        // Categorical into a String would be a schema change, not a speedup.
        if (cat != nullptr) {
            std::vector<Column<Categorical>::code_type> out_codes(out_code.begin(), out_code.end());
            out.add_column(
                extra_group_by[0].name,
                Column<Categorical>{cat->dictionary_ptr(), cat->index_ptr(), std::move(out_codes)});
        } else if (!str_dict.empty()) {
            Column<std::string> key_out;
            key_out.reserve(n_out);
            for (const std::int32_t c : out_code) {
                key_out.push_back(str_dict[static_cast<std::size_t>(c)]);
            }
            out.add_column(extra_group_by[0].name, std::move(key_out));
        } else {
            Column<std::int64_t> key_out;
            key_out.reserve(n_out);
            for (const std::int32_t c : out_code) {
                key_out.push_back(int_dict[static_cast<std::size_t>(c)]);
            }
            out.add_column(extra_group_by[0].name, std::move(key_out));
        }

        for (const auto& agg : aggregations) {
            if (agg.func == ir::AggFunc::Count) {
                Column<std::int64_t> c;
                c.reserve(n_out);
                for (const std::int64_t v : out_count) {
                    c.push_back(v);
                }
                out.add_column(agg.alias, std::move(c));
                continue;
            }
            const ColumnValue& cv = *input.find_entry(agg.column.name)->column;
            std::visit(
                [&](const auto& src) {
                    using T = std::decay_t<decltype(src)>::value_type;
                    if constexpr (std::is_same_v<T, std::int64_t> || std::is_same_v<T, double>) {
                        if (agg.func == ir::AggFunc::First || agg.func == ir::AggFunc::Last) {
                            const auto& pick =
                                agg.func == ir::AggFunc::First ? first_row : last_row;
                            Column<T> c;
                            c.reserve(n_out);
                            for (std::size_t o = 0; o < n_out; ++o) {
                                c.push_back(src[pick[o]]);
                            }
                            out.add_column(agg.alias, std::move(c));
                            return;
                        }
                        if (agg.func == ir::AggFunc::Mean) {
                            std::vector<double> acc(n_out, 0.0);
                            for (std::size_t i = 0; i < rows; ++i) {
                                acc[row_out[i]] += static_cast<double>(src[i]);
                            }
                            Column<double> c;
                            c.reserve(n_out);
                            for (std::size_t o = 0; o < n_out; ++o) {
                                c.push_back(acc[o] / static_cast<double>(out_count[o]));
                            }
                            out.add_column(agg.alias, std::move(c));
                            return;
                        }
                        const T init = agg.func == ir::AggFunc::Min ? std::numeric_limits<T>::max()
                                       : agg.func == ir::AggFunc::Max
                                           ? std::numeric_limits<T>::lowest()
                                           : T{};
                        std::vector<T> acc(n_out, init);
                        switch (agg.func) {
                            case ir::AggFunc::Min:
                                for (std::size_t i = 0; i < rows; ++i) {
                                    T& a = acc[row_out[i]];
                                    a = std::min(a, src[i]);
                                }
                                break;
                            case ir::AggFunc::Max:
                                for (std::size_t i = 0; i < rows; ++i) {
                                    T& a = acc[row_out[i]];
                                    a = std::max(a, src[i]);
                                }
                                break;
                            default:  // Sum
                                for (std::size_t i = 0; i < rows; ++i) {
                                    acc[row_out[i]] += src[i];
                                }
                                break;
                        }
                        Column<T> c;
                        c.reserve(n_out);
                        for (std::size_t o = 0; o < n_out; ++o) {
                            c.push_back(acc[o]);
                        }
                        out.add_column(agg.alias, std::move(c));
                    }
                },
                cv);
        }
        // Resample establishes the time index, and its bars come out in time
        // order -- `time_frame` states both, so neither can be set without the
        // other.
        apply_table_properties(out, TableProperties::time_frame(ts_name));
        return std::expected<Table, std::string>{std::move(out)};
    };
    if (auto fast = grouped_simple_resample(); fast.has_value()) {
        return std::move(*fast);
    }

    // Build bucket column: floor(ts.nanos / dur_ns) * dur_ns
    Column<std::int64_t> bucket_col;
    bucket_col.reserve(rows);
    for (std::size_t i = 0; i < rows; ++i) {
        const std::int64_t nanos = (*ts_col)[i].nanos;
        std::int64_t q = nanos / dur_ns;
        if (nanos < 0 && nanos % dur_ns != 0)
            --q;  // floor for negative timestamps
        bucket_col.push_back(q * dur_ns);
    }

    // Clone input, add _bucket column
    Table temp = input;
    temp.add_column("_bucket", std::move(bucket_col));

    // Prepend _bucket to the group-by list
    std::vector<ir::ColumnRef> full_group_by;
    full_group_by.push_back(ir::ColumnRef{.name = "_bucket"});
    full_group_by.insert(full_group_by.end(), extra_group_by.begin(), extra_group_by.end());

    // Run standard aggregation
    auto result = aggregate_table(temp, full_group_by, aggregations);
    if (!result.has_value())
        return result;

    // Convert _bucket (int64) → Timestamp, rename to ts_name
    Table& out = result.value();
    auto it = out.index.find("_bucket");
    if (it == out.index.end())
        return std::unexpected("resample: internal error — _bucket missing from output");
    const std::size_t pos = it->second;

    const auto& i64_col = std::get<Column<std::int64_t>>(*out.columns[pos].column);
    Column<Timestamp> ts_out;
    ts_out.reserve(i64_col.size());
    for (auto v : i64_col)
        ts_out.push_back(Timestamp{v});
    // Bucket starts are instants in the same zone as the index they came from.
    ts_out.set_meta(ts_col->meta());

    out.rename_column(pos, ts_name);
    out.replace_column(pos, ColumnValue{std::move(ts_out)});
    apply_table_properties(out, TableProperties::time_frame(ts_name));

    return out;
}

/// Public entry: run the resample, then tag the result with the grouping.
///
/// The tag lives here rather than inside the implementation because that
/// function has three separate table-returning exits (an ungrouped fast path, a
/// grouped fast path, and the generic aggregate). Tagging at each is one `return`
/// away from a silent gap -- which is exactly how the first attempt at this
/// missed the grouped fast path.
auto resample_table(const Table& input, ir::Duration bucket_dur,
                    const std::vector<ir::ColumnRef>& extra_group_by,
                    const std::vector<ir::AggSpec>& aggregations)
    -> std::expected<Table, std::string> {
    auto result = resample_table_impl(input, bucket_dur, extra_group_by, aggregations);
    if (!result.has_value() || extra_group_by.empty()) {
        return result;
    }
    Table& out = result.value();
    // Record that `extra_group_by` partitions these rows. Unlike a grouped
    // window this output is time-major with the groups INTERLEAVED, so an
    // unpartitioned lag/lead downstream reads into another group on every row
    // rather than only at a run boundary -- the more destructive of the two
    // shapes, and the easier to reach by accident since resampling is usually
    // the first thing a pipeline does.
    //
    // Only claim it when two rows actually share a bucket timestamp. With a
    // single group the output is one row per bucket, nothing interleaves, and
    // an unpartitioned lead over it is correct.
    if (!out.time_index().has_value() || out.rows() < 2) {
        return result;
    }
    const auto* ts = out.find(*out.time_index());
    const auto* stamps = ts != nullptr ? std::get_if<Column<Timestamp>>(ts) : nullptr;
    if (stamps == nullptr) {
        return result;
    }
    for (std::size_t i = 1; i < stamps->size(); ++i) {
        if ((*stamps)[i].nanos == (*stamps)[i - 1].nanos) {
            std::vector<std::string> keys;
            keys.reserve(extra_group_by.size());
            for (const auto& key : extra_group_by) {
                keys.push_back(key.name);
            }
            out.set_properties(out.properties().with_grouping(std::move(keys)));
            break;
        }
    }
    return result;
}

}  // namespace ibex::runtime
