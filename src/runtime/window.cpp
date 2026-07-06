// window.cpp — time-window operators: rolling aggregates over duration
// windows (apply_rolling_func) and timeframe resampling (resample_table).
// Split out of interpreter.cpp; shared declarations live in interpreter_internal.hpp.

#include <ibex/runtime/interpreter.hpp>

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <limits>
#include <optional>
#include <set>
#include <type_traits>
#include <unordered_map>
#include <vector>

#if defined(__AVX2__) || defined(__BMI2__)
#include <immintrin.h>
#endif

#include "interpreter_internal.hpp"

namespace ibex::runtime {

namespace {

// Find the first row index lo in [0, row] where time[lo] >= time[row] - duration.
// The time index column must be Timestamp or Date and sorted ascending.
auto window_lo(const ColumnValue& time_col, std::size_t row, ir::Duration duration) -> std::size_t {
    if (const auto* ts_col = std::get_if<Column<Timestamp>>(&time_col)) {
        std::int64_t threshold = (*ts_col)[row].nanos - duration.count();
        std::size_t lo = 0;
        std::size_t hi = row;
        while (lo < hi) {
            std::size_t mid = lo + ((hi - lo) / 2);
            if ((*ts_col)[mid].nanos < threshold) {
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
        std::size_t mid = lo + ((hi - lo) / 2);
        if (date_col[mid].days < threshold) {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    return lo;
}

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
auto apply_rolling_func(const ir::CallExpr& call, const Table& table, ir::Duration duration)
    -> std::expected<ComputedColumn, std::string> {
    const auto& time_col = *table.find(*table.time_index);
    std::size_t rows = table.rows();

    // Map the time column to a contiguous int64 array and express duration in the same unit.
    // Timestamp: nanoseconds (layout-compatible with int64, no copy needed).
    // Date: days (int32, must be widened into a temporary buffer).
    const std::int64_t* time_vals = nullptr;
    std::vector<std::int64_t> time_vals_buf;  // only allocated for the Date path
    std::int64_t dur_val = 0;
    if (const auto* ts_col = std::get_if<Column<Timestamp>>(&time_col)) {
        // Timestamp is {int64_t nanos} — pointer-cast avoids an 8 MB copy.
        static_assert(sizeof(Timestamp) == sizeof(std::int64_t) &&
                      alignof(Timestamp) == alignof(std::int64_t));
        // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
        time_vals = reinterpret_cast<const std::int64_t*>(ts_col->data());
        dur_val = duration.count();
    } else {
        const auto& date_col = std::get<Column<Date>>(time_col);
        time_vals_buf.resize(rows);
        for (std::size_t i = 0; i < rows; ++i)
            time_vals_buf[i] = date_col[i].days;
        time_vals = time_vals_buf.data();
        static constexpr std::int64_t kNsPerDay = 86'400'000'000'000LL;
        dur_val = duration.count() / kNsPerDay;
    }

    // Two-pointer helper: advances lo to the first row still inside the window.
    // Because the TimeFrame is sorted ascending, lo never decreases across rows.
    auto advance_lo = [&](std::size_t& lo, std::size_t i) {
        std::int64_t threshold = time_vals[i] - dur_val;
        while (lo < i && time_vals[lo] < threshold)
            ++lo;
    };

    if (call.callee == "rolling_count") {
        Column<std::int64_t> result;
        result.resize(rows);
        std::size_t lo = 0;
        for (std::size_t i = 0; i < rows; ++i) {
            advance_lo(lo, i);
            result[i] = static_cast<std::int64_t>(i - lo + 1);
        }
        return ComputedColumn{std::move(result), std::nullopt};
    }

    if (call.args.empty()) {
        return std::unexpected(call.callee + ": expected column argument");
    }
    const auto* col_ref = std::get_if<ir::ColumnRef>(&call.args[0]->node);
    if (!col_ref) {
        return std::unexpected(call.callee + ": argument must be a column name");
    }
    const auto* src = table.find(col_ref->name);
    if (!src) {
        return std::unexpected(call.callee + ": unknown column '" + col_ref->name + "'");
    }
    // Source validity: nullptr means every element is valid (the common case).
    // valid_at(j) is false only for true NULLs — never read col[j] when false.
    const auto* src_entry = table.find_entry(col_ref->name);
    const ValidityBitmap* sv =
        (src_entry != nullptr && src_entry->validity.has_value()) ? &*src_entry->validity : nullptr;
    auto valid_at = [sv](std::size_t j) noexcept -> bool {
        return sv == nullptr || (*sv)[j];
    };

    if (call.callee == "rolling_mean") {
        return std::visit(
            [&](const auto& col) -> std::expected<ComputedColumn, std::string> {
                using T = std::decay_t<decltype(col)>::value_type;
                if constexpr (!std::is_same_v<T, std::int64_t> && !std::is_same_v<T, double>) {
                    return std::unexpected("rolling_mean: column must be numeric (Int or Float)");
                } else {
                    Column<double> result;
                    result.resize(rows);
                    std::optional<ValidityBitmap> out_valid;  // built lazily on first null result
                    double sum = 0.0;
                    std::size_t val_cnt = 0;  // finite, non-null elements in the window
                    std::size_t nan_cnt = 0;  // valid-but-NaN elements in the window
                    auto add = [&](std::size_t j) {
                        if (!valid_at(j))
                            return;  // NULL: skip, payload undefined
                        auto v = static_cast<double>(col[j]);
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
                        auto v = static_cast<double>(col[j]);
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
                        std::int64_t threshold = time_vals[i] - dur_val;
                        while (lo < i && time_vals[lo] < threshold) {
                            drop(lo);
                            ++lo;
                        }
                        if (nan_cnt > 0) {
                            result[i] = std::numeric_limits<double>::quiet_NaN();
                        } else if (val_cnt > 0) {
                            result[i] = sum / static_cast<double>(val_cnt);
                        } else {
                            result[i] = 0.0;  // window of only nulls -> null
                            if (!out_valid)
                                out_valid.emplace(rows, true);
                            out_valid->set(i, false);
                        }
                    }
                    return ComputedColumn{std::move(result), std::move(out_valid)};
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
                    result.resize(rows);
                    std::optional<ValidityBitmap> out_valid;
                    T sum{};
                    std::size_t val_cnt = 0;  // non-null, non-NaN elements in window
                    std::size_t nan_cnt = 0;  // valid-but-NaN (Float only; Int never NaN)
                    auto add = [&](std::size_t j) {
                        if (!valid_at(j))
                            return;
                        if constexpr (std::is_floating_point_v<T>) {
                            if (std::isnan(col[j])) {
                                ++nan_cnt;
                                return;
                            }
                        }
                        sum += col[j];
                        ++val_cnt;
                    };
                    auto drop = [&](std::size_t j) {
                        if (!valid_at(j))
                            return;
                        if constexpr (std::is_floating_point_v<T>) {
                            if (std::isnan(col[j])) {
                                --nan_cnt;
                                return;
                            }
                        }
                        sum -= col[j];
                        --val_cnt;
                    };
                    std::size_t lo = 0;
                    for (std::size_t i = 0; i < rows; ++i) {
                        add(i);
                        std::int64_t threshold = time_vals[i] - dur_val;
                        while (lo < i && time_vals[lo] < threshold) {
                            drop(lo);
                            ++lo;
                        }
                        if (nan_cnt > 0) {
                            // Only reachable for Float columns (Int has no NaN).
                            if constexpr (std::is_floating_point_v<T>)
                                result[i] = std::numeric_limits<T>::quiet_NaN();
                        } else if (val_cnt > 0) {
                            result[i] = sum;
                        } else {
                            result[i] = T{};  // window of only nulls -> null
                            if (!out_valid)
                                out_valid.emplace(rows, true);
                            out_valid->set(i, false);
                        }
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
                    result.resize(rows);
                    std::optional<ValidityBitmap> out_valid;
                    std::size_t nan_cnt = 0;  // valid-but-NaN values in window
                    // Only finite, non-null values enter the multisets; NULLs are
                    // skipped and NaNs counted (NaN can't participate in an ordered
                    // structure and would corrupt the median).
                    auto add = [&](std::size_t j) {
                        if (!valid_at(j))
                            return;
                        auto v = static_cast<double>(col[j]);
                        if (std::isnan(v))
                            ++nan_cnt;
                        else
                            insert_val(v);
                    };
                    auto drop = [&](std::size_t j) {
                        if (!valid_at(j))
                            return;
                        auto v = static_cast<double>(col[j]);
                        if (std::isnan(v))
                            --nan_cnt;
                        else
                            remove_val(v);
                    };
                    std::size_t lo_ptr = 0;
                    for (std::size_t i = 0; i < rows; ++i) {
                        add(i);
                        std::int64_t threshold = time_vals[i] - dur_val;
                        while (lo_ptr < i && time_vals[lo_ptr] < threshold) {
                            drop(lo_ptr);
                            ++lo_ptr;
                        }
                        if (nan_cnt > 0) {
                            result[i] = std::numeric_limits<double>::quiet_NaN();
                        } else if (lo.empty() && hi.empty()) {
                            result[i] = 0.0;  // window of only nulls -> null
                            if (!out_valid)
                                out_valid.emplace(rows, true);
                            out_valid->set(i, false);
                        } else {
                            result[i] = (lo.size() > hi.size())
                                            ? static_cast<double>(*lo.rbegin())
                                            : (*lo.rbegin() + *hi.begin()) / 2.0;
                        }
                    }
                    return ComputedColumn{std::move(result), std::move(out_valid)};
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
                    result.resize(rows);
                    std::optional<ValidityBitmap> out_valid;
                    double mean = 0.0;
                    double m2 = 0.0;
                    std::size_t cnt = 0;      // finite, non-null values in window
                    std::size_t nan_cnt = 0;  // valid-but-NaN values in window
                    auto add = [&](std::size_t j) {
                        if (!valid_at(j))
                            return;
                        auto x = static_cast<double>(col[j]);
                        if (std::isnan(x)) {
                            ++nan_cnt;
                            return;
                        }
                        ++cnt;
                        double delta = x - mean;
                        mean += delta / static_cast<double>(cnt);
                        m2 += delta * (x - mean);
                    };
                    auto drop = [&](std::size_t j) {
                        if (!valid_at(j))
                            return;
                        auto y = static_cast<double>(col[j]);
                        if (std::isnan(y)) {
                            --nan_cnt;
                            return;
                        }
                        double mean_old = mean;
                        --cnt;
                        mean = cnt == 0 ? 0.0
                                        : (((static_cast<double>(cnt) + 1.0) * mean_old) - y) /
                                              static_cast<double>(cnt);
                        m2 -= (y - mean_old) * (y - mean);
                    };
                    std::size_t lo = 0;
                    for (std::size_t i = 0; i < rows; ++i) {
                        add(i);
                        std::int64_t threshold = time_vals[i] - dur_val;
                        while (lo < i && time_vals[lo] < threshold) {
                            drop(lo);
                            ++lo;
                        }
                        if (nan_cnt > 0) {
                            result[i] = std::numeric_limits<double>::quiet_NaN();
                        } else if (cnt == 0) {
                            result[i] = 0.0;  // window of only nulls -> null
                            if (!out_valid)
                                out_valid.emplace(rows, true);
                            out_valid->set(i, false);
                        } else {
                            // Clamp away tiny negative m2 from floating-point drift.
                            result[i] =
                                cnt < 2
                                    ? 0.0
                                    : std::sqrt(std::max(0.0, m2) / static_cast<double>(cnt - 1));
                        }
                    }
                    return ComputedColumn{std::move(result), std::move(out_valid)};
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
                    //   result[i] = alpha*R_i + (1-alpha)*beta^(i-lo)*col[lo]
                    // with R_i = sum_{j=lo..i} beta^(i-j)*col[j], maintained as
                    //   add right:  R = beta*R + col[i]
                    //   drop left:  R -= beta^(i-lo)*col[lo]
                    // reproducing the from-scratch O(n*w) recurrence in one pass.
                    // beta_pow caches beta^k (k bounded by the window width).
                    //
                    // Null handling: the weights are position-based, so a null
                    // can't be skipped without renumbering the whole window.
                    // Instead a null contributes 0 (a "no-return" tick) — its
                    // payload is never read. (NaN still propagates through the
                    // recurrence; genuine NaNs in an EWMA input are out of scope.)
                    const double beta = 1.0 - alpha;
                    auto val = [&](std::size_t j) -> double {
                        return valid_at(j) ? static_cast<double>(col[j]) : 0.0;
                    };
                    Column<double> result;
                    result.resize(rows);
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
                        r = (beta * r) + val(i);  // add col[i] at weight 1
                        std::int64_t threshold = time_vals[i] - dur_val;
                        while (lo < i && time_vals[lo] < threshold) {
                            r -= bpow(i - lo) * val(lo);
                            ++lo;
                        }
                        result[i] = (alpha * r) + ((1.0 - alpha) * bpow(i - lo) * val(lo));
                    }
                    return ComputedColumn{std::move(result), std::nullopt};
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
                    result.resize(rows);
                    std::optional<ValidityBitmap> out_valid;
                    std::vector<double> window;
                    for (std::size_t i = 0; i < rows; ++i) {
                        std::size_t lo = window_lo(time_col, i, duration);
                        // Collect finite, non-null values; flag any valid NaN.
                        window.clear();
                        bool has_nan = false;
                        for (std::size_t j = lo; j <= i; ++j) {
                            if (!valid_at(j))
                                continue;
                            auto v = static_cast<double>(col[j]);
                            if (std::isnan(v))
                                has_nan = true;
                            else
                                window.push_back(v);
                        }
                        if (has_nan) {
                            result[i] = std::numeric_limits<double>::quiet_NaN();
                            continue;
                        }
                        if (window.empty()) {
                            result[i] = 0.0;  // window of only nulls -> null
                            if (!out_valid)
                                out_valid.emplace(rows, true);
                            out_valid->set(i, false);
                            continue;
                        }
                        std::ranges::sort(window);
                        std::size_t n = window.size();
                        double idx = p * static_cast<double>(n - 1);
                        auto idx_lo = static_cast<std::size_t>(idx);
                        std::size_t idx_hi = idx_lo + 1 < n ? idx_lo + 1 : idx_lo;
                        double frac = idx - static_cast<double>(idx_lo);
                        result[i] = window[idx_lo] + (frac * (window[idx_hi] - window[idx_lo]));
                    }
                    return ComputedColumn{std::move(result), std::move(out_valid)};
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
                    result.resize(rows);
                    std::optional<ValidityBitmap> out_valid;
                    std::vector<double> window;
                    for (std::size_t i = 0; i < rows; ++i) {
                        std::size_t lo = window_lo(time_col, i, duration);
                        window.clear();
                        bool has_nan = false;
                        for (std::size_t j = lo; j <= i; ++j) {
                            if (!valid_at(j))
                                continue;
                            auto v = static_cast<double>(col[j]);
                            if (std::isnan(v))
                                has_nan = true;
                            else
                                window.push_back(v);
                        }
                        if (has_nan) {
                            result[i] = std::numeric_limits<double>::quiet_NaN();
                            continue;
                        }
                        if (window.empty()) {
                            result[i] = 0.0;  // window of only nulls -> null
                            if (!out_valid)
                                out_valid.emplace(rows, true);
                            out_valid->set(i, false);
                            continue;
                        }
                        std::size_t n = window.size();
                        if (n < 3) {
                            result[i] = 0.0;
                            continue;
                        }
                        double mean = 0.0;
                        for (double v : window)
                            mean += v;
                        mean /= static_cast<double>(n);
                        double m2 = 0.0;
                        double m3 = 0.0;
                        for (double v : window) {
                            double d = v - mean;
                            m2 += d * d;
                            m3 += d * d * d;
                        }
                        if (m2 == 0.0) {
                            result[i] = 0.0;
                        } else {
                            auto dn = static_cast<double>(n);
                            result[i] =
                                (dn * std::sqrt(dn - 1.0) / (dn - 2.0)) * (m3 / std::pow(m2, 1.5));
                        }
                    }
                    return ComputedColumn{std::move(result), std::move(out_valid)};
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
                    result.resize(rows);
                    std::optional<ValidityBitmap> out_valid;
                    std::vector<double> window;
                    for (std::size_t i = 0; i < rows; ++i) {
                        std::size_t lo = window_lo(time_col, i, duration);
                        window.clear();
                        bool has_nan = false;
                        for (std::size_t j = lo; j <= i; ++j) {
                            if (!valid_at(j))
                                continue;
                            auto v = static_cast<double>(col[j]);
                            if (std::isnan(v))
                                has_nan = true;
                            else
                                window.push_back(v);
                        }
                        if (has_nan) {
                            result[i] = std::numeric_limits<double>::quiet_NaN();
                            continue;
                        }
                        if (window.empty()) {
                            result[i] = 0.0;  // window of only nulls -> null
                            if (!out_valid)
                                out_valid.emplace(rows, true);
                            out_valid->set(i, false);
                            continue;
                        }
                        std::size_t n = window.size();
                        if (n < 4) {
                            result[i] = 0.0;
                            continue;
                        }
                        double mean = 0.0;
                        for (double v : window)
                            mean += v;
                        mean /= static_cast<double>(n);
                        double m2 = 0.0;
                        double m4 = 0.0;
                        for (double v : window) {
                            double d = v - mean;
                            double d2 = d * d;
                            m2 += d2;
                            m4 += d2 * d2;
                        }
                        if (m2 == 0.0) {
                            result[i] = 0.0;
                        } else {
                            auto dn = static_cast<double>(n);
                            // Fisher excess kurtosis (unbiased, matches scipy/pandas):
                            result[i] = (dn - 1.0) / ((dn - 2.0) * (dn - 3.0)) *
                                        (((dn + 1.0) * dn * m4 / (m2 * m2)) - (3.0 * (dn - 1.0)));
                        }
                    }
                    return ComputedColumn{std::move(result), std::move(out_valid)};
                }
            },
            *src);
    }

    // rolling_min / rolling_max — O(n·w), monotonic deque not yet implemented.
    bool is_min = call.callee == "rolling_min";
    return std::visit(
        [&](const auto& col) -> std::expected<ComputedColumn, std::string> {
            using ColT = std::decay_t<decltype(col)>;
            if constexpr (std::is_same_v<ColT, Column<Categorical>> ||
                          std::is_same_v<ColT, Column<std::string>>) {
                return std::unexpected(call.callee + ": string columns not supported");
            } else {
                using T = ColT::value_type;
                ColT result;
                result.resize(rows);
                std::optional<ValidityBitmap> out_valid;
                for (std::size_t i = 0; i < rows; ++i) {
                    std::size_t win_lo = window_lo(time_col, i, duration);
                    T best{};
                    bool seen = false;
                    bool has_nan = false;
                    for (std::size_t j = win_lo; j <= i; ++j) {
                        if (!valid_at(j))
                            continue;  // NULL: skip, payload undefined
                        if constexpr (std::is_floating_point_v<T>) {
                            if (std::isnan(col[j])) {
                                has_nan = true;
                                continue;
                            }
                        }
                        if (!seen || (is_min ? (col[j] < best) : (col[j] > best))) {
                            best = col[j];
                            seen = true;
                        }
                    }
                    if (has_nan) {
                        if constexpr (std::is_floating_point_v<T>)
                            result[i] = std::numeric_limits<T>::quiet_NaN();
                    } else if (seen) {
                        result[i] = best;
                    } else {
                        result[i] = T{};  // window of only nulls -> null
                        if (!out_valid)
                            out_valid.emplace(rows, true);
                        out_valid->set(i, false);
                    }
                }
                return ComputedColumn{std::move(result), std::move(out_valid)};
            }
        },
        *src);
}

auto resample_table(const Table& input, ir::Duration bucket_dur,
                    const std::vector<ir::ColumnRef>& extra_group_by,
                    const std::vector<ir::AggSpec>& aggregations)
    -> std::expected<Table, std::string> {
    if (!input.time_index.has_value())
        return std::unexpected("resample requires a TimeFrame — use as_timeframe() first");

    const std::string& ts_name = *input.time_index;
    const auto* ts_cv = input.find(ts_name);
    if (ts_cv == nullptr)
        return std::unexpected("resample: time index column '" + ts_name + "' not found");
    const auto* ts_col = std::get_if<Column<Timestamp>>(ts_cv);
    if (ts_col == nullptr)
        return std::unexpected("resample: time index must be a Timestamp column");

    const std::int64_t dur_ns = bucket_dur.count();
    if (dur_ns <= 0)
        return std::unexpected("resample: duration must be positive");

    const auto rows = input.rows();
    const auto bucket_of = [&](std::size_t i) -> std::int64_t {
        const std::int64_t nanos = (*ts_col)[i].nanos;
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
        out.time_index = ts_name;
        return std::expected<Table, std::string>{std::move(out)};
    };
    if (auto fast = simple_resample(); fast.has_value()) {
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

    out.rename_column(pos, ts_name);
    out.replace_column(pos, ColumnValue{std::move(ts_out)});
    out.time_index = ts_name;

    return out;
}

}  // namespace ibex::runtime
