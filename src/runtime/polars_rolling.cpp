// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen
#include "polars_rolling.hpp"

#include <algorithm>
#include <bit>
#include <cmath>
#include <limits>
#include <type_traits>

#include "runtime_internal.hpp"

namespace ibex::runtime {
namespace {

// Polars uses separate Kahan errors for additions and removals. Non-finite
// values participate in the sample count but must age out without poisoning
// subsequent windows.
template <typename T>
struct FloatingTotal {
    T total{}, add_error{}, remove_error{};
    std::size_t nan{}, positive_inf{}, negative_inf{};

    template <bool Add>
    void change(T v) {
        if (std::isfinite(v)) {
            auto& error = Add ? add_error : remove_error;
            const T increment = (Add ? v : T{} - v) - error;
            const T next = total + increment;
            error = (next - total) - increment;
            total = next;
        } else {
            auto& count = std::isnan(v) ? nan : (v > 0 ? positive_inf : negative_inf);
            if constexpr (Add)
                ++count;
            else
                --count;
        }
    }
    auto value() const -> T {
        if (nan || (positive_inf && negative_inf))
            return std::numeric_limits<T>::quiet_NaN();
        if (positive_inf)
            return std::numeric_limits<T>::infinity();
        if (negative_inf)
            return -std::numeric_limits<T>::infinity();
        return total;
    }
};

struct IntegerTotal {
    std::uint64_t total{};
    template <bool Add>
    void change(std::int64_t v) {
        if constexpr (Add)
            total += static_cast<std::uint64_t>(v);
        else
            total -= static_cast<std::uint64_t>(v);
    }
    auto value() const -> std::int64_t { return std::bit_cast<std::int64_t>(total); }
};

struct Frame {
    std::size_t size, minimum;
    bool centered;
};

template <class Total, typename Logical, bool Mean, typename T>
auto roll(const Column<T>& col, const std::optional<ValidityBitmap>& validity, Frame frame)
    -> ComputedColumn {
    using Out = std::conditional_t<std::is_integral_v<Logical>, std::int64_t, double>;
    const auto n = col.size();
    Column<Out> out;
    if (frame.size == 0) {
        out.resize(n);
        return {.column = std::move(out),
                .validity = Mean ? std::optional{ValidityBitmap(n, false)} : std::nullopt};
    }
    out.resize_for_overwrite(n);
    auto* dest = out.data();
    std::optional<ValidityBitmap> valid;
    const auto values = value_reader(col);
    const auto right = frame.centered ? (frame.size + 1) / 2 : 1;
    const auto left = frame.size - right;
    Total total;
    std::size_t lo = 0, hi = 0, present = 0;
    for (std::size_t i = 0; i < n; ++i) {
        const auto start = i > left ? i - left : 0;
        const auto end = i + std::min(right, n - i);
        if (start == hi) {
            total = {};
            present = 0;
            lo = start;
        }
        while (lo < start) {
            if (!validity || (*validity)[lo]) {
                total.template change<false>(static_cast<Logical>(values[lo]));
                --present;
            }
            ++lo;
        }
        while (hi < end) {
            if (!validity || (*validity)[hi]) {
                total.template change<true>(static_cast<Logical>(values[hi]));
                ++present;
            }
            ++hi;
        }
        if (present < frame.minimum || (Mean && !present)) {
            if (!valid)
                valid.emplace(n, true);
            valid->set(i, false);
            dest[i] = {};
        } else {
            const auto sum = static_cast<Logical>(total.value());
            if constexpr (Mean)
                dest[i] = sum / static_cast<Logical>(present);
            else
                dest[i] = sum;
        }
    }
    return {.column = std::move(out), .validity = std::move(valid)};
}

}  // namespace

auto polars_rolling_column(const ir::CallExpr& call, const Table& input)
    -> std::expected<ComputedColumn, std::string> {
    if (call.args.size() != 5)
        return std::unexpected("rolling requires five arguments");
    const auto* ref = ir::as_column_ref(*call.args[0]);
    const auto* entry = ref ? input.find_entry(ref->name) : nullptr;
    const auto literal = [&](std::size_t i) -> const std::int64_t* {
        const auto* lit = std::get_if<ir::Literal>(&call.args[i]->node);
        return lit ? std::get_if<std::int64_t>(&lit->value) : nullptr;
    };
    const auto* size = literal(1);
    const auto* min = literal(2);
    const auto* centered = literal(3);
    const auto* single = literal(4);
    if (!entry || !size || !min || !centered || !single || *size < 0 || *min < 0 || *min > *size ||
        (*centered != 0 && *centered != 1) || (*single != 0 && *single != 1))
        return std::unexpected("invalid rolling column or frame parameters");
    const Frame frame{static_cast<std::size_t>(*size), static_cast<std::size_t>(*min),
                      *centered != 0};
    const bool mean = call.callee == "__ibex_rolling_mean";
    if (const auto* col = std::get_if<Column<double>>(&*entry->column)) {
        if (*single != 0) {
            if (mean)
                return roll<FloatingTotal<double>, float, true>(*col, entry->validity, frame);
            return roll<FloatingTotal<float>, float, false>(*col, entry->validity, frame);
        }
        if (mean)
            return roll<FloatingTotal<double>, double, true>(*col, entry->validity, frame);
        return roll<FloatingTotal<double>, double, false>(*col, entry->validity, frame);
    }
    if (const auto* col = std::get_if<Column<std::int64_t>>(&*entry->column)) {
        if (mean)
            return roll<FloatingTotal<double>, double, true>(*col, entry->validity, frame);
        return roll<IntegerTotal, std::int64_t, false>(*col, entry->validity, frame);
    }
    return std::unexpected("unsupported rolling input type");
}
}  // namespace ibex::runtime
