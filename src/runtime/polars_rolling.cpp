// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen
#include "polars_rolling.hpp"

#include <ibex/runtime/worker_pool.hpp>

#include <algorithm>
#include <atomic>
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
    const Column<std::int64_t>* index;
    const ValidityBitmap* index_validity;
    bool closed_left, closed_right;
};

template <class Total, typename Logical, bool Mean, typename T>
auto roll(const Column<T>& col, const std::optional<ValidityBitmap>& validity, Frame frame,
          RollingPartitions partitions) -> ComputedColumn {
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
    const auto scan = [&](std::size_t length, auto position, auto mark_null, auto bounds) {
        Total total;
        std::size_t lo = 0, hi = 0, present = 0;
        for (std::size_t i = 0; i < length; ++i) {
            const auto [start, end] = bounds(i, length);
            if (start >= hi) {
                total = {};
                present = 0;
                lo = hi = start;
            }
            while (lo < start) {
                if (!validity || (*validity)[position(lo)]) {
                    total.template change<false>(static_cast<Logical>(values[position(lo)]));
                    --present;
                }
                ++lo;
            }
            while (hi < end) {
                if (!validity || (*validity)[position(hi)]) {
                    total.template change<true>(static_cast<Logical>(values[position(hi)]));
                    ++present;
                }
                ++hi;
            }
            if (present < frame.minimum || (Mean && !present)) {
                mark_null(position(i));
                dest[position(i)] = {};
            } else {
                const auto sum = static_cast<Logical>(total.value());
                if constexpr (Mean)
                    dest[position(i)] = sum / static_cast<Logical>(present);
                else
                    dest[position(i)] = sum;
            }
        }
    };
    const auto scan_partition = [&](std::size_t length, auto position, auto mark_null) {
        if (!frame.index) {
            scan(length, position, mark_null, [&](std::size_t i, std::size_t len) {
                return std::pair{i > left ? i - left : 0, i + std::min(right, len - i)};
            });
            return;
        }
        const auto ticks = value_reader(*frame.index);
        // Advance both bounds over sorted times. Peers at the same time get
        // the same frame, even when they follow the current row.
        const auto ordered = [&](std::size_t len, auto row) {
            std::size_t start = 0, end = 0;
            scan(len, row, mark_null, [&](std::size_t i, std::size_t) {
                const auto now = ticks[row(i)];
                while (end < len &&
                       (ticks[row(end)] < now || (frame.closed_right && ticks[row(end)] == now)))
                    ++end;
                while (start < end) {
                    // Unsigned subtraction is the exact nonnegative distance
                    // even when the two signed timestamps straddle zero.
                    const auto age = static_cast<std::uint64_t>(now) -
                                     static_cast<std::uint64_t>(ticks[row(start)]);
                    if (age < frame.size || (frame.closed_left && age == frame.size))
                        break;
                    ++start;
                }
                return std::pair{start, end};
            });
        };
        bool sorted = !frame.index_validity;
        for (std::size_t i = 1; sorted && i < length; ++i)
            sorted = ticks[position(i - 1)] <= ticks[position(i)];
        if (sorted) {
            ordered(length, position);
        } else {
            std::vector<std::size_t> rows;
            rows.reserve(length);
            for (std::size_t i = 0; i < length; ++i) {
                const auto row = position(i);
                if (frame.index_validity && !(*frame.index_validity)[row]) {
                    dest[row] = {};
                    mark_null(row);
                } else
                    rows.push_back(row);
            }
            std::stable_sort(rows.begin(), rows.end(),
                             [&](auto a, auto b) { return ticks[a] < ticks[b]; });
            ordered(rows.size(), [&](std::size_t i) { return rows[i]; });
        }
    };
    if (partitions.offsets.empty()) {
        scan_partition(
            n, [](std::size_t i) { return i; },
            [&](std::size_t i) {
                if (!valid)
                    valid.emplace(n, true);
                valid->set(i, false);
            });
    } else {
        // Each partition owns its output positions. Mark nulls in separate bytes
        // since adjacent rows from different partitions may share a bitmap word.
        std::vector<std::uint8_t> invalid(n, 0);
        const auto groups = partitions.offsets.size() - 1;
        const auto group = [&](std::size_t g) {
            auto indices = partitions.rows.subspan(
                partitions.offsets[g], partitions.offsets[g + 1] - partitions.offsets[g]);
            scan_partition(
                indices.size(), [&](std::size_t i) { return indices[i]; },
                [&](std::size_t i) { invalid[i] = 1; });
        };
        if (partitions.workers < 2) {
            for (std::size_t g = 0; g < groups; ++g)
                group(g);
        } else {
            std::atomic<std::size_t> next{0};
            auto batch = process_worker_pool().submit(partitions.workers, [&](std::size_t) {
                for (auto g = next.fetch_add(1, std::memory_order_relaxed); g < groups;
                     g = next.fetch_add(1, std::memory_order_relaxed))
                    group(g);
            });
            batch.wait();
        }
        for (std::size_t i = 0; i < n; ++i) {
            if (invalid[i]) {
                if (!valid)
                    valid.emplace(n, true);
                valid->set(i, false);
            }
        }
    }
    return {.column = std::move(out), .validity = std::move(valid)};
}

}  // namespace

auto polars_rolling_column(const ir::CallExpr& call, const Table& input,
                           RollingPartitions partitions)
    -> std::expected<ComputedColumn, std::string> {
    const bool indexed = call.callee.ends_with("_by");
    if (call.args.size() != (indexed ? 6U : 5U))
        return std::unexpected("invalid rolling arguments");
    const auto* ref = ir::as_column_ref(*call.args[0]);
    const auto* entry = ref ? input.find_entry(ref->name) : nullptr;
    const auto literal = [&](std::size_t i) -> const std::int64_t* {
        const auto* lit = std::get_if<ir::Literal>(&call.args[i]->node);
        return lit ? std::get_if<std::int64_t>(&lit->value) : nullptr;
    };
    const std::size_t base = indexed ? 2 : 1;
    const auto* size = literal(base);
    const auto* min = literal(base + 1);
    const auto* mode = literal(base + 2);
    const auto* single = literal(base + 3);
    if (!entry || !size || !min || !mode || !single || *size < 0 || *min < 0 ||
        (!indexed && *min > *size) || (indexed && *size == 0) || *mode < 0 ||
        *mode > (indexed ? 3 : 1) || (*single != 0 && *single != 1))
        return std::unexpected("invalid rolling column or frame parameters");
    Frame frame{.size = static_cast<std::size_t>(*size),
                .minimum = static_cast<std::size_t>(*min),
                .centered = !indexed && *mode != 0,
                .index = nullptr,
                .index_validity = nullptr,
                .closed_left = (*mode & 1) != 0,
                .closed_right = (*mode & 2) != 0};
    Column<std::int64_t> date_days;
    if (indexed) {
        const auto* by = ir::as_column_ref(*call.args[1]);
        const auto* key = by ? input.find_entry(by->name) : nullptr;
        if (!key)
            return std::unexpected("rolling index requires a column");
        if (const auto* dates = std::get_if<Column<Date>>(&*key->column)) {
            const auto values = value_reader(*dates);
            date_days.resize_for_overwrite(dates->size());
            for (std::size_t i = 0; i < dates->size(); ++i)
                date_days[i] = values[i].days;
            frame.index = &date_days;
        } else if (!(frame.index = std::get_if<Column<std::int64_t>>(&*key->column))) {
            return std::unexpected("rolling index requires Int64 or Date storage");
        }
        if (key->validity)
            frame.index_validity = &*key->validity;
    }
    const bool mean = call.callee.starts_with("__ibex_rolling_mean");
    if (const auto* col = std::get_if<Column<double>>(&*entry->column)) {
        if (*single != 0) {
            if (mean)
                return roll<FloatingTotal<double>, float, true>(*col, entry->validity, frame,
                                                                partitions);
            return roll<FloatingTotal<float>, float, false>(*col, entry->validity, frame,
                                                            partitions);
        }
        if (mean)
            return roll<FloatingTotal<double>, double, true>(*col, entry->validity, frame,
                                                             partitions);
        return roll<FloatingTotal<double>, double, false>(*col, entry->validity, frame, partitions);
    }
    if (const auto* col = std::get_if<Column<std::int64_t>>(&*entry->column)) {
        if (mean)
            return roll<FloatingTotal<double>, double, true>(*col, entry->validity, frame,
                                                             partitions);
        return roll<IntegerTotal, std::int64_t, false>(*col, entry->validity, frame, partitions);
    }
    return std::unexpected("unsupported rolling input type");
}
}  // namespace ibex::runtime
