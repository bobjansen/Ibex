// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#include <algorithm>
#include <array>
#include <cmath>
#include <cstdint>
#include <iterator>
#include <memory>
#include <optional>
#include <vector>

#include "interpreter_internal.hpp"

namespace ibex::runtime {
namespace {
constexpr std::size_t kPairBlockRows = 4096;
constexpr std::size_t kPairLanes = 4;

auto lane_sum(const std::array<double, kPairLanes>& a) -> double {
    return (a[0] + a[1]) + (a[2] + a[3]);
}

template <bool Nullable, typename X, typename Y>
auto pair_block(const X* xs, const Y* ys, const ValidityBitmap* xv, const ValidityBitmap* yv,
                std::size_t begin, std::size_t end, double origin_x, double origin_y) -> CoMoment {
    const auto valid = [&]([[maybe_unused]] std::size_t row) {
        if constexpr (Nullable) {
            return (xv == nullptr || (*xv)[row]) && (yv == nullptr || (*yv)[row]);
        }
        return true;
    };
    std::array<double, kPairLanes> sx{};
    std::array<double, kPairLanes> sy{};
    std::size_t count = 0;
    for (std::size_t row = begin; row < end; row += kPairLanes) {
        const std::size_t lanes = std::min(kPairLanes, end - row);
        for (std::size_t lane = 0; lane < lanes; ++lane) {
            if (valid(row + lane)) {
                sx[lane] += static_cast<double>(xs[row + lane]) - origin_x;
                sy[lane] += static_cast<double>(ys[row + lane]) - origin_y;
                ++count;
            }
        }
    }
    CoMoment m;
    if (count == 0) {
        return m;
    }
    m.n = static_cast<double>(count);
    m.mean_x = lane_sum(sx) / m.n;
    m.mean_y = lane_sum(sy) / m.n;
    std::array<double, kPairLanes> sxy{};
    std::array<double, kPairLanes> sxx{};
    std::array<double, kPairLanes> syy{};
    for (std::size_t row = begin; row < end; row += kPairLanes) {
        const std::size_t lanes = std::min(kPairLanes, end - row);
        for (std::size_t lane = 0; lane < lanes; ++lane) {
            if (valid(row + lane)) {
                const double dx = (static_cast<double>(xs[row + lane]) - origin_x) - m.mean_x;
                const double dy = (static_cast<double>(ys[row + lane]) - origin_y) - m.mean_y;
                sxy[lane] = std::fma(dx, dy, sxy[lane]);
                sxx[lane] = std::fma(dx, dx, sxx[lane]);
                syy[lane] = std::fma(dy, dy, syy[lane]);
            }
        }
    }
    m.cxy = lane_sum(sxy);
    m.m2x = lane_sum(sxx);
    m.m2y = lane_sum(syy);
    return m;
}

template <typename X, typename Y>
auto pair_moment(const Column<X>& xs, const Column<Y>& ys, const ValidityBitmap* xv,
                 const ValidityBitmap* yv, const ExecutionContext* exec) -> CoMoment {
    const std::size_t rows = xs.size();
    std::size_t first = 0;
    while (first < rows && ((xv != nullptr && !(*xv)[first]) || (yv != nullptr && !(*yv)[first]))) {
        ++first;
    }
    if (first == rows) {
        return {};
    }
    // Translate all blocks by the SAME observed pair. Means and Chan merges
    // then operate on small residuals, even for 1e12-offset inputs. Covariance
    // and correlation are translation-invariant; no rounded absolute mean is
    // needed. Do not use sum(x*x)-sum(x)^2/n, which loses small spreads.
    const double origin_x = static_cast<double>(xs[first]);
    const double origin_y = static_cast<double>(ys[first]);
    const std::size_t blocks = (rows + kPairBlockRows - 1) / kPairBlockRows;
    std::vector<CoMoment> partial(blocks);
    const auto reduce = [&](std::size_t block) {
        const std::size_t begin = block * kPairBlockRows;
        const std::size_t end = std::min(rows, begin + kPairBlockRows);
        if (xv == nullptr && yv == nullptr) {
            partial[block] =
                pair_block<false>(xs.data(), ys.data(), xv, yv, begin, end, origin_x, origin_y);
        } else {
            partial[block] =
                pair_block<true>(xs.data(), ys.data(), xv, yv, begin, end, origin_x, origin_y);
        }
    };
    const std::size_t workers =
        std::min(blocks, exec != nullptr ? group_barrier_worker_count(*exec, rows) : 0);
    if (workers >= 2) {
        auto batch = process_worker_pool().submit(workers, [&](std::size_t worker) {
            for (std::size_t block = worker; block < blocks; block += workers) {
                reduce(block);
            }
        });
        batch.wait();
    } else {
        for (std::size_t block = 0; block < blocks; ++block) {
            reduce(block);
        }
    }
    CoMoment total;
    for (const auto& block : partial) {
        co_moment_merge(total, block);
    }
    return total;
}

auto pair_moment(const ColumnEntry& x, const ColumnEntry& y, const ExecutionContext* exec)
    -> CoMoment {
    const ValidityBitmap* xv = x.validity.has_value() ? &*x.validity : nullptr;
    const ValidityBitmap* yv = y.validity.has_value() ? &*y.validity : nullptr;
    const auto with_x = [&](const auto& xs) {
        if (const auto* ys = std::get_if<Column<double>>(y.column.get())) {
            return pair_moment(xs, *ys, xv, yv, exec);
        }
        return pair_moment(xs, std::get<Column<std::int64_t>>(*y.column), xv, yv, exec);
    };
    if (const auto* xs = std::get_if<Column<double>>(x.column.get())) {
        return with_x(*xs);
    }
    return with_x(std::get<Column<std::int64_t>>(*x.column));
}
}  // namespace

auto global_paired_aggregate(const Table& input, const std::vector<ir::AggSpec>& aggregations,
                             const ExecutionContext* exec) -> Table {
    Table output;
    // corr(x,y) and cov(x,y) in the same select share their sufficient stats.
    struct Pair {
        const ColumnEntry* x;
        const ColumnEntry* y;
        CoMoment moment;
    };
    std::vector<Pair> pairs;
    for (const auto& agg : aggregations) {
        const auto* x = input.find_entry(agg.column.name);
        const auto* y = input.find_entry(agg.paired.name);
        auto pair =
            std::ranges::find_if(pairs, [&](const Pair& p) { return p.x == x && p.y == y; });
        if (pair == pairs.end()) {
            pairs.push_back({x, y, pair_moment(*x, *y, exec)});
            pair = std::prev(pairs.end());
        }
        Column<double> value;
        std::optional<ValidityBitmap> validity;
        if (input.rows() != 0) {
            const auto result = agg.func == ir::AggFunc::Corr ? co_moment_corr(pair->moment)
                                                              : co_moment_cov(pair->moment);
            value.push_back(result.value_or(0.0));
            if (!result.has_value()) {
                validity.emplace(1, false);
            }
        }
        output.add_column_shared(agg.alias, std::make_shared<ColumnValue>(std::move(value)),
                                 std::move(validity));
    }
    return output;
}
}  // namespace ibex::runtime
