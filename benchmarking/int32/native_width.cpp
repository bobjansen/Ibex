// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen
// Width-control experiment using Ibex Column and its robin_hood dependency.
#include <ibex/core/column.hpp>
#include <robin_hood.h>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <numeric>
#include <random>
#include <string>
#include <vector>

#ifndef VALUE_BITS
#define VALUE_BITS 64
#endif
using Value = std::conditional_t<VALUE_BITS == 32, std::int32_t, std::int64_t>;
static std::uint64_t sink = 0;
int main(int argc, char** argv) {
    if (argc != 4) return 2;
    const auto n = std::stoull(argv[1]);
    const auto groups = std::stoull(argv[2]);
    const std::string op = argv[3];
    if (n == 0 || groups == 0 || groups > 1000000000) return 2;
    ibex::Column<Value> source;
    source.resize_for_overwrite(n);
    std::vector<std::size_t> indices(n);
    std::mt19937_64 random(20261108);
    for (std::size_t i = 0; i < n; ++i) {
        source[i] = static_cast<Value>(random() % groups);
        indices[i] = random() % n;
    }
    const auto& input = source;
    // Reference output and every-cell checks are outside each sample's timer.
    std::uint64_t reference = 0;
    if (op == "scan") {
        for (const auto value : input) reference += static_cast<std::uint64_t>(value);
    } else if (op == "filter") {
        for (const auto value : input) if (value < static_cast<Value>(groups / 2)) reference += static_cast<std::uint64_t>(value);
    } else if (op == "gather") {
        for (const auto index : indices) reference += static_cast<std::uint64_t>(input[index]);
    } else if (op == "group") {
        reference = n;
    } else return 2;
    std::cout << "ready " << VALUE_BITS << '\n' << std::flush;
    std::string command;
    while (std::getline(std::cin, command)) {
        std::uint64_t result = 0;
        ibex::Column<Value> output;
        robin_hood::unordered_flat_map<Value, std::uint64_t> counts;
        const auto start = std::chrono::steady_clock::now();
        if (op == "scan") {
            for (const auto value : input) result += static_cast<std::uint64_t>(value);
        } else if (op == "filter") {
            output = input.filter([&](Value v) { return v < static_cast<Value>(groups / 2); });
        } else if (op == "gather") {
            output.resize_for_overwrite(n);
            auto* dst = output.data();
            const auto* src = input.data();
            for (std::size_t i = 0; i < n; ++i) dst[i] = src[indices[i]];
        } else {
            counts.reserve(groups);
            for (const auto value : input) ++counts[value];
        }
        const auto stop = std::chrono::steady_clock::now();
        if (op == "filter") {
            std::size_t pos = 0;
            for (const auto value : input) if (value < static_cast<Value>(groups / 2)) {
                if (pos >= output.size() || output[pos++] != value) return 3;
            }
            if (pos != output.size()) return 3;
        }
        if (op == "gather") for (std::size_t i = 0; i < n; ++i) if (output[i] != input[indices[i]]) return 3;
        if (op == "filter" || op == "gather") for (const auto value : std::as_const(output)) result += static_cast<std::uint64_t>(value);
        if (op == "group") {
            std::vector<std::uint64_t> expected(groups);
            for (const auto value : input) ++expected[static_cast<std::size_t>(value)];
            for (std::size_t k = 0; k < groups; ++k) {
                const auto it = counts.find(static_cast<Value>(k));
                if ((it == counts.end() ? 0 : it->second) != expected[k]) return 3;
            }
            for (const auto& [key, count] : counts) { (void)key; result += count; }
        }
        if (result != reference) return 3;
        sink ^= result;
        std::cout << std::chrono::duration<double, std::milli>(stop - start).count() << ' ' << result << '\n' << std::flush;
    }
    return sink == UINT64_MAX ? 4 : 0;
}
