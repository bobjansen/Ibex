// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen
#include <ibex/parser/lower.hpp>
#include <ibex/parser/parser.hpp>
#include <ibex/runtime/interpreter.hpp>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <ctime>
#include <iostream>
#include <numeric>
#include <random>
#include <string>
#include <vector>
int main(int argc, char** argv) {
    if (argc != 3)
        return 2;
    const std::size_t n = std::stoull(argv[1]);
    const bool compact = std::string(argv[2]) == "1";
    if (n == 0 || n > 100000000)
        return 2;
    std::vector<std::int64_t> keys(n);
    std::iota(keys.begin(), keys.end(), INT64_C(0));
    ibex::runtime::Table right, left;
    right.add_column("id", ibex::Column<std::int64_t>{keys});
    std::mt19937_64 rng(20261108);
    std::shuffle(keys.begin(), keys.end(), rng);
    left.add_column("id", ibex::Column<std::int64_t>{keys});
    for (int c = 0; c < 3; ++c) {
        ibex::detail::NoInitVector<std::int32_t> values(n);
        for (std::size_t i = 0; i < n; ++i)
            values[i] = static_cast<std::int32_t>(i % 1000000) - 500000 + c;
        if (compact) {
            right.add_column("p" + std::to_string(c),
                             ibex::Column<std::int64_t>::from_compact_int32(std::move(values)));
        } else {
            ibex::Column<std::int64_t> wide;
            wide.assign(values.begin(), values.end());
            right.add_column("p" + std::to_string(c), std::move(wide));
        }
    }
    ibex::runtime::TableRegistry registry;
    registry.emplace("lhs", std::move(left));
    registry.emplace("rhs", std::move(right));
    auto parsed = ibex::parser::parse("lhs join rhs on id;");
    if (!parsed)
        return 7;
    auto plan = ibex::parser::lower(*parsed);
    if (!plan)
        return 7;
    std::cout << "ready\n" << std::flush;
    std::string command;
    while (std::getline(std::cin, command)) {
        const auto cpu_start = std::clock();
        const auto start = std::chrono::steady_clock::now();
        auto result = ibex::runtime::interpret(**plan, registry);
        const auto end = std::chrono::steady_clock::now();
        const double cpu_ms =
            1000.0 * static_cast<double>(std::clock() - cpu_start) / CLOCKS_PER_SEC;
        if (!result || result->rows() != n)
            return 3;
        const auto& output_keys =
            std::get<ibex::Column<std::int64_t>>(*result->find_entry("id")->column);
        std::vector<bool> seen(n, false);
        for (std::size_t i = 0; i < n; ++i) {
            const auto key = output_keys[i];
            if (key < 0 || static_cast<std::size_t>(key) >= n ||
                seen[static_cast<std::size_t>(key)])
                return 6;
            seen[static_cast<std::size_t>(key)] = true;
        }
        std::int64_t checksum = 0;
        for (int c = 0; c < 3; ++c) {
            const auto& out = std::get<ibex::Column<std::int64_t>>(
                *result->find_entry("p" + std::to_string(c))->column);
            if (compact && !out.is_compact_int32())
                return 4;
            for (std::size_t i = 0; i < n; ++i) {
                const std::int64_t expected = output_keys[i] % 1000000 - 500000 + c;
                const auto actual =
                    compact ? static_cast<std::int64_t>(out.compact_int32_data()[i]) : out[i];
                if (actual != expected)
                    return 5;
                checksum += actual;
            }
        }
        const double ms = std::chrono::duration<double, std::milli>(end - start).count();
        result = std::unexpected(
            std::string{});  // Deallocate before signalling readiness for the next sample.
        std::cout << ms << ' ' << checksum << ' ' << cpu_ms << '\n' << std::flush;
    }
}
