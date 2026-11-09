// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen
// Resident filter query. Input creation, checking and destruction are untimed.
#include <ibex/parser/lower.hpp>
#include <ibex/parser/parser.hpp>
#include <ibex/runtime/interpreter.hpp>

#include <chrono>
#include <cstdint>
#include <ctime>
#include <iostream>
#include <numeric>
#include <string>
#include <vector>

auto value(std::size_t row, int column) -> std::int32_t {
    return column == 0 ? static_cast<std::int32_t>(row % 100)
                       : static_cast<std::int32_t>(row % 100000) - 50000 + column;
}
int main(int argc, char** argv) {
    if (argc != 4)
        return 2;
    const std::size_t n = std::stoull(argv[1]);
    const bool compact = std::string(argv[2]) == "1";
    const std::size_t keep = std::stoull(argv[3]);
    if (!n || n > 100000000 || n % 100 || !keep || keep > 100)
        return 2;
    using Integer = ibex::Column<std::int64_t>;
    ibex::runtime::Table table;
    std::vector<std::int64_t> ids(n);
    std::iota(ids.begin(), ids.end(), INT64_C(0));
    table.add_column("id", Integer(std::move(ids)));
    for (int c = 0; c < 4; ++c) {
        ibex::detail::NoInitVector<std::int32_t> values(n);
        for (std::size_t i = 0; i < n; ++i)
            values[i] = value(i, c);
        Integer col;
        if (compact)
            col = Integer::from_compact_int32(std::move(values));
        else
            col.assign(values.begin(), values.end());
        const std::string name = c == 0 ? "k" : "p" + std::to_string(c);
        if (c == 3) {
            ibex::runtime::ValidityBitmap valid(n, true);
            for (std::size_t i = 0; i < n; i += 97)
                valid.set(i, false);
            table.add_column(name, std::move(col), std::move(valid));
        } else
            table.add_column(name, std::move(col));
    }
    ibex::runtime::TableRegistry registry;
    registry.emplace("t", std::move(table));
    const auto parsed = ibex::parser::parse("t[filter k < " + std::to_string(keep) + "];");
    if (!parsed)
        return 3;
    const auto plan = ibex::parser::lower(*parsed);
    if (!plan)
        return 3;
    std::cout << "ready\n" << std::flush;
    std::string command;
    while (std::getline(std::cin, command)) {
        const auto cpu_start = std::clock();
        const auto start = std::chrono::steady_clock::now();
        auto result = ibex::runtime::interpret(**plan, registry);
        const auto end = std::chrono::steady_clock::now();
        const double cpu_ms =
            1000.0 * static_cast<double>(std::clock() - cpu_start) / CLOCKS_PER_SEC;
        if (!result || result->rows() != n / 100 * keep)
            return 4;
        std::int64_t checksum = 0;
        std::size_t bytes = 0, compact_columns = 0;
        for (std::size_t c = 0; c < result->columns.size(); ++c) {
            const auto& entry = result->columns[c];
            const auto& col = std::get<Integer>(*entry.column);
            compact_columns += col.is_compact_int32() ? 1U : 0U;
            bytes += col.size() * (col.is_compact_int32() ? 4U : 8U);
            for (std::size_t i = 0; i < col.size(); ++i) {
                const std::size_t row = i / keep * 100 + i % keep;
                const std::int64_t expected =
                    c == 0 ? static_cast<std::int64_t>(row) : value(row, static_cast<int>(c - 1));
                if (col[i] != expected)
                    return 5;
                if (c == 4 && (!entry.validity || (*entry.validity)[i] != (row % 97 != 0)))
                    return 6;
                checksum += col[i];
            }
        }
        const double ms = std::chrono::duration<double, std::milli>(end - start).count();
        result = std::unexpected(std::string{});
        std::cout << ms << ' ' << cpu_ms << ' ' << checksum << ' ' << bytes << ' '
                  << compact_columns << '\n'
                  << std::flush;
    }
}
