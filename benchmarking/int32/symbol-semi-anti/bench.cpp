// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen
// Native semi/anti joins with actual compact or wide integer keys.
#include <ibex/parser/lower.hpp>
#include <ibex/parser/parser.hpp>
#include <ibex/runtime/interpreter.hpp>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <iostream>
#include <stdexcept>
#include <string>
#include <vector>

extern "C" {
const char* malloc_conf = "dirty_decay_ms:-1,muzzy_decay_ms:-1";
}

int main(int argc, char** argv) {
    if (argc != 6)
        return 2;
    const std::size_t rows = std::stoull(argv[1]);
    const std::size_t keys = std::stoull(argv[2]);
    const bool compact = std::string(argv[3]) == "1";
    const bool nullable = std::string(argv[4]) == "1";
    const std::size_t right_rows = std::stoull(argv[5]);
    if (rows == 0 || keys < 2 || keys > 1000000 || right_rows < keys / 2)
        return 2;
    using Col = ibex::Column<std::int64_t>;
    const auto key_at = [keys](std::size_t row) {
        return static_cast<std::int64_t>((row * 7919) % keys);
    };
    const auto make_keys = [compact](const auto& generate, std::size_t count) {
        ibex::detail::NoInitVector<std::int32_t> values(count);
        for (std::size_t i = 0; i < count; ++i)
            values[i] = static_cast<std::int32_t>(generate(i));
        if (compact)
            return Col::from_compact_int32(std::move(values));
        Col result;
        result.assign(values.begin(), values.end());
        return result;
    };
    ibex::runtime::Table left, right;
    auto left_keys = make_keys(key_at, rows);
    ibex::runtime::ValidityBitmap valid(rows, true);
    for (std::size_t i = 0; i < rows; ++i)
        valid.set(i, i % 29 != 0);
    if (nullable)
        left.add_column("k", std::move(left_keys), valid);
    else
        left.add_column("k", std::move(left_keys));
    Col ids;
    for (std::size_t i = 0; i < rows; ++i)
        ids.push_back(static_cast<std::int64_t>(i));
    left.add_column("id", std::move(ids));
    right.add_column(
        "k",
        make_keys([keys](std::size_t i) { return static_cast<std::int64_t>((i % (keys / 2)) * 2); },
                  right_rows));
    ibex::runtime::TableRegistry registry{{"l", std::move(left)}, {"r", std::move(right)}};
    // Build an independent row/value oracle once, outside all query timers.
    std::vector<std::int64_t> reference_keys(rows);
    std::vector<std::size_t> semi_rows, anti_rows;
    for (std::size_t i = 0; i < rows; ++i) {
        reference_keys[i] = key_at(i);
        const bool match = (!nullable || valid[i]) && reference_keys[i] % 2 == 0;
        (match ? semi_rows : anti_rows).push_back(i);
    }
    std::cout << "ready\n" << std::flush;
    std::string kind;
    std::size_t iterations = 0;
    while (std::cin >> kind >> iterations) {
        if ((kind != "semi" && kind != "anti") || iterations == 0)
            return 3;
        auto parsed = ibex::parser::parse("l " + kind + " join r on k;");
        if (!parsed)
            return 4;
        auto plan = ibex::parser::lower(*parsed);
        if (!plan)
            return 4;
        std::vector<double> times;
        std::int64_t checksum = 0;
        for (std::size_t iteration = 0; iteration < iterations; ++iteration) {
            const auto start = std::chrono::steady_clock::now();
            auto result = ibex::runtime::interpret(**plan, registry);
            const auto end = std::chrono::steady_clock::now();
            if (!result)
                throw std::runtime_error(result.error());
            times.push_back(std::chrono::duration<double, std::milli>(end - start).count());
            const auto& out_ids = std::get<Col>(*result->find("id"));
            const auto& out_keys = std::get<Col>(*result->find("k"));
            const auto* key_entry = result->find_entry("k");
            const auto& expected_rows = kind == "semi" ? semi_rows : anti_rows;
            if (result->rows() != expected_rows.size())
                throw std::runtime_error("incorrect row count");
            checksum = 0;
            for (std::size_t output = 0; output < expected_rows.size(); ++output) {
                const auto i = expected_rows[output];
                const bool live = !nullable || valid[i];
                if (out_ids[output] != static_cast<std::int64_t>(i) ||
                    ibex::runtime::is_null(*key_entry, output) != !live ||
                    (live && out_keys[output] != reference_keys[i]))
                    throw std::runtime_error("incorrect key/row association or null");
                checksum += out_ids[output];
            }
            if ((compact && !out_keys.is_compact_int32()) ||
                std::get<Col>(*registry.at("l").find("k")).is_compact_int32() != compact ||
                std::get<Col>(*registry.at("r").find("k")).is_compact_int32() != compact)
                throw std::runtime_error("unexpected input or output widening");
        }
        std::sort(times.begin(), times.end());
        const auto n = times.size();
        const auto ms = n % 2 ? times[n / 2] : (times[n / 2 - 1] + times[n / 2]) / 2;
        std::cout << ms << ' ' << checksum << '\n' << std::flush;
    }
}
