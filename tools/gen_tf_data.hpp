#pragma once
// gen_tf_data — synthetic TimeFrame data generator for .ibex script benchmarks.
//
// Usage in .ibex:
//   extern fn gen_tf_data(n: Int) -> DataFrame from "gen_tf_data.hpp";
//   let tf = gen_tf_data(1000000);

#include <ibex/core/column.hpp>
#include <ibex/core/time.hpp>
#include <ibex/runtime/interpreter.hpp>

#include <cstdint>
#include <stdexcept>

inline auto gen_tf_data(std::int64_t n) -> ibex::runtime::Table {
    if (n < 0)
        throw std::invalid_argument("gen_tf_data: n must be non-negative");
    auto rows = static_cast<std::size_t>(n);

    ibex::Column<ibex::Timestamp> ts_col;
    ibex::Column<double> price_col;
    ts_col.reserve(rows);
    price_col.reserve(rows);

    for (std::size_t i = 0; i < rows; ++i) {
        ts_col.push_back(ibex::Timestamp{static_cast<std::int64_t>(i) * 1'000'000'000LL});
        price_col.push_back(100.0 + static_cast<double>(i % 100));
    }

    ibex::runtime::Table t;
    t.add_column("ts", std::move(ts_col));
    t.add_column("price", std::move(price_col));
    return t;
}
