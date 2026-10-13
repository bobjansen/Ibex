// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#include <ibex/core/column.hpp>
#include <ibex/ir/required_columns.hpp>
#include <ibex/runtime/interpreter.hpp>
#include <ibex/runtime/table_properties.hpp>

#include <catch2/catch_test_macros.hpp>

#include <cstdint>
#include <map>
#include <optional>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "../src/repl/eager_projection.hpp"

namespace {

using ibex::ir::ColumnDemand;
using ibex::repl::detail::narrow_demanded_tables;
using ibex::runtime::Table;
using ibex::runtime::TableRegistry;

auto wide_table() -> Table {
    Table t;
    t.add_column("g", ibex::Column<std::int64_t>{1, 1, 1, 2, 2, 3});
    t.add_column("pad1", ibex::Column<std::int64_t>{0, 1, 0, 1, 0, 1});
    t.add_column("v", ibex::Column<double>{5.0, 9.0, 7.0, 1.0, 3.0, 4.0});
    t.add_column("pad2", ibex::Column<std::int64_t>{7, 7, 7, 7, 7, 7});
    return t;
}

auto names_of(const Table& t) -> std::vector<std::string> {
    std::vector<std::string> out;
    out.reserve(t.columns.size());
    for (const auto& entry : t.columns) {
        out.push_back(entry.name);
    }
    return out;
}

auto demand_of(std::set<std::string> names) -> ColumnDemand {
    ColumnDemand d;
    d.names = std::move(names);
    return d;
}

}  // namespace

TEST_CASE("eager projection: a bounded demand keeps only its columns, sharing their storage",
          "[repl][eager_projection]") {
    TableRegistry tables;
    tables.emplace("x", wide_table());
    // Names the table lacks (demand over-approximates across sources) are
    // ignored; a source the registry lacks is not a table to narrow.
    const std::map<std::string, ColumnDemand> demand{{"x", demand_of({"v", "g", "elsewhere"})},
                                                     {"lazy_source", demand_of({"a"})}};

    auto narrowed = narrow_demanded_tables(demand, tables);
    REQUIRE(narrowed.size() == 1);
    REQUIRE(narrowed.front().first == "x");
    const Table& cut = narrowed.front().second;
    // Source order, not demand order.
    REQUIRE(names_of(cut) == std::vector<std::string>{"g", "v"});
    REQUIRE(cut.rows() == 6);
    REQUIRE(cut.find_entry("g")->column == tables.at("x").find_entry("g")->column);
    REQUIRE(cut.find_entry("v")->column == tables.at("x").find_entry("v")->column);
    REQUIRE(cut.find("pad1") == nullptr);
}

TEST_CASE("eager projection: unbounded, full and empty demands keep the table whole",
          "[repl][eager_projection]") {
    TableRegistry tables;
    tables.emplace("x", wide_table());

    ColumnDemand all;
    all.widen();
    REQUIRE(narrow_demanded_tables({{"x", all}}, tables).empty());
    REQUIRE(narrow_demanded_tables({{"x", demand_of({"g", "pad1", "v", "pad2"})}}, tables).empty());
    // `x[select { n = count() }]` reads no column but needs the row count.
    REQUIRE(narrow_demanded_tables({{"x", demand_of({})}}, tables).empty());
}

TEST_CASE("eager projection: columns the table's properties name survive the cut",
          "[repl][eager_projection]") {
    TableRegistry tables;
    Table sorted = wide_table();
    sorted.set_properties(ibex::runtime::TableProperties::sorted_by({{.name = "pad1"}}));
    tables.emplace("sorted", std::move(sorted));
    Table frame = wide_table();
    frame.set_properties(ibex::runtime::TableProperties::time_frame("pad2"));
    tables.emplace("frame", std::move(frame));

    const std::map<std::string, ColumnDemand> demand{{"sorted", demand_of({"v"})},
                                                     {"frame", demand_of({"v"})}};
    std::map<std::string, Table> cut;
    for (auto& [name, table] : narrow_demanded_tables(demand, tables)) {
        cut.emplace(name, std::move(table));
    }
    REQUIRE(names_of(cut.at("sorted")) == std::vector<std::string>{"pad1", "v"});
    REQUIRE(cut.at("sorted").ordering().has_value());
    REQUIRE(cut.at("sorted").ordering()->size() == 1);
    REQUIRE(cut.at("sorted").ordering()->front().name == "pad1");
    REQUIRE(names_of(cut.at("frame")) == std::vector<std::string>{"v", "pad2"});
    REQUIRE(cut.at("frame").time_index() == std::optional<std::string>{"pad2"});

    // An unbounded demand over a sorted table is not "just its ordering key".
    ColumnDemand all;
    all.widen();
    REQUIRE(narrow_demanded_tables({{"sorted", all}}, tables).empty());
}
