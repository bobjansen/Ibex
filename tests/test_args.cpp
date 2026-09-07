// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#include <ibex/core/column.hpp>
#include <ibex/repl/repl.hpp>
#include <ibex/runtime/extern_registry.hpp>
#include <ibex/runtime/interpreter.hpp>

#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>

#include <args.hpp>
#include <cstdint>
#include <exception>
#include <stdexcept>
#include <string>
#include <variant>
#include <vector>

namespace {

using ibex::args::parse_args_from_tokens;

struct Row {
    std::string kind;
    std::string name;
    std::int64_t index;
    std::string value;
    auto operator==(const Row&) const -> bool = default;
};

auto rows_of(const ibex::runtime::Table& table) -> std::vector<Row> {
    const auto* kind = std::get_if<ibex::Column<std::string>>(table.find("kind"));
    const auto* name = std::get_if<ibex::Column<std::string>>(table.find("name"));
    const auto* index = std::get_if<ibex::Column<std::int64_t>>(table.find("index"));
    const auto* value = std::get_if<ibex::Column<std::string>>(table.find("value"));
    REQUIRE(kind != nullptr);
    REQUIRE(name != nullptr);
    REQUIRE(index != nullptr);
    REQUIRE(value != nullptr);
    std::vector<Row> out;
    for (std::size_t i = 0; i < kind->size(); ++i) {
        out.push_back(Row{std::string((*kind)[i]), std::string((*name)[i]), (*index)[i],
                          std::string((*value)[i])});
    }
    return out;
}

auto tokens(std::vector<std::string> v) -> std::vector<std::string> { return v; }

}  // namespace

TEST_CASE("parse_args: schema is always {kind, name, index, value}", "[args]") {
    auto table = parse_args_from_tokens("v : flag", {});
    REQUIRE(table.columns.size() == 4);
    CHECK(table.columns[0].name == "kind");
    CHECK(table.columns[1].name == "name");
    CHECK(table.columns[2].name == "index");
    CHECK(table.columns[3].name == "value");
}

TEST_CASE("parse_args: option, alias, flag, positional", "[args]") {
    const std::string spec = R"(
        threads (t)  : int    = 4
        verbose (v)  : flag
        out          : string?
        input        : positional+
    )";
    const auto rows =
        rows_of(parse_args_from_tokens(spec, tokens({"--threads", "8", "-v", "a.csv", "b.csv"})));

    // Occurrence rows come first (argv order), then flag/default fallbacks.
    CHECK(rows[0] == Row{"option", "threads", 0, "8"});
    CHECK(rows[1] == Row{"flag", "verbose", 0, "true"});
    CHECK(rows[2] == Row{"positional", "input", 0, "a.csv"});
    CHECK(rows[3] == Row{"positional", "input", 1, "b.csv"});
    CHECK(rows.size() == 4);  // `out` is optional and absent -> no row
}

TEST_CASE("parse_args: defaults and flag fallback", "[args]") {
    const auto rows = rows_of(parse_args_from_tokens("threads : int = 4 ; debug : flag", {}));
    CHECK(rows.size() == 2);
    CHECK(rows[0] == Row{"option", "threads", 0, "4"});
    CHECK(rows[1] == Row{"flag", "debug", 0, "false"});
}

TEST_CASE("parse_args: --name=value, --no-flag, repeated", "[args]") {
    const std::string spec = "out : string ; verbose : flag ; inc (I) : string +";
    const auto rows = rows_of(parse_args_from_tokens(
        spec, tokens({"--out=/tmp/x", "--no-verbose", "-I", "one", "-I", "two"})));
    CHECK(rows[0] == Row{"option", "out", 0, "/tmp/x"});
    CHECK(rows[1] == Row{"flag", "verbose", 0, "false"});
    CHECK(rows[2] == Row{"option", "inc", 0, "one"});
    CHECK(rows[3] == Row{"option", "inc", 1, "two"});
}

TEST_CASE("parse_args: `--` forces positionals", "[args]") {
    const auto rows = rows_of(parse_args_from_tokens("rest : positional*", tokens({"--", "-x", "--y"})));
    CHECK(rows[0] == Row{"positional", "rest", 0, "-x"});
    CHECK(rows[1] == Row{"positional", "rest", 1, "--y"});
}

TEST_CASE("parse_args: errors", "[args]") {
    CHECK_THROWS_WITH(parse_args_from_tokens("port : int", tokens({"--port", "abc"})),
                      Catch::Matchers::ContainsSubstring("invalid value for --port"));
    CHECK_THROWS_WITH(parse_args_from_tokens("port : int", {}),
                      Catch::Matchers::ContainsSubstring("missing required option: --port"));
    CHECK_THROWS_WITH(parse_args_from_tokens("port : int", tokens({"--bogus", "1"})),
                      Catch::Matchers::ContainsSubstring("unknown option: --bogus"));
    CHECK_THROWS_WITH(parse_args_from_tokens("port : int", tokens({"--port"})),
                      Catch::Matchers::ContainsSubstring("needs a value"));
    CHECK_THROWS_WITH(parse_args_from_tokens("a : int flag positional", {}),
                      Catch::Matchers::ContainsSubstring("trailing tokens"));
    CHECK_THROWS_WITH(parse_args_from_tokens("x : notatype", {}),
                      Catch::Matchers::ContainsSubstring("unknown option type"));
}

TEST_CASE("parse_args: a single-value option given twice is an error", "[args]") {
    CHECK_THROWS_WITH(
        parse_args_from_tokens("out : string", tokens({"--out", "a", "--out", "b"})),
        Catch::Matchers::ContainsSubstring("more than once"));
}

TEST_CASE("parse_args: bare positional when none declared gets name \"\"", "[args]") {
    const auto rows = rows_of(parse_args_from_tokens("v : flag", tokens({"one", "two"})));
    CHECK(rows[0] == Row{"positional", "", 0, "one"});
    CHECK(rows[1] == Row{"positional", "", 1, "two"});
}

TEST_CASE("parse_args: end-to-end through the REPL with scalar() + casts", "[args][e2e]") {
    ibex::runtime::ExternRegistry registry;
    registry.register_table(
        "parse_args",
        [](const ibex::runtime::ExternArgs& args)
            -> std::expected<ibex::runtime::ExternValue, std::string> {
            const auto* spec = args.empty() ? nullptr : std::get_if<std::string>(&args[0]);
            const auto* argv = args.size() >= 2 ? std::get_if<std::string>(&args[1]) : nullptr;
            if (spec == nullptr) {
                return std::unexpected("parse_args() expects a string spec");
            }
            try {
                return ibex::runtime::ExternValue{
                    parse_args(*spec, argv != nullptr ? *argv : std::string{})};
            } catch (const std::exception& e) {
                return std::unexpected(std::string(e.what()));
            }
        });

    ibex::repl::ReplSession session(ibex::repl::ReplConfig{}, registry);
    const auto setup = session.execute(R"(
extern fn parse_args(spec: String, argv: String = "") -> DataFrame from "args.hpp";
let spec = "threads (t) : int = 4 ; limit : int? ; verbose (v) : flag";
let args = parse_args(spec, "-t 9 -v");
)");
    REQUIRE(setup.ok);

    SECTION("cast of a present option") {
        const auto r = session.execute(
            "Int64(scalar(args[filter name == \"threads\", select { value }]));");
        REQUIRE(r.ok);
        REQUIRE(r.scalar.has_value());
        CHECK(std::get<std::int64_t>(*r.scalar) == 9);
    }
    SECTION("absent optional -> null -> coalesced default") {
        const auto r = session.execute(
            "Int64(coalesce(scalar(args[filter name == \"limit\", select { value }]), \"-1\"));");
        REQUIRE(r.ok);
        REQUIRE(r.scalar.has_value());
        CHECK(std::get<std::int64_t>(*r.scalar) == -1);
    }
    SECTION("flag reads back as a string") {
        const auto r =
            session.execute("scalar(args[filter name == \"verbose\", select { value }]);");
        REQUIRE(r.ok);
        REQUIRE(r.scalar.has_value());
        CHECK(std::get<std::string>(*r.scalar) == "true");
    }
}
