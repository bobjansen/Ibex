// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#include <ibex/core/column.hpp>
#include <ibex/core/decimal.hpp>
#include <ibex/repl/repl.hpp>
#include <ibex/runtime/env.hpp>
#include <ibex/runtime/extern_registry.hpp>
#include <ibex/runtime/interpreter.hpp>
#include <ibex/runtime/lazy_table.hpp>

#include <catch2/catch_message.hpp>
#include <catch2/catch_test_macros.hpp>

#include <algorithm>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <csv.hpp>
#include <exception>
#include <expected>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <limits>
#include <memory>
#include <optional>
#include <ranges>
#include <set>
#include <stdio.h>
#include <string>
#include <variant>
#ifdef _WIN32
#include <io.h>
#include <process.h>
#else
#include <unistd.h>
#endif
#include <utility>
#include <vector>

using ibex::repl::normalize_input;

TEST_CASE("REPL normalizes implicit semicolons") {
    REQUIRE(normalize_input("1+1") == "1+1;");
    REQUIRE(normalize_input("let x = 1;") == "let x = 1;");
    REQUIRE(normalize_input("  1+1  ") == "1+1;");
    REQUIRE(normalize_input("").empty());
}

TEST_CASE("REPL loads script with inferred lets") {
    const std::filesystem::path script_path =
        std::filesystem::path(IBEX_SOURCE_DIR) / "tests" / "data" / "repl_infer.ibex";
    std::ifstream input(script_path);
    REQUIRE(input.good());
    std::string source((std::istreambuf_iterator<char>(input)), std::istreambuf_iterator<char>());
    const std::string token = "${IBEX_SOURCE_DIR}";
    std::size_t pos = 0;
    while ((pos = source.find(token, pos)) != std::string::npos) {
        source.replace(pos, token.size(), IBEX_SOURCE_DIR);
        pos += std::char_traits<char>::length(IBEX_SOURCE_DIR);
    }

    ibex::runtime::ExternRegistry registry;
    registry.register_table("csv::read",
                            [](const ibex::runtime::ExternArgs& args)
                                -> std::expected<ibex::runtime::ExternValue, std::string> {
                                if (args.size() != 1) {
                                    return std::unexpected("csv::read() expects 1 argument");
                                }
                                const auto* path = std::get_if<std::string>(args.data());
                                if (path == nullptr) {
                                    return std::unexpected("csv::read() expects a string path");
                                }
                                try {
                                    return ibex::runtime::ExternValue{ibex::ext::csv::read(*path)};
                                } catch (const std::exception& e) {
                                    return std::unexpected(std::string(e.what()));
                                }
                            });

    REQUIRE(ibex::repl::execute_script(source, registry));
}

TEST_CASE("REPL executes a multi-line statement via execute_script", "[repl][multiline]") {
    ibex::runtime::ExternRegistry registry;
    const std::string src =
        "let t = Table {\n"
        "  x = [1.0, 2.0, 3.0],\n"
        "  y = [3.0, 5.0, 7.0]\n"
        "};\n"
        "let m = t[model {\n"
        "  y ~ x,\n"
        "  method = ols\n"
        "}];\n"
        "print(model_coef(m));\n";
    REQUIRE(ibex::repl::execute_script(src, registry));
}

TEST_CASE("REPL run_file runs a script file with multi-line statements", "[repl][multiline]") {
    auto path = std::filesystem::temp_directory_path() / "ibex_multiline_test.ibex";
    {
        std::ofstream out(path);
        REQUIRE(out.good());
        out << "let t = Table {\n"
               "  x = [1.0, 2.0, 3.0],\n"
               "  y = [3.0, 5.0, 7.0]\n"
               "};\n"
               "let m = t[model {\n"
               "  y ~ x,\n"
               "  method = ols\n"
               "}];\n";
    }
    ibex::runtime::ExternRegistry registry;
    const ibex::repl::ReplConfig config;
    REQUIRE(ibex::repl::run_file(path.string(), config, registry));
    std::filesystem::remove(path);
}

TEST_CASE("REPL run_file reports a missing file", "[repl][multiline]") {
    ibex::runtime::ExternRegistry registry;
    const ibex::repl::ReplConfig config;
    REQUIRE_FALSE(ibex::repl::run_file("/nonexistent/path/does_not_exist.ibex", config, registry));
}

TEST_CASE("REPL accepts scalar expression statements") {
    ibex::runtime::ExternRegistry registry;
    REQUIRE(ibex::repl::execute_script("1+1;", registry));
}

TEST_CASE("REPL session preserves bindings and returns structured results", "[repl][session]") {
    ibex::runtime::ExternRegistry registry;
    ibex::repl::ReplSession session(ibex::repl::ReplConfig{}, registry);

    const auto result = session.execute(
        "let numbers = Table { value = [1, 2, 3] };\n"
        "numbers[filter value > 1];");
    REQUIRE(result.ok);
    REQUIRE(result.table.has_value());
    REQUIRE(result.table->rows() == 2);
    REQUIRE(result.tables.size() == 1);

    const auto environment = session.environment();
    REQUIRE(std::ranges::any_of(
        environment, [](const auto& entry) { return entry.name == "numbers" && entry.rows == 3; }));
    REQUIRE(session.erase("numbers"));
    REQUIRE_FALSE(session.erase("numbers"));
}

TEST_CASE("REPL session collects every rendered table", "[repl][session]") {
    ibex::runtime::ExternRegistry registry;
    ibex::repl::ReplSession session(ibex::repl::ReplConfig{}, registry);

    const auto result = session.execute("trades[select { price }]; trades[select { symbol }];");
    REQUIRE(result.ok);
    REQUIRE(result.tables.size() == 2);
    REQUIRE(result.tables[0].columns[0].name == "price");
    REQUIRE(result.tables[1].columns[0].name == "symbol");
    REQUIRE(result.table.has_value());
    REQUIRE(result.table->columns[0].name == "symbol");
}

TEST_CASE("REPL session returns parser locations and runtime diagnostics", "[repl][session]") {
    ibex::runtime::ExternRegistry registry;
    ibex::repl::ReplSession session(ibex::repl::ReplConfig{}, registry);

    const auto parse_error = session.execute("trades[filter price >];");
    REQUIRE_FALSE(parse_error.ok);
    REQUIRE_FALSE(parse_error.error.empty());
    REQUIRE(parse_error.error_line.has_value());
    REQUIRE(parse_error.error_column.has_value());

    const auto runtime_error = session.execute("unknown_table;");
    REQUIRE_FALSE(runtime_error.ok);
    REQUIRE(runtime_error.error.find("unknown table") != std::string::npos);
}

TEST_CASE("REPL print builtin displays tables, scalars, and columns", "[repl][print]") {
    ibex::runtime::ExternRegistry registry;
    // Table expression, scalar, and a column all render without error.
    REQUIRE(ibex::repl::execute_script("print(trades);", registry));
    REQUIRE(ibex::repl::execute_script("print(trades[select { n = count() }]);", registry));
    REQUIRE(ibex::repl::execute_script("print(1 + 1);", registry));
    REQUIRE(ibex::repl::execute_script("print(trades[select { price }]);", registry));
}

TEST_CASE("REPL print displays text, not just tables", "[repl][print]") {
    ibex::runtime::ExternRegistry registry;
    // A bare string literal and a bound string scalar both print as text.
    REQUIRE(ibex::repl::execute_script("print(\"=== a header line ===\");", registry));
    REQUIRE(ibex::repl::execute_script("let s = \"some text\"; print(s);", registry));
    // A bare string-literal statement also renders (no print needed).
    REQUIRE(ibex::repl::execute_script("\"bare text\";", registry));
}

TEST_CASE("REPL evaluates scalar math builtins at top level", "[repl][math]") {
    ibex::runtime::ExternRegistry registry;
    // Math builtins now work in scalar position, not just in update/select.
    REQUIRE(ibex::repl::execute_script("print(sqrt(4.0));", registry));
    REQUIRE(ibex::repl::execute_script("print(sin(0.0));", registry));
    REQUIRE(ibex::repl::execute_script("print(log10(1000.0));", registry));
    REQUIRE(ibex::repl::execute_script("print(abs(-5));", registry));
    REQUIRE(ibex::repl::execute_script("print(pmax(3, 7));", registry));
    // Composes in arithmetic, let-binding, and interpolation.
    REQUIRE(ibex::repl::execute_script("print(sqrt(2.0) * 2.0);", registry));
    REQUIRE(ibex::repl::execute_script("let r = log2(8.0); print(r);", registry));
    REQUIRE(ibex::repl::execute_script("let x = 16.0; print(`sqrt = ${sqrt(x)}`);", registry));
    // Casts keep their dedicated handling (better error than the registry).
    REQUIRE_FALSE(ibex::repl::execute_script("print(Int64(3.5));", registry));
    // A genuinely unknown function still errors.
    REQUIRE_FALSE(ibex::repl::execute_script("print(bogus(1.0));", registry));
}

TEST_CASE("REPL: scalar() of an empty one-column table is null, and propagates", "[repl][null]") {
    ibex::runtime::ExternRegistry registry;
    ibex::repl::ReplSession session(ibex::repl::ReplConfig{}, registry);

    const auto setup = session.execute("let t = Table { x = [10, 20], y = [\"a\", \"b\"] };");
    REQUIRE(setup.ok);

    SECTION("bare null") {
        const auto r = session.execute("scalar(t[filter x > 100, select { x }]);");
        REQUIRE(r.ok);
        REQUIRE(r.scalar.has_value());
        CHECK(ibex::runtime::is_null_scalar(*r.scalar));
    }
    SECTION("coalesce recovers a default") {
        const auto r = session.execute("coalesce(scalar(t[filter x > 100, select { x }]), 999);");
        REQUIRE(r.ok);
        REQUIRE(r.scalar.has_value());
        REQUIRE(std::holds_alternative<std::int64_t>(*r.scalar));
        CHECK(std::get<std::int64_t>(*r.scalar) == 999);
    }
    SECTION("cast of null is null, not an error") {
        const auto r = session.execute("Int64(scalar(t[filter x > 100, select { x }]));");
        REQUIRE(r.ok);
        REQUIRE(r.scalar.has_value());
        CHECK(ibex::runtime::is_null_scalar(*r.scalar));
    }
    SECTION("a present value still extracts") {
        const auto r = session.execute("scalar(t[filter x == 10, select { x }]);");
        REQUIRE(r.ok);
        REQUIRE(r.scalar.has_value());
        REQUIRE(std::holds_alternative<std::int64_t>(*r.scalar));
        CHECK(std::get<std::int64_t>(*r.scalar) == 10);
    }
    SECTION("two-argument scalar() still requires exactly one row") {
        const auto r = session.execute("scalar(t[filter x > 100], x);");
        CHECK_FALSE(r.ok);
    }
}

TEST_CASE("REPL aggregates reduce a series to a scalar", "[repl][series]") {
    ibex::runtime::ExternRegistry registry;
    REQUIRE(ibex::repl::execute_script("let s = [1, 5, 3]; print(max(s));", registry));
    REQUIRE(ibex::repl::execute_script("let s = [1, 5, 3]; print(sum(s));", registry));
    REQUIRE(ibex::repl::execute_script("let s = [1, 5, 3]; print(mean(s));", registry));
    REQUIRE(ibex::repl::execute_script("let s = [1, 5, 3]; print(min(s));", registry));
    // Aggregate result is a scalar, usable in arithmetic and interpolation.
    REQUIRE(ibex::repl::execute_script("let s = [1.0, 5.0]; print(max(s) + 1.0);", registry));
    REQUIRE(ibex::repl::execute_script("let s = [1, 5, 3]; print(`max=${max(s)}`);", registry));
    // ewma/quantile carry a numeric parameter.
    REQUIRE(
        ibex::repl::execute_script("let s = [1.0, 2.0, 3.0]; print(quantile(s, 0.5));", registry));
}

TEST_CASE("REPL scalar builtins apply element-wise to series", "[repl][series]") {
    ibex::runtime::ExternRegistry registry;
    // pmax over two series returns the element-wise max series.
    REQUIRE(ibex::repl::execute_script("print(pmax([1,2,3], [3,2,1]));", registry));
    // A scalar argument broadcasts.
    REQUIRE(ibex::repl::execute_script("print(pmax([1,5,3], 4));", registry));
    // Unary math builtins map over a series.
    REQUIRE(ibex::repl::execute_script("print(sqrt([1.0, 4.0, 9.0]));", registry));
    REQUIRE(ibex::repl::execute_script("print(abs([-1, 2, -3]));", registry));
    // Mismatched series lengths error.
    REQUIRE_FALSE(ibex::repl::execute_script("print(pmax([1,2,3], [1,2]));", registry));
    // A series result where a scalar is required (arithmetic) errors.
    REQUIRE_FALSE(ibex::repl::execute_script("print(pmax([1,2],[3,4]) + 1);", registry));
}

TEST_CASE("REPL string interpolation in scalar position", "[repl][interp]") {
    ibex::runtime::ExternRegistry registry;
    // Backtick templates with ${expr} interpolate at runtime.
    REQUIRE(ibex::repl::execute_script("let r2 = 0.5; print(`R-squared = ${r2}`);", registry));
    REQUIRE(ibex::repl::execute_script(
        "let a = 3; let b = 4; print(`sum of ${a} and ${b} is ${a + b}`);", registry));
    // A let-bound interpolation is a String scalar usable downstream.
    REQUIRE(ibex::repl::execute_script("let n = 5; let msg = `n is ${n}`; print(msg);", registry));
    // A plain backtick (no ${}) is still a quoted identifier (column ref), not text.
    REQUIRE(ibex::repl::execute_script(
        "let t = Table { `od.d` = [1, 2] }; t[select { y = `od.d` * 2 }];", registry));
    // A malformed embedded expression is a parse error.
    REQUIRE_FALSE(ibex::repl::execute_script("print(`x = ${1 +}`);", registry));
}

TEST_CASE("REPL print passes its argument through for binding", "[repl][print]") {
    ibex::runtime::ExternRegistry registry;
    // print(x) returns x, so it can be bound and used downstream.
    REQUIRE(ibex::repl::execute_script(
        "let m = print(trades[select { hi = max(price) }]); m[select { hi }];", registry));
}

TEST_CASE("REPL print rejects the wrong number of arguments", "[repl][print]") {
    ibex::runtime::ExternRegistry registry;
    REQUIRE_FALSE(ibex::repl::execute_script("print(trades, trades);", registry));
    REQUIRE_FALSE(ibex::repl::execute_script("print();", registry));
}

TEST_CASE("REPL: integer literal widens to Float64 in let binding", "[repl][coerce]") {
    ibex::runtime::ExternRegistry registry;
    REQUIRE(ibex::repl::execute_script("let x: Float64 = 42; x + 0.5;", registry));
}

TEST_CASE("REPL: integer literal widens to Float32 in let binding", "[repl][coerce]") {
    ibex::runtime::ExternRegistry registry;
    REQUIRE(ibex::repl::execute_script("let x: Float32 = 7; x;", registry));
}

TEST_CASE("REPL: float literal does NOT silently narrow to Int", "[repl][coerce]") {
    ibex::runtime::ExternRegistry registry;
    REQUIRE_FALSE(ibex::repl::execute_script("let x: Int64 = 3.14;", registry));
}

TEST_CASE("REPL: integer literal does NOT auto-coerce to Bool or String", "[repl][coerce]") {
    ibex::runtime::ExternRegistry registry;
    REQUIRE_FALSE(ibex::repl::execute_script("let b: Bool = 1;", registry));
    REQUIRE_FALSE(ibex::repl::execute_script("let s: String = 5;", registry));
}

TEST_CASE("REPL preserves a Decimal scalar literal", "[repl][decimal]") {
    ibex::runtime::ExternRegistry registry;
    ibex::repl::ReplSession session(ibex::repl::ReplConfig{}, registry);
    const auto result = session.execute("let amount = decimal\"-12.30\"; amount;");
    REQUIRE(result.ok);
    REQUIRE(result.scalar.has_value());
    const auto* value = std::get_if<ibex::DecimalValue>(&*result.scalar);
    REQUIRE(value != nullptr);
    CHECK(ibex::decimal::to_string(*value) == "-12.30");
}

TEST_CASE("REPL binds a date or timestamp literal as a scalar", "[repl][time]") {
    // These parse into LiteralExpr like any other literal, but the top-level
    // scalar binder only unpacked Int/Double/Bool/String and rejected the rest
    // as an "unsupported scalar literal".
    ibex::runtime::ExternRegistry registry;
    REQUIRE(ibex::repl::execute_script("let d = date\"2013-01-02\"; d;", registry));
    REQUIRE(ibex::repl::execute_script("let t = ts\"2013-01-02T03:04:05\"; t;", registry));
    REQUIRE(ibex::repl::execute_script("let d: Date = date\"2013-01-02\"; d;", registry));
}

TEST_CASE("REPL applies Date() to a top-level scalar and column", "[repl][time][cast]") {
    // The REPL evaluates casts outside a table through its own scalar/column
    // implementations, separate from the runtime's builtin registry.
    ibex::runtime::ExternRegistry registry;
    REQUIRE(ibex::repl::execute_script("let d = Date(ts\"2013-01-02T23:59:59\"); d;", registry));
    REQUIRE(ibex::repl::execute_script("Date(date\"2013-01-02\");", registry));
    REQUIRE(
        ibex::repl::execute_script("let t = trades[select { ts = ts\"2013-01-02T23:59:59\" }];\n"
                                   "let (c) = t;\n"
                                   "Date(c);",
                                   registry));
    REQUIRE_FALSE(ibex::repl::execute_script("Date(\"2013-01-02\");", registry));
}

TEST_CASE("REPL executes multi-statement script with chained let bindings") {
    ibex::runtime::TableRegistry tables;
    ibex::runtime::Table t;
    t.add_column("price", ibex::Column<std::int64_t>{10, 20, 30});
    t.add_column("symbol", ibex::Column<std::string>{"A", "B", "A"});
    tables.emplace("trades", t);

    ibex::runtime::ExternRegistry registry;
    // Inject "trades" as a table-returning extern so the script can bind it.
    registry.register_table("get_trades",
                            [&tables](const ibex::runtime::ExternArgs&)
                                -> std::expected<ibex::runtime::ExternValue, std::string> {
                                return ibex::runtime::ExternValue{tables.at("trades")};
                            });

    const char* src = R"(
extern fn get_trades() -> DataFrame from "fake.hpp";
let t = get_trades();
let filtered = t[filter price > 15];
filtered[select { price }];
)";
    REQUIRE(ibex::repl::execute_script(src, registry));
}

TEST_CASE("REPL executes compile-time map expansion with string-list let binding") {
    ibex::runtime::ExternRegistry registry;

    const char* src = R"(
let trades = Table { price = [10.0, 20.0], qty = [2.0, 4.0] };
let measures = ["price", "qty"];
trades[select {
    map m in measures => `avg_${m}` = mean(get(m))
}];
)";

    REQUIRE(ibex::repl::execute_script(src, registry));
}

TEST_CASE("REPL binds array literal as Series") {
    ibex::runtime::ExternRegistry registry;

    const char* src = R"(
let x = [1, 2, 3];
let t = Table { x = x };
t[select { total = sum(x) }];
)";

    REQUIRE(ibex::repl::execute_script(src, registry));
}

TEST_CASE("REPL uses Series annotation for empty array literal") {
    ibex::runtime::ExternRegistry registry;

    const char* src = R"(
let x: Series<Float64> = [];
x;
)";

    REQUIRE(ibex::repl::execute_script(src, registry));
}

TEST_CASE("REPL rejects untyped empty array literal") {
    ibex::runtime::ExternRegistry registry;

    const char* src = R"(
let x = [];
)";

    REQUIRE_FALSE(ibex::repl::execute_script(src, registry));
}

TEST_CASE("REPL executes compile-time map expansion from columns metadata table") {
    ibex::runtime::ExternRegistry registry;

    const char* src = R"(
let trades = Table { price = [10.0, 20.0], qty = [2.0, 4.0] };
let cols = columns(trades);
trades[update {
    map c in cols => `copy_${c}` = get(c)
}];
)";

    REQUIRE(ibex::repl::execute_script(src, registry));
}

TEST_CASE("REPL executes function-local compile-time map expansion") {
    ibex::runtime::ExternRegistry registry;

    const char* src = R"(
fn summarize(t: DataFrame) -> DataFrame {
    let measures = ["price", "qty"];
    t[select {
        map m in measures => `avg_${m}` = mean(get(m))
    }];
}

let trades = Table { price = [10.0, 20.0], qty = [2.0, 4.0] };
summarize(trades);
)";

    REQUIRE(ibex::repl::execute_script(src, registry));
}

TEST_CASE("REPL supports model bindings with default ols and model accessors") {
    ibex::runtime::ExternRegistry registry;

    const char* src = R"(
let t = Table { x = [1.0, 2.0, 3.0, 4.0, 5.0], y = [3.0, 5.0, 7.0, 9.0, 11.0] };
let m = t[model { y ~ x }];
let coefs = model_coef(m);
let stats = model_summary(m);
let yhat = model_fitted(m);
let resid = model_residuals(m);
let r2 = model_r_squared(m);
r2;
)";

    REQUIRE(ibex::repl::execute_script(src, registry));
}

TEST_CASE("REPL returns false on parse error") {
    ibex::runtime::ExternRegistry registry;
    // Missing closing bracket
    REQUIRE_FALSE(ibex::repl::execute_script("trades[filter price >];", registry));
}

TEST_CASE("REPL returns false on unknown table") {
    ibex::runtime::ExternRegistry registry;
    REQUIRE_FALSE(ibex::repl::execute_script("no_such_table[select { x }];", registry));
}

TEST_CASE("REPL executes pipeline: filter, aggregate, order") {
    ibex::runtime::TableRegistry tables;
    ibex::runtime::Table t;
    t.add_column("price", ibex::Column<std::int64_t>{10, 20, 30, 5});
    t.add_column("symbol", ibex::Column<std::string>{"A", "B", "A", "B"});
    tables.emplace("trades", t);

    ibex::runtime::ExternRegistry registry;
    registry.register_table("get_trades",
                            [&tables](const ibex::runtime::ExternArgs&)
                                -> std::expected<ibex::runtime::ExternValue, std::string> {
                                return ibex::runtime::ExternValue{tables.at("trades")};
                            });

    const char* src = R"(
extern fn get_trades() -> DataFrame from "fake.hpp";
let t = get_trades();
t[select { symbol, total = sum(price) }, by symbol, order { total desc }];
)";
    REQUIRE(ibex::repl::execute_script(src, registry));
}

TEST_CASE("REPL executes :load via execute_script with file path") {
    // Write a small script to a temp file and execute it via execute_script.
    auto script_path = std::filesystem::temp_directory_path() / "ibex_repl_test_load.ibex";
    {
        std::ofstream out(script_path);
        out << "let x = 1 + 1;\n";
        out << "x;\n";
    }

    ibex::runtime::ExternRegistry registry;
    // Read and execute manually (same as :load internals)
    std::ifstream in(script_path);
    REQUIRE(in.good());
    const std::string src((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
    REQUIRE(ibex::repl::execute_script(src, registry));
}

TEST_CASE("REPL executes tuple let binding from DataFrame") {
    ibex::runtime::ExternRegistry registry;

    // Returns a 2-column DataFrame: (x, y)
    registry.register_table("make_xy",
                            [](const ibex::runtime::ExternArgs&)
                                -> std::expected<ibex::runtime::ExternValue, std::string> {
                                ibex::runtime::Table t;
                                t.add_column("x", ibex::Column<std::int64_t>{1, 2, 3});
                                t.add_column("y", ibex::Column<std::int64_t>{4, 5, 6});
                                return ibex::runtime::ExternValue{std::move(t)};
                            });

    const char* src = R"(
extern fn make_xy() -> DataFrame from "fake.hpp";
let (x, y) = make_xy();
x;
)";
    REQUIRE(ibex::repl::execute_script(src, registry));
}

TEST_CASE("REPL tuple let binding: column count mismatch fails") {
    ibex::runtime::ExternRegistry registry;

    registry.register_table("make_two",
                            [](const ibex::runtime::ExternArgs&)
                                -> std::expected<ibex::runtime::ExternValue, std::string> {
                                ibex::runtime::Table t;
                                t.add_column("a", ibex::Column<std::int64_t>{1, 2});
                                t.add_column("b", ibex::Column<std::int64_t>{3, 4});
                                return ibex::runtime::ExternValue{std::move(t)};
                            });

    const char* src = R"(
extern fn make_two() -> DataFrame from "fake.hpp";
let (a, b, c) = make_two();
a;
)";
    REQUIRE_FALSE(ibex::repl::execute_script(src, registry));
}

TEST_CASE("seed_rng is accepted as a statement and returns its argument") {
    ibex::runtime::ExternRegistry registry;
    // seed_rng(n) should execute without error and echo the seed as a scalar.
    REQUIRE(ibex::repl::execute_script("seed_rng(42);", registry));
}

TEST_CASE("seed_rng rejects wrong argument type") {
    ibex::runtime::ExternRegistry registry;
    REQUIRE_FALSE(ibex::repl::execute_script("seed_rng(1.5);", registry));
}

TEST_CASE("seed_rng rejects wrong argument count") {
    ibex::runtime::ExternRegistry registry;
    REQUIRE_FALSE(ibex::repl::execute_script("seed_rng(1, 2);", registry));
    REQUIRE_FALSE(ibex::repl::execute_script("seed_rng();", registry));
}

TEST_CASE("REPL supports named arguments and defaults for user functions") {
    ibex::runtime::ExternRegistry registry;

    const char* src = R"(
fn add3(x: Int, y: Int = 1, z: Int = 2) -> Int {
  x + y + z;
}
let a = add3(10);
let b = add3(10, z = 5);
let c = add3(x = 10, z = 5, y = 7);
c;
)";

    REQUIRE(ibex::repl::execute_script(src, registry));
}

TEST_CASE("REPL resolves forward references between user functions") {
    ibex::runtime::ExternRegistry registry;

    const char* src = R"(
fn caller(x: Int) -> Int {
  callee(x) + 1;
}
fn callee(x: Int) -> Int {
  x * 2;
}
let r = caller(5);
r;
)";

    REQUIRE(ibex::repl::execute_script(src, registry));
}

TEST_CASE("REPL resolves forward reference from a DataFrame helper to a later helper") {
    ibex::runtime::ExternRegistry registry;
    ibex::runtime::Table employee;
    employee.add_column("salary", ibex::Column<std::int64_t>{100, 200, 300, 200, 100});
    registry.register_table("employee_src",
                            [employee](const ibex::runtime::ExternArgs&)
                                -> std::expected<ibex::runtime::ExternValue, std::string> {
                                return ibex::runtime::ExternValue{employee};
                            });

    const char* src = R"(
extern fn employee_src() -> DataFrame from "fake.hpp";

fn second_highest(df: DataFrame<{salary: Int64}>) -> DataFrame {
  nth_highest(df, 2);
}
fn nth_highest(df: DataFrame<{salary: Int64}>, n: Int) -> DataFrame {
  df[distinct { salary }, order { salary desc }, head n];
}

let employee = employee_src();
let result = second_highest(employee);
result;
)";

    REQUIRE(ibex::repl::execute_script(src, registry));
}

// --- Plan litmus tests: user-defined query helpers (plans/done/udf-dataframe-plan.md) ---
// These exercise the motivating examples end to end through the REPL/runtime path
// and assert on the produced values, not just that the script runs. A scalar
// table-consumer extern ("capture") stashes the helper's result table so the test
// can inspect it.

TEST_CASE("REPL litmus: table-in table-out helper (top three salaries per dept)", "[repl][udf]") {
    ibex::runtime::ExternRegistry registry;
    ibex::runtime::Table employee;
    employee.add_column("departmentId", ibex::Column<std::int64_t>{1, 1, 1, 1, 2, 2});
    employee.add_column("salary", ibex::Column<std::int64_t>{100, 200, 300, 300, 400, 500});
    registry.register_table("employee_src",
                            [employee](const ibex::runtime::ExternArgs&)
                                -> std::expected<ibex::runtime::ExternValue, std::string> {
                                return ibex::runtime::ExternValue{employee};
                            });
    ibex::runtime::Table captured;
    registry.register_scalar_table_consumer(
        "capture", ibex::runtime::ScalarKind::Int,
        [&captured](const ibex::runtime::Table& t, const ibex::runtime::ExternArgs&)
            -> std::expected<ibex::runtime::ExternValue, std::string> {
            captured = t;
            return ibex::runtime::ExternValue{std::int64_t{0}};
        });

    const char* src = R"(
extern fn employee_src() -> DataFrame from "fake.hpp";
extern fn capture(df: DataFrame) -> Int from "fake.hpp";

fn top_three_salaries(employee: DataFrame) -> DataFrame {
  let distinct_salaries = employee[distinct { departmentId, salary }];
  distinct_salaries[order { salary desc }, head 3, by departmentId];
}

let employee = employee_src();
capture(top_three_salaries(employee));
)";
    REQUIRE(ibex::repl::execute_script(src, registry));

    // dept 1 distinct salaries {100,200,300} -> top 3 = {300,200,100}
    // dept 2 distinct salaries {400,500}     -> top 3 = {500,400}
    const auto* dept = std::get_if<ibex::Column<std::int64_t>>(captured.find("departmentId"));
    const auto* salary = std::get_if<ibex::Column<std::int64_t>>(captured.find("salary"));
    REQUIRE(dept != nullptr);
    REQUIRE(salary != nullptr);
    REQUIRE(salary->size() == 5);
    std::set<std::pair<std::int64_t, std::int64_t>> pairs;
    for (std::size_t i = 0; i < salary->size(); ++i) {
        pairs.insert({(*dept)[i], (*salary)[i]});
    }
    const std::set<std::pair<std::int64_t, std::int64_t>> expected{
        {1, 300}, {1, 200}, {1, 100}, {2, 500}, {2, 400},
    };
    REQUIRE(pairs == expected);
}

TEST_CASE("REPL litmus: table plus scalar helper (nth highest salaries)", "[repl][udf]") {
    ibex::runtime::ExternRegistry registry;
    ibex::runtime::Table employee;
    employee.add_column("salary", ibex::Column<std::int64_t>{100, 200, 300, 300, 400, 500});
    registry.register_table("employee_src",
                            [employee](const ibex::runtime::ExternArgs&)
                                -> std::expected<ibex::runtime::ExternValue, std::string> {
                                return ibex::runtime::ExternValue{employee};
                            });
    ibex::runtime::Table captured;
    registry.register_scalar_table_consumer(
        "capture", ibex::runtime::ScalarKind::Int,
        [&captured](const ibex::runtime::Table& t, const ibex::runtime::ExternArgs&)
            -> std::expected<ibex::runtime::ExternValue, std::string> {
            captured = t;
            return ibex::runtime::ExternValue{std::int64_t{0}};
        });

    const char* src = R"(
extern fn employee_src() -> DataFrame from "fake.hpp";
extern fn capture(df: DataFrame) -> Int from "fake.hpp";

fn nth_highest_salary(employee: DataFrame, n: Int) -> DataFrame {
  let top_n = employee[distinct { salary }, order { salary desc }, head n];
  top_n;
}

let employee = employee_src();
capture(nth_highest_salary(employee, 3));
)";
    REQUIRE(ibex::repl::execute_script(src, registry));

    // distinct salaries {100,200,300,400,500} ordered desc, head 3 -> {500,400,300}
    const auto* salary = std::get_if<ibex::Column<std::int64_t>>(captured.find("salary"));
    REQUIRE(salary != nullptr);
    const std::vector<std::int64_t> values(salary->begin(), salary->end());
    REQUIRE(values == std::vector<std::int64_t>{500, 400, 300});
}

TEST_CASE("REPL litmus: DataFrame<{salary}> contract requires the declared column", "[repl][udf]") {
    // A helper whose argument declares a required column. The contract is a
    // minimum-required-columns check: extra columns are allowed, a missing
    // required column is a call-time error.
    const char* fn_src = R"(
extern fn employee_src() -> DataFrame from "fake.hpp";
extern fn capture(df: DataFrame) -> Int from "fake.hpp";

fn second_highest_salary(employee: DataFrame<{ salary: Int }>) -> DataFrame {
  employee[distinct { salary }, order { salary desc }, head 2];
}
)";

    SECTION("accepts a table that provides the required column (plus extras)") {
        ibex::runtime::ExternRegistry registry;
        ibex::runtime::Table employee;
        employee.add_column("departmentId", ibex::Column<std::int64_t>{1, 1, 2});
        employee.add_column("salary", ibex::Column<std::int64_t>{100, 300, 200});
        registry.register_table("employee_src",
                                [employee](const ibex::runtime::ExternArgs&)
                                    -> std::expected<ibex::runtime::ExternValue, std::string> {
                                    return ibex::runtime::ExternValue{employee};
                                });
        ibex::runtime::Table captured;
        registry.register_scalar_table_consumer(
            "capture", ibex::runtime::ScalarKind::Int,
            [&captured](const ibex::runtime::Table& t, const ibex::runtime::ExternArgs&)
                -> std::expected<ibex::runtime::ExternValue, std::string> {
                captured = t;
                return ibex::runtime::ExternValue{std::int64_t{0}};
            });

        const std::string src = std::string(fn_src) + "let employee = employee_src();\n" +
                                "capture(second_highest_salary(employee));\n";
        REQUIRE(ibex::repl::execute_script(src, registry));

        const auto* salary = std::get_if<ibex::Column<std::int64_t>>(captured.find("salary"));
        REQUIRE(salary != nullptr);
        const std::vector<std::int64_t> values(salary->begin(), salary->end());
        REQUIRE(values == std::vector<std::int64_t>{300, 200});
    }

    SECTION("rejects a table missing the required column") {
        ibex::runtime::ExternRegistry registry;
        ibex::runtime::Table no_salary;
        no_salary.add_column("departmentId", ibex::Column<std::int64_t>{1, 2, 3});
        registry.register_table("employee_src",
                                [no_salary](const ibex::runtime::ExternArgs&)
                                    -> std::expected<ibex::runtime::ExternValue, std::string> {
                                    return ibex::runtime::ExternValue{no_salary};
                                });
        registry.register_scalar_table_consumer(
            "capture", ibex::runtime::ScalarKind::Int,
            [](const ibex::runtime::Table&, const ibex::runtime::ExternArgs&)
                -> std::expected<ibex::runtime::ExternValue, std::string> {
                return ibex::runtime::ExternValue{std::int64_t{0}};
            });

        const std::string src = std::string(fn_src) + "let employee = employee_src();\n" +
                                "capture(second_highest_salary(employee));\n";
        REQUIRE_FALSE(ibex::repl::execute_script(src, registry));
    }
}

TEST_CASE("REPL batch planner executes non-final table sinks", "[repl][batch]") {
    ibex::runtime::ExternRegistry registry;
    int source_instances = 0;
    registry.register_lazy_table(
        "read_fake",
        [&source_instances](const ibex::runtime::ExternArgs&)
            -> std::expected<ibex::runtime::LazyTablePtr, std::string> {
            ++source_instances;
            ibex::runtime::Table schema;
            schema.add_column("a", ibex::Column<std::int64_t>{});
            return std::make_shared<ibex::runtime::LazyTable>(
                std::move(schema), 3,
                [](const std::vector<std::string>& names, const ibex::runtime::Selection* selection)
                    -> std::expected<ibex::runtime::Table, std::string> {
                    const std::vector<std::int64_t> values{1, 2, 3};
                    const ibex::runtime::Selection all{0, 1, 2};
                    const auto& rows = selection == nullptr ? all : *selection;
                    ibex::runtime::Table table;
                    for (const auto& name : names) {
                        if (name != "a") {
                            continue;
                        }
                        std::vector<std::int64_t> selected;
                        selected.reserve(rows.size());
                        for (const auto row : rows) {
                            selected.push_back(values[row]);
                        }
                        table.add_column("a", ibex::Column<std::int64_t>{std::move(selected)});
                    }
                    table.logical_rows = rows.size();
                    return table;
                });
        });

    std::vector<std::pair<std::string, std::vector<std::int64_t>>> captures;
    registry.register_scalar_table_consumer(
        "capture", ibex::runtime::ScalarKind::Int,
        [&captures](const ibex::runtime::Table& table, const ibex::runtime::ExternArgs& args)
            -> std::expected<ibex::runtime::ExternValue, std::string> {
            const auto* tag = std::get_if<std::string>(&args.at(0));
            const auto* values = std::get_if<ibex::Column<std::int64_t>>(table.find("a"));
            if (tag == nullptr || values == nullptr) {
                return std::unexpected("capture expected a tag and Int64 column a");
            }
            captures.emplace_back(*tag, std::vector<std::int64_t>(values->begin(), values->end()));
            return ibex::runtime::ExternValue{std::int64_t{0}};
        });

    const char* src = R"(
extern fn read_fake() -> DataFrame from "fake.hpp";
extern fn capture(df: DataFrame, tag: String) -> Int from "fake.hpp";

let input = read_fake();
let early = input[filter a > 1, select { a }];
let early_tag = "early";
capture(early, early_tag);
let result = input[filter a > 2, select { a }];
capture(result, "result");
result;
)";
    REQUIRE(ibex::repl::execute_script(src, registry));
    REQUIRE(captures == std::vector<std::pair<std::string, std::vector<std::int64_t>>>{
                            {"early", {2, 3}},
                            {"result", {3}},
                        });
    // The result sink's table is reused for the final expression, while the
    // earlier sink is independently planned and executed -- two instances. The
    // third is the schema probe: lowering resolves each distinct reader once up
    // front to learn what it returns, which is what lets join filter pushdown
    // see a reader's schema at all (it runs inside lower_script, before
    // canonicalize fuses the Filter it needs to move).
    REQUIRE(source_instances == 3);
}

TEST_CASE("REPL footer estimates handle the full signed integer range", "[repl][batch]") {
    // The footer's min/max are legitimate signed values. Their mathematical
    // inclusive span is 2^64, which does not fit in uint64_t; the planner must
    // saturate the estimate and decline its small-span proof rather than first
    // overflowing a signed subtraction while deriving source statistics.
    ibex::runtime::ExternRegistry registry;
    registry.register_lazy_table(
        "read_extreme",
        [](const ibex::runtime::ExternArgs&)
            -> std::expected<ibex::runtime::LazyTablePtr, std::string> {
            ibex::runtime::Table schema;
            schema.add_column("k", ibex::Column<std::int64_t>{});
            ibex::runtime::SourceColumnStats stats;
            stats.emplace("k", ibex::runtime::ColumnStats{
                                   .min = std::numeric_limits<std::int64_t>::min(),
                                   .max = std::numeric_limits<std::int64_t>::max(),
                                   .null_count = 0,
                               });
            return std::make_shared<ibex::runtime::LazyTable>(
                std::move(schema), 2,
                [](const std::vector<std::string>& names, const ibex::runtime::Selection* selection)
                    -> std::expected<ibex::runtime::Table, std::string> {
                    const std::vector<std::int64_t> values{
                        std::numeric_limits<std::int64_t>::min(),
                        std::numeric_limits<std::int64_t>::max(),
                    };
                    const ibex::runtime::Selection all{0, 1};
                    const auto& rows = selection == nullptr ? all : *selection;
                    ibex::runtime::Table table;
                    if (std::ranges::find(names, "k") != names.end()) {
                        std::vector<std::int64_t> selected;
                        selected.reserve(rows.size());
                        for (const auto row : rows) {
                            selected.push_back(values[row]);
                        }
                        table.add_column("k", ibex::Column<std::int64_t>{std::move(selected)});
                    }
                    table.logical_rows = rows.size();
                    return table;
                },
                std::move(stats));
        });

    REQUIRE(
        ibex::repl::execute_script("extern fn read_extreme() -> DataFrame from \"x.hpp\"; "
                                   "read_extreme()[select { n = count() }, by { k }];",
                                   registry));
}

namespace {

/// Lazy two-column source recording every decode request, for asserting what a
/// plan actually reads. Column `a` is {1,2,3}, column `b` is {10,20,30}.
auto register_recording_lazy_source(
    ibex::runtime::ExternRegistry& registry, std::vector<std::vector<std::string>>& decode_calls,
    std::vector<std::optional<ibex::runtime::Selection>>* decode_selections = nullptr) -> void {
    registry.register_lazy_table(
        "read_fake",
        [&decode_calls, decode_selections](const ibex::runtime::ExternArgs&)
            -> std::expected<ibex::runtime::LazyTablePtr, std::string> {
            ibex::runtime::Table schema;
            schema.add_column("a", ibex::Column<std::int64_t>{});
            schema.add_column("b", ibex::Column<std::int64_t>{});
            return std::make_shared<ibex::runtime::LazyTable>(
                std::move(schema), 3,
                [&decode_calls, decode_selections](const std::vector<std::string>& names,
                                                   const ibex::runtime::Selection* selection)
                    -> std::expected<ibex::runtime::Table, std::string> {
                    decode_calls.push_back(names);
                    if (decode_selections != nullptr) {
                        decode_selections->push_back(
                            selection == nullptr ? std::nullopt : std::optional{*selection});
                    }
                    const ibex::runtime::Selection all{0, 1, 2};
                    const auto& rows = selection == nullptr ? all : *selection;
                    ibex::runtime::Table table;
                    for (const auto& name : names) {
                        std::vector<std::int64_t> values;
                        values.reserve(rows.size());
                        for (const auto row : rows) {
                            values.push_back(
                                static_cast<std::int64_t>(name == "a" ? row + 1 : (row + 1) * 10));
                        }
                        table.add_column(name, ibex::Column<std::int64_t>{std::move(values)});
                    }
                    table.logical_rows = rows.size();
                    return table;
                });
        });
}

auto register_int_capture(ibex::runtime::ExternRegistry& registry, const std::string& column,
                          std::vector<std::int64_t>& captured) -> void {
    registry.register_scalar_table_consumer(
        "capture", ibex::runtime::ScalarKind::Int,
        [&captured, column](const ibex::runtime::Table& table, const ibex::runtime::ExternArgs&)
            -> std::expected<ibex::runtime::ExternValue, std::string> {
            const auto* values = std::get_if<ibex::Column<std::int64_t>>(table.find(column));
            if (values == nullptr) {
                return std::unexpected("capture expected Int64 column " + column);
            }
            captured.assign(values->begin(), values->end());
            return ibex::runtime::ExternValue{std::int64_t{0}};
        });
}

}  // namespace

TEST_CASE("REPL lazy scan: a filtered count() decodes only the predicate column", "[repl][lazy]") {
    // The filter is absorbed by the scan, so demand is computed on the reduced
    // plan: count() reads no column, and `a` matters only for the selection.
    // The scan materializes zero columns and the count comes from logical_rows.
    ibex::runtime::ExternRegistry registry;
    std::vector<std::vector<std::string>> decode_calls;
    std::vector<std::int64_t> captured;
    register_recording_lazy_source(registry, decode_calls);
    register_int_capture(registry, "n", captured);

    SECTION("batch planner path") {
        const char* src = R"(
extern fn read_fake() -> DataFrame from "fake.hpp";
extern fn capture(df: DataFrame) -> Int from "fake.hpp";

let input = read_fake();
let result = input[filter a > 1, select { n = count() }];
capture(result);
result;
)";
        REQUIRE(ibex::repl::execute_script(src, registry));
        REQUIRE(captured == std::vector<std::int64_t>{2});
        REQUIRE(decode_calls == std::vector<std::vector<std::string>>{{"a"}});
    }

    SECTION("statement-at-a-time path") {
        // A scalar let is outside the batch planner's shape, so the same
        // script runs through the statement executor.
        const char* src = R"(
extern fn read_fake() -> DataFrame from "fake.hpp";
extern fn capture(df: DataFrame) -> Int from "fake.hpp";

let force_statement_path = 1;
let input = read_fake();
let result = input[filter a > 1, select { n = count() }];
capture(result);
result;
)";
        REQUIRE(ibex::repl::execute_script(src, registry));
        REQUIRE(captured == std::vector<std::int64_t>{2});
        REQUIRE(decode_calls == std::vector<std::vector<std::string>>{{"a"}});
    }
}

TEST_CASE("REPL batch planner materializes a shared aggregate binding once", "[repl][batch]") {
    // `agg` is consumed by two different sinks. Cloned per reference, each
    // sink's plan would re-read the source and re-run the aggregate (one lazy
    // instance per sink plan); shared, the binding is evaluated once and both
    // sinks scan the materialized table.
    ibex::runtime::ExternRegistry registry;
    int source_instances = 0;
    registry.register_lazy_table(
        "read_fake",
        [&source_instances](const ibex::runtime::ExternArgs&)
            -> std::expected<ibex::runtime::LazyTablePtr, std::string> {
            ++source_instances;
            ibex::runtime::Table schema;
            schema.add_column("a", ibex::Column<std::int64_t>{});
            schema.add_column("b", ibex::Column<std::int64_t>{});
            return std::make_shared<ibex::runtime::LazyTable>(
                std::move(schema), 4,
                [](const std::vector<std::string>& names, const ibex::runtime::Selection* selection)
                    -> std::expected<ibex::runtime::Table, std::string> {
                    const std::vector<std::int64_t> a_values{1, 2, 3, 4};
                    const std::vector<std::int64_t> b_values{1, 1, 2, 2};
                    const ibex::runtime::Selection all{0, 1, 2, 3};
                    const auto& rows = selection == nullptr ? all : *selection;
                    ibex::runtime::Table table;
                    for (const auto& name : names) {
                        const auto& source = name == "a" ? a_values : b_values;
                        std::vector<std::int64_t> values;
                        values.reserve(rows.size());
                        for (const auto row : rows) {
                            values.push_back(source[row]);
                        }
                        table.add_column(name, ibex::Column<std::int64_t>{std::move(values)});
                    }
                    table.logical_rows = rows.size();
                    return table;
                });
        });

    std::vector<std::pair<std::string, std::vector<std::int64_t>>> captures;
    registry.register_scalar_table_consumer(
        "capture", ibex::runtime::ScalarKind::Int,
        [&captures](const ibex::runtime::Table& table, const ibex::runtime::ExternArgs& args)
            -> std::expected<ibex::runtime::ExternValue, std::string> {
            const auto* tag = std::get_if<std::string>(&args.at(0));
            const auto* values = std::get_if<ibex::Column<std::int64_t>>(table.find("s"));
            if (tag == nullptr || values == nullptr) {
                return std::unexpected("capture expected a tag and Int64 column s");
            }
            captures.emplace_back(*tag, std::vector<std::int64_t>(values->begin(), values->end()));
            return ibex::runtime::ExternValue{std::int64_t{0}};
        });

    const char* src = R"(
extern fn read_fake() -> DataFrame from "fake.hpp";
extern fn capture(df: DataFrame, tag: String) -> Int from "fake.hpp";

let input = read_fake();
let agg = input[select { b, s = sum(a) }, by { b }];
let lo = agg[filter b == 1, select { s }];
let hi = agg[filter b == 2, select { s }];
capture(lo, "lo");
capture(hi, "hi");
hi;
)";
    REQUIRE(ibex::repl::execute_script(src, registry));
    REQUIRE(captures == std::vector<std::pair<std::string, std::vector<std::int64_t>>>{
                            {"lo", {3}},
                            {"hi", {7}},
                        });
    // One instance materializes the shared binding -- the point of this test.
    // The second is the schema probe, which resolves each distinct reader once
    // before lowering so join filter pushdown can see what a reader returns.
    REQUIRE(source_instances == 2);
}

TEST_CASE("REPL lazy scan: a source scanned twice is decoded once, filters stay", "[repl][lazy]") {
    // The two subqueries filter the same lazy source differently. Applying one
    // occurrence's selection to the shared decode would be unsound, so the
    // source is decoded whole exactly once for the union of what both need and
    // each filter runs over the shared table.
    ibex::runtime::ExternRegistry registry;
    std::vector<std::vector<std::string>> decode_calls;
    std::vector<std::optional<ibex::runtime::Selection>> decode_selections;
    std::vector<std::int64_t> captured;
    register_recording_lazy_source(registry, decode_calls, &decode_selections);
    register_int_capture(registry, "b", captured);

    const char* src = R"(
extern fn read_fake() -> DataFrame from "fake.hpp";
extern fn capture(df: DataFrame) -> Int from "fake.hpp";

let input = read_fake();
let one = input[filter a > 1, select { b }];
let two = input[filter a > 2, select { b }];
let result = one join two on b;
capture(result);
result;
)";
    REQUIRE(ibex::repl::execute_script(src, registry));
    REQUIRE(captured == std::vector<std::int64_t>{30});
    REQUIRE(decode_calls == std::vector<std::vector<std::string>>{{"a", "b"}});
    REQUIRE(decode_selections.size() == 1);
    CHECK_FALSE(decode_selections[0].has_value());
}

TEST_CASE("REPL supports named arguments and defaults for extern functions") {
    ibex::runtime::ExternRegistry registry;
    registry.register_table(
        "read_fake",
        [](const ibex::runtime::ExternArgs& args)
            -> std::expected<ibex::runtime::ExternValue, std::string> {
            if (args.size() != 4) {
                return std::unexpected("read_fake() expects 4 arguments");
            }
            const auto* path = std::get_if<std::string>(args.data());
            const auto* nulls = std::get_if<std::string>(&args[1]);
            const auto* delimiter = std::get_if<std::string>(&args[2]);
            const auto* has_header = std::get_if<bool>(&args[3]);
            if (path == nullptr || nulls == nullptr || delimiter == nullptr ||
                has_header == nullptr) {
                return std::unexpected("read_fake(): wrong argument types");
            }
            if (*path != "data.csv" || !nulls->empty() || *delimiter != "," || *has_header) {
                return std::unexpected("read_fake(): defaults/named binding mismatch");
            }
            ibex::runtime::Table t;
            t.add_column("x", ibex::Column<std::int64_t>{1, 2, 3});
            return ibex::runtime::ExternValue{std::move(t)};
        });

    const char* src = R"(
extern fn read_fake(
    path: String,
    nulls: String = "",
    delimiter: String = ",",
    has_header: Bool = true
) -> DataFrame from "fake.hpp";

let t = read_fake("data.csv", has_header = false);
t;
)";

    REQUIRE(ibex::repl::execute_script(src, registry));
}

TEST_CASE("REPL rejects duplicate named arguments") {
    ibex::runtime::ExternRegistry registry;
    const char* src = R"(
fn add3(x: Int, y: Int = 1) -> Int {
  x + y;
}
add3(10, y = 2, y = 3);
)";
    REQUIRE_FALSE(ibex::repl::execute_script(src, registry));
}

TEST_CASE("REPL rejects unknown named arguments") {
    ibex::runtime::ExternRegistry registry;
    const char* src = R"(
fn add3(x: Int, y: Int = 1) -> Int {
  x + y;
}
add3(10, z = 2);
)";
    REQUIRE_FALSE(ibex::repl::execute_script(src, registry));
}

// Clause-level column-reference validation over a statically known schema.
// In the REPL the complete set of in-scope scalar names is known, so filter and
// computed-expression references are validated too — a bare name is rejected
// only when it is neither a column nor an in-scope binding.
TEST_CASE("REPL rejects a filter on a missing column over a known schema", "[repl][schema]") {
    ibex::runtime::ExternRegistry registry;
    REQUIRE_FALSE(ibex::repl::execute_script("Table { a = [1, 2] }[filter b > 0];", registry));
}

TEST_CASE("REPL allows a filter referencing an in-scope scalar", "[repl][schema]") {
    ibex::runtime::ExternRegistry registry;
    REQUIRE(ibex::repl::execute_script("let thr = 1;\nTable { a = [1, 2] }[filter a > thr];",
                                       registry));
}

TEST_CASE("REPL rejects an update expression on a missing column", "[repl][schema]") {
    ibex::runtime::ExternRegistry registry;
    REQUIRE_FALSE(
        ibex::repl::execute_script("Table { a = [1, 2] }[update { x = nope * 2 }];", registry));
}

TEST_CASE("REPL allows an update expression referencing an in-scope scalar", "[repl][schema]") {
    ibex::runtime::ExternRegistry registry;
    REQUIRE(ibex::repl::execute_script("let k = 10;\nTable { a = [1, 2] }[update { x = a * k }];",
                                       registry));
}

// A let-bound table carries its (exact) schema into later statements, so a
// reference to a column it does not have is caught at lower time.
TEST_CASE("REPL checks references to a let-bound table's schema", "[repl][schema]") {
    ibex::runtime::ExternRegistry registry;
    REQUIRE_FALSE(ibex::repl::execute_script(
        "let trades = Table { symbol = [\"A\"], price = [1.0] };\ntrades[select { prize }];",
        registry));
}

TEST_CASE("REPL accepts a valid reference to a let-bound table", "[repl][schema]") {
    ibex::runtime::ExternRegistry registry;
    REQUIRE(ibex::repl::execute_script(
        "let trades = Table { symbol = [\"A\"], price = [1.0] };\ntrades[select { symbol }];",
        registry));
}

// Scalar UDFs called inside clause expressions are inlined at lower time. Only
// single-expression bodies inline; recursion is rejected.
TEST_CASE("REPL inlines a scalar UDF in a clause expression", "[repl][udf]") {
    ibex::runtime::ExternRegistry registry;
    REQUIRE(ibex::repl::execute_script(
        "fn adjust(p: Float64) -> Float64 { p * 1.01; }\n"
        "Table { price = [100.0, 200.0] }[select { adj = adjust(price) }];",
        registry));
}

TEST_CASE("REPL rejects a recursive scalar UDF in a clause expression", "[repl][udf]") {
    ibex::runtime::ExternRegistry registry;
    REQUIRE_FALSE(ibex::repl::execute_script(
        "fn f(x: Int) -> Int { f(x); }\nTable { a = [1] }[select { y = f(a) }];", registry));
}

TEST_CASE("REPL inlines a let-prefixed scalar UDF in a clause expression", "[repl][udf]") {
    ibex::runtime::ExternRegistry registry;
    REQUIRE(ibex::repl::execute_script(
        "fn g(x: Int) -> Int { let y = x; y; }\nTable { a = [1] }[select { y = g(a) }];",
        registry));
}

TEST_CASE("REPL tuple let binding: bound columns usable in expressions") {
    ibex::runtime::ExternRegistry registry;

    registry.register_table("make_cols",
                            [](const ibex::runtime::ExternArgs&)
                                -> std::expected<ibex::runtime::ExternValue, std::string> {
                                ibex::runtime::Table t;
                                t.add_column("p", ibex::Column<std::int64_t>{10, 20, 30});
                                t.add_column("q", ibex::Column<std::int64_t>{1, 2, 3});
                                return ibex::runtime::ExternValue{std::move(t)};
                            });

    // After tuple binding, each name should be available as a Series in further expressions.
    const char* src = R"(
extern fn make_cols() -> DataFrame from "fake.hpp";
let (p, q) = make_cols();
p;
)";
    REQUIRE(ibex::repl::execute_script(src, registry));
}

// --- Type annotation validation ---

TEST_CASE("type annotation: Int64 let binding accepts int literal") {
    ibex::runtime::ExternRegistry registry;
    REQUIRE(ibex::repl::execute_script("let x: Int64 = 3;", registry));
}

TEST_CASE("type annotation: Int64 let binding rejects float literal") {
    ibex::runtime::ExternRegistry registry;
    REQUIRE_FALSE(ibex::repl::execute_script("let x: Int64 = 3.14;", registry));
}

TEST_CASE("type annotation: Float64 let binding accepts float literal") {
    ibex::runtime::ExternRegistry registry;
    REQUIRE(ibex::repl::execute_script("let x: Float64 = 3.0;", registry));
}

TEST_CASE("type annotation: Float64 let binding widens int literal") {
    // Implicit Int → Float widening on let-binding values: this is the only
    // automatic numeric coercion we accept. Float → Int still errors.
    ibex::runtime::ExternRegistry registry;
    REQUIRE(ibex::repl::execute_script("let x: Float64 = 3; x + 0.5;", registry));
}

TEST_CASE("type annotation: untyped let binding accepts either scalar") {
    ibex::runtime::ExternRegistry registry;
    REQUIRE(ibex::repl::execute_script("let x = 3;", registry));
    REQUIRE(ibex::repl::execute_script("let y = 3.14;", registry));
}

TEST_CASE("type annotation: function call accepts matching Float64 argument") {
    ibex::runtime::ExternRegistry registry;
    const char* src = R"(
fn f(x: Float64) -> Float64 {
    x;
}
f(3.0);
)";
    REQUIRE(ibex::repl::execute_script(src, registry));
}

TEST_CASE("type annotation: function call widens Int argument when Float64 expected") {
    // Symmetric with the let-binding widening: passing 3 to f(x: Float64)
    // should not require f(Float64(3)) at the call site.
    ibex::runtime::ExternRegistry registry;
    const char* src = R"(
fn f(x: Float64) -> Float64 {
    x;
}
f(3);
)";
    REQUIRE(ibex::repl::execute_script(src, registry));
}

TEST_CASE("type annotation: function call accepts matching Int64 argument") {
    ibex::runtime::ExternRegistry registry;
    const char* src = R"(
fn f(x: Int64) -> Int64 {
    x;
}
f(42);
)";
    REQUIRE(ibex::repl::execute_script(src, registry));
}

TEST_CASE("type annotation: function call rejects Float where Int64 expected") {
    ibex::runtime::ExternRegistry registry;
    const char* src = R"(
fn f(x: Int64) -> Int64 {
    x;
}
f(1.5);
)";
    REQUIRE_FALSE(ibex::repl::execute_script(src, registry));
}

TEST_CASE("type annotation: DataFrame schema validates correct column types") {
    ibex::runtime::ExternRegistry registry;
    registry.register_table("get_data",
                            [](const ibex::runtime::ExternArgs&)
                                -> std::expected<ibex::runtime::ExternValue, std::string> {
                                ibex::runtime::Table t;
                                t.add_column("price", ibex::Column<double>{1.0, 2.0});
                                return ibex::runtime::ExternValue{std::move(t)};
                            });

    const char* src = R"(
extern fn get_data() -> DataFrame from "fake.hpp";
let df: DataFrame<{price: Float64}> = get_data();
df;
)";
    REQUIRE(ibex::repl::execute_script(src, registry));
}

TEST_CASE("type annotation: DataFrame schema allows extra columns") {
    ibex::runtime::ExternRegistry registry;
    registry.register_table("get_data",
                            [](const ibex::runtime::ExternArgs&)
                                -> std::expected<ibex::runtime::ExternValue, std::string> {
                                ibex::runtime::Table t;
                                t.add_column("price", ibex::Column<double>{1.0, 2.0});
                                t.add_column("symbol", ibex::Column<std::string>{"A", "B"});
                                return ibex::runtime::ExternValue{std::move(t)};
                            });

    // Schema only declares 'price'; 'symbol' is an extra column and should be allowed.
    const char* src = R"(
extern fn get_data() -> DataFrame from "fake.hpp";
let df: DataFrame<{price: Float64}> = get_data();
df;
)";
    REQUIRE(ibex::repl::execute_script(src, registry));
}

TEST_CASE("type annotation: DataFrame schema rejects wrong column type") {
    ibex::runtime::ExternRegistry registry;
    registry.register_table("get_data",
                            [](const ibex::runtime::ExternArgs&)
                                -> std::expected<ibex::runtime::ExternValue, std::string> {
                                ibex::runtime::Table t;
                                // price is Int64 but schema declares Float64
                                t.add_column("price", ibex::Column<std::int64_t>{1, 2});
                                return ibex::runtime::ExternValue{std::move(t)};
                            });

    const char* src = R"(
extern fn get_data() -> DataFrame from "fake.hpp";
let df: DataFrame<{price: Float64}> = get_data();
df;
)";
    REQUIRE_FALSE(ibex::repl::execute_script(src, registry));
}

TEST_CASE("type annotation: DataFrame schema rejects missing column") {
    ibex::runtime::ExternRegistry registry;
    registry.register_table("get_data",
                            [](const ibex::runtime::ExternArgs&)
                                -> std::expected<ibex::runtime::ExternValue, std::string> {
                                ibex::runtime::Table t;
                                t.add_column("symbol", ibex::Column<std::string>{"A", "B"});
                                return ibex::runtime::ExternValue{std::move(t)};
                            });

    // Schema requires 'price' which is absent from the table.
    const char* src = R"(
extern fn get_data() -> DataFrame from "fake.hpp";
let df: DataFrame<{price: Float64}> = get_data();
df;
)";
    REQUIRE_FALSE(ibex::repl::execute_script(src, registry));
}

TEST_CASE("type annotation: bare DataFrame schema (no fields) skips validation") {
    ibex::runtime::ExternRegistry registry;
    registry.register_table("get_data",
                            [](const ibex::runtime::ExternArgs&)
                                -> std::expected<ibex::runtime::ExternValue, std::string> {
                                ibex::runtime::Table t;
                                t.add_column("price", ibex::Column<std::int64_t>{1, 2});
                                return ibex::runtime::ExternValue{std::move(t)};
                            });

    // Bare DataFrame with no schema should not validate column types.
    const char* src = R"(
extern fn get_data() -> DataFrame from "fake.hpp";
let df: DataFrame = get_data();
df;
)";
    REQUIRE(ibex::repl::execute_script(src, registry));
}

TEST_CASE("function parameter: DataFrame schema validates required columns and allows extras") {
    ibex::runtime::ExternRegistry registry;
    registry.register_table("get_data",
                            [](const ibex::runtime::ExternArgs&)
                                -> std::expected<ibex::runtime::ExternValue, std::string> {
                                ibex::runtime::Table t;
                                t.add_column("salary", ibex::Column<std::int64_t>{100, 200, 300});
                                t.add_column("dept", ibex::Column<std::string>{"A", "B", "A"});
                                return ibex::runtime::ExternValue{std::move(t)};
                            });

    const char* src = R"(
extern fn get_data() -> DataFrame from "fake.hpp";

fn keep_salary(df: DataFrame<{salary: Int64}>) -> DataFrame {
    df[select { salary }];
}

let df = get_data();
keep_salary(df);
)";

    REQUIRE(ibex::repl::execute_script(src, registry));
}

TEST_CASE("function parameter: DataFrame schema rejects missing required column") {
    ibex::runtime::ExternRegistry registry;
    registry.register_table("get_data",
                            [](const ibex::runtime::ExternArgs&)
                                -> std::expected<ibex::runtime::ExternValue, std::string> {
                                ibex::runtime::Table t;
                                t.add_column("dept", ibex::Column<std::string>{"A", "B"});
                                return ibex::runtime::ExternValue{std::move(t)};
                            });

    const char* src = R"(
extern fn get_data() -> DataFrame from "fake.hpp";

fn keep_salary(df: DataFrame<{salary: Int64}>) -> DataFrame {
    df;
}

let df = get_data();
keep_salary(df);
)";

    REQUIRE_FALSE(ibex::repl::execute_script(src, registry));
}

TEST_CASE("function parameter: DataFrame schema rejects wrong required column type") {
    ibex::runtime::ExternRegistry registry;
    registry.register_table("get_data",
                            [](const ibex::runtime::ExternArgs&)
                                -> std::expected<ibex::runtime::ExternValue, std::string> {
                                ibex::runtime::Table t;
                                t.add_column("salary", ibex::Column<double>{100.0, 200.0});
                                return ibex::runtime::ExternValue{std::move(t)};
                            });

    const char* src = R"(
extern fn get_data() -> DataFrame from "fake.hpp";

fn keep_salary(df: DataFrame<{salary: Int64}>) -> DataFrame {
    df;
}

let df = get_data();
keep_salary(df);
)";

    REQUIRE_FALSE(ibex::repl::execute_script(src, registry));
}

TEST_CASE("function return: DataFrame schema validates declared required columns") {
    ibex::runtime::ExternRegistry registry;

    const char* src = R"(
fn only_salary(df: DataFrame<{salary: Int64}>) -> DataFrame<{salary: Int64}> {
    df[select { salary }];
}

let df = Table { salary = [100, 200], dept = ["A", "B"] };
only_salary(df);
)";

    REQUIRE(ibex::repl::execute_script(src, registry));
}

TEST_CASE("function return: DataFrame schema rejects missing declared column") {
    ibex::runtime::ExternRegistry registry;

    const char* src = R"(
fn broken(df: DataFrame<{salary: Int64}>) -> DataFrame<{salary: Int64, dept: String}> {
    df[select { salary }];
}

let df = Table { salary = [100, 200] };
broken(df);
)";

    REQUIRE_FALSE(ibex::repl::execute_script(src, registry));
}

TEST_CASE("function parameter: Series type validates declared scalar element type") {
    ibex::runtime::ExternRegistry registry;

    const char* src = R"(
fn id_col(x: Series<Int64>) -> Series<Int64> {
    x;
}

let t = Table { x = [1, 2, 3] };
id_col(t[select { x }]);
)";

    REQUIRE(ibex::repl::execute_script(src, registry));
}

TEST_CASE("function parameter: Series type rejects wrong scalar element type") {
    ibex::runtime::ExternRegistry registry;

    const char* src = R"(
fn id_col(x: Series<Int64>) -> Series<Int64> {
    x;
}

let t = Table { x = [1.0, 2.0, 3.0] };
id_col(t[select { x }]);
)";

    REQUIRE_FALSE(ibex::repl::execute_script(src, registry));
}

TEST_CASE("function return: Series type validates declared scalar element type") {
    ibex::runtime::ExternRegistry registry;

    const char* src = R"(
fn salary_col(df: DataFrame<{salary: Int64}>) -> Series<Int64> {
    df[select { salary }];
}

let df = Table { salary = [100, 200] };
salary_col(df);
)";

    REQUIRE(ibex::repl::execute_script(src, registry));
}

TEST_CASE("function return: Series type rejects wrong scalar element type") {
    ibex::runtime::ExternRegistry registry;

    const char* src = R"(
fn salary_col(df: DataFrame<{salary: Float64}>) -> Series<Int64> {
    df[select { salary }];
}

let df = Table { salary = [100.0, 200.0] };
salary_col(df);
)";

    REQUIRE_FALSE(ibex::repl::execute_script(src, registry));
}

TEST_CASE("REPL accepts head with scalar let binding") {
    ibex::runtime::ExternRegistry registry;

    const char* src = R"(
let n = 2;
let t = Table { x = [10, 20, 30] };
t[head n];
)";

    REQUIRE(ibex::repl::execute_script(src, registry));
}

TEST_CASE("REPL accepts head with function scalar parameter") {
    ibex::runtime::ExternRegistry registry;

    const char* src = R"(
fn take_n(df: DataFrame<{x: Int64}>, n: Int64) -> DataFrame<{x: Int64}> {
    df[head n];
}

let t = Table { x = [10, 20, 30] };
take_n(t, 2);
)";

    REQUIRE(ibex::repl::execute_script(src, registry));
}

TEST_CASE("REPL rejects negative head count expression") {
    ibex::runtime::ExternRegistry registry;

    const char* src = R"(
let n = -1;
let t = Table { x = [10, 20, 30] };
t[head n];
)";

    REQUIRE_FALSE(ibex::repl::execute_script(src, registry));
}

// --- Explicit cast expressions ---

TEST_CASE("cast: Float64(int_literal) allows passing to Float64 param") {
    ibex::runtime::ExternRegistry registry;
    const char* src = R"(
fn f(x: Float64) -> Float64 {
    x;
}
f(Float64(3));
)";
    REQUIRE(ibex::repl::execute_script(src, registry));
}

TEST_CASE("cast: Int64(whole float literal) allows passing to Int64 param") {
    ibex::runtime::ExternRegistry registry;
    const char* src = R"(
fn f(x: Int64) -> Int64 {
    x;
}
f(Int64(4.0));
)";
    REQUIRE(ibex::repl::execute_script(src, registry));
}

TEST_CASE("cast: Int64(non-integer float) is a runtime error") {
    ibex::runtime::ExternRegistry registry;
    const char* src = R"(
fn f(x: Int64) -> Int64 {
    x;
}
f(Int64(3.9));
)";
    REQUIRE_FALSE(ibex::repl::execute_script(src, registry));
}

TEST_CASE("cast: Float64(int_var) converts scalar variable across function calls") {
    ibex::runtime::ExternRegistry registry;
    const char* src = R"(
fn f(x: Float64) -> Float64 {
    x;
}
fn g(n: Int64) -> Float64 {
    f(Float64(n));
}
g(5);
)";
    REQUIRE(ibex::repl::execute_script(src, registry));
}

TEST_CASE("cast: let binding with Float64 cast from Int") {
    ibex::runtime::ExternRegistry registry;
    REQUIRE(ibex::repl::execute_script("let x: Float64 = Float64(3);", registry));
}

TEST_CASE("cast: let binding with Int64 cast from whole Float") {
    ibex::runtime::ExternRegistry registry;
    REQUIRE(ibex::repl::execute_script("let x: Int64 = Int64(9.0);", registry));
}

TEST_CASE("cast: let binding Int64 from fractional Float is a runtime error") {
    ibex::runtime::ExternRegistry registry;
    REQUIRE_FALSE(ibex::repl::execute_script("let x: Int64 = Int64(9.9);", registry));
}

TEST_CASE("cast: Float64 applied to Int column converts element types") {
    ibex::runtime::ExternRegistry registry;
    registry.register_table("get_data",
                            [](const ibex::runtime::ExternArgs&)
                                -> std::expected<ibex::runtime::ExternValue, std::string> {
                                ibex::runtime::Table t;
                                t.add_column("n", ibex::Column<std::int64_t>{1, 2, 3});
                                return ibex::runtime::ExternValue{std::move(t)};
                            });

    const char* src = R"(
extern fn get_data() -> DataFrame from "fake.hpp";
let df = get_data();
let (n) = df;
let f: Series<Float64> = Float64(n);
f;
)";
    REQUIRE(ibex::repl::execute_script(src, registry));
}

TEST_CASE("cast: Int64 applied to whole-number Float column converts element types") {
    ibex::runtime::ExternRegistry registry;
    registry.register_table("get_data",
                            [](const ibex::runtime::ExternArgs&)
                                -> std::expected<ibex::runtime::ExternValue, std::string> {
                                ibex::runtime::Table t;
                                t.add_column("v", ibex::Column<double>{1.0, 2.0, 3.0});
                                return ibex::runtime::ExternValue{std::move(t)};
                            });

    const char* src = R"(
extern fn get_data() -> DataFrame from "fake.hpp";
let df = get_data();
let (v) = df;
let i: Series<Int64> = Int64(v);
i;
)";
    REQUIRE(ibex::repl::execute_script(src, registry));
}

TEST_CASE("cast: Int64 applied to fractional Float column is a runtime error") {
    ibex::runtime::ExternRegistry registry;
    registry.register_table("get_data",
                            [](const ibex::runtime::ExternArgs&)
                                -> std::expected<ibex::runtime::ExternValue, std::string> {
                                ibex::runtime::Table t;
                                t.add_column("v", ibex::Column<double>{1.1, 2.9, 3.5});
                                return ibex::runtime::ExternValue{std::move(t)};
                            });

    const char* src = R"(
extern fn get_data() -> DataFrame from "fake.hpp";
let df = get_data();
let (v) = df;
let i: Series<Int64> = Int64(v);
i;
)";
    REQUIRE_FALSE(ibex::repl::execute_script(src, registry));
}

// --- round() with rounding mode ---

TEST_CASE("round: nearest mode rounds half away from zero") {
    ibex::runtime::ExternRegistry registry;
    REQUIRE(ibex::repl::execute_script("let x: Int64 = round(2.5, nearest);", registry));
}

TEST_CASE("round: floor mode rounds down") {
    ibex::runtime::ExternRegistry registry;
    REQUIRE(ibex::repl::execute_script("let x: Int64 = round(2.9, floor);", registry));
}

TEST_CASE("round: ceil mode rounds up") {
    ibex::runtime::ExternRegistry registry;
    REQUIRE(ibex::repl::execute_script("let x: Int64 = round(2.1, ceil);", registry));
}

TEST_CASE("round: trunc mode rounds toward zero") {
    ibex::runtime::ExternRegistry registry;
    REQUIRE(ibex::repl::execute_script("let x: Int64 = round(2.9, trunc);", registry));
}

TEST_CASE("round: negative value with floor") {
    ibex::runtime::ExternRegistry registry;
    REQUIRE(ibex::repl::execute_script("let x: Int64 = round(-2.1, floor);", registry));
}

TEST_CASE("round: bankers mode rounds half-to-even") {
    ibex::runtime::ExternRegistry registry;
    // 2.5 rounds to 2 (nearest even), 3.5 rounds to 4 (nearest even)
    REQUIRE(ibex::repl::execute_script("let x: Int64 = round(2.5, bankers);", registry));
    REQUIRE(ibex::repl::execute_script("let x: Int64 = round(3.5, bankers);", registry));
    REQUIRE(ibex::repl::execute_script("let x: Int64 = round(2.1, bankers);", registry));
}

TEST_CASE("round: unknown mode is an error") {
    ibex::runtime::ExternRegistry registry;
    REQUIRE_FALSE(ibex::repl::execute_script("let x: Int64 = round(2.5, halfway);", registry));
}

TEST_CASE("round: Int argument is an error") {
    ibex::runtime::ExternRegistry registry;
    REQUIRE_FALSE(ibex::repl::execute_script("let x: Int64 = round(3, nearest);", registry));
}

TEST_CASE("round: applied to Float column with nearest mode") {
    ibex::runtime::ExternRegistry registry;
    registry.register_table("get_data",
                            [](const ibex::runtime::ExternArgs&)
                                -> std::expected<ibex::runtime::ExternValue, std::string> {
                                ibex::runtime::Table t;
                                t.add_column("v", ibex::Column<double>{1.4, 2.5, 3.6});
                                return ibex::runtime::ExternValue{std::move(t)};
                            });

    const char* src = R"(
extern fn get_data() -> DataFrame from "fake.hpp";
let df = get_data();
let (v) = df;
let r: Series<Int64> = round(v, nearest);
r;
)";
    REQUIRE(ibex::repl::execute_script(src, registry));
}

TEST_CASE("round: applied to Int column is an error") {
    ibex::runtime::ExternRegistry registry;
    registry.register_table("get_data",
                            [](const ibex::runtime::ExternArgs&)
                                -> std::expected<ibex::runtime::ExternValue, std::string> {
                                ibex::runtime::Table t;
                                t.add_column("n", ibex::Column<std::int64_t>{1, 2, 3});
                                return ibex::runtime::ExternValue{std::move(t)};
                            });

    const char* src = R"(
extern fn get_data() -> DataFrame from "fake.hpp";
let df = get_data();
let (n) = df;
let r: Series<Int64> = round(n, nearest);
r;
)";
    REQUIRE_FALSE(ibex::repl::execute_script(src, registry));
}

TEST_CASE("REPL correlated subquery reads a shared source once", "[repl][subquery]") {
    // The outer query and the subquery both scan `supply`. A `let`-bound source
    // is one binding named twice, not two reads: decorrelation must not clone
    // the read into the subquery's plan. The counter is the proof.
    int reads = 0;

    ibex::runtime::ExternRegistry registry;
    registry.register_table(
        "load_supply",
        [&reads](const ibex::runtime::ExternArgs&)
            -> std::expected<ibex::runtime::ExternValue, std::string> {
            ++reads;
            ibex::runtime::Table table;
            table.add_column("ps_partkey", ibex::Column<std::int64_t>{1, 1, 2, 2});
            table.add_column("ps_suppkey", ibex::Column<std::int64_t>{10, 11, 10, 12});
            table.add_column("ps_cost", ibex::Column<double>{5.0, 3.0, 9.0, 8.0});
            return ibex::runtime::ExternValue{std::move(table)};
        });

    const std::string source = R"(
extern fn load_supply() -> DataFrame from "x.hpp";
let parts = Table { p_partkey = [1, 2], p_name = ["nut", "bolt"] };
let supply = load_supply();
let cheapest = (
    parts join supply[select { p_partkey = ps_partkey, ps_suppkey, ps_cost }] on p_partkey
)[
    filter ps_cost == scalar(
        supply[filter ps_partkey == outer(p_partkey), select { m = min(ps_cost) }]
    )
];
print(cheapest[select { n = count() }]);
)";

    REQUIRE(ibex::repl::execute_script(source, registry));
    CHECK(reads == 1);
}

namespace {

auto current_process_id() -> int {
#ifdef _WIN32
    return _getpid();
#else
    return getpid();
#endif
}

auto duplicate_file_descriptor(int descriptor) -> int {
#ifdef _WIN32
    return _dup(descriptor);
#else
    return dup(descriptor);
#endif
}

auto replace_file_descriptor(int source, int destination) -> int {
#ifdef _WIN32
    return _dup2(source, destination);
#else
    return dup2(source, destination);
#endif
}

auto close_file_descriptor(int descriptor) -> int {
#ifdef _WIN32
    return _close(descriptor);
#else
    return close(descriptor);
#endif
}

auto file_descriptor(std::FILE* stream) -> int {
#ifdef _WIN32
    return _fileno(stream);
#else
    return fileno(stream);
#endif
}

/// Runs `source` with planner reporting on; returns what the REPL wrote to stderr.
/// The `planner:` line there is what bench_ibex.py reads to record which engine
/// path each query measured, so its shape is a contract, not just a log line.
auto capture_planner_line(std::string_view source, ibex::runtime::ExternRegistry& registry,
                          ibex::repl::ReplConfig config = {}) -> std::string {
    // Per-pid: catch_discover_tests gives each TEST_CASE its own process, and
    // ctest -j runs them concurrently -- a fixed name would let two captures
    // clobber each other's file.
    const auto path = std::filesystem::temp_directory_path() /
                      ("ibex_planner_capture_" + std::to_string(current_process_id()) + ".txt");
    config.report_planner = true;
    config.persistent_history = false;

    std::fflush(stderr);
    const int saved = duplicate_file_descriptor(file_descriptor(stderr));
    REQUIRE(std::freopen(path.string().c_str(), "w", stderr) != nullptr);
    static_cast<void>(ibex::repl::execute_script(source, registry, config));
    std::fflush(stderr);
    REQUIRE(replace_file_descriptor(saved, file_descriptor(stderr)) != -1);
    static_cast<void>(close_file_descriptor(saved));

    std::ifstream in{path};
    const std::string text((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
    std::filesystem::remove(path);
    for (const auto& line : std::views::split(text, '\n')) {
        const std::string_view view{line.begin(), line.end()};
        if (view.starts_with("planner:")) {
            return std::string{view};
        }
    }
    return {};
}

}  // namespace

namespace {

/// Runs `source` and returns everything it wrote to stderr.
auto capture_script_stderr(std::string_view source, ibex::runtime::ExternRegistry& registry)
    -> std::string {
    const auto path = std::filesystem::temp_directory_path() /
                      ("ibex_stderr_capture_" + std::to_string(current_process_id()) + ".txt");
    ibex::repl::ReplConfig config;
    config.persistent_history = false;
    std::fflush(stderr);
    const int saved = duplicate_file_descriptor(file_descriptor(stderr));
    REQUIRE(std::freopen(path.string().c_str(), "w", stderr) != nullptr);
    const bool ok = ibex::repl::execute_script(source, registry, config);
    std::fflush(stderr);
    REQUIRE(replace_file_descriptor(saved, file_descriptor(stderr)) != -1);
    static_cast<void>(close_file_descriptor(saved));
    std::ifstream in{path};
    std::string text((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
    std::filesystem::remove(path);
    INFO(text);
    REQUIRE(ok);
    return text;
}

/// A lazy `customer`-like source of `rows` rows: `c_custkey` = 1..rows (unique,
/// with footer min/max as a Parquet reader reports them) and `c_group`, which
/// the key determines.
void register_keyed_source(ibex::runtime::ExternRegistry& registry, const std::string& name,
                           std::size_t rows) {
    registry.register_lazy_table(
        name,
        [rows](const ibex::runtime::ExternArgs&)
            -> std::expected<ibex::runtime::LazyTablePtr, std::string> {
            ibex::runtime::Table schema;
            schema.add_column("c_custkey", ibex::Column<std::int64_t>{});
            schema.add_column("c_group", ibex::Column<std::int64_t>{});
            ibex::runtime::SourceColumnStats stats;
            stats.emplace("c_custkey", ibex::runtime::ColumnStats{
                                           .min = 1,
                                           .max = static_cast<std::int64_t>(rows),
                                           .null_count = 0,
                                       });
            return std::make_shared<ibex::runtime::LazyTable>(
                std::move(schema), rows,
                [rows](const std::vector<std::string>& names,
                       const ibex::runtime::Selection* selection)
                    -> std::expected<ibex::runtime::Table, std::string> {
                    const std::size_t n = selection == nullptr ? rows : selection->size();
                    ibex::runtime::Table table;
                    for (const auto& col : names) {
                        std::vector<std::int64_t> values;
                        values.reserve(n);
                        for (std::size_t i = 0; i < n; ++i) {
                            const std::size_t row = selection == nullptr ? i : (*selection)[i];
                            values.push_back(col == "c_custkey"
                                                 ? static_cast<std::int64_t>(row + 1)
                                                 : static_cast<std::int64_t>((row + 1) % 7));
                        }
                        table.add_column(col, ibex::Column<std::int64_t>{std::move(values)});
                    }
                    table.logical_rows = n;
                    return table;
                },
                std::move(stats));
        });
}

}  // namespace

TEST_CASE("group-key uniqueness is proved at any size when the key is read in full",
          "[repl][lazy][batch]") {
    // A multi-key aggregate whose keys one column determines is reduced to that
    // column once it is proved unique -- PDS-H q10's `c_custkey`. The proof is
    // a whole-column decode, and used to be capped at 1,310,720 rows, so q10
    // lost the reduction from SF-8.7 up (customer is 150k rows per SF) and ran
    // 2x slower at SF-10. When every scan of the source reads the key in full
    // anyway, the decode is the scan's own and the cap is gone; a filtered
    // source, which the scan decodes selectively, keeps it.
    constexpr std::size_t kRows = 1'400'000;  // past the old 1,310,720-row cap
    ibex::runtime::ExternRegistry registry;
    register_keyed_source(registry, "read_customer", kRows);
    registry.register_lazy_table(
        "read_orders",
        [](const ibex::runtime::ExternArgs&)
            -> std::expected<ibex::runtime::LazyTablePtr, std::string> {
            constexpr std::size_t kOrders = 1000;
            ibex::runtime::Table schema;
            schema.add_column("o_custkey", ibex::Column<std::int64_t>{});
            schema.add_column("o_value", ibex::Column<std::int64_t>{});
            return std::make_shared<ibex::runtime::LazyTable>(
                std::move(schema), kOrders,
                [](const std::vector<std::string>& names, const ibex::runtime::Selection* selection)
                    -> std::expected<ibex::runtime::Table, std::string> {
                    const std::size_t n = selection == nullptr ? kOrders : selection->size();
                    ibex::runtime::Table table;
                    for (const auto& col : names) {
                        std::vector<std::int64_t> values;
                        values.reserve(n);
                        for (std::size_t i = 0; i < n; ++i) {
                            const std::size_t row = selection == nullptr ? i : (*selection)[i];
                            values.push_back(col == "o_custkey" ? static_cast<std::int64_t>(
                                                                      ((row * 997) % kRows) + 1)
                                                                : static_cast<std::int64_t>(row));
                        }
                        table.add_column(col, ibex::Column<std::int64_t>{std::move(values)});
                    }
                    table.logical_rows = n;
                    return table;
                });
        });

    const auto script = [](std::string_view customer) {
        return std::string{R"(
extern fn read_customer() -> DataFrame from "x.hpp";
extern fn read_orders() -> DataFrame from "x.hpp";
let customer = read_customer();
let orders = read_orders();
let result = ()"} +
               std::string{customer} + R"( join orders on { c_custkey = o_custkey })[
    select { total = sum(o_value) },
    by { c_custkey, c_group }
][order { total desc }, head 3];
result;
)";
    };

    const std::optional<std::string> saved = [] -> std::optional<std::string> {
        if (const char* value = std::getenv("IBEX_UNIQUE_KEY_STATS"); value != nullptr) {
            return value;
        }
        return std::nullopt;
    }();
    ibex::runtime::set_env("IBEX_UNIQUE_KEY_STATS", "1");
    const std::string proved = "[unique] c_custkey rows=" + std::to_string(kRows);

    const auto full = capture_script_stderr(script("customer"), registry);
    CHECK(full.find(proved) != std::string::npos);

    const auto filtered = capture_script_stderr(script("customer[filter c_group != 3]"), registry);
    CHECK(filtered.find(proved) == std::string::npos);

    if (saved.has_value()) {
        ibex::runtime::set_env("IBEX_UNIQUE_KEY_STATS", *saved);
    } else {
        ibex::runtime::unset_env("IBEX_UNIQUE_KEY_STATS");
    }
}

TEST_CASE("REPL reports which planner path a script took", "[repl][lazy][planner]") {
    std::vector<std::vector<std::string>> decode_calls;
    ibex::runtime::ExternRegistry registry;
    register_recording_lazy_source(registry, decode_calls);

    const std::string source = R"(
extern fn read_fake() -> DataFrame from "fake.hpp";
let rows = read_fake();
rows[filter a > 1, select { n = count() }];
)";
    CHECK(capture_planner_line(source, registry) == "planner: whole-script");
}

TEST_CASE("REPL whole-script planner resolves scalar reader arguments",
          "[repl][lazy][planner][scalar]") {
    const auto register_sources = [](ibex::runtime::ExternRegistry& registry,
                                     std::vector<std::string>& paths) {
        registry.register_table("manifest",
                                [](const ibex::runtime::ExternArgs&)
                                    -> std::expected<ibex::runtime::ExternValue, std::string> {
                                    ibex::runtime::Table table;
                                    table.add_column("path", ibex::Column<std::string>{"chosen"});
                                    return ibex::runtime::ExternValue{std::move(table)};
                                });
        registry.register_table(
            "read_path",
            [](const ibex::runtime::ExternArgs& args)
                -> std::expected<ibex::runtime::ExternValue, std::string> {
                const auto* path = args.size() == 1 ? std::get_if<std::string>(&args[0]) : nullptr;
                if (path == nullptr) {
                    return std::unexpected("read_path expects one String argument");
                }
                ibex::runtime::Table table;
                if (*path == "manifest") {
                    table.add_column("path", ibex::Column<std::string>{"chosen"});
                } else {
                    table.add_column("a", ibex::Column<std::int64_t>{1, 2, 3});
                }
                return ibex::runtime::ExternValue{std::move(table)};
            });
        registry.register_lazy_table(
            "read_path",
            [&paths](const ibex::runtime::ExternArgs& args)
                -> std::expected<ibex::runtime::LazyTablePtr, std::string> {
                const auto* path = args.size() == 1 ? std::get_if<std::string>(&args[0]) : nullptr;
                if (path == nullptr) {
                    return std::unexpected("read_path expects one String argument");
                }
                paths.push_back(*path);
                const bool is_manifest = *path == "manifest";
                ibex::runtime::Table schema;
                if (is_manifest) {
                    schema.add_column("path", ibex::Column<std::string>{});
                } else {
                    schema.add_column("a", ibex::Column<std::int64_t>{});
                }
                return std::make_shared<ibex::runtime::LazyTable>(
                    std::move(schema), is_manifest ? 1 : 3,
                    [is_manifest](const std::vector<std::string>& names,
                                  const ibex::runtime::Selection* selection)
                        -> std::expected<ibex::runtime::Table, std::string> {
                        const ibex::runtime::Selection all{0, 1, 2};
                        const auto& rows = selection == nullptr ? all : *selection;
                        ibex::runtime::Table table;
                        if (is_manifest && std::ranges::find(names, "path") != names.end()) {
                            table.add_column("path", ibex::Column<std::string>{"chosen"});
                            table.logical_rows = 1;
                        } else if (!is_manifest && std::ranges::find(names, "a") != names.end()) {
                            std::vector<std::int64_t> values;
                            for (const auto row : rows) {
                                values.push_back(static_cast<std::int64_t>(row + 1));
                            }
                            table.add_column("a", ibex::Column<std::int64_t>{std::move(values)});
                            table.logical_rows = rows.size();
                        }
                        return table;
                    });
            });
    };

    SECTION("compile-time scalar expression") {
        ibex::runtime::ExternRegistry registry;
        std::vector<std::string> paths;
        register_sources(registry, paths);
        const std::string source = R"(
extern fn read_path(path: String) -> DataFrame from "fake.hpp";
let prefix = "cho";
let threshold = 1;
read_path(`${prefix}sen`)[filter a > threshold, select { n = count() }];
)";
        CHECK(capture_planner_line(source, registry) == "planner: whole-script");
        REQUIRE_FALSE(paths.empty());
        CHECK(std::ranges::all_of(paths, [](const auto& path) { return path == "chosen"; }));
    }

    SECTION("deferred scalar expression") {
        ibex::runtime::ExternRegistry registry;
        std::vector<std::string> paths;
        register_sources(registry, paths);
        const std::string source = R"(
extern fn manifest() -> DataFrame from "fake.hpp";
extern fn read_path(path: String) -> DataFrame from "fake.hpp";
let paths = manifest();
let path = scalar(paths[select { path }]);
read_path(path)[select { n = count() }];
)";
        CHECK(capture_planner_line(source, registry) == "planner: whole-script");
        REQUIRE_FALSE(paths.empty());
        CHECK(std::ranges::all_of(paths, [](const auto& path) { return path == "chosen"; }));
    }

    SECTION("a scalar sourced from the lazy reader stays on the statement path") {
        ibex::runtime::ExternRegistry registry;
        std::vector<std::string> paths;
        register_sources(registry, paths);
        const std::string source = R"(
extern fn read_path(path: String) -> DataFrame from "fake.hpp";
let path = scalar(read_path("manifest")[select { path }]);
read_path(path)[select { n = count() }];
)";
        CHECK(capture_planner_line(source, registry) ==
              "planner: statements (a deferred scalar depends on a lazy table source)");
    }
}

TEST_CASE("REPL reports why the whole-script planner declined", "[repl][lazy][planner]") {
    // Declining is otherwise invisible, and the fallback path has different
    // performance -- a benchmark that cannot see the mode reads a gate change
    // as a regression. Each decline must name its cause.
    std::vector<std::vector<std::string>> decode_calls;

    SECTION("no lazy source to plan against") {
        ibex::runtime::ExternRegistry registry;
        const auto line = capture_planner_line("let t = Table { a = [1, 2] };\nt;\n", registry);
        CHECK(line == "planner: statements (script has no lazy table sources to plan against)");
    }

    SECTION("a function declaration no longer forces the statement path") {
        // A `fn` declaration used to be a blanket decline (W3): `lower_script`
        // now registers and inlines UDFs, so a function-organised script plans
        // as one block.
        ibex::runtime::ExternRegistry registry;
        register_recording_lazy_source(registry, decode_calls);
        const std::string source = R"(
extern fn read_fake() -> DataFrame from "fake.hpp";
fn double_it(x: Int) -> Int {
  x * 2;
}
read_fake()[select { n = count() }];
)";
        CHECK(capture_planner_line(source, registry) == "planner: whole-script");
    }

    SECTION("a let that binds an extern call's result") {
        // The plan has no step for `let n = f(...)`: planning it would skip the
        // call and leave `n` unbound, so it stays on the statement path, which
        // runs the call where the statement is.
        ibex::runtime::ExternRegistry registry;
        register_recording_lazy_source(registry, decode_calls);
        registry.register_scalar_table_consumer(
            "count_rows", ibex::runtime::ScalarKind::Int,
            [](const ibex::runtime::Table& t, const ibex::runtime::ExternArgs&)
                -> std::expected<ibex::runtime::ExternValue, std::string> {
                return ibex::runtime::ExternValue{static_cast<std::int64_t>(t.rows())};
            });
        const std::string source = R"(
extern fn read_fake() -> DataFrame from "fake.hpp";
extern fn count_rows(df: DataFrame) -> Int from "fake.hpp";
let t = read_fake();
let n = count_rows(t);
t[select { c = count() }];
)";
        CHECK(capture_planner_line(source, registry) ==
              "planner: statements (`let n` binds the result of an extern call)");
    }

    SECTION("a script that ends in an effect") {
        // `lower_script` lets a program end in an effect (a compiled program may);
        // the batch executor has no result plan to run, so it stays on the
        // statement path.
        ibex::runtime::ExternRegistry registry;
        register_recording_lazy_source(registry, decode_calls);
        registry.register_scalar_table_consumer(
            "count_rows", ibex::runtime::ScalarKind::Int,
            [](const ibex::runtime::Table& t, const ibex::runtime::ExternArgs&)
                -> std::expected<ibex::runtime::ExternValue, std::string> {
                return ibex::runtime::ExternValue{static_cast<std::int64_t>(t.rows())};
            });
        const std::string source = R"(
extern fn read_fake() -> DataFrame from "fake.hpp";
extern fn count_rows(df: DataFrame) -> Int from "fake.hpp";
let t = read_fake();
count_rows(t);
)";
        CHECK(capture_planner_line(source, registry) ==
              "planner: statements (script ends in an effect, with no result plan)");
    }

    SECTION("a non-DataFrame type annotation") {
        ibex::runtime::ExternRegistry registry;
        register_recording_lazy_source(registry, decode_calls);
        const std::string source = R"(
extern fn read_fake() -> DataFrame from "fake.hpp";
let n: Int = 3;
read_fake()[select { c = count() }];
)";
        CHECK(capture_planner_line(source, registry) ==
              "planner: statements (`let n` has a non-DataFrame type annotation)");
    }
}

TEST_CASE("REPL plans a script that imports as one block", "[repl][lazy][planner]") {
    // An `import` used to send the whole script to the statement-at-a-time
    // path, which runs no optimizer at all -- so `import "csv"` silently cost
    // canonicalize and every pass after it. The stub's declarations are now a
    // prelude to the same plan.
    std::vector<std::vector<std::string>> decode_calls;
    ibex::runtime::ExternRegistry registry;
    register_recording_lazy_source(registry, decode_calls);

    const auto dir = std::filesystem::temp_directory_path() /
                     ("ibex_import_planner_" + std::to_string(current_process_id()));
    std::filesystem::create_directories(dir);
    {
        std::ofstream stub{dir / "fakelib.ibex"};
        stub << "extern fn read_fake() -> DataFrame from \"fake.hpp\";\n";
    }

    const std::string source = R"(
import "fakelib";
read_fake()[select { n = count() }];
)";
    ibex::repl::ReplConfig config;
    config.import_search_paths = {dir.string()};
    config.plugin_search_paths = {dir.string()};
    CHECK(capture_planner_line(source, registry, config) == "planner: whole-script");

    // Function declarations in a stub are part of the lowering prelude, just
    // like functions declared in the main script.
    {
        std::ofstream stub{dir / "fnlib.ibex"};
        stub << "extern fn read_fake() -> DataFrame from \"fake.hpp\";\n"
             << "fn count_rows(df: DataFrame) -> DataFrame {\n"
             << "  df[select { n = count() }];\n}\n";
    }
    const std::string with_fn = R"(
import "fnlib";
count_rows(read_fake());
)";
    CHECK(capture_planner_line(with_fn, registry, config) == "planner: whole-script");

    // Imported helpers with map bodies use the same whole-script MapNode path
    // as directly written clauses, including runtime extern expressions.
    {
        std::ofstream stub{dir / "maplib.ibex"};
        stub << "extern fn read_fake() -> DataFrame from \"fake.hpp\";\n"
             << "extern fn touch(x: Int) -> Int from \"fake.hpp\";\n"
             << "extern fn consume(df: DataFrame) -> Int from \"fake.hpp\";\n"
             << "fn touch_rows(df: DataFrame) -> DataFrame {\n"
             << "  df[map { value = touch(a), rows = consume(read_fake()) }];\n}\n";
    }
    registry.register_scalar(
        "touch", ibex::runtime::ScalarKind::Int,
        [](const ibex::runtime::ExternArgs& args)
            -> std::expected<ibex::runtime::ExternValue, std::string> { return args.at(0); });
    registry.register_table("read_fake",
                            [](const ibex::runtime::ExternArgs&)
                                -> std::expected<ibex::runtime::ExternValue, std::string> {
                                ibex::runtime::Table table;
                                table.add_column("a", ibex::Column<std::int64_t>{1, 2, 3});
                                table.add_column("b", ibex::Column<std::int64_t>{10, 20, 30});
                                return ibex::runtime::ExternValue{std::move(table)};
                            });
    int consume_calls = 0;
    registry.register_scalar_table_consumer(
        "consume", ibex::runtime::ScalarKind::Int,
        [&consume_calls](const ibex::runtime::Table& table, const ibex::runtime::ExternArgs&)
            -> std::expected<ibex::runtime::ExternValue, std::string> {
            ++consume_calls;
            return ibex::runtime::ExternValue{
                ibex::runtime::ScalarValue{static_cast<std::int64_t>(table.rows())}};
        });
    register_recording_lazy_source(registry, decode_calls);
    const std::string with_effectful_map = R"(
import "maplib";
touch_rows(read_fake());
)";
    CHECK(capture_planner_line(with_effectful_map, registry, config) == "planner: whole-script");
    CHECK(consume_calls == 3);

    std::filesystem::remove_all(dir);
}

TEST_CASE("REPL shared bindings keep their schema for scalar() decorrelation",
          "[repl][lazy][planner]") {
    // A binding is shared exactly when it is expensive and referenced twice --
    // and the second reference is often the one inside `scalar(...)`. Sharing
    // rewrites those references to Scan(name), whose schema resolves through
    // source_schemas(); if the binding's schema is not recorded there, the
    // decorrelation check sees an unknown outer schema and the whole script
    // silently falls back to statements. That regressed PDS-H q15 and q22.
    std::vector<std::vector<std::string>> decode_calls;
    ibex::runtime::ExternRegistry registry;
    register_recording_lazy_source(registry, decode_calls);

    const std::string source = R"(
extern fn read_fake() -> DataFrame from "fake.hpp";
let totals = read_fake()[select { total = sum(b) }, by { a }];
totals[filter total == scalar(totals[select { m = max(total) }])];
)";
    CHECK(capture_planner_line(source, registry) == "planner: whole-script");
}

TEST_CASE("REPL wildcard ascription survives the IR clone", "[repl][lazy][planner][ascribe]") {
    // `as DataFrame<{ a: Int, * }>` allows extra columns. Whole-script lowering
    // clones bound IR per reference, and the clone rebuilt the AscribeNode
    // without its `open` flag -- silently turning the wildcard into an exact
    // schema, so any unlisted column became an error. Statement mode never
    // cloned, so only whole-script execution saw it.
    std::vector<std::vector<std::string>> decode_calls;
    ibex::runtime::ExternRegistry registry;
    register_recording_lazy_source(registry, decode_calls);  // yields columns a, b

    const std::string source = R"(
extern fn read_fake() -> DataFrame from "fake.hpp";
let rows = read_fake() as DataFrame<{ a: Int, * }>;
rows[select { n = count() }];
)";
    CHECK(capture_planner_line(source, registry) == "planner: whole-script");
    REQUIRE(ibex::repl::execute_script(source, registry));
}

TEST_CASE("REPL wildcard ascription does not materialize what it only asserts",
          "[repl][lazy][planner][ascribe]") {
    // Naming a column in an ascription asserts its shape; it does not ask for
    // its data. A wildcard allows extras, so whether they are materialized is
    // unobservable -- demand must stay narrow or ascribing a reader silently
    // costs a full decode (~4x on a lineitem scan).
    std::vector<std::vector<std::string>> decode_calls;
    ibex::runtime::ExternRegistry registry;
    register_recording_lazy_source(registry, decode_calls);  // columns a, b

    SECTION("only the asserted column is decoded, not the whole source") {
        const std::string source = R"(
extern fn read_fake() -> DataFrame from "fake.hpp";
let rows = read_fake() as DataFrame<{ a: Int, * }>;
rows[select { n = count() }];
)";
        REQUIRE(ibex::repl::execute_script(source, registry));
        // Not even an empty decode: count() comes from logical_rows, so the
        // scan never calls the decoder at all.
        REQUIRE(decode_calls.empty());
    }

    SECTION("an ascription proved from source metadata reads nothing") {
        // The source schema proves the required names and types without
        // decoding a page. The scan is left to materialize only what the query
        // actually reads: here, count(), which reads nothing.
        const std::string source = R"(
extern fn read_fake() -> DataFrame from "fake.hpp";
let rows = read_fake() as DataFrame<{ a: Int, b: Int }>;
rows[select { n = count() }];
)";
        REQUIRE(ibex::repl::execute_script(source, registry));
        // Not even an empty decode: count() comes from logical_rows, so the
        // scan never calls the decoder at all.
        REQUIRE(decode_calls.empty());
    }

    SECTION("an ascription permits an unlisted physical column") {
        const std::string source = R"(
extern fn read_fake() -> DataFrame from "fake.hpp";
let rows = read_fake() as DataFrame<{ a: Int }>;
rows[select { n = count() }];
)";
        REQUIRE(ibex::repl::execute_script(source, registry));
        REQUIRE(decode_calls.empty());
    }
}

TEST_CASE("REPL ascribing a column the source lacks is a fatal error",
          "[repl][lazy][planner][ascribe]") {
    // Fatal user error, even when nothing else in the script reads the column:
    // narrowing demand must not let a bad ascription through unchecked.
    std::vector<std::vector<std::string>> decode_calls;
    ibex::runtime::ExternRegistry registry;
    register_recording_lazy_source(registry, decode_calls);

    SECTION("missing column") {
        const std::string source = R"(
extern fn read_fake() -> DataFrame from "fake.hpp";
let rows = read_fake() as DataFrame<{ nope: Int, * }>;
rows[select { n = count() }];
)";
        CHECK_FALSE(ibex::repl::execute_script(source, registry));
    }

    SECTION("wrong type") {
        const std::string source = R"(
extern fn read_fake() -> DataFrame from "fake.hpp";
let rows = read_fake() as DataFrame<{ a: String, * }>;
rows[select { n = count() }];
)";
        CHECK_FALSE(ibex::repl::execute_script(source, registry));
    }
}

TEST_CASE("REPL keeps a join's clauses when a function body is inlined", "[repl][join]") {
    // A join written inside a function body has to mean there what it means
    // anywhere else. `expect 1:1` makes that observable rather than silent:
    // customer 10 matches two orders, so the run must fail. A clause lost on
    // the way through the function would succeed instead.
    ibex::runtime::ExternRegistry registry;
    ibex::repl::ReplConfig config;
    config.persistent_history = false;

    const std::string violating = R"(
fn pair(a: DataFrame, b: DataFrame) -> DataFrame {
  a join b on cust expect 1:1;
}
let orders = Table { id = [1, 2, 3], cust = [10, 10, 20] };
let customers = Table { cust = [10, 20], tier = [7, 8] };
pair(orders, customers);
)";
    CHECK_FALSE(ibex::repl::execute_script(violating, registry, config));

    // The same shape the data does satisfy, so the clause is carried and the
    // check passes rather than the clause simply being absent.
    const std::string satisfied = R"(
fn pair(a: DataFrame, b: DataFrame) -> DataFrame {
  a join b on cust expect n:1;
}
let orders = Table { id = [1, 2, 3], cust = [10, 10, 20] };
let customers = Table { cust = [10, 20], tier = [7, 8] };
pair(orders, customers);
)";
    CHECK(ibex::repl::execute_script(satisfied, registry, config));
}

// The REPL evaluates a statement through `lower_expr`, which — unlike
// `parser::lower()` for a whole program — does no plan rewriting of its own.
// The statement path therefore runs the schema-aware join pushdowns and the
// optimizer itself. All three rewrites below are semantics-preserving, so what
// these lock in is that enabling them did not change an answer: each query is
// paired with a formulation the rewrite cannot apply to, and the two must agree.
TEST_CASE("REPL statement path: semi join pushed under an inner join keeps its rows",
          "[repl][join][semi]") {
    ibex::runtime::ExternRegistry registry;
    ibex::repl::ReplConfig config;
    config.persistent_history = false;
    std::vector<std::int64_t> pushed;
    std::vector<std::int64_t> direct;

    // `(events ⋈ users) ⋉ syms on sym` — the semi key lives entirely on the
    // left of the inner join, so it may be pushed below it.
    register_int_capture(registry, "amount", pushed);
    const char* pushed_src = R"(
extern fn capture(df: DataFrame) -> Int from "fake.hpp";
let events = Table { uid = [1, 2, 3, 4, 5], sym = ["A", "B", "A", "C", "B"], amount = [10, 20, 30, 40, 50] };
let users = Table { uid = [1, 2, 3, 4, 5], tier = [7, 8, 9, 7, 8] };
let syms = Table { sym = ["A", "C"] };
let out = (events join users on { uid }) semi join syms on { sym };
capture(out[order { amount }]);
)";
    REQUIRE(ibex::repl::execute_script(pushed_src, registry, config));

    // The same answer written so no pushdown applies: filter first, then join.
    register_int_capture(registry, "amount", direct);
    const char* direct_src = R"(
extern fn capture(df: DataFrame) -> Int from "fake.hpp";
let events = Table { uid = [1, 2, 3, 4, 5], sym = ["A", "B", "A", "C", "B"], amount = [10, 20, 30, 40, 50] };
let users = Table { uid = [1, 2, 3, 4, 5], tier = [7, 8, 9, 7, 8] };
let out = (events[filter sym != "B"]) join users on { uid };
capture(out[order { amount }]);
)";
    REQUIRE(ibex::repl::execute_script(direct_src, registry, config));

    CHECK(pushed == std::vector<std::int64_t>{10, 30, 40});
    CHECK(pushed == direct);
}

TEST_CASE("REPL statement path: anti join pushed under an inner join keeps its rows",
          "[repl][join][anti]") {
    ibex::runtime::ExternRegistry registry;
    ibex::repl::ReplConfig config;
    config.persistent_history = false;
    std::vector<std::int64_t> pushed;

    register_int_capture(registry, "amount", pushed);
    const char* src = R"(
extern fn capture(df: DataFrame) -> Int from "fake.hpp";
let events = Table { uid = [1, 2, 3, 4, 5], sym = ["A", "B", "A", "C", "B"], amount = [10, 20, 30, 40, 50] };
let users = Table { uid = [1, 2, 3, 4, 5], tier = [7, 8, 9, 7, 8] };
let syms = Table { sym = ["A", "C"] };
let out = (events join users on { uid }) anti join syms on { sym };
capture(out[order { amount }]);
)";
    REQUIRE(ibex::repl::execute_script(src, registry, config));

    // The complement of the semi join above.
    CHECK(pushed == std::vector<std::int64_t>{20, 50});
}

TEST_CASE("REPL statement path: order followed by head returns the true top rows",
          "[repl][order][topk]") {
    // Canonicalize R16 fuses this into a TopK the runtime answers with a
    // partial heap-select instead of a full sort. Ties and the row count are
    // where a heap-select and a sort-then-truncate could disagree.
    ibex::runtime::ExternRegistry registry;
    ibex::repl::ReplConfig config;
    config.persistent_history = false;
    std::vector<std::int64_t> topk;
    std::vector<std::int64_t> sorted_head;

    register_int_capture(registry, "v", topk);
    const char* fused = R"(
extern fn capture(df: DataFrame) -> Int from "fake.hpp";
let t = Table { v = [5, 1, 9, 3, 9, 7, 2] };
capture(t[order { v desc }][head 3]);
)";
    REQUIRE(ibex::repl::execute_script(fused, registry, config));

    register_int_capture(registry, "v", sorted_head);
    const char* unfused = R"(
extern fn capture(df: DataFrame) -> Int from "fake.hpp";
let t = Table { v = [5, 1, 9, 3, 9, 7, 2] };
let ordered = t[order { v desc }];
capture(ordered[head 3]);
)";
    REQUIRE(ibex::repl::execute_script(unfused, registry, config));

    CHECK(topk == std::vector<std::int64_t>{9, 9, 7});
    CHECK(topk == sorted_head);
}

namespace {

/// Records, for one column, whether each row is valid — the one thing a plain
/// value capture cannot see.
auto register_validity_capture(ibex::runtime::ExternRegistry& registry, const std::string& column,
                               std::vector<bool>& captured) -> void {
    registry.register_scalar_table_consumer(
        "capture", ibex::runtime::ScalarKind::Int,
        [&captured, column](const ibex::runtime::Table& table, const ibex::runtime::ExternArgs&)
            -> std::expected<ibex::runtime::ExternValue, std::string> {
            auto it = table.index.find(column);
            if (it == table.index.end()) {
                return std::unexpected("capture expected a column named " + column);
            }
            const auto& entry = table.columns[it->second];
            captured.clear();
            for (std::size_t row = 0; row < table.rows(); ++row) {
                captured.push_back(!ibex::runtime::is_null(entry, row));
            }
            return ibex::runtime::ExternValue{std::int64_t{0}};
        });
}

}  // namespace

TEST_CASE("REPL Table literal: a null element makes that cell null", "[repl][null][construct]") {
    // `null` has no type of its own, so the column takes the type of its first
    // non-null element and the null becomes a cleared validity bit. Both REPL
    // paths build the column, so both are checked.
    ibex::runtime::ExternRegistry registry;
    std::vector<bool> valid;
    std::vector<std::int64_t> values;
    register_validity_capture(registry, "v", valid);

    SECTION("batch planner path") {
        const char* src = R"(
extern fn capture(df: DataFrame) -> Int from "fake.hpp";
capture(Table { v = [1, null, 3] });
)";
        REQUIRE(ibex::repl::execute_script(src, registry));
        CHECK(valid == std::vector<bool>{true, false, true});
    }

    SECTION("statement-at-a-time path") {
        // A `let`-bound table is executed statement by statement.
        const char* src = R"(
extern fn capture(df: DataFrame) -> Int from "fake.hpp";
let t = Table { v = [1, null, 3], s = ["a", null, "c"] };
let n = scalar(t[select { c = count() }]);
capture(t);
)";
        REQUIRE(ibex::repl::execute_script(src, registry));
        CHECK(valid == std::vector<bool>{true, false, true});
    }

    SECTION("null survives arithmetic and is testable with is null") {
        register_int_capture(registry, "v", values);
        const char* src = R"(
extern fn capture(df: DataFrame) -> Int from "fake.hpp";
capture(Table { x = [1, null, 3] }[filter { x is not null }, select { v = x + 1 }]);
)";
        REQUIRE(ibex::repl::execute_script(src, registry));
        CHECK(values == std::vector<std::int64_t>{2, 4});
    }
}

TEST_CASE("REPL Table literal: an all-null column is an all-null Int64 column",
          "[repl][null][construct]") {
    // With no non-null element there is no type to take, so the column falls
    // back to Int64 — the same default `col = []` uses.
    ibex::runtime::ExternRegistry registry;
    std::vector<bool> valid;
    register_validity_capture(registry, "v", valid);

    const char* src = R"(
extern fn capture(df: DataFrame) -> Int from "fake.hpp";
capture(Table { v = [null, null] });
)";
    REQUIRE(ibex::repl::execute_script(src, registry));
    CHECK(valid == std::vector<bool>{false, false});
}

TEST_CASE("REPL a null element still has to agree with the column's type",
          "[repl][null][construct]") {
    ibex::runtime::ExternRegistry registry;
    ibex::repl::ReplConfig config;
    config.persistent_history = false;

    // A standalone Series binding is a bare column with no validity bitmap, so
    // there is nowhere to record the null.
    CHECK_FALSE(ibex::repl::execute_script("let s = [1, null];", registry, config));
    // Nulls do not excuse mixed element types.
    CHECK_FALSE(ibex::repl::execute_script("Table { v = [1, null, \"a\"] };", registry, config));
}

// --- Namespaces ---------------------------------------------------------------

namespace {

/// Library stubs for the namespace tests, in a per-process directory.
auto namespace_stub_dir() -> std::filesystem::path {
    const auto dir = std::filesystem::temp_directory_path() /
                     ("ibex_namespaces_" + std::to_string(current_process_id()));
    std::filesystem::create_directories(dir);
    const auto write = [&](const char* name, const char* text) {
        std::ofstream stub{dir / name};
        stub << text;
    };
    write("geo.ibex",
          "namespace geo {\n"
          "    extern fn points() -> DataFrame from \"geo.hpp\";\n"
          "    extern fn dist(x: Int) -> Int from \"geo.hpp\";\n"
          "    fn scale(x: Int) -> Int { x * 10; }\n"
          "    fn far(df: DataFrame) -> DataFrame { df[filter x > 1]; }\n"
          "}\n");
    // A second file contributing to the same namespace; `dist` and `scale`
    // are siblings declared in geo.ibex.
    write("geo_extra.ibex", "namespace geo { fn twice(x: Int) -> Int { scale(x) + dist(x); } }\n");
    write("geo_clash.ibex",
          "namespace geo { extern fn dist(x: String) -> Int from \"geo.hpp\"; }\n");
    write("metric.ibex", "namespace metric { extern fn dist(x: Int) -> Int from \"geo.hpp\"; }\n");
    return dir;
}

void register_geo(ibex::runtime::ExternRegistry& registry) {
    registry.register_table("geo::points",
                            [](const ibex::runtime::ExternArgs&)
                                -> std::expected<ibex::runtime::ExternValue, std::string> {
                                ibex::runtime::Table table;
                                table.add_column("x", ibex::Column<std::int64_t>{1, 2, 3});
                                table.add_column("geo", ibex::Column<std::int64_t>{4, 5, 6});
                                return ibex::runtime::ExternValue{std::move(table)};
                            });
    registry.register_scalar(
        "geo::dist", ibex::runtime::ScalarKind::Int,
        [](const ibex::runtime::ExternArgs& args)
            -> std::expected<ibex::runtime::ExternValue, std::string> { return args.at(0); });
    registry.register_scalar("metric::dist", ibex::runtime::ScalarKind::Int,
                             [](const ibex::runtime::ExternArgs& args)
                                 -> std::expected<ibex::runtime::ExternValue, std::string> {
                                 return ibex::runtime::ExternValue{ibex::runtime::ScalarValue{
                                     std::get<std::int64_t>(args.at(0)) * 100}};
                             });
}

auto namespace_session(ibex::runtime::ExternRegistry& registry) -> ibex::repl::ReplSession {
    ibex::repl::ReplConfig config;
    const auto dir = namespace_stub_dir();
    config.import_search_paths = {dir.string()};
    config.plugin_search_paths = {dir.string()};
    config.persistent_history = false;
    register_geo(registry);
    return {config, registry};
}

auto scalar_int(const ibex::repl::ExecutionResult& result) -> std::int64_t {
    REQUIRE(result.scalar.has_value());
    return std::get<std::int64_t>(*result.scalar);
}

}  // namespace

TEST_CASE("Namespaces: import brings in qualified names only", "[repl][namespaces]") {
    ibex::runtime::ExternRegistry registry;
    auto session = namespace_session(registry);
    REQUIRE(session.execute("import \"geo\";").ok);

    CHECK(scalar_int(session.execute("geo::dist(7);")) == 7);
    CHECK(scalar_int(session.execute("geo::scale(2);")) == 20);
    const auto filtered = session.execute("geo::far(geo::points());");
    REQUIRE(filtered.ok);
    CHECK(filtered.table->rows() == 2);

    // Without `using`, the bare name is not visible.
    const auto bare = session.execute("dist(7);");
    CHECK_FALSE(bare.ok);

    const auto typo = session.execute("geo::dits(7);");
    CHECK_FALSE(typo.ok);
    CHECK(typo.error.contains("did you mean 'geo::dist'"));
}

TEST_CASE("Namespaces: using a namespace or one name", "[repl][namespaces]") {
    ibex::runtime::ExternRegistry registry;
    {
        auto session = namespace_session(registry);
        REQUIRE(session.execute("import \"geo\";\nusing geo;").ok);
        // A `using` typed at the prompt holds for the rest of the session.
        CHECK(scalar_int(session.execute("dist(7);")) == 7);
        CHECK(scalar_int(session.execute("scale(3);")) == 30);
    }
    {
        auto session = namespace_session(registry);
        REQUIRE(session.execute("import \"geo\";\nusing geo::scale;").ok);
        CHECK(scalar_int(session.execute("scale(3);")) == 30);
        CHECK_FALSE(session.execute("dist(7);").ok);
    }
    {
        auto session = namespace_session(registry);
        const auto missing = session.execute("using geo;");
        CHECK_FALSE(missing.ok);
        CHECK(missing.error.contains("is it imported?"));
    }
}

TEST_CASE("Namespaces: two usings that disagree fail where the name is used",
          "[repl][namespaces]") {
    ibex::runtime::ExternRegistry registry;
    auto session = namespace_session(registry);
    REQUIRE(session.execute("import \"geo\";\nimport \"metric\";\nusing geo;\nusing metric;").ok);
    CHECK(scalar_int(session.execute("scale(1);")) == 10);
    const auto ambiguous = session.execute("dist(1);");
    CHECK_FALSE(ambiguous.ok);
    CHECK(ambiguous.error.contains("'dist' is ambiguous"));
    CHECK(scalar_int(session.execute("metric::dist(1);")) == 100);
}

TEST_CASE("Namespaces: a column or let named like the namespace does not shadow it",
          "[repl][namespaces]") {
    ibex::runtime::ExternRegistry registry;
    auto session = namespace_session(registry);
    REQUIRE(session.execute("import \"geo\";\nlet geo = 2;").ok);
    // `geo` is a column of the table and a scalar binding; `geo::scale` is the
    // function, and its argument `geo` the column.
    const auto result =
        session.execute("geo::points()[select { y = geo::scale(geo), z = x + ^geo }];");
    REQUIRE(result.ok);
    const auto* y = std::get_if<ibex::Column<std::int64_t>>(result.table->find("y"));
    REQUIRE(y != nullptr);
    CHECK((*y)[0] == 40);
    CHECK((*y)[2] == 60);
    const auto* z = std::get_if<ibex::Column<std::int64_t>>(result.table->find("z"));
    REQUIRE(z != nullptr);
    CHECK((*z)[0] == 3);
}

TEST_CASE("Namespaces: files merge into one namespace; a clash is an error", "[repl][namespaces]") {
    ibex::runtime::ExternRegistry registry;
    auto session = namespace_session(registry);
    REQUIRE(session.execute("import \"geo\";\nimport \"geo_extra\";").ok);
    // `twice` reaches its siblings in another file without a prefix.
    CHECK(scalar_int(session.execute("geo::twice(2);")) == 22);
    // Importing the same library again redeclares nothing new.
    REQUIRE(session.execute("import \"geo\";").ok);

    const auto clash = session.execute("import \"geo_clash\";");
    CHECK_FALSE(clash.ok);
    CHECK(clash.error.contains("'geo::dist' is already declared with a different signature"));
}

TEST_CASE("Namespaces: a script declares and uses its own namespace", "[repl][namespaces]") {
    ibex::runtime::ExternRegistry registry;
    auto session = namespace_session(registry);
    const auto result = session.execute(
        "namespace util {\n"
        "    fn inc(x: Int) -> Int { x + 1; }\n"
        "    fn inc2(x: Int) -> Int { inc(inc(x)); }\n"
        "}\n"
        "fn inc_global(x: Int) -> Int { x + 100; }\n"
        "using util::inc2;\n"
        "inc2(1) + util::inc(0) + ::inc_global(0);\n");
    INFO(result.error);
    REQUIRE(result.ok);
    CHECK(scalar_int(result) == 104);
}

TEST_CASE("Namespaces: a script with imports and namespaces plans as one block",
          "[repl][namespaces][planner]") {
    ibex::runtime::ExternRegistry registry;
    registry.register_lazy_table(
        "lz::read",
        [](const ibex::runtime::ExternArgs&)
            -> std::expected<ibex::runtime::LazyTablePtr, std::string> {
            ibex::runtime::Table schema;
            schema.add_column("a", ibex::Column<std::int64_t>{});
            return std::make_shared<ibex::runtime::LazyTable>(
                std::move(schema), 3,
                [](const std::vector<std::string>& names, const ibex::runtime::Selection*)
                    -> std::expected<ibex::runtime::Table, std::string> {
                    ibex::runtime::Table table;
                    for (const auto& name : names) {
                        table.add_column(name, ibex::Column<std::int64_t>{1, 2, 3});
                    }
                    return table;
                });
        });
    ibex::repl::ReplConfig config;
    const auto dir = namespace_stub_dir();
    {
        std::ofstream stub{dir / "lz.ibex"};
        stub << "namespace lz {\n"
             << "    extern fn read() -> DataFrame from \"fake.hpp\";\n"
             << "    fn big(df: DataFrame) -> DataFrame { df[filter a > 1]; }\n"
             << "}\n";
    }
    config.import_search_paths = {dir.string()};
    config.plugin_search_paths = {dir.string()};
    CHECK(capture_planner_line("import \"lz\";\nlz::big(lz::read())[select { n = count() }];\n",
                               registry, config) == "planner: whole-script");
    CHECK(capture_planner_line("import \"lz\";\nusing lz;\nbig(read())[select { n = count() }];\n",
                               registry, config) == "planner: whole-script");
}

TEST_CASE("REPL: a table function's scalar lets survive a call nested in another call",
          "[repl][function]") {
    // A call nested in `rbind(...)` is inlined into the plan, unlike a call that
    // is a whole statement. Two of its body's scalar `let`s used to go wrong
    // there: `let n = x` (a parameter) was taken for a table named `x`, and
    // `let m = scalar(...)` became a column reference -- both failed at run
    // time with "column not found".
    ibex::runtime::ExternRegistry registry;
    ibex::repl::ReplSession session(ibex::repl::ReplConfig{}, registry);
    const auto setup = session.execute(R"(
let base = Table { a = [0], b = [0] };
let src = Table { id = [1, 2], v = [10, 20] };
fn renamed(x: Int) -> DataFrame {
    let n = x;
    Table { a = [1] }[update { b = n }];
}
fn looked_up(k: Int) -> DataFrame {
    let m = scalar(src[filter id == k], v);
    Table { a = [1] }[update { b = m }];
}
)");
    CAPTURE(setup.error);
    REQUIRE(setup.ok);
    const auto b_of = [&](const char* src) {
        CAPTURE(src);
        const auto r = session.execute(src);
        REQUIRE(r.ok);
        REQUIRE(r.table.has_value());
        const auto* b = std::get_if<ibex::Column<std::int64_t>>(r.table->find("b"));
        REQUIRE(b != nullptr);
        return std::vector<std::int64_t>(b->begin(), b->end());
    };
    CHECK(b_of("rbind(base, renamed(7));") == std::vector<std::int64_t>{0, 7});
    CHECK(b_of("rbind(base, looked_up(2));") == std::vector<std::int64_t>{0, 20});
    CHECK(b_of("rbind(base, looked_up(1), renamed(3));") == std::vector<std::int64_t>{0, 10, 3});
    // The same calls as whole statements, which never went through the plan.
    CHECK(b_of("looked_up(2);") == std::vector<std::int64_t>{20});
}

TEST_CASE("REPL session accepts input that ends with a function definition", "[repl][function]") {
    // The REPL supplies a missing final `;`. After a declaration's closing `}`
    // that `;` used to become an empty statement and a parse error.
    ibex::runtime::ExternRegistry registry;
    ibex::repl::ReplSession session(ibex::repl::ReplConfig{}, registry);
    const auto declared = session.execute("fn twice(x: Int) -> Int { let y = x * 2; y; }");
    CAPTURE(declared.error);
    REQUIRE(declared.ok);
    const auto r = session.execute("twice(21)");
    REQUIRE(r.ok);
    REQUIRE(r.scalar.has_value());
    CHECK(std::get<std::int64_t>(*r.scalar) == 42);
}

TEST_CASE("ReplSession: a call's own tables and scalars last for that call", "[repl][session]") {
    ibex::runtime::ExternRegistry registry;
    ibex::repl::ReplSession session(ibex::repl::ReplConfig{}, registry);
    REQUIRE(session.execute("let kept = Table { v = [1, 2, 3] };").ok);

    const auto int_column = [](const ibex::runtime::Table& table, const char* name) {
        const auto* column = std::get_if<ibex::Column<std::int64_t>>(table.find(name));
        REQUIRE(column != nullptr);
        return std::vector<std::int64_t>(column->begin(), column->end());
    };
    ibex::runtime::Table input;
    input.add_column("v", ibex::Column<std::int64_t>{10, 20});
    ibex::repl::ReplSession::ExecuteOptions options;
    options.tables.insert_or_assign("kept", input);  // shadows the session's `kept`
    options.scalars.insert_or_assign("cutoff", std::int64_t{15});
    options.render = false;
    const auto r = session.execute("let out = kept[filter v > cutoff]; out;", std::move(options));
    CAPTURE(r.error);
    REQUIRE(r.ok);
    REQUIRE(r.table.has_value());
    CHECK(int_column(*r.table, "v") == std::vector<std::int64_t>{20});
    CHECK(r.output.empty());  // nothing formatted

    // After the call: the session's `kept` is back, `cutoff` is gone, and the
    // call's own `let out` stays.
    auto kept = session.table_binding("kept");
    REQUIRE(kept.has_value());
    CHECK(int_column(*kept, "v") == std::vector<std::int64_t>{1, 2, 3});
    CHECK_FALSE(session.execute("kept[filter v > cutoff];").ok);
    auto out = session.table_binding("out");
    REQUIRE(out.has_value());
    CHECK(int_column(*out, "v") == std::vector<std::int64_t>{20});

    // A call that rebinds its own table's name keeps the new binding.
    ibex::repl::ReplSession::ExecuteOptions rebinding;
    rebinding.tables.insert_or_assign("scratch", input);
    REQUIRE(session.execute("let scratch = scratch[filter v > 10];", std::move(rebinding)).ok);
    auto scratch = session.table_binding("scratch");
    REQUIRE(scratch.has_value());
    CHECK(int_column(*scratch, "v") == std::vector<std::int64_t>{20});
    CHECK_FALSE(session.table_binding("nothing").has_value());
}

TEST_CASE("ReplSession: print output comes back without rendering results", "[repl][session]") {
    ibex::runtime::ExternRegistry registry;
    ibex::repl::ReplSession session(ibex::repl::ReplConfig{}, registry);
    ibex::repl::ReplSession::ExecuteOptions quiet;
    quiet.render = false;
    const auto r = session.execute("print(\"hello\"); Table { a = [1] };", std::move(quiet));
    REQUIRE(r.ok);
    CHECK(r.output.find("hello") != std::string::npos);
    CHECK(r.output.find("rows:") == std::string::npos);
    REQUIRE(r.table.has_value());

    const auto rendered = session.execute("Table { a = [1] };");
    REQUIRE(rendered.ok);
    CHECK(rendered.output.find("rows: 1") != std::string::npos);

    // A failure's message is the error, not part of the output as well.
    const auto failed = session.execute("print(\"before\"); missing_table[select { a }];");
    REQUIRE_FALSE(failed.ok);
    CHECK(failed.error.find("missing_table") != std::string::npos);
    CHECK(failed.output.find("before") != std::string::npos);
    CHECK(failed.output.find("missing_table") == std::string::npos);
}

TEST_CASE("ReplSession: infer_schema lowers against the session's bindings", "[repl][session]") {
    ibex::runtime::ExternRegistry registry;
    ibex::repl::ReplSession session(ibex::repl::ReplConfig{}, registry);
    REQUIRE(session.execute("let t = Table { k = [1, 2], v = [1.5, 2.5] };").ok);
    // `limit` is bound by the call that will run it, so it is no column.
    const auto schema =
        session.infer_schema("t[filter k > limit, select { k, w = v * 2.0 }]", {"limit"});
    REQUIRE(schema.has_value());
    REQUIRE(schema->fields().size() == 2);
    CHECK(schema->fields()[0].name == "k");
    CHECK(schema->fields()[1].name == "w");
    CHECK(schema->fields()[1].type == ibex::ir::ColumnType::Float64);
    CHECK_FALSE(session.infer_schema("t[filter missing > 1]", {}).has_value());
}
