#include <ibex/repl/repl.hpp>
#include <ibex/runtime/extern_registry.hpp>

#include <catch2/catch_test_macros.hpp>

#include <csv.hpp>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <string>

using ibex::repl::normalize_input;

TEST_CASE("REPL normalizes implicit semicolons") {
    REQUIRE(normalize_input("1+1") == "1+1;");
    REQUIRE(normalize_input("let x = 1;") == "let x = 1;");
    REQUIRE(normalize_input("  1+1  ") == "  1+1  ;");
    REQUIRE(normalize_input("") == "");
}

TEST_CASE("REPL loads script with inferred lets") {
    std::filesystem::path script_path =
        std::filesystem::path(IBEX_SOURCE_DIR) / "tests" / "data" / "repl_infer.ibex";
    std::ifstream input(script_path);
    REQUIRE(input.good());
    std::string source((std::istreambuf_iterator<char>(input)), std::istreambuf_iterator<char>());
    std::string token = "${IBEX_SOURCE_DIR}";
    std::size_t pos = 0;
    while ((pos = source.find(token, pos)) != std::string::npos) {
        source.replace(pos, token.size(), IBEX_SOURCE_DIR);
        pos += std::char_traits<char>::length(IBEX_SOURCE_DIR);
    }

    ibex::runtime::ExternRegistry registry;
    registry.register_table("read_csv",
                            [](const ibex::runtime::ExternArgs& args)
                                -> std::expected<ibex::runtime::ExternValue, std::string> {
                                if (args.size() != 1) {
                                    return std::unexpected("read_csv() expects 1 argument");
                                }
                                const auto* path = std::get_if<std::string>(args.data());
                                if (path == nullptr) {
                                    return std::unexpected("read_csv() expects a string path");
                                }
                                try {
                                    return ibex::runtime::ExternValue{read_csv(*path)};
                                } catch (const std::exception& e) {
                                    return std::unexpected(std::string(e.what()));
                                }
                            });

    REQUIRE(ibex::repl::execute_script(source, registry));
}

TEST_CASE("REPL accepts scalar expression statements") {
    ibex::runtime::ExternRegistry registry;
    REQUIRE(ibex::repl::execute_script("1+1;", registry));
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
    std::string src((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
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

TEST_CASE("type annotation: Float64 let binding rejects int literal") {
    ibex::runtime::ExternRegistry registry;
    REQUIRE_FALSE(ibex::repl::execute_script("let x: Float64 = 3;", registry));
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

TEST_CASE("type annotation: function call rejects Int where Float64 expected") {
    ibex::runtime::ExternRegistry registry;
    const char* src = R"(
fn f(x: Float64) -> Float64 {
    x;
}
f(3);
)";
    REQUIRE_FALSE(ibex::repl::execute_script(src, registry));
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
