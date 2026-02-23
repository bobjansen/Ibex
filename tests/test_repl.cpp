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
