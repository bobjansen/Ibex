#include <ibex/repl/repl.hpp>
#include <ibex/runtime/extern_registry.hpp>

#include <catch2/catch_test_macros.hpp>

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
    std::string source((std::istreambuf_iterator<char>(input)),
                       std::istreambuf_iterator<char>());
    ibex::runtime::ExternRegistry registry;
    REQUIRE(ibex::repl::execute_script(source, registry));
}
