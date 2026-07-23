#include <ibex/parser/lower.hpp>
#include <ibex/parser/parser.hpp>
#include <ibex/runtime/interpreter.hpp>
#include <ibex/runtime/table_compare.hpp>

#include <fstream>
#include <iostream>
#include <iterator>
#include <string>

ibex::runtime::Table ibex_generated_execute();

auto main(int argc, char** argv) -> int {
    if (argc != 2)
        return 2;
    std::ifstream input(argv[1]);
    std::string source{std::istreambuf_iterator<char>{input}, {}};
    auto parsed = ibex::parser::parse(source);
    if (!parsed) {
        std::cerr << parsed.error().message << '\n';
        return 1;
    }
    auto lowered = ibex::parser::lower(*parsed);
    if (!lowered) {
        std::cerr << lowered.error().message << '\n';
        return 1;
    }
    auto interpreted = ibex::runtime::interpret(**lowered, {});
    if (!interpreted) {
        std::cerr << interpreted.error() << '\n';
        return 1;
    }
    if (auto mismatch = ibex::runtime::compare_tables(*interpreted, ibex_generated_execute())) {
        std::cerr << mismatch->message() << '\n';
        return 1;
    }
    return 0;
}
