#include <ibex/parser/lower.hpp>
#include <ibex/parser/parser.hpp>
#include <ibex/runtime/interpreter.hpp>
#include <ibex/runtime/ops.hpp>

#include <fstream>
#include <iostream>
#include <iterator>
#include <stdexcept>
#include <string>

namespace {

auto read_file(const std::string& path) -> std::string {
    std::ifstream in(path);
    if (!in) {
        throw std::runtime_error("unable to open file: " + path);
    }
    return std::string((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
}

}  // namespace

auto main(int argc, char** argv) -> int {
    if (argc != 2) {
        std::cerr << "usage: ibex_eval <file.ibex>\n";
        return 2;
    }

    try {
        const std::string source = read_file(argv[1]);
        auto parsed = ibex::parser::parse(source);
        if (!parsed) {
            std::cerr << "parse error: " << parsed.error().message << '\n';
            return 1;
        }

        auto lowered = ibex::parser::lower(*parsed);
        if (!lowered) {
            std::cerr << "lower error: " << lowered.error().message << '\n';
            return 1;
        }

        ibex::runtime::TableRegistry tables;
        ibex::runtime::ScalarRegistry scalars;
        auto result = ibex::runtime::interpret(*lowered.value(), tables, &scalars, nullptr);
        if (!result) {
            std::cerr << "runtime error: " << result.error() << '\n';
            return 1;
        }

        ibex::ops::print(*result);
        return 0;
    } catch (const std::exception& ex) {
        std::cerr << "error: " << ex.what() << '\n';
        return 1;
    }
}
