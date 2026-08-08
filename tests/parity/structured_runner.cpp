// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#include <ibex/parser/lower.hpp>
#include <ibex/parser/parser.hpp>
#include <ibex/parser/scalar_bindings.hpp>
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
    // Scalar `let` bindings are not lowered into IR — reconstruct the registry
    // the same way ibex_compile does so the interpreter reference resolves them.
    auto scalar_bindings = ibex::parser::collect_scalar_bindings(*parsed);
    if (!scalar_bindings) {
        std::cerr << scalar_bindings.error() << '\n';
        return 1;
    }
    ibex::runtime::ScalarRegistry scalars;
    for (auto& [name, value] : *scalar_bindings) {
        scalars[name] = std::move(value);
    }
    auto interpreted = ibex::runtime::interpret(**lowered, {}, &scalars);
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
