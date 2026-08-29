// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#pragma once

// A deliberately small formatting surface backed by C++23. It has no external
// dependency.

#include <cstdio>
#include <format>
#include <ostream>
#include <string>
#include <utility>

namespace ibex::formatting {

template <typename... Args>
using format_string = std::format_string<Args...>;

template <typename... Args>
auto format(format_string<Args...> pattern, Args&&... args) -> std::string {
    return std::format(pattern, std::forward<Args>(args)...);
}

template <typename... Args>
void print(std::FILE* stream, format_string<Args...> pattern, Args&&... args) {
    const auto text = std::format(pattern, std::forward<Args>(args)...);
    std::fputs(text.c_str(), stream);
}

template <typename... Args>
void print(format_string<Args...> pattern, Args&&... args) {
    // Qualified so argument-dependent lookup cannot pull in `std::print`: the
    // pattern's type is a `std` alias, and libc++'s <print> (transitively
    // included by other standard headers) otherwise makes this call ambiguous.
    ibex::formatting::print(stdout, pattern, std::forward<Args>(args)...);
}

// Formatting into streams explicitly keeps this surface independent from the
// still uneven C++23 std::print implementation across standard libraries.
template <typename... Args>
void print(std::ostream& stream, format_string<Args...> pattern, Args&&... args) {
    stream << std::format(pattern, std::forward<Args>(args)...);
}

}  // namespace ibex::formatting
