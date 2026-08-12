// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#pragma once

// A deliberately small formatting surface backed by C++23. It has no external
// dependency.

#include <cstdio>
#include <format>
#include <ostream>
#include <print>
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
void print(format_string<Args...> pattern, Args&&... args) {
    std::print(pattern, std::forward<Args>(args)...);
}

template <typename... Args>
void print(std::FILE* stream, format_string<Args...> pattern, Args&&... args) {
    std::print(stream, pattern, std::forward<Args>(args)...);
}

// Apple's initial std::print implementation omitted this overload. Formatting
// into the stream explicitly provides the same behaviour on every supported
// standard library.
template <typename... Args>
void print(std::ostream& stream, format_string<Args...> pattern, Args&&... args) {
    stream << std::format(pattern, std::forward<Args>(args)...);
}

}  // namespace ibex::formatting
