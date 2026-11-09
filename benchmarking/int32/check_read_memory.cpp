// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen
// Standalone mechanism check: peak RSS before/after logical reads of compact
// storage. Compile the identical source against each revision's column.hpp.
#include <ibex/core/column.hpp>

#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <sys/resource.h>
#include <utility>

auto peak_kib() -> long {
    rusage usage{};
    if (getrusage(RUSAGE_SELF, &usage) != 0)
        std::abort();
    return usage.ru_maxrss;
}
auto main(int argc, char** argv) -> int {
    const std::size_t rows = argc > 1 ? std::stoull(argv[1]) : 50000000;
    ibex::detail::NoInitVector<std::int32_t> values(rows);
    for (std::size_t i = 0; i < rows; ++i)
        values[i] = static_cast<std::int32_t>(i % 2001) - 1000;
    const auto column = ibex::Column<std::int64_t>::from_compact_int32(std::move(values));
    const long before = peak_kib();
    std::int64_t sum = 0;
    for (std::size_t i = 0; i < rows; ++i)
        sum += column[i];
    const long after = peak_kib();
    const auto tail = static_cast<std::int64_t>(rows % 2001);
    const auto expected = tail * (tail - 1) / 2 - 1000 * tail;
    if (sum != expected)
        return 2;
    std::cout << "{\"rows\":" << rows << ",\"before_kib\":" << before << ",\"after_kib\":" << after
              << ",\"checksum\":" << sum << "}\n";
}
