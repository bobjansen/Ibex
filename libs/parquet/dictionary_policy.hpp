// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#pragma once

#include <cstdint>

namespace ibex::parquet_dict {

/// Small dictionaries are cheap to intern even when every row is distinct.
inline constexpr std::int64_t kMaxEntries = 8192;

/// Use the actual dictionary entry count, summed over row groups. Byte size
/// cannot bound cardinality: strings may be empty or arbitrarily long, and
/// dictionary pages may be compressed. Retain small dictionaries and larger
/// dictionaries that replace at least two rows per entry.
[[nodiscard]] constexpr auto worth_keeping(std::int64_t entries, std::int64_t rows) -> bool {
    return entries <= kMaxEntries || entries <= rows / 2;
}

}  // namespace ibex::parquet_dict
