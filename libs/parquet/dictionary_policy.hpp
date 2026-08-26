// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#pragma once

#include <cstdint>

/// Whether a dictionary-encoded string column is worth reading back as a
/// dictionary (`Column<Categorical>`) rather than materializing its strings.
///
/// Split out from `parquet.hpp` so the decision can be tested without a Parquet
/// file: everything here is arithmetic on two numbers the file's footer already
/// carries. Getting it wrong is a performance choice, never a correctness one —
/// a column read either way holds the same values.
namespace ibex::parquet_dict {

/// A dictionary this small is worth keeping whatever it compresses. Interning
/// costs roughly 400ns an entry, so a few thousand are free, and the codes let
/// every group-by above hash by code instead of by string — which is why
/// `nation`'s 25 names must stay a dictionary even though they compress
/// nothing. TPC-H's small categoricals top out at `o_clerk`'s 6000; its
/// per-row string dictionaries start at 16384.
inline constexpr std::int64_t kMaxEntries = 8192;

/// The average entry length assumed when bounding an entry count from the
/// dictionary's size in bytes. Used ONLY to skip reading the dictionary page:
/// a dictionary whose bytes cannot hold a worthwhile number of entries even at
/// this length is rejected unread. Low enough that TPC-H's per-row string
/// dictionaries are all decided from metadata alone, because reading one is
/// paid by every query that opens the file — q05 wants two integer columns of
/// `supplier` and should not fund a 1.4MB read about its strings.
inline constexpr std::int64_t kAssumedEntryBytes = 16;

/// A BYTE_ARRAY dictionary entry is a 4-byte length plus its characters, so a
/// dictionary of `bytes` holds at most this many entries.
[[nodiscard]] constexpr auto most_entries_in(std::int64_t bytes) -> std::int64_t {
    return bytes / 5;
}

/// ...and, assuming entries no longer than `kAssumedEntryBytes`, at least this
/// many. Only sound as a lower bound to the extent that assumption holds, which
/// is why it may only be used to decline, never to accept.
[[nodiscard]] constexpr auto fewest_entries_in(std::int64_t bytes) -> std::int64_t {
    return bytes / (4 + kAssumedEntryBytes);
}

/// Keep the dictionary when it is small, or when it genuinely compresses.
///
/// These are two different reasons and neither implies the other. A small
/// dictionary is cheap to intern however little it compresses. A large one
/// earns its keep by standing in for many rows: `c_phone`'s 55k entries cover
/// 300k rows, so every row-wise string operation above runs once per entry
/// instead of once per row. What fails both tests is a dictionary with an entry
/// per row — `supplier`'s s_name — which pays the interning and returns
/// nothing.
[[nodiscard]] constexpr auto worth_keeping(std::int64_t entries, std::int64_t rows) -> bool {
    return entries <= kMaxEntries || entries * 2 <= rows;
}

/// What the byte size alone can settle.
enum class Verdict : std::uint8_t {
    Keep,        ///< worthwhile even at the most entries it could hold
    Dense,       ///< not worthwhile even at the fewest
    NeedsCount,  ///< straddles: read the dictionary page's entry count
};

[[nodiscard]] constexpr auto verdict_from_bytes(std::int64_t bytes, std::int64_t rows) -> Verdict {
    if (worth_keeping(most_entries_in(bytes), rows)) {
        return Verdict::Keep;
    }
    if (!worth_keeping(fewest_entries_in(bytes), rows)) {
        return Verdict::Dense;
    }
    return Verdict::NeedsCount;
}

}  // namespace ibex::parquet_dict
