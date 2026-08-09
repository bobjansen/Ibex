// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#pragma once

#include <chrono>
#include <cstdint>
#include <functional>

namespace ibex {

/// Calendar date in days since 1970-01-01 (Unix epoch).
struct Date {
    std::int32_t days = 0;
    auto operator<=>(const Date&) const = default;
};

/// Instant in nanoseconds since 1970-01-01T00:00:00Z (Unix epoch).
struct Timestamp {
    std::int64_t nanos = 0;
    auto operator<=>(const Timestamp&) const = default;
};

/// Truncate an instant to the UTC calendar day containing it.
///
/// A `Timestamp` carries no zone, so every calendar-boundary operation over one
/// cuts on UTC (SPEC section 2) and this is no exception. `floor` rather than a
/// division, so a pre-epoch instant lands on its own day instead of rounding
/// toward the epoch. A `Timestamp` spans at most ~1.07e5 days, so the day count
/// always fits `Date`'s int32.
[[nodiscard]] inline auto timestamp_to_date(Timestamp ts) -> Date {
    const std::chrono::sys_time<std::chrono::nanoseconds> point{std::chrono::nanoseconds{ts.nanos}};
    return Date{static_cast<std::int32_t>(
        std::chrono::floor<std::chrono::days>(point).time_since_epoch().count())};
}

}  // namespace ibex

namespace std {

template <>
struct hash<ibex::Date> {
    auto operator()(const ibex::Date& d) const noexcept -> std::size_t {
        return std::hash<std::int32_t>{}(d.days);
    }
};

template <>
struct hash<ibex::Timestamp> {
    auto operator()(const ibex::Timestamp& ts) const noexcept -> std::size_t {
        return std::hash<std::int64_t>{}(ts.nanos);
    }
};

}  // namespace std
