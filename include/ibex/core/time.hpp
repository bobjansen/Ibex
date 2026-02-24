#pragma once

#include <compare>
#include <cstdint>

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
