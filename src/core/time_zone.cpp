// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#include <ibex/core/time_zone.hpp>

#include <algorithm>
#include <cstdint>
#include <cstdlib>
#include <ctime>
#include <deque>
#include <filesystem>
#include <limits>
#include <mutex>
#include <optional>
#include <robin_hood.h>
#include <stdexcept>
#include <stdlib.h>
#include <string>
#include <string_view>
#include <system_error>
#include <time.h>
#include <utility>

namespace ibex {

namespace {

/// Names live in a `deque` because its references are stable across growth:
/// `zone_name` hands out a reference that must outlive later interning.
struct ZoneTable {
    std::mutex mutex;
    std::deque<std::string> names;
    robin_hood::unordered_map<std::string, ZoneId> ids;
};

auto table() -> ZoneTable& {
    static ZoneTable instance;
    return instance;
}

}  // namespace

auto intern_zone(std::string_view name) -> ZoneId {
    auto& t = table();
    const std::scoped_lock lock(t.mutex);
    std::string key(name);
    if (auto it = t.ids.find(key); it != t.ids.end()) {
        return it->second;
    }
    if (t.names.size() >= std::size_t{1} << 16U) {
        // 65k distinct zones is not a workload, it is a leak: the table is
        // process-lifetime, so failing loudly beats growing without bound.
        throw std::length_error("time zone table exhausted");
    }
    const auto id = static_cast<ZoneId>(t.names.size());
    t.names.push_back(key);
    t.ids.emplace(std::move(key), id);
    return id;
}

auto zone_name(ZoneId id) -> const std::string& {
    auto& t = table();
    const std::scoped_lock lock(t.mutex);
    const auto index = static_cast<std::size_t>(id);
    if (index >= t.names.size()) {
        throw std::out_of_range("unknown time zone id");
    }
    // Safe to return after unlocking: `deque` never moves an existing element.
    return t.names[index];
}

auto is_known_zone(std::string_view name) -> bool {
#if defined(IBEX_HAS_STD_CHRONO_TIME_ZONES)
    try {
        static_cast<void>(std::chrono::locate_zone(std::string(name)));
        return true;
    } catch (const std::exception&) {
        return false;
    }
#else
    // macOS's libc++ does not expose chrono's tzdb, but the OS still ships the
    // same IANA database used by localtime(3). Canonicalise before accepting a
    // name: a lexical relative-path check alone would let a symlink beneath the
    // database root escape it.
    if (name.empty() || name.find('\0') != std::string_view::npos) {
        return false;
    }
    const std::filesystem::path relative{std::string(name)};
    if (relative.is_absolute()) {
        return false;
    }
    for (const auto& component : relative) {
        if (component == "..") {
            return false;
        }
    }

    std::error_code error;
    const std::filesystem::path root = std::filesystem::canonical("/usr/share/zoneinfo", error);
    if (error) {
        return false;
    }
    const std::filesystem::path candidate = std::filesystem::canonical(root / relative, error);
    if (error || !std::filesystem::is_regular_file(candidate, error) || error) {
        return false;
    }

    const auto mismatch =
        std::mismatch(root.begin(), root.end(), candidate.begin(), candidate.end());
    return mismatch.first == root.end();
#endif
}

#if !defined(IBEX_HAS_STD_CHRONO_TIME_ZONES)
namespace {

auto floor_div(std::int64_t value, std::int64_t divisor) -> std::int64_t {
    std::int64_t quotient = value / divisor;
    if (value < 0 && value % divisor != 0) {
        --quotient;
    }
    return quotient;
}

auto gmtime_safe(const std::time_t& value, std::tm& out) -> bool {
    return gmtime_r(&value, &out) != nullptr;
}

auto localtime_safe(const std::time_t& value, std::tm& out) -> bool {
    return localtime_r(&value, &out) != nullptr;
}

auto same_civil_time(const std::tm& lhs, const std::tm& rhs) -> bool {
    return lhs.tm_year == rhs.tm_year && lhs.tm_mon == rhs.tm_mon && lhs.tm_mday == rhs.tm_mday &&
           lhs.tm_hour == rhs.tm_hour && lhs.tm_min == rhs.tm_min && lhs.tm_sec == rhs.tm_sec;
}

template <typename F>
auto with_zone(std::string_view zone, F&& fn) -> decltype(fn()) {
    // TZ and tzset are process-global. The fallback is used only on platforms
    // whose standard library lacks chrono tzdb; serialising here preserves
    // correctness for concurrent resamples and casts.
    static std::mutex timezone_mutex;
    const std::scoped_lock lock(timezone_mutex);
    const char* previous = std::getenv("TZ");
    const std::optional<std::string> saved =
        previous != nullptr ? std::optional<std::string>(previous) : std::nullopt;
    setenv("TZ", std::string(zone).c_str(), 1);
    tzset();
    auto result = fn();
    if (saved.has_value()) {
        setenv("TZ", saved->c_str(), 1);
    } else {
        unsetenv("TZ");
    }
    tzset();
    return result;
}

}  // namespace

auto local_time_to_sys(std::string_view zone, std::int64_t nanos) -> std::optional<std::int64_t> {
    constexpr std::int64_t nanos_per_second = 1'000'000'000;
    const std::int64_t seconds = floor_div(nanos, nanos_per_second);
    const std::int64_t remainder = nanos - seconds * nanos_per_second;
    const std::time_t wall_seconds = static_cast<std::time_t>(seconds);
    if (static_cast<std::int64_t>(wall_seconds) != seconds) {
        return std::nullopt;
    }
    std::tm wall{};
    if (!gmtime_safe(wall_seconds, wall)) {
        return std::nullopt;
    }

    return with_zone(zone, [&] -> std::optional<std::int64_t> {
        std::optional<std::time_t> earliest;
        for (const int is_dst : {-1, 0, 1}) {
            std::tm candidate = wall;
            candidate.tm_isdst = is_dst;
            const std::time_t instant = std::mktime(&candidate);
            std::tm rendered{};
            if (!localtime_safe(instant, rendered) || !same_civil_time(rendered, wall)) {
                continue;
            }
            if (!earliest.has_value() || instant < *earliest) {
                earliest = instant;
            }
        }
        if (!earliest.has_value()) {
            return std::nullopt;
        }
        const auto instant_seconds = static_cast<std::int64_t>(*earliest);
        if ((instant_seconds > 0 &&
             instant_seconds >
                 (std::numeric_limits<std::int64_t>::max() - remainder) / nanos_per_second) ||
            (instant_seconds < 0 &&
             instant_seconds <
                 (std::numeric_limits<std::int64_t>::min() - remainder) / nanos_per_second)) {
            return std::nullopt;
        }
        return instant_seconds * nanos_per_second + remainder;
    });
}

auto local_bucket_start(std::string_view zone, std::int64_t nanos, std::int64_t duration_nanos)
    -> std::int64_t {
    constexpr std::int64_t nanos_per_second = 1'000'000'000;
    const std::int64_t seconds = floor_div(nanos, nanos_per_second);
    const std::int64_t remainder = nanos - seconds * nanos_per_second;
    const std::time_t instant = static_cast<std::time_t>(seconds);
    if (static_cast<std::int64_t>(instant) != seconds) {
        return nanos;
    }
    const auto local_nanos = with_zone(zone, [&] {
        std::tm local{};
        if (!localtime_safe(instant, local)) {
            return nanos;
        }
        const std::time_t civil_seconds = timegm(&local);
        return static_cast<std::int64_t>(civil_seconds) * nanos_per_second + remainder;
    });
    const std::int64_t boundary = floor_div(local_nanos, duration_nanos) * duration_nanos;
    // A bucket boundary is normally a valid local time. If a transition skips
    // it, mktime-based conversion below follows chrono::choose::earliest.
    return local_time_to_sys(zone, boundary).value_or(nanos);
}
#endif

}  // namespace ibex
