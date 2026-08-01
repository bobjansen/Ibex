#include <ibex/core/time_zone.hpp>

#include <deque>
#include <mutex>
#include <robin_hood.h>
#include <stdexcept>
#include <string>
#include <string_view>

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

}  // namespace ibex
