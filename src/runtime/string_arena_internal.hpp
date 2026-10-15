// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#pragma once

#include <algorithm>
#include <cstddef>
#include <cstring>
#include <memory>
#include <string_view>
#include <vector>

namespace ibex::runtime {

/// Append-only bytes for string group keys, handed out as views that stay
/// valid for the arena's life -- moves included, since its blocks are on the
/// heap and never reallocate. One writer per arena.
///
/// The single-string key used to be a `std::string` three times over: in a
/// partition's index, in its key list, and in `str_order_`, and the index
/// moved them as it grew. At h2o's 5M groups that was most of the probe.
/// Now a group's bytes are copied once, here, and everything else holds a
/// 16-byte view.
class StringArena {
   public:
    StringArena() = default;
    StringArena(const StringArena&) = delete;
    auto operator=(const StringArena&) -> StringArena& = delete;
    StringArena(StringArena&&) noexcept = default;
    auto operator=(StringArena&&) noexcept -> StringArena& = default;
    ~StringArena() = default;

    [[nodiscard]] auto intern(std::string_view value) -> std::string_view {
        if (value.empty()) {
            return {};
        }
        if (value.size() > left_) {
            const std::size_t size = std::max(kBlockBytes, value.size());
            blocks_.push_back(std::make_unique_for_overwrite<char[]>(size));
            next_ = blocks_.back().get();
            left_ = size;
        }
        char* at = next_;
        std::memcpy(at, value.data(), value.size());
        next_ += value.size();
        left_ -= value.size();
        return {at, value.size()};
    }

   private:
    static constexpr std::size_t kBlockBytes = std::size_t{1} << 20;
    std::vector<std::unique_ptr<char[]>> blocks_;
    char* next_ = nullptr;
    std::size_t left_ = 0;
};

}  // namespace ibex::runtime
