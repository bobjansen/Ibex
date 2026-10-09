// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#include <ibex/core/enums.hpp>

#include <algorithm>
#include <array>
#include <cctype>

namespace ibex {

namespace {

constexpr std::array<std::string_view, 5> kRoundModeMembers{"Nearest", "Bankers", "Floor", "Ceil",
                                                            "Trunc"};

const BuiltinEnum kRoundMode{.name = "RoundMode", .members = kRoundModeMembers};

const std::array<const BuiltinEnum*, 1> kBuiltinEnums{&kRoundMode};

auto equal_ignoring_case(std::string_view a, std::string_view b) -> bool {
    return std::ranges::equal(a, b, [](char x, char y) {
        return std::tolower(static_cast<unsigned char>(x)) ==
               std::tolower(static_cast<unsigned char>(y));
    });
}

auto member_list(const BuiltinEnum& type) -> std::string {
    std::string out;
    for (const auto member : type.members) {
        if (!out.empty()) {
            out += ", ";
        }
        out += member;
    }
    return out;
}

}  // namespace

auto round_mode_enum() -> const BuiltinEnum& {
    return kRoundMode;
}

auto find_builtin_enum(std::string_view name) -> const BuiltinEnum* {
    const auto* const it = std::ranges::find_if(
        kBuiltinEnums, [&](const BuiltinEnum* type) { return type->name == name; });
    return it == kBuiltinEnums.end() ? nullptr : *it;
}

auto resolve_enum_member(const BuiltinEnum& type, std::string_view text)
    -> std::expected<std::size_t, std::string> {
    std::string_view member = text;
    if (const auto cut = text.rfind("::"); cut != std::string_view::npos) {
        const std::string_view qualifier = text.substr(0, cut);
        if (qualifier != type.name) {
            return std::unexpected("expected a " + std::string(type.name) + " value, got '" +
                                   std::string(text) + "'");
        }
        member = text.substr(cut + 2);
    }
    for (std::size_t i = 0; i < type.members.size(); ++i) {
        if (type.members[i] == member) {
            return i;
        }
    }
    std::string message = "unknown " + std::string(type.name) + " member '" + std::string(member) +
                          "' (expected one of " + member_list(type) + ")";
    for (const auto candidate : type.members) {
        if (equal_ignoring_case(candidate, member)) {
            message += "; did you mean '" + std::string(candidate) + "'?";
            break;
        }
    }
    return std::unexpected(std::move(message));
}

auto qualified_enum_member(const BuiltinEnum& type, std::size_t member) -> std::string {
    return std::string(type.name) + "::" + std::string(type.members[member]);
}

auto resolve_round_mode(std::string_view text) -> std::expected<RoundMode, std::string> {
    auto member = resolve_enum_member(kRoundMode, text);
    if (!member.has_value()) {
        return std::unexpected(std::move(member.error()));
    }
    return static_cast<RoundMode>(*member);
}

}  // namespace ibex
