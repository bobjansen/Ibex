// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#pragma once

#include <cstddef>
#include <cstdint>
#include <expected>
#include <span>
#include <string>
#include <string_view>

namespace ibex {

/// Built-in enum types (SPEC.md Section 3.7). Ibex defines them; scripts name
/// their members as `Type::Member`, or as a bare `Member` where a parameter of
/// that type is expected. No column or binding can hold an enum value, so a
/// bare member never competes with a column or `let` of the same name.
struct BuiltinEnum {
    std::string_view name;
    /// In declaration order; a member's index is its value.
    std::span<const std::string_view> members;
};

/// How `round` maps a value to an integer. The order matches `kRoundMode`'s
/// members, so a resolved member index converts directly.
enum class RoundMode : std::uint8_t { Nearest, Bankers, Floor, Ceil, Trunc };

/// The `RoundMode` built-in enum.
[[nodiscard]] auto round_mode_enum() -> const BuiltinEnum&;

/// The built-in enum called `name`, or null.
[[nodiscard]] auto find_builtin_enum(std::string_view name) -> const BuiltinEnum*;

/// Resolve `text` written where a value of `type` is expected: a bare member
/// (`Nearest`) or a qualified one (`RoundMode::Nearest`). Returns the member's
/// index, or an error that names the valid members and, for a near miss such
/// as `nearest`, the intended spelling.
[[nodiscard]] auto resolve_enum_member(const BuiltinEnum& type, std::string_view text)
    -> std::expected<std::size_t, std::string>;

/// `Type::Member`, the canonical spelling lowering writes into the IR.
[[nodiscard]] auto qualified_enum_member(const BuiltinEnum& type, std::size_t member)
    -> std::string;

/// `resolve_enum_member` for `RoundMode`.
[[nodiscard]] auto resolve_round_mode(std::string_view text)
    -> std::expected<RoundMode, std::string>;

}  // namespace ibex
