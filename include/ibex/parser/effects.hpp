#pragma once

#include <ibex/parser/ast.hpp>
#include <ibex/parser/parser.hpp>

#include <cstdint>
#include <expected>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace ibex::parser {

using EffectMask = std::uint32_t;

constexpr EffectMask kEffIoRead = 1U << 0;
constexpr EffectMask kEffIoWrite = 1U << 1;
constexpr EffectMask kEffNondet = 1U << 2;
constexpr EffectMask kEffState = 1U << 3;
constexpr EffectMask kEffBlocking = 1U << 4;
constexpr EffectMask kEffMayFail = 1U << 5;
constexpr EffectMask kEffAllCore =
    kEffIoRead | kEffIoWrite | kEffNondet | kEffState | kEffBlocking | kEffMayFail;

struct EffectSummary {
    EffectMask mask = 0;
    bool io_read_unscoped = false;
    bool io_write_unscoped = false;
    std::unordered_set<std::string> io_read_resources;
    std::unordered_set<std::string> io_write_resources;

    [[nodiscard]] auto is_pure() const noexcept -> bool { return mask == 0; }
};

struct CallableSummary {
    EffectSummary effects;
    std::vector<Param::Effect> param_modes;
    bool is_extern = false;
};

struct EffectAnalysis {
    std::unordered_map<std::string, CallableSummary> user_functions;
    std::unordered_map<std::string, CallableSummary> externs;
    std::unordered_map<std::string, CallableSummary> builtins;

    [[nodiscard]] auto find_callee(std::string_view name) const -> const CallableSummary*;
    [[nodiscard]] auto merged_callee_summaries() const
        -> std::unordered_map<std::string, CallableSummary>;
};

[[nodiscard]] auto effect_kind_name(EffectKind kind) -> std::string_view;
[[nodiscard]] auto effect_kind_mask(EffectKind kind) -> EffectMask;
[[nodiscard]] auto effect_summary_from_specs(const std::optional<std::vector<EffectSpec>>& effects)
    -> EffectSummary;
[[nodiscard]] auto format_effect_summary(const EffectSummary& effects) -> std::string;
[[nodiscard]] auto analyze_effects(const Program& program)
    -> std::expected<EffectAnalysis, ParseError>;

}  // namespace ibex::parser
