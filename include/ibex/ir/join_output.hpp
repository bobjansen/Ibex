#pragma once

#include <ibex/ir/node.hpp>

#include <cstddef>
#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace ibex::ir {

/// Which input a planned join output column is taken from.
enum class JoinOutputSide : std::uint8_t {
    Left,
    Right,
};

/// One column of a join's output, resolved to its source column and the name
/// it carries in the result.
struct JoinOutputColumn {
    JoinOutputSide side = JoinOutputSide::Left;
    std::size_t source_index = 0;  ///< Index into that side's column list.
    std::string name;              ///< Output name after collision renaming.

    auto operator==(const JoinOutputColumn&) const -> bool = default;
};

/// The single authority on a join's output column list and naming.
///
/// IR schema inference, the materialized interpreter, the chunked executor and
/// any adapter that reconstructs a join result must agree on this plan; each
/// one derives its own column list from here rather than reimplementing the
/// rules. The contract is:
///
///   - semi/anti joins return the left columns only;
///   - every left column is emitted, in input order, under its own name;
///   - a same-name equijoin key contributes one output column (the left one);
///   - differently named equijoin keys keep both native columns;
///   - any other right column whose name is already taken gains a `_right`
///     suffix, repeated until the name is free.
[[nodiscard]] auto plan_join_output(JoinKind kind, const std::vector<JoinKey>& keys,
                                    std::span<const std::string_view> left_names,
                                    std::span<const std::string_view> right_names)
    -> std::vector<JoinOutputColumn>;

}  // namespace ibex::ir
