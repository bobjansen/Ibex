#pragma once

#include <ibex/ir/node.hpp>

#include <cstddef>
#include <expected>
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
///   - a same-name equijoin key contributes one output column (the left one),
///     so it is never a collision;
///   - differently named equijoin keys keep both native columns;
///   - any other name held by both inputs is a collision. Without a suffix
///     policy that is an error naming the column and both sides; with one,
///     each side takes its suffix, and a suffixed name that still collides is
///     an error in turn.
///
/// Returning an error rather than inventing a name is the point: repeated
/// suffixing until a name was free (`val_right_right`) hid accidental
/// collisions behind the same mechanism that served deliberate ones.
[[nodiscard]] auto plan_join_output(JoinKind kind, const std::vector<JoinKey>& keys,
                                    std::span<const std::string_view> left_names,
                                    std::span<const std::string_view> right_names,
                                    const JoinSuffixPolicy& suffix = {})
    -> std::expected<std::vector<JoinOutputColumn>, std::string>;

}  // namespace ibex::ir
