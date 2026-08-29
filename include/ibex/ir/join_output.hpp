// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#pragma once

#include <ibex/ir/node.hpp>

#include <cstddef>
#include <cstdint>
#include <expected>
#include <optional>
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
///
/// The last two fields are provenance the planner knows and no consumer should
/// reconstruct. Recovering "is this an equijoin key" or "did the two sides fold
/// together" by matching output names against `JoinKey`s means restating the
/// planner's rules somewhere it cannot see them -- which is the drift this type
/// exists to prevent, and which the schema pass and the executor were each
/// doing separately.
struct JoinOutputColumn {
    JoinOutputSide side = JoinOutputSide::Left;
    std::size_t source_index = 0;  ///< Index into that side's column list.
    std::string name;              ///< Output name after collision renaming.

    /// True when this column is one side of an equijoin key.
    bool is_key = false;

    /// When a same-name equijoin key collapses both inputs' columns into this
    /// single output column, the *other* input's index for it; `nullopt` when
    /// nothing folded. A row missing this column's own side draws its value
    /// from there, so the column's guarantees are the weaker of the two.
    std::optional<std::size_t> folded_peer_index;

    auto operator==(const JoinOutputColumn&) const -> bool = default;
};

/// One equijoin key resolved against the ordered physical columns of both
/// inputs. Names are a logical/schema concern; build and probe kernels consume
/// these stable positions.
struct JoinKeyColumns {
    std::size_t left_index = 0;
    std::size_t right_index = 0;

    auto operator==(const JoinKeyColumns&) const -> bool = default;
};

/// The complete column-name resolution for one join: positional key bindings
/// plus the authoritative output gather/rename plan. This is the join analogue
/// of `ColumnNameMap`: consumers resolve names once, then share this value
/// instead of independently looking them up or reconstructing output names.
struct JoinColumnMapping {
    std::vector<JoinKeyColumns> keys;
    std::vector<JoinOutputColumn> output;

    auto operator==(const JoinColumnMapping&) const -> bool = default;
};

/// The single authority on a join's output column list and naming.
///
/// IR schema inference, the materialized interpreter, the chunked executor and
/// any adapter that reconstructs a join result must agree on this plan; each
/// one derives its own column list from here rather than reimplementing the
/// rules. The contract is:
///
///   - semi/anti joins return the left columns only;
///   - every left column is emitted in input order; a folded key may assign
///     its surviving left column an explicit logical output label;
///   - a folded equijoin key contributes one output column (the left one), so
///     it is never a collision; same-name keys are always folded;
///   - differently named, non-folded keys keep both native columns;
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

/// Resolve every textual join key to its input position and compute the output
/// plan through `plan_join_output`. Known schemas call this during physical
/// planning; lazy/unknown schemas call it once when their concrete columns
/// first reach the build/probe barrier.
[[nodiscard]] auto resolve_join_columns(JoinKind kind, const std::vector<JoinKey>& keys,
                                        std::span<const std::string_view> left_names,
                                        std::span<const std::string_view> right_names,
                                        const JoinSuffixPolicy& suffix = {})
    -> std::expected<JoinColumnMapping, std::string>;

}  // namespace ibex::ir
