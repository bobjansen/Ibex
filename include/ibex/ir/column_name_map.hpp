// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#pragma once

#include <ibex/ir/node.hpp>

#include <expected>
#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace ibex::ir {

/// One authoritative interpretation of a RenameNode's mappings.
///
/// Every entry is applied simultaneously: `output_name` maps names from the
/// input schema to the output schema, while `input_name` performs the inverse
/// translation used when an operator moves below a Rename.  Keeping both
/// directions here prevents schema propagation, demand analysis, optimizer
/// rewrites, and execution from each inventing subtly different semantics.
class ColumnNameMap {
   public:
    explicit ColumnNameMap(std::span<const RenameSpec> renames);

    [[nodiscard]] auto output_name(std::string_view input) const -> std::string;
    [[nodiscard]] auto input_name(std::string_view output) const -> std::string;

    /// Validate the mapping itself: source and target names must each be
    /// unique. Identity entries are permitted and canonicalization removes
    /// them later.
    [[nodiscard]] auto validate() const -> std::expected<void, std::string>;

    /// Validate against a concrete input schema. Every source must exist and a
    /// non-identity target must not already name an untouched input column.
    [[nodiscard]] auto validate_input(std::span<const std::string_view> input_names) const
        -> std::expected<void, std::string>;

    /// Rewrite every non-lexical ColumnRef in an expression from output names
    /// to the corresponding input names.
    void remap_expr_to_input(Expr& expr) const;

    /// Compose `outer(inner(x))` into one simultaneous mapping.
    [[nodiscard]] static auto compose(std::span<const RenameSpec> outer,
                                      std::span<const RenameSpec> inner) -> std::vector<RenameSpec>;

   private:
    std::vector<RenameSpec> renames_;
};

}  // namespace ibex::ir
