// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#pragma once

#include <ibex/ir/node.hpp>
#include <ibex/ir/schema.hpp>

namespace ibex::ir {

/// Drop `order` / top-k keys that are a proved-constant null flag.
///
/// A frontend that wants nulls FIRST stages a flag and sorts on it ahead of
/// the value, because native `order` puts nulls last:
///
///     [update { f = is_null(x) }][order { f desc, x desc }]
///
/// The Polars adapter writes exactly that for `sort(..., nulls_last=False)`,
/// Polars' default. When `x` is proved never null the flag is `false` on every
/// row, so it cannot reorder anything; sorting on it costs a second key in the
/// heap or radix pass and its column (adapter PDS-H q10: ~15 ms at 8 cores).
///
/// Nullability is the same proof `infer_schema` carries (`expr_nullability`).
/// The flag's definition is found by walking down from the sort node through
/// `Update`, `Project` and `Filter` only, taking the first update that assigns
/// the key's name. A key is never dropped from a node whose other keys would
/// all go too. Declines silently whenever anything is unproven.
[[nodiscard]] auto drop_constant_null_flag_sort_keys(NodePtr root, const SourceSchemas& sources)
    -> NodePtr;

}  // namespace ibex::ir
