// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#pragma once

#include <ibex/core/decimal.hpp>

#include <cstddef>
#include <cstdint>
#include <functional>

namespace ibex {

/// Which value a scalar is, or stands in for when null. Mirrors the value
/// alternatives of `ScalarValue` one for one. `Unknown` is a null nothing has
/// typed yet: a bare `NULL` expression.
enum class ScalarKind : std::uint8_t {
    Unknown,
    Int64,
    Float64,
    Bool,
    String,
    Date,
    Timestamp,
    Decimal,
};

/// The null alternative of `ScalarValue`. A scalar binding carries its type
/// even when it is null, exactly as a column cell does (SPEC.md Section 6.7),
/// so the null records the kind it stands in for -- and, for a Decimal, the
/// precision and scale. A default-constructed `NullScalar` (and so a
/// default-constructed `ScalarValue`) is an untyped null.
struct NullScalar {
    ScalarKind kind = ScalarKind::Unknown;
    /// Meaningful only when `kind` is Decimal.
    DecimalType decimal{};

    [[nodiscard]] auto operator==(const NullScalar&) const -> bool = default;
};

}  // namespace ibex

/// Consistent with `operator==`: nulls of one kind hash alike.
template <>
struct std::hash<ibex::NullScalar> {
    auto operator()(const ibex::NullScalar& n) const noexcept -> std::size_t {
        return std::hash<std::uint32_t>{}((static_cast<std::uint32_t>(n.kind) << 16U) |
                                          (static_cast<std::uint32_t>(n.decimal.precision) << 8U) |
                                          n.decimal.scale);
    }
};
