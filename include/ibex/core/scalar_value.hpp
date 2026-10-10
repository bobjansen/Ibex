// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#pragma once

#include <ibex/core/decimal.hpp>
#include <ibex/core/time.hpp>

#include <cstddef>
#include <cstdint>
#include <functional>
#include <optional>
#include <string>
#include <variant>

namespace ibex {

/// Which value a scalar is: one per value alternative of `ScalarValue`. Also
/// how an extern declares a scalar return (`ExternRegistry::register_scalar`).
enum class ScalarKind : std::uint8_t {
    Int,
    Double,
    Bool,
    String,
    Date,
    Timestamp,
    Decimal,
};

/// The null alternative of `ScalarValue`. A scalar carries its type even when
/// it is null, exactly as a column cell does (SPEC.md Section 6.7), so the null
/// records the kind it stands in for -- and, for a Decimal, the precision and
/// scale. There is no untyped null: a bare `NULL` is typed where it is written
/// (an annotation, a parameter, a `case` arm's siblings, a cast) or rejected.
/// Hence no default constructor, and no default-constructed ScalarValue.
struct NullScalar {
    explicit constexpr NullScalar(ScalarKind k, DecimalType d = {}) noexcept
        : kind(k), decimal(d) {}

    ScalarKind kind;
    /// Meaningful only when `kind` is Decimal.
    DecimalType decimal;

    [[nodiscard]] auto operator==(const NullScalar&) const -> bool = default;
};

/// A scalar value, or a null of a recorded type. The one definition every layer
/// uses: the parser (compile-time scalar bindings), the runtime and codegen
/// alias it.
using ScalarValue = std::variant<NullScalar, std::int64_t, double, bool, std::string, Date,
                                 Timestamp, DecimalValue>;

}  // namespace ibex

/// Consistent with `operator==`: nulls of one kind hash alike.
template <>
struct std::hash<ibex::NullScalar> {
    auto operator()(const ibex::NullScalar& n) const noexcept -> std::size_t {
        const auto kind = static_cast<std::uint32_t>(n.kind);
        return std::hash<std::uint32_t>{}((kind << 16U) |
                                          (static_cast<std::uint32_t>(n.decimal.precision) << 8U) |
                                          n.decimal.scale);
    }
};
