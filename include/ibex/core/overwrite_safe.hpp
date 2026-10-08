// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#pragma once

#include <type_traits>

namespace ibex::detail {

/// Opt-in for value types whose only non-trivial part is a `= 0` default
/// member (Date, Timestamp, Decimal). Specialized next to each type.
template <typename T>
struct zero_default_value : std::false_type {};

/// True when a presized buffer of `T` may be left unconstructed because every
/// slot is written before it is read (`Column::resize_for_overwrite`,
/// `NoInitAllocator`). Trivially default constructible types qualify, and so
/// do the opted-in zero-default value types as long as they are trivially
/// copyable and destructible: the bytes the overwrite stores are then the
/// whole object. Without the opt-in a Date or Timestamp column paid a full
/// serial zero fill on every presize, only to overwrite it.
template <typename T>
inline constexpr bool overwrite_safe_v =
    std::is_trivially_default_constructible_v<T> ||
    (zero_default_value<T>::value && std::is_trivially_copyable_v<T> &&
     std::is_trivially_destructible_v<T>);

}  // namespace ibex::detail
