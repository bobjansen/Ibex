// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>

namespace ibex {

/// An interned IANA time zone name, e.g. "America/New_York".
///
/// Interned rather than stored inline for two reasons. The obvious one is size:
/// a zone rides on every `Column<T>`, and `std::optional<std::string>` is 40
/// bytes against this one's 4. The less obvious one is allocation --
/// "America/New_York" is 16 characters and libstdc++'s small-string capacity is
/// 15, so an inline zone name heap-allocates on *every copy of the column*, and
/// chunked execution copies columns per chunk.
///
/// Zones are few, repeated across every column of a table, and compared far more
/// often than they are created, so an id is the right shape: copies are trivial,
/// comparison is an integer compare, and a zone-aware operation resolves the id
/// to a `std::chrono::time_zone` once per query rather than once per column.
enum class ZoneId : std::uint16_t;

/// Intern `name`, returning its id. Repeated names return the same id.
[[nodiscard]] auto intern_zone(std::string_view name) -> ZoneId;

/// The name behind an id. The reference is stable for the process lifetime.
[[nodiscard]] auto zone_name(ZoneId id) -> const std::string&;

/// Whether `name` is available in the host's IANA time-zone database.
[[nodiscard]] auto is_known_zone(std::string_view name) -> bool;

#if !defined(IBEX_HAS_STD_CHRONO_TIME_ZONES)
/// Convert a wall-clock Timestamp in `zone` to its instant. A nonexistent
/// local time returns nullopt; an ambiguous one selects its earlier instant.
[[nodiscard]] auto local_time_to_sys(std::string_view zone, std::int64_t nanos)
    -> std::optional<std::int64_t>;

/// Start of the local `duration_nanos` bucket containing `nanos` in `zone`.
[[nodiscard]] auto local_bucket_start(std::string_view zone, std::int64_t nanos,
                                      std::int64_t duration_nanos) -> std::int64_t;
#endif

/// What a column's VALUES mean, as opposed to where its rows sit.
///
/// The distinction decides where this lives. Table metadata (ordering, time
/// index, grouping) describes the row layout, so every operator has to re-derive
/// it -- a filter's output is ordered differently from its input's. Column
/// metadata describes the data itself: a zone says which wall clock these
/// instants render on, and that stays true wherever the column goes. So it
/// travels WITH the column rather than being restated, and the helpers that
/// build a column from another one carry it automatically.
struct ColumnMeta {
    /// Set only on Timestamp columns. `nullopt` means UTC, which is also what a
    /// producer that supplies no zone means (SPEC 2.4).
    std::optional<ZoneId> zone;

    [[nodiscard]] auto operator==(const ColumnMeta&) const -> bool = default;
};

}  // namespace ibex
