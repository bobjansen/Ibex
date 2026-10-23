// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#pragma once

#include <optional>
#include <string>
#include <string_view>

namespace ibex::runtime {

/// Portable env-var mutation. `std::getenv` is enough to read one everywhere,
/// but writing needs `setenv`/`unsetenv` on POSIX vs `_putenv_s` on MSVC —
/// this pair hides that split for the few callers (tests, mostly) that need
/// to set or clear a variable rather than just read it.
void set_env(const std::string& name, const std::string& value);
void unset_env(const std::string& name);

/// The `GLIBC_TUNABLES` value to re-exec with for whole-heap huge pages, or
/// nullopt to leave the process as it is. Pure, so it can be tested: the inputs
/// are the current `GLIBC_TUNABLES` (null when unset), the `IBEX_NO_HUGEPAGES`
/// opt-out, the `IBEX_HUGEPAGES_REEXEC` marker a re-exec sets, and the text of
/// /sys/kernel/mm/transparent_hugepage/enabled (empty when unreadable).
[[nodiscard]] auto huge_pages_tunables(const char* tunables, const char* opt_out,
                                       const char* marker, std::string_view thp_enabled)
    -> std::optional<std::string>;

/// Re-exec this process once with `glibc.malloc.hugetlb=1` added to
/// `GLIBC_TUNABLES`, so glibc backs its heaps with transparent huge pages.
/// glibc reads tunables only at start-up, hence the re-exec. Call it first in
/// `main`, before any thread or allocation that matters. Returns without doing
/// anything when `huge_pages_tunables` says no, off Linux/glibc, or when the
/// exec fails; the process then simply runs on small pages.
///
/// Fresh-process PDS-H SF-10, 8 cores: -6.1% / -6.8% total, all 22 queries
/// faster (q21 -19%), from far fewer first-touch page faults. Measured
/// 2026-10-23; see `tune_allocator_once` in interpreter.cpp for the history.
void enable_huge_pages() noexcept;

}  // namespace ibex::runtime
