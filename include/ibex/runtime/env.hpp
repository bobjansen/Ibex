// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#pragma once

#include <string>

namespace ibex::runtime {

/// Portable env-var mutation. `std::getenv` is enough to read one everywhere,
/// but writing needs `setenv`/`unsetenv` on POSIX vs `_putenv_s` on MSVC —
/// this pair hides that split for the few callers (tests, mostly) that need
/// to set or clear a variable rather than just read it.
void set_env(const std::string& name, const std::string& value);
void unset_env(const std::string& name);

}  // namespace ibex::runtime
