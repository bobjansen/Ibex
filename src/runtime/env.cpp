// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#include <ibex/runtime/env.hpp>

#include <cstdlib>
#include <string>

#ifndef _WIN32
// POSIX declares setenv/unsetenv here; <cstdlib> does not guarantee them.
// NOLINTNEXTLINE(modernize-deprecated-headers)
#include <stdlib.h>
#endif

namespace ibex::runtime {

void set_env(const std::string& name, const std::string& value) {
#ifdef _WIN32
    _putenv_s(name.c_str(), value.c_str());
#else
    ::setenv(name.c_str(), value.c_str(), 1);  // NOLINT(concurrency-mt-unsafe)
#endif
}

void unset_env(const std::string& name) {
#ifdef _WIN32
    _putenv_s(name.c_str(), "");
#else
    ::unsetenv(name.c_str());  // NOLINT(concurrency-mt-unsafe)
#endif
}

}  // namespace ibex::runtime
