// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#pragma once

#include <ibex/runtime/extern_registry.hpp>

#include <cstdint>
#include <robin_hood.h>
#include <string>
#include <vector>

namespace ibex::repl {

enum class PluginLoadStatus : std::uint8_t { Loaded, NotFound, LoadError };

struct PluginLoadResult {
    PluginLoadStatus status;
    std::string message;
};

/// Load the plugin `<stem>` (`.so`, `.dylib` on macOS, `.dll` on Windows) from
/// the first search path that holds it and run its `ibex_register` entry point
/// on `externs`. A stem already in `loaded_plugins` or registered in `externs`
/// counts as loaded. `NotFound` means no search path holds the file;
/// `LoadError` means a file exists but would not load, with the reason in
/// `message`.
auto try_load_plugin(const std::string& stem, const std::vector<std::string>& search_paths,
                     robin_hood::unordered_set<std::string>& loaded_plugins,
                     runtime::ExternRegistry& externs) -> PluginLoadResult;

}  // namespace ibex::repl
