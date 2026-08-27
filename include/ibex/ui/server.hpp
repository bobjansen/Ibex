// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#pragma once

#include <ibex/repl/repl.hpp>
#include <ibex/runtime/extern_registry.hpp>

#include <cstdint>
#include <filesystem>

namespace ibex::ui {

struct ServerConfig {
    /// Bind address is deliberately fixed to loopback by the implementation.
    std::uint16_t port = 8765;
    std::filesystem::path web_root;
    /// Directory the browser workbench may access for user data.  When empty,
    /// the server uses its launch directory.
    std::filesystem::path data_directory;
    repl::ReplConfig repl;
};

/// Run the localhost-only browser UI server until the process is interrupted.
/// Returns non-zero if the listener cannot be started or static assets are missing.
auto run_server(const ServerConfig& config, runtime::ExternRegistry& registry) -> int;

}  // namespace ibex::ui
