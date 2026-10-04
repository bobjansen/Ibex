// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#pragma once

#include <atomic>
#include <cstdio>
#include <string_view>

namespace ibex::runtime {

/// Warnings that have no caller to return an error to: chiefly cleanup that
/// fails in a destructor, such as a connection closed because its last binding
/// went away. They go to one process-wide sink, stderr by default.
///
/// A plugin is loaded RTLD_LOCAL with its own copy of the sink, so
/// `ExternRegistry` points the plugin at the host's when the plugin registers
/// its functions, as for the interrupt flag (interrupt.hpp).

using WarningSink = void (*)(std::string_view message) noexcept;

namespace detail {
inline void warn_to_stderr(std::string_view message) noexcept {
    // Nowhere to report a failed write to stderr.
    (void)std::fputs("warning: ", stderr);
    (void)std::fwrite(message.data(), 1, message.size(), stderr);
    (void)std::fputc('\n', stderr);
}

// Mutable by design: the host may redirect it, and plugins bind to the host's.
inline std::atomic<std::atomic<WarningSink>*> active_warning_sink{nullptr};
inline std::atomic<WarningSink> warning_sink{&warn_to_stderr};

[[nodiscard]] inline auto active_sink() noexcept -> std::atomic<WarningSink>& {
    auto* bound = active_warning_sink.load(std::memory_order_acquire);
    return bound != nullptr ? *bound : warning_sink;
}
}  // namespace detail

/// Report `message` as a warning. Never throws.
inline void warn(std::string_view message) noexcept {
    detail::active_sink().load(std::memory_order_acquire)(message);
}

/// Send warnings to `sink`; returns the previous sink. The host's choice holds
/// for plugins too.
inline auto set_warning_sink(WarningSink sink) noexcept -> WarningSink {
    return detail::active_sink().exchange(sink, std::memory_order_acq_rel);
}

/// This module's sink slot, for `bind_warning_sink` in another module.
[[nodiscard]] inline auto warning_sink_address() noexcept -> std::atomic<WarningSink>* {
    return &detail::active_sink();
}

/// Make this module use `slot` (the host's) instead of its own. A no-op in
/// the host, where `slot` is its own.
inline void bind_warning_sink(std::atomic<WarningSink>* slot) noexcept {
    detail::active_warning_sink.store(slot, std::memory_order_release);
}

}  // namespace ibex::runtime
