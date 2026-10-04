// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#pragma once

#include <atomic>

namespace ibex::runtime {

/// Cooperative interruption for long-running evaluations.
///
/// A signal handler (or another thread) calls `request_interrupt()`; long
/// running evaluation paths poll `interrupt_requested()` at safe boundaries
/// (per IR node, per chunk, per statement) and unwind with
/// `interrupt_message()` through the usual `std::expected` error channel.
///
/// The flag is process-wide and sticky: checks observe it without consuming
/// it so every layer unwinds, and the driver (the REPL loop) clears it
/// before starting the next evaluation.

namespace detail {
// Mutable by design: the whole point is a signal handler can set it.
inline std::atomic<bool> interrupt_flag{false};

// The flag this module reads and sets. A plugin is loaded RTLD_LOCAL with its
// own copy of `interrupt_flag`, which the host's Ctrl+C handler never sets, so
// `ExternRegistry` points the plugin at the host's flag when the plugin
// registers its functions (see `bind_interrupt_flag`).
inline std::atomic<std::atomic<bool>*> active_interrupt_flag{&interrupt_flag};

[[nodiscard]] inline auto active_flag() noexcept -> std::atomic<bool>& {
    return *active_interrupt_flag.load(std::memory_order_relaxed);
}
}  // namespace detail

/// Make this module use `flag` (the host's) instead of its own. A no-op in the
/// host, where `flag` is its own.
inline void bind_interrupt_flag(std::atomic<bool>* flag) noexcept {
    detail::active_interrupt_flag.store(flag, std::memory_order_relaxed);
}

/// The flag this module uses, for passing to `bind_interrupt_flag`.
[[nodiscard]] inline auto interrupt_flag_address() noexcept -> std::atomic<bool>* {
    return detail::active_interrupt_flag.load(std::memory_order_relaxed);
}

/// Async-signal-safe: lock-free relaxed loads and a store.
inline void request_interrupt() noexcept {
    detail::active_flag().store(true, std::memory_order_relaxed);
}

inline void clear_interrupt() noexcept {
    detail::active_flag().store(false, std::memory_order_relaxed);
}

[[nodiscard]] inline auto interrupt_requested() noexcept -> bool {
    return detail::active_flag().load(std::memory_order_relaxed);
}

/// Returns whether an interrupt was pending and clears it in one step.
[[nodiscard]] inline auto consume_interrupt() noexcept -> bool {
    return detail::active_flag().exchange(false, std::memory_order_relaxed);
}

/// Error string carried through `std::expected` when evaluation is
/// interrupted. Kept exact so callers can distinguish interruption from
/// ordinary failures.
[[nodiscard]] inline auto interrupt_message() -> const char* {
    return "interrupted";
}

}  // namespace ibex::runtime
