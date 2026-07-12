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
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
inline std::atomic<bool> interrupt_flag{false};
}  // namespace detail

/// Async-signal-safe: a lock-free relaxed store.
inline void request_interrupt() noexcept {
    detail::interrupt_flag.store(true, std::memory_order_relaxed);
}

inline void clear_interrupt() noexcept {
    detail::interrupt_flag.store(false, std::memory_order_relaxed);
}

[[nodiscard]] inline auto interrupt_requested() noexcept -> bool {
    return detail::interrupt_flag.load(std::memory_order_relaxed);
}

/// Returns whether an interrupt was pending and clears it in one step.
[[nodiscard]] inline auto consume_interrupt() noexcept -> bool {
    return detail::interrupt_flag.exchange(false, std::memory_order_relaxed);
}

/// Error string carried through `std::expected` when evaluation is
/// interrupted. Kept exact so callers can distinguish interruption from
/// ordinary failures.
[[nodiscard]] inline auto interrupt_message() -> const char* {
    return "interrupted";
}

}  // namespace ibex::runtime
