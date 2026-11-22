// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#pragma once

// A deliberately small formatting surface backed by C++23. It has no external
// dependency.

#include <atomic>
#include <cstdio>
#include <format>
#include <mutex>
#include <ostream>
#include <string>
#include <string_view>
#include <utility>

namespace ibex::formatting {

template <typename... Args>
using format_string = std::format_string<Args...>;

template <typename... Args>
auto format(format_string<Args...> pattern, Args&&... args) -> std::string {
    return std::format(pattern, std::forward<Args>(args)...);
}

/// Collects what `print` would have written to stdout while it is installed
/// with `install_stdout_sink` (`ReplSession::execute` captures this way).
///
/// Capture works on the stream, never on file descriptor 1: on Windows,
/// `_dup2` onto fd 1 closes the console handle the process started with, and
/// hosts that cached that handle (IPython's prompt_toolkit) fail on their next
/// console call with no way to restore the original handle value.
class StdoutSink {
   public:
    void append(std::string_view text) {
        const std::scoped_lock lock(mutex_);
        text_.append(text);
    }
    [[nodiscard]] auto take() -> std::string {
        const std::scoped_lock lock(mutex_);
        return std::exchange(text_, {});
    }

   private:
    std::mutex mutex_;
    std::string text_;
};

namespace detail {
inline auto stdout_sink() -> std::atomic<StdoutSink*>& {
    static std::atomic<StdoutSink*> sink{nullptr};
    return sink;
}
}  // namespace detail

/// Routes stdout writes made through `print` to `sink` (nullptr restores
/// stdout) and returns the previously installed sink. Process-wide: worker
/// threads printing during a capture land in the same sink.
inline auto install_stdout_sink(StdoutSink* sink) -> StdoutSink* {
    return detail::stdout_sink().exchange(sink, std::memory_order_acq_rel);
}

template <typename... Args>
void print(std::FILE* stream, format_string<Args...> pattern, Args&&... args) {
    // A named type, not `auto`: deduced from the pack, `text` would make the
    // append below a dependent call, which misc-const-correctness cannot see
    // into, so it asks for a const sink that `append` cannot take.
    const std::string text = std::format(pattern, std::forward<Args>(args)...);
    if (stream == stdout) {
        if (auto* sink = detail::stdout_sink().load(std::memory_order_acquire)) {
            sink->append(text);
            return;
        }
    }
    (void)std::fputs(text.c_str(), stream);
}

template <typename... Args>
void print(format_string<Args...> pattern, Args&&... args) {
    // Qualified so argument-dependent lookup cannot pull in `std::print`: the
    // pattern's type is a `std` alias, and libc++'s <print> (transitively
    // included by other standard headers) otherwise makes this call ambiguous.
    ibex::formatting::print(stdout, pattern, std::forward<Args>(args)...);
}

// Formatting into streams explicitly keeps this surface independent from the
// still uneven C++23 std::print implementation across standard libraries.
template <typename... Args>
void print(std::ostream& stream, format_string<Args...> pattern, Args&&... args) {
    stream << std::format(pattern, std::forward<Args>(args)...);
}

}  // namespace ibex::formatting
