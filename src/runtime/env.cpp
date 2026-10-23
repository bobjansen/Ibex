// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#include <ibex/runtime/env.hpp>

#include <cstdlib>
#include <fstream>
#include <ios>
#include <iterator>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#ifndef _WIN32
// POSIX declares setenv/unsetenv here; <cstdlib> does not guarantee them.
// NOLINTNEXTLINE(modernize-deprecated-headers)
#include <stdlib.h>
#endif

#if defined(__linux__) && defined(__GLIBC__)
#include <unistd.h>
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

auto huge_pages_tunables(const char* tunables, const char* opt_out, const char* marker,
                         std::string_view thp_enabled) -> std::optional<std::string> {
    constexpr std::string_view kTunable = "glibc.malloc.hugetlb";
    if (opt_out != nullptr && opt_out[0] != '\0' && std::string_view{opt_out} != "0") {
        return std::nullopt;
    }
    // Already re-executed once: never loop, whatever the exec did.
    if (marker != nullptr) {
        return std::nullopt;
    }
    // No transparent huge pages to ask for (unreadable, or switched off).
    if (thp_enabled.empty() || thp_enabled.find("[never]") != std::string_view::npos) {
        return std::nullopt;
    }
    const std::string_view current = tunables == nullptr ? std::string_view{} : tunables;
    // The caller chose a value, on or off: theirs wins.
    if (current.find(kTunable) != std::string_view::npos) {
        return std::nullopt;
    }
    std::string out{current};
    if (!out.empty()) {
        out += ':';
    }
    out += kTunable;
    out += "=1";
    return out;
}

void enable_huge_pages() noexcept {
#if defined(__linux__) && defined(__GLIBC__)
    try {
        std::string thp;
        if (std::ifstream in{"/sys/kernel/mm/transparent_hugepage/enabled"}; in) {
            std::getline(in, thp);
        }
        const char* tunables = std::getenv("GLIBC_TUNABLES");  // NOLINT(concurrency-mt-unsafe)
        const auto next = huge_pages_tunables(
            tunables, std::getenv("IBEX_NO_HUGEPAGES"),     // NOLINT(concurrency-mt-unsafe)
            std::getenv("IBEX_HUGEPAGES_REEXEC"), thp);  // NOLINT(concurrency-mt-unsafe)
        if (!next.has_value()) {
            return;
        }
        // The same arguments, read back from the kernel: the generated
        // programs' `main()` takes no argv.
        std::ifstream cmdline{"/proc/self/cmdline", std::ios::binary};
        const std::string raw{std::istreambuf_iterator<char>(cmdline),
                              std::istreambuf_iterator<char>()};
        std::vector<std::string> args;
        for (std::size_t start = 0; start < raw.size();) {
            const std::size_t end = raw.find('\0', start);
            args.push_back(raw.substr(start, end - start));
            if (end == std::string::npos) {
                break;
            }
            start = end + 1;
        }
        if (args.empty()) {
            return;
        }
        std::vector<char*> argv;
        argv.reserve(args.size() + 1);
        for (auto& arg : args) {
            argv.push_back(arg.data());
        }
        argv.push_back(nullptr);

        const std::optional<std::string> previous =
            tunables == nullptr ? std::nullopt : std::optional<std::string>{tunables};
        set_env("GLIBC_TUNABLES", *next);
        set_env("IBEX_HUGEPAGES_REEXEC", "1");
        ::execv("/proc/self/exe", argv.data());
        // Still here: the exec failed. Run on as we are, and do not hand the
        // tunable to child processes this one did not get itself.
        if (previous.has_value()) {
            set_env("GLIBC_TUNABLES", *previous);
        } else {
            unset_env("GLIBC_TUNABLES");
        }
        unset_env("IBEX_HUGEPAGES_REEXEC");
    } catch (...) {  // NOLINT(bugprone-empty-catch): an optimization, never a failure
    }
#endif
}

}  // namespace ibex::runtime
