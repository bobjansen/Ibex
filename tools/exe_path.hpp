// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#pragma once

#include <filesystem>
#include <string>
#include <vector>

#ifdef _WIN32
#include <windows.h>
#elifdef __APPLE__
#include <mach-o/dyld.h>
#else
#include <unistd.h>
#endif

namespace ibex::tools {

/// Directory containing the running executable. Plugins (*.so/*.dll and
/// their *.ibex stubs) are built and packaged alongside `ibex`/`ibex.exe`
/// (see IBEX_PLUGIN_OUTPUT_DIR in the top-level CMakeLists.txt), so this
/// doubles as a sane default plugin search path with no configuration.
inline auto executable_directory() -> std::filesystem::path {
    std::error_code ec;
#ifdef _WIN32
    std::vector<char> buf(MAX_PATH);
    DWORD len = GetModuleFileNameA(nullptr, buf.data(), static_cast<DWORD>(buf.size()));
    if (len == 0 || len >= buf.size()) {
        return {};
    }
    return std::filesystem::path(std::string(buf.data(), len)).parent_path();
#elifdef __APPLE__
    uint32_t size = 0;
    _NSGetExecutablePath(nullptr, &size);
    std::vector<char> buf(size);
    if (_NSGetExecutablePath(buf.data(), &size) != 0) {
        return {};
    }
    auto resolved = std::filesystem::canonical(std::filesystem::path(buf.data()), ec);
    return ec ? std::filesystem::path{} : resolved.parent_path();
#else
    auto resolved = std::filesystem::canonical("/proc/self/exe", ec);
    return ec ? std::filesystem::path{} : resolved.parent_path();
#endif
}

}  // namespace ibex::tools
