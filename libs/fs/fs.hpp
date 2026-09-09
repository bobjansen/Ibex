// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#pragma once

// Ibex filesystem library — directory listing as a DataFrame.
//
//   import "fs";
//   let files = list_files("data/csv");                 // everything, one level
//   let csvs  = list_files("data/csv", "*.csv");        // glob on the file name
//   let all   = list_files("data", "*.csv", true);      // recurse into subdirs
//
// Returns one row per entry, sorted by `path`:
//   path        : String  — dir joined with the entry name
//   name        : String  — file name including any extension
//   stem        : String  — file name without the final extension
//   ext         : String  — final extension without the dot ("" if none)
//   size_bytes  : Int64    — file_size() for regular files, 0 otherwise
//   is_dir      : Bool
//
// The pattern is a shell-style glob (`*`, `?`, `[set]`) matched against `name`
// only. A missing or non-directory `dir` is a runtime error.

#include <ibex/core/column.hpp>
#include <ibex/runtime/interpreter.hpp>

#include <algorithm>
#include <cstdint>
#include <filesystem>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <vector>

namespace ibex::fs {

namespace detail {

/// Shell-style glob match: `*` (any run), `?` (one char), `[abc]` / `[a-z]` /
/// `[!..]` character classes. Anchored at both ends. Hand-rolled because POSIX
/// `fnmatch` is not available on the Windows target.
[[nodiscard]] inline auto wildcard_match(std::string_view text, std::string_view pattern) -> bool {
    std::size_t ti = 0;
    std::size_t pi = 0;
    std::size_t star_pi = std::string_view::npos;
    std::size_t star_ti = 0;

    while (ti < text.size()) {
        if (pi < pattern.size() && pattern[pi] == '?') {
            ++ti;
            ++pi;
            continue;
        }
        if (pi < pattern.size() && pattern[pi] == '*') {
            star_pi = pi++;
            star_ti = ti;
            continue;
        }
        if (pi < pattern.size() && pattern[pi] == '[') {
            const std::size_t close = pattern.find(']', pi + 2);
            if (close != std::string_view::npos) {
                std::string_view set = pattern.substr(pi + 1, close - pi - 1);
                bool negate = false;
                if (!set.empty() && (set.front() == '!' || set.front() == '^')) {
                    negate = true;
                    set.remove_prefix(1);
                }
                bool matched = false;
                for (std::size_t i = 0; i < set.size(); ++i) {
                    if (i + 2 < set.size() && set[i + 1] == '-') {
                        if (text[ti] >= set[i] && text[ti] <= set[i + 2]) {
                            matched = true;
                        }
                        i += 2;
                    } else if (text[ti] == set[i]) {
                        matched = true;
                    }
                }
                if (matched != negate) {
                    ++ti;
                    pi = close + 1;
                    continue;
                }
            }
            // Fall through to the backtrack path on a non-match / malformed set.
        } else if (pi < pattern.size() && pattern[pi] == text[ti]) {
            ++ti;
            ++pi;
            continue;
        }

        if (star_pi != std::string_view::npos) {
            pi = star_pi + 1;
            ti = ++star_ti;
            continue;
        }
        return false;
    }
    while (pi < pattern.size() && pattern[pi] == '*') {
        ++pi;
    }
    return pi == pattern.size();
}

struct Entry {
    std::string path;
    std::string name;
    std::string stem;
    std::string ext;
    std::int64_t size_bytes = 0;
    bool is_dir = false;
};

[[nodiscard]] inline auto make_entry(const std::filesystem::directory_entry& de) -> Entry {
    std::error_code ec;
    Entry e;
    e.path = de.path().generic_string();
    e.name = de.path().filename().string();
    e.stem = de.path().stem().string();
    const std::string extension = de.path().extension().string();  // includes the leading dot
    e.ext = extension.empty() ? std::string{} : extension.substr(1);
    e.is_dir = de.is_directory(ec);
    if (!e.is_dir) {
        const auto sz = de.file_size(ec);
        e.size_bytes = ec ? 0 : static_cast<std::int64_t>(sz);
    }
    return e;
}

}  // namespace detail

[[nodiscard]] inline auto list_files(const std::string& dir, const std::string& pattern,
                                     bool recursive) -> ibex::runtime::Table {
    namespace stdfs = std::filesystem;
    std::error_code ec;
    if (!stdfs::exists(dir, ec) || ec) {
        throw std::runtime_error("list_files: directory not found: '" + dir + "'");
    }
    if (!stdfs::is_directory(dir, ec) || ec) {
        throw std::runtime_error("list_files: not a directory: '" + dir + "'");
    }

    std::vector<detail::Entry> entries;
    const auto keep = [&](const stdfs::directory_entry& de) {
        if (detail::wildcard_match(de.path().filename().string(), pattern)) {
            entries.push_back(detail::make_entry(de));
        }
    };
    if (recursive) {
        for (auto it = stdfs::recursive_directory_iterator(
                 dir, stdfs::directory_options::skip_permission_denied, ec);
             !ec && it != stdfs::recursive_directory_iterator(); it.increment(ec)) {
            keep(*it);
        }
    } else {
        for (auto it = stdfs::directory_iterator(
                 dir, stdfs::directory_options::skip_permission_denied, ec);
             !ec && it != stdfs::directory_iterator(); it.increment(ec)) {
            keep(*it);
        }
    }
    std::ranges::sort(
        entries, [](const detail::Entry& a, const detail::Entry& b) { return a.path < b.path; });

    ibex::Column<std::string> path;
    ibex::Column<std::string> name;
    ibex::Column<std::string> stem;
    ibex::Column<std::string> ext;
    ibex::Column<std::int64_t> size_bytes;
    ibex::Column<bool> is_dir;
    path.reserve(entries.size());
    name.reserve(entries.size());
    stem.reserve(entries.size());
    ext.reserve(entries.size());
    size_bytes.reserve(entries.size());
    is_dir.reserve(entries.size());
    for (const auto& e : entries) {
        path.push_back(e.path);
        name.push_back(e.name);
        stem.push_back(e.stem);
        ext.push_back(e.ext);
        size_bytes.push_back(e.size_bytes);
        is_dir.push_back(e.is_dir);
    }

    ibex::runtime::Table table;
    table.add_column("path", std::move(path));
    table.add_column("name", std::move(name));
    table.add_column("stem", std::move(stem));
    table.add_column("ext", std::move(ext));
    table.add_column("size_bytes", std::move(size_bytes));
    table.add_column("is_dir", std::move(is_dir));
    return table;
}

}  // namespace ibex::fs

// Public entry point at global scope so the transpiler's bare `list_files(...)`
// call in generated C++ resolves (matching csv.hpp's `read_csv`).
[[nodiscard]] inline auto list_files(const std::string& dir, const std::string& pattern = "*",
                                     bool recursive = false) -> ibex::runtime::Table {
    return ibex::fs::list_files(dir, pattern, recursive);
}
