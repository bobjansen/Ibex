// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

// Parser for the `options` string of `read_adbc`.
//
// Header-only and free of any ADBC dependency so the parsing rules can be
// tested in builds that do not have a driver manager installed.
//
// Grammar: entries are separated by unescaped `;` or newlines; each entry is
// `key=value`, split at the first unescaped `=`. A backslash escapes the next
// character: `\;`, `\=` and `\\` produce the literal character, `\n` and `\t`
// produce a newline and a tab. Unescaped whitespace around keys and values is
// trimmed. Values may be empty (`key=`).
//
// Key prefixes pick the ADBC object that receives the option:
//   db.<key>         AdbcDatabaseSetOption, before AdbcDatabaseInit
//   conn.<key>       AdbcConnectionSetOption, before AdbcConnectionInit
//   conn.post.<key>  AdbcConnectionSetOption, after AdbcConnectionInit (for
//                    options a driver only accepts on an open connection, such
//                    as SQLite extension loading)
//   stmt.<key>       AdbcStatementSetOption, before the query is set
//   entrypoint       driver entrypoint symbol
// A key without a prefix is a database option.

#pragma once

#include <algorithm>
#include <array>
#include <expected>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace ibex::adbc {

using OptionList = std::vector<std::pair<std::string, std::string>>;

struct ParsedOptions {
    std::string entrypoint;
    OptionList database;
    OptionList connection;
    OptionList connection_post;
    OptionList statement;
};

namespace detail {

inline auto is_space(char c) -> bool {
    return c == ' ' || c == '\t' || c == '\r' || c == '\n';
}

/// Accumulates one key or value, dropping unescaped leading and trailing
/// whitespace while keeping escaped whitespace.
struct Field {
    std::string text;
    std::size_t keep = 0;

    void append(char c, bool escaped) {
        if (text.empty() && !escaped && is_space(c)) {
            return;
        }
        text += c;
        if (escaped || !is_space(c)) {
            keep = text.size();
        }
    }

    auto finish() -> std::string {
        text.resize(keep);
        std::string out = std::move(text);
        text.clear();
        keep = 0;
        return out;
    }
};

inline auto valid_key(std::string_view key) -> bool {
    return !key.empty() && std::ranges::all_of(key, [](char c) {
        const auto u = static_cast<unsigned char>(c);
        return u > 0x20 && u != 0x7f;
    });
}

inline auto add_unique(OptionList& list, std::string key, std::string value,
                       std::string_view original) -> std::expected<void, std::string> {
    for (const auto& [existing, ignored] : list) {
        if (existing == key) {
            return std::unexpected("read_adbc options: duplicate option '" + std::string(original) +
                                   "'");
        }
    }
    list.emplace_back(std::move(key), std::move(value));
    return {};
}

inline auto route(ParsedOptions& parsed, std::string key, std::string value, bool& have_entrypoint)
    -> std::expected<void, std::string> {
    if (!valid_key(key)) {
        return std::unexpected("read_adbc options: invalid key '" + key +
                               "' (keys must be non-empty and contain no whitespace)");
    }
    if (key == "entrypoint") {
        if (have_entrypoint) {
            return std::unexpected("read_adbc options: duplicate option 'entrypoint'");
        }
        have_entrypoint = true;
        parsed.entrypoint = std::move(value);
        return {};
    }
    if (key == "driver" || key == "uri" || key == "db.driver" || key == "db.uri") {
        return std::unexpected("read_adbc options: '" + key +
                               "' must be passed as a positional argument of read_adbc");
    }

    struct Scope {
        std::string_view prefix;
        OptionList* list;
    };
    // `conn.post.` must be tested before `conn.`.
    const std::array<Scope, 4> scopes{{
        {.prefix = "conn.post.", .list = &parsed.connection_post},
        {.prefix = "conn.", .list = &parsed.connection},
        {.prefix = "stmt.", .list = &parsed.statement},
        {.prefix = "db.", .list = &parsed.database},
    }};
    for (const auto& scope : scopes) {
        if (key.starts_with(scope.prefix)) {
            std::string stripped = key.substr(scope.prefix.size());
            if (stripped.empty()) {
                return std::unexpected("read_adbc options: '" + key +
                                       "' is missing an option name");
            }
            return add_unique(*scope.list, std::move(stripped), std::move(value), key);
        }
    }
    const std::string original = key;
    return add_unique(parsed.database, std::move(key), std::move(value), original);
}

}  // namespace detail

inline auto parse_options(std::string_view spec) -> std::expected<ParsedOptions, std::string> {
    ParsedOptions parsed;
    bool have_entrypoint = false;
    detail::Field key;
    detail::Field value;
    bool in_value = false;
    bool saw_text = false;

    auto finish_entry = [&]() -> std::expected<void, std::string> {
        std::string k = key.finish();
        std::string v = value.finish();
        const bool had_value = in_value;
        const bool had_text = saw_text;
        in_value = false;
        saw_text = false;
        if (!had_value) {
            if (k.empty() && !had_text) {
                return {};  // blank entry, e.g. a trailing `;`
            }
            return std::unexpected("read_adbc options: entry '" + k +
                                   "' is not of the form key=value");
        }
        return detail::route(parsed, std::move(k), std::move(v), have_entrypoint);
    };

    for (std::size_t i = 0; i < spec.size(); ++i) {
        const char c = spec[i];
        detail::Field& field = in_value ? value : key;
        if (c == '\\') {
            if (i + 1 == spec.size()) {
                return std::unexpected("read_adbc options: trailing backslash");
            }
            const char next = spec[++i];
            char literal = next;
            if (next == 'n') {
                literal = '\n';
            } else if (next == 't') {
                literal = '\t';
            } else if (next != ';' && next != '=' && next != '\\') {
                return std::unexpected(std::string("read_adbc options: unknown escape '\\") + next +
                                       "'");
            }
            field.append(literal, true);
            saw_text = true;
            continue;
        }
        if (c == ';' || c == '\n') {
            if (auto ok = finish_entry(); !ok) {
                return std::unexpected(std::move(ok.error()));
            }
            continue;
        }
        if (c == '=' && !in_value) {
            in_value = true;
            continue;
        }
        field.append(c, false);
        if (!detail::is_space(c)) {
            saw_text = true;
        }
    }
    if (auto ok = finish_entry(); !ok) {
        return std::unexpected(std::move(ok.error()));
    }
    return parsed;
}

}  // namespace ibex::adbc
