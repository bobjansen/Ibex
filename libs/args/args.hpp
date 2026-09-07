// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#pragma once
// Ibex `parse_args` library -- declarative command-line argument parsing.
//
//   import "args";
//   let spec = "
//     threads (t)  : int    = 4      # worker threads
//     verbose (v)  : flag             # extra logging
//     out          : string?          # output path; stdout if absent
//     input        : positional+      # one or more input files
//   ";
//   let args   = parse_args(spec);
//   let n      = Int64(scalar(args[filter name == \"threads\", select { value }]));
//   let files  = args[filter kind == \"positional\", select { path = value }];
//
// `parse_args(spec)` returns one row per argument, with a fixed schema:
//
//   kind:  String   -- "option" | "flag" | "positional"
//   name:  String   -- canonical option name; the positional's declared name
//   index: Int64    -- 0-based occurrence within (kind, name)
//   value: String   -- the string value; a flag is "true" / "false"
//
// The value column is always String -- the caller casts (`Int64(...)`, `Date(...)`).
// The spec drives parsing (aliases, which tokens take a value, defaults,
// required checks, lexical type validation); it does not shape the schema.
//
// argv comes from the `IBEX_ARGS` environment variable (one argument per line).
// The two-argument form `parse_args(spec, argv)` takes an explicit newline- or
// space-separated argv string instead -- handy for tests and embedding.

#include <ibex/core/column.hpp>
#include <ibex/runtime/interpreter.hpp>

#include <algorithm>
#include <cctype>
#include <cstdint>
#include <cstdlib>
#include <optional>
#include <robin_hood.h>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

namespace ibex::args {

enum class ArgType : std::uint8_t { Int, Float, Bool, String, Date, Timestamp };

enum class ArgArity : std::uint8_t {
    Single,              // --name VALUE
    Flag,                // --name / --no-name, no value
    Repeated,            // --name VALUE, more than once (`+` / `*`)
    PositionalSingle,    // one bare argument
    PositionalRepeated,  // trailing bare arguments (`positional+` / `positional*`)
};

struct OptionSpec {
    std::string name;
    std::vector<std::string> spellings;  // "--name", "-t", ... (matched verbatim)
    ArgType type = ArgType::String;
    ArgArity arity = ArgArity::Single;
    bool optional = false;      // trailing `?`: absent => no row (not an error)
    bool at_least_one = false;  // `+` (vs `*`) and `positional+` (vs `positional*`)
    std::optional<std::string> default_value;
    std::string help;

    [[nodiscard]] auto is_positional() const -> bool {
        return arity == ArgArity::PositionalSingle || arity == ArgArity::PositionalRepeated;
    }
    [[nodiscard]] auto is_flag() const -> bool { return arity == ArgArity::Flag; }
    [[nodiscard]] auto is_repeated() const -> bool {
        return arity == ArgArity::Repeated || arity == ArgArity::PositionalRepeated;
    }
};

struct ArgRow {
    std::string kind;
    std::string name;
    std::int64_t index = 0;
    std::string value;
};

namespace detail {

[[nodiscard]] inline auto is_space_char(char c) -> bool {
    return std::isspace(static_cast<unsigned char>(c)) != 0;
}

[[nodiscard]] inline auto trim(std::string_view s) -> std::string {
    std::size_t b = 0;
    std::size_t e = s.size();
    while (b < e && is_space_char(s[b])) {
        ++b;
    }
    while (e > b && is_space_char(s[e - 1])) {
        --e;
    }
    return std::string(s.substr(b, e - b));
}

[[nodiscard]] inline auto split_ws(std::string_view s) -> std::vector<std::string> {
    std::vector<std::string> out;
    std::string cur;
    for (const char c : s) {
        if (is_space_char(c)) {
            if (!cur.empty()) {
                out.push_back(std::move(cur));
                cur.clear();
            }
        } else {
            cur.push_back(c);
        }
    }
    if (!cur.empty()) {
        out.push_back(std::move(cur));
    }
    return out;
}

[[nodiscard]] inline auto dash_form(std::string_view name) -> std::string {
    std::string out(name);
    std::ranges::replace(out, '_', '-');
    return out;
}

[[nodiscard]] inline auto parse_arg_type(std::string_view word) -> ArgType {
    if (word == "int" || word == "int64" || word == "int32") {
        return ArgType::Int;
    }
    if (word == "float" || word == "float64" || word == "float32") {
        return ArgType::Float;
    }
    if (word == "bool") {
        return ArgType::Bool;
    }
    if (word == "string") {
        return ArgType::String;
    }
    if (word == "date") {
        return ArgType::Date;
    }
    if (word == "timestamp") {
        return ArgType::Timestamp;
    }
    throw std::runtime_error("parse_args: unknown option type '" + std::string(word) + "'");
}

// Parse one spec line into an OptionSpec. `line` has the trailing `# help`
// already removed. Grammar:
//   <name> [ "(" alias ["," alias]* ")" ] ":" <typeslot> [ "?" ] [ "=" default ]
//   <typeslot> := "flag" | <type> [ arity ]
//   arity      := "+" | "*" | "positional" | "positional+" | "positional*"
[[nodiscard]] inline auto parse_spec_line(std::string_view line, std::string help) -> OptionSpec {
    const auto colon = line.find(':');
    if (colon == std::string_view::npos) {
        throw std::runtime_error("parse_args: spec line missing ':' -> " + std::string(line));
    }
    std::string head = trim(line.substr(0, colon));
    std::string rest = trim(line.substr(colon + 1));

    OptionSpec spec;
    spec.help = std::move(help);

    // Name and aliases.
    std::string aliases;
    if (const auto lparen = head.find('('); lparen != std::string::npos) {
        const auto rparen = head.find(')', lparen);
        if (rparen == std::string::npos) {
            throw std::runtime_error("parse_args: unmatched '(' in spec line -> " + std::string(line));
        }
        aliases = head.substr(lparen + 1, rparen - lparen - 1);
        head = trim(head.substr(0, lparen));
    }
    spec.name = trim(head);
    if (spec.name.empty()) {
        throw std::runtime_error("parse_args: spec line has no option name -> " + std::string(line));
    }

    // Default value: everything after the first '='.
    if (const auto eq = rest.find('='); eq != std::string::npos) {
        spec.default_value = trim(rest.substr(eq + 1));
        rest = trim(rest.substr(0, eq));
    }
    // Trailing '?': optional.
    if (!rest.empty() && rest.back() == '?') {
        spec.optional = true;
        rest.pop_back();
        rest = trim(rest);
    }

    const auto tokens = split_ws(rest);
    if (tokens.empty()) {
        throw std::runtime_error("parse_args: spec line missing a type -> " + std::string(line));
    }
    const auto apply_arity = [&](const std::string& a) -> bool {
        if (a == "flag") {
            spec.type = ArgType::Bool;
            spec.arity = ArgArity::Flag;
        } else if (a == "+") {
            spec.arity = ArgArity::Repeated;
            spec.at_least_one = true;
        } else if (a == "*") {
            spec.arity = ArgArity::Repeated;
        } else if (a == "positional") {
            spec.arity = ArgArity::PositionalSingle;
        } else if (a == "positional+") {
            spec.arity = ArgArity::PositionalRepeated;
            spec.at_least_one = true;
        } else if (a == "positional*") {
            spec.arity = ArgArity::PositionalRepeated;
        } else {
            return false;
        }
        return true;
    };

    std::size_t next = 0;
    spec.type = ArgType::String;
    spec.arity = ArgArity::Single;
    if (!apply_arity(tokens[0])) {
        // tokens[0] is a type name; an arity word may follow.
        spec.type = parse_arg_type(tokens[0]);
        next = 1;
        if (next < tokens.size() && !apply_arity(tokens[next])) {
            throw std::runtime_error("parse_args: unknown arity '" + tokens[next] + "' -> " +
                                     std::string(line));
        }
        if (next < tokens.size()) {
            ++next;
        }
    } else {
        next = 1;
    }
    if (next < tokens.size()) {
        throw std::runtime_error("parse_args: trailing tokens in spec line -> " + std::string(line));
    }

    // Spellings (for non-positional options).
    if (!spec.is_positional()) {
        spec.spellings.push_back("--" + spec.name);
        if (const std::string dashed = dash_form(spec.name); dashed != spec.name) {
            spec.spellings.push_back("--" + dashed);
        }
        for (auto& alias : split_ws(aliases)) {
            while (!alias.empty() && (alias.back() == ',' )) {
                alias.pop_back();
            }
            std::string a;
            for (const char c : alias) {
                if (c != ',') {
                    a.push_back(c);
                }
            }
            if (a.empty()) {
                continue;
            }
            spec.spellings.push_back((a.size() == 1 ? "-" : "--") + a);
        }
    } else if (!aliases.empty()) {
        throw std::runtime_error("parse_args: a positional option takes no aliases -> " +
                                 std::string(line));
    }
    return spec;
}

[[nodiscard]] inline auto parse_spec(const std::string& spec) -> std::vector<OptionSpec> {
    std::vector<OptionSpec> out;
    std::string chunk;
    const auto flush = [&] {
        // Split trailing `# help`.
        std::string line = chunk;
        std::string help;
        if (const auto hash = line.find('#'); hash != std::string::npos) {
            help = trim(line.substr(hash + 1));
            line = line.substr(0, hash);
        }
        line = trim(line);
        chunk.clear();
        if (line.empty()) {
            return;
        }
        out.push_back(parse_spec_line(line, std::move(help)));
    };
    for (const char c : spec) {
        if (c == '\n' || c == ';') {
            flush();
        } else {
            chunk.push_back(c);
        }
    }
    flush();

    // Reject duplicate names / spellings.
    for (std::size_t i = 0; i < out.size(); ++i) {
        for (std::size_t j = i + 1; j < out.size(); ++j) {
            if (out[i].name == out[j].name) {
                throw std::runtime_error("parse_args: duplicate option name '" + out[i].name + "'");
            }
        }
    }
    return out;
}

inline void validate_value(const OptionSpec& spec, const std::string& value) {
    const auto fail = [&] {
        throw std::runtime_error("parse_args: invalid value for --" + spec.name + ": '" + value +
                                 "'");
    };
    switch (spec.type) {
        case ArgType::Int: {
            if (value.empty()) {
                fail();
            }
            std::size_t pos = (value[0] == '-' || value[0] == '+') ? 1 : 0;
            if (pos == value.size()) {
                fail();
            }
            for (; pos < value.size(); ++pos) {
                if (std::isdigit(static_cast<unsigned char>(value[pos])) == 0) {
                    fail();
                }
            }
            break;
        }
        case ArgType::Float: {
            char* end = nullptr;
            std::strtod(value.c_str(), &end);
            if (end == value.c_str() || *end != '\0') {
                fail();
            }
            break;
        }
        case ArgType::Bool:
            if (value != "true" && value != "false") {
                fail();
            }
            break;
        case ArgType::String:
        case ArgType::Date:
        case ArgType::Timestamp:
            // Date/Timestamp are validated by the downstream cast; a bare string
            // check here would duplicate that logic without the calendar rules.
            break;
    }
}

[[nodiscard]] inline auto split_argv(const std::string& text) -> std::vector<std::string> {
    std::vector<std::string> out;
    std::string cur;
    bool have = false;
    for (const char c : text) {
        if (c == '\n' || c == '\r' || c == '\t' || c == ' ') {
            if (have) {
                out.push_back(std::move(cur));
                cur.clear();
                have = false;
            }
        } else {
            cur.push_back(c);
            have = true;
        }
    }
    if (have) {
        out.push_back(std::move(cur));
    }
    return out;
}

[[nodiscard]] inline auto argv_from_env() -> std::vector<std::string> {
    const char* raw = std::getenv("IBEX_ARGS");
    if (raw == nullptr) {
        return {};
    }
    // Environment values keep embedded spaces in a single argument only when
    // separated by newlines; fall back to whitespace splitting otherwise.
    std::string text(raw);
    if (text.find('\n') != std::string::npos) {
        std::vector<std::string> out;
        std::string line;
        std::istringstream stream(text);
        while (std::getline(stream, line)) {
            if (!line.empty() && line.back() == '\r') {
                line.pop_back();
            }
            if (!line.empty()) {
                out.push_back(std::move(line));
            }
        }
        return out;
    }
    return split_argv(text);
}

[[nodiscard]] inline auto find_spec(std::vector<OptionSpec>& specs, std::string_view spelling)
    -> OptionSpec* {
    for (auto& spec : specs) {
        if (std::ranges::find(spec.spellings, spelling) != spec.spellings.end()) {
            return &spec;
        }
    }
    return nullptr;
}

[[nodiscard]] inline auto parse_arguments(std::vector<OptionSpec>& specs,
                                          const std::vector<std::string>& argv)
    -> std::vector<ArgRow> {
    std::vector<ArgRow> rows;
    robin_hood::unordered_map<std::string, std::int64_t> occ;

    // Positional specs, in declaration order.
    std::vector<OptionSpec*> positionals;
    for (auto& spec : specs) {
        if (spec.is_positional()) {
            positionals.push_back(&spec);
        }
    }
    std::size_t pos_idx = 0;
    std::int64_t bare_ordinal = 0;

    const auto take_positional = [&](const std::string& value) {
        if (positionals.empty()) {
            rows.push_back(ArgRow{.kind = "positional", .name = "", .index = bare_ordinal++,
                                  .value = value});
            return;
        }
        if (pos_idx >= positionals.size()) {
            throw std::runtime_error("parse_args: unexpected positional argument: '" + value + "'");
        }
        OptionSpec* spec = positionals[pos_idx];
        const std::int64_t index = occ[spec->name]++;
        validate_value(*spec, value);
        rows.push_back(
            ArgRow{.kind = "positional", .name = spec->name, .index = index, .value = value});
        if (spec->arity == ArgArity::PositionalSingle) {
            ++pos_idx;
        }
    };

    bool positional_only = false;
    for (std::size_t i = 0; i < argv.size(); ++i) {
        const std::string& token = argv[i];
        if (!positional_only && token == "--") {
            positional_only = true;
            continue;
        }
        if (positional_only || token.size() < 2 || token[0] != '-') {
            take_positional(token);
            continue;
        }

        std::string key = token;
        std::optional<std::string> inline_value;
        if (token.rfind("--", 0) == 0) {
            if (const auto eq = token.find('='); eq != std::string::npos) {
                key = token.substr(0, eq);
                inline_value = token.substr(eq + 1);
            }
        }

        bool negated = false;
        OptionSpec* spec = find_spec(specs, key);
        if (spec == nullptr && key.rfind("--no-", 0) == 0) {
            if (OptionSpec* base = find_spec(specs, "--" + key.substr(5));
                base != nullptr && base->is_flag()) {
                spec = base;
                negated = true;
            }
        }
        if (spec == nullptr) {
            throw std::runtime_error("parse_args: unknown option: " + token);
        }

        if (spec->is_flag()) {
            std::string value = negated ? "false" : "true";
            if (inline_value.has_value()) {
                if (*inline_value != "true" && *inline_value != "false") {
                    throw std::runtime_error("parse_args: --" + spec->name +
                                             " takes true/false, got '" + *inline_value + "'");
                }
                value = *inline_value;
            }
            rows.push_back(ArgRow{.kind = "flag", .name = spec->name, .index = occ[spec->name]++,
                                  .value = std::move(value)});
            continue;
        }

        std::string value;
        if (inline_value.has_value()) {
            value = *inline_value;
        } else {
            if (i + 1 >= argv.size()) {
                throw std::runtime_error("parse_args: option " + token + " needs a value");
            }
            value = argv[++i];
        }
        validate_value(*spec, value);
        const std::int64_t index = occ[spec->name]++;
        if (index > 0 && spec->arity == ArgArity::Single) {
            throw std::runtime_error("parse_args: option --" + spec->name + " given more than once");
        }
        rows.push_back(ArgRow{.kind = "option", .name = spec->name, .index = index,
                              .value = std::move(value)});
    }

    // Defaults, flag fallbacks, and required checks.
    for (auto& spec : specs) {
        const bool seen = occ.find(spec.name) != occ.end() && occ[spec.name] > 0;
        if (spec.is_flag()) {
            if (!seen) {
                std::string value = spec.default_value.value_or("false");
                if (value != "true" && value != "false") {
                    throw std::runtime_error("parse_args: flag --" + spec.name +
                                             " default must be true/false");
                }
                rows.push_back(
                    ArgRow{.kind = "flag", .name = spec.name, .index = 0, .value = std::move(value)});
            }
            continue;
        }
        if (seen) {
            continue;
        }
        if (spec.default_value.has_value()) {
            validate_value(spec, *spec.default_value);
            rows.push_back(ArgRow{.kind = spec.is_positional() ? "positional" : "option",
                                  .name = spec.name, .index = 0, .value = *spec.default_value});
            continue;
        }
        if (spec.optional) {
            continue;  // absent optional: no row
        }
        if (spec.arity == ArgArity::Repeated && !spec.at_least_one) {
            continue;  // `*`: zero is fine
        }
        if (spec.arity == ArgArity::PositionalRepeated && !spec.at_least_one) {
            continue;  // `positional*`
        }
        throw std::runtime_error(
            spec.is_positional() ? ("parse_args: missing required positional argument: " + spec.name)
                                 : ("parse_args: missing required option: --" + spec.name));
    }

    return rows;
}

[[nodiscard]] inline auto build_table(const std::vector<ArgRow>& rows) -> ibex::runtime::Table {
    ibex::Column<std::string> kind;
    ibex::Column<std::string> name;
    ibex::Column<std::int64_t> index;
    ibex::Column<std::string> value;
    kind.reserve(rows.size());
    name.reserve(rows.size());
    index.reserve(rows.size());
    value.reserve(rows.size());
    for (const auto& row : rows) {
        kind.push_back(row.kind);
        name.push_back(row.name);
        index.push_back(row.index);
        value.push_back(row.value);
    }
    ibex::runtime::Table table;
    table.add_column("kind", std::move(kind));
    table.add_column("name", std::move(name));
    table.add_column("index", std::move(index));
    table.add_column("value", std::move(value));
    return table;
}

}  // namespace detail

[[nodiscard]] inline auto parse_args_from_tokens(const std::string& spec,
                                                 const std::vector<std::string>& argv)
    -> ibex::runtime::Table {
    auto specs = detail::parse_spec(spec);
    auto rows = detail::parse_arguments(specs, argv);
    return detail::build_table(rows);
}

}  // namespace ibex::args

// Public entry points, at global scope so the transpiler's `parse_args(...)`
// call in generated C++ resolves (matching csv.hpp's `read_csv`).

// argv from the IBEX_ARGS environment variable (one argument per line).
[[nodiscard]] inline auto parse_args(const std::string& spec) -> ibex::runtime::Table {
    return ibex::args::parse_args_from_tokens(spec, ibex::args::detail::argv_from_env());
}

// Explicit argv string (newline- or space-separated).
[[nodiscard]] inline auto parse_args(const std::string& spec, const std::string& argv)
    -> ibex::runtime::Table {
    if (argv.empty()) {
        return parse_args(spec);
    }
    return ibex::args::parse_args_from_tokens(spec, ibex::args::detail::split_argv(argv));
}
