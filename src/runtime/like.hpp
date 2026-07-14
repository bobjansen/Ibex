// like.hpp — SQL-LIKE pattern matching, the semantic core of the `like` builtin.
//
// A pattern is compiled once (compile_like_pattern) and then matched against
// many values (like_match). Compilation classifies the common shapes — exact,
// prefix, suffix, substring — so the hot column kernel runs a plain
// starts_with/ends_with/find instead of the general matcher, and it resolves
// escapes up front so matching never re-scans them.
//
// Semantics (SPEC.md): the pattern must match the *whole* value; `%` matches
// zero or more code points, `_` exactly one; matching is case-sensitive and
// byte-oriented apart from `_`, which advances one UTF-8 code point. `\`
// escapes the next character; a trailing `\` is an invalid pattern.
#pragma once

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace ibex::runtime {

// One element of a general (non-fast-path) pattern. Consecutive `%` collapse
// into a single Any at compile time, so the matcher never backtracks twice over
// the same position.
struct LikeToken {
    enum class Kind : std::uint8_t { Literal, One, Any };
    Kind kind = Kind::Literal;
    std::string text;  // Literal only: the escape-resolved bytes
};

enum class LikeKind : std::uint8_t {
    MatchAll,   // "%"
    Exact,      // no wildcard
    Prefix,     // "abc%"
    Suffix,     // "%abc"
    Contains,   // "%abc%"
    Fragments,  // literals separated by `%`, no `_`: "%special%requests%", "a%b%c"
    General,    // anything with `_`
};

struct LikePattern {
    LikeKind kind = LikeKind::Exact;
    std::string text;  // Exact / Prefix / Suffix / Contains: the fixed bytes

    // Fragments: the literal runs in order, plus whether the pattern is pinned
    // to the start/end of the value (i.e. does not begin/end with `%`).
    std::vector<std::string> fragments;
    bool anchored_start = false;
    bool anchored_end = false;

    std::vector<LikeToken> tokens;  // General
};

// Advance one UTF-8 code point from byte offset `i`. A malformed leading byte is
// consumed as one unit, so the matcher always makes progress.
[[nodiscard]] inline auto like_next_code_point(std::string_view s, std::size_t i) -> std::size_t {
    std::size_t j = i + 1;
    while (j < s.size() && (static_cast<unsigned char>(s[j]) & 0xC0U) == 0x80U) {
        ++j;
    }
    return j;
}

[[nodiscard]] inline auto compile_like_pattern(std::string_view pattern)
    -> std::expected<LikePattern, std::string> {
    std::vector<LikeToken> tokens;
    tokens.reserve(4);

    const auto push_literal_char = [&tokens](char c) {
        if (tokens.empty() || tokens.back().kind != LikeToken::Kind::Literal) {
            tokens.push_back(LikeToken{.kind = LikeToken::Kind::Literal, .text = {}});
        }
        tokens.back().text.push_back(c);
    };

    for (std::size_t i = 0; i < pattern.size(); ++i) {
        const char c = pattern[i];
        if (c == '\\') {
            if (i + 1 == pattern.size()) {
                return std::unexpected(
                    "like: pattern ends with a trailing escape '\\' (write \"\\\\\\\\\" for a "
                    "literal backslash)");
            }
            push_literal_char(pattern[++i]);
        } else if (c == '%') {
            if (tokens.empty() || tokens.back().kind != LikeToken::Kind::Any) {
                tokens.push_back(LikeToken{.kind = LikeToken::Kind::Any, .text = {}});
            }
        } else if (c == '_') {
            tokens.push_back(LikeToken{.kind = LikeToken::Kind::One, .text = {}});
        } else {
            push_literal_char(c);
        }
    }

    LikePattern out;
    if (tokens.empty()) {
        out.kind = LikeKind::Exact;  // the empty pattern matches only the empty string
        return out;
    }
    if (tokens.size() == 1 && tokens[0].kind == LikeToken::Kind::Any) {
        out.kind = LikeKind::MatchAll;
        return out;
    }

    // A pattern free of `_` is a sequence of literal runs separated by `%`, so
    // it can be matched by successive find()s (a tuned memmem) rather than by
    // the backtracking matcher — worth a lot on `%special%requests%`-shaped
    // patterns. Only `_` needs the general form.
    const bool has_one = std::ranges::any_of(
        tokens, [](const LikeToken& t) { return t.kind == LikeToken::Kind::One; });
    if (has_one) {
        out.kind = LikeKind::General;
        out.tokens = std::move(tokens);
        return out;
    }

    out.anchored_start = tokens.front().kind != LikeToken::Kind::Any;
    out.anchored_end = tokens.back().kind != LikeToken::Kind::Any;
    for (auto& tok : tokens) {
        if (tok.kind == LikeToken::Kind::Literal) {
            out.fragments.push_back(std::move(tok.text));
        }
    }
    if (out.fragments.size() != 1) {
        out.kind = LikeKind::Fragments;
        return out;
    }
    // One literal run: the four shapes that need no loop at all.
    out.text = std::move(out.fragments.front());
    out.fragments.clear();
    if (out.anchored_start && out.anchored_end) {
        out.kind = LikeKind::Exact;
    } else if (out.anchored_start) {
        out.kind = LikeKind::Prefix;
    } else if (out.anchored_end) {
        out.kind = LikeKind::Suffix;
    } else {
        out.kind = LikeKind::Contains;
    }
    return out;
}

// `%`-separated literals: match each fragment at its earliest possible position.
// Greedy left-to-right is exact here — `%` matches anything, so consuming the
// earliest occurrence of a fragment always leaves the most room for the rest.
[[nodiscard]] inline auto like_match_fragments(const LikePattern& pattern, std::string_view value)
    -> bool {
    const auto& fragments = pattern.fragments;
    std::size_t pos = 0;
    for (std::size_t k = 0; k < fragments.size(); ++k) {
        const std::string_view fragment = fragments[k];
        if (k == 0 && pattern.anchored_start) {
            if (!value.starts_with(fragment)) {
                return false;
            }
            pos = fragment.size();
            continue;
        }
        if (k + 1 == fragments.size() && pattern.anchored_end) {
            // The tail fragment must end the value, and must not overlap what
            // the earlier fragments already consumed.
            return value.size() - pos >= fragment.size() && value.ends_with(fragment);
        }
        const std::size_t at = value.find(fragment, pos);
        if (at == std::string_view::npos) {
            return false;
        }
        pos = at + fragment.size();
    }
    return true;
}

// Classic linear wildcard match: walk the tokens, and on a mismatch resume from
// one code point past the most recent `%`. Linear in practice, no allocation,
// no regex engine.
[[nodiscard]] inline auto like_match_general(const std::vector<LikeToken>& tokens,
                                             std::string_view value) -> bool {
    constexpr auto kNone = static_cast<std::size_t>(-1);
    const std::size_t n = value.size();
    std::size_t i = 0;  // byte offset into value
    std::size_t t = 0;  // token index
    std::size_t star_token = kNone;
    std::size_t star_value = 0;

    while (t < tokens.size() || i < n) {
        bool matched = false;
        if (t < tokens.size()) {
            const auto& tok = tokens[t];
            switch (tok.kind) {
                case LikeToken::Kind::Any:
                    star_token = t;
                    star_value = i;
                    ++t;
                    matched = true;
                    break;
                case LikeToken::Kind::One:
                    if (i < n) {
                        i = like_next_code_point(value, i);
                        ++t;
                        matched = true;
                    }
                    break;
                case LikeToken::Kind::Literal:
                    if (n - i >= tok.text.size() &&
                        value.compare(i, tok.text.size(), tok.text) == 0) {
                        i += tok.text.size();
                        ++t;
                        matched = true;
                    }
                    break;
            }
        }
        if (matched) {
            continue;
        }
        if (star_token == kNone || star_value >= n) {
            return false;
        }
        // Backtrack: let the most recent `%` eat one more code point. If a
        // literal follows that `%`, every viable resume point starts with it, so
        // jump straight to its next occurrence instead of retrying the tail at
        // each position (UTF-8 is self-synchronizing, so a valid needle can only
        // be found on a code-point boundary).
        star_value = like_next_code_point(value, star_value);
        if (star_token + 1 < tokens.size() &&
            tokens[star_token + 1].kind == LikeToken::Kind::Literal) {
            const std::size_t at = value.find(tokens[star_token + 1].text, star_value);
            if (at == std::string_view::npos) {
                return false;
            }
            star_value = at;
        }
        i = star_value;
        t = star_token + 1;
    }
    return true;
}

[[nodiscard]] inline auto like_match(const LikePattern& pattern, std::string_view value) -> bool {
    switch (pattern.kind) {
        case LikeKind::MatchAll:
            return true;
        case LikeKind::Exact:
            return value == pattern.text;
        case LikeKind::Prefix:
            return value.starts_with(pattern.text);
        case LikeKind::Suffix:
            return value.ends_with(pattern.text);
        case LikeKind::Contains:
            return value.contains(pattern.text);
        case LikeKind::Fragments:
            return like_match_fragments(pattern, value);
        case LikeKind::General:
            return like_match_general(pattern.tokens, value);
    }
    return false;  // unreachable; MSVC C4715
}

}  // namespace ibex::runtime
