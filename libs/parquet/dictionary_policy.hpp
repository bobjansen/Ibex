// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>

namespace ibex::parquet_dict {

/// Small dictionaries are cheap to intern even when every row is distinct.
inline constexpr std::int64_t kMaxEntries = 8192;

/// Use the actual dictionary entry count, summed over row groups. Byte size
/// cannot bound cardinality: strings may be empty or arbitrarily long, and
/// dictionary pages may be compressed. Retain small dictionaries and larger
/// dictionaries that replace at least two rows per entry.
[[nodiscard]] constexpr auto worth_keeping(std::int64_t entries, std::int64_t rows) -> bool {
    return entries <= kMaxEntries || entries <= rows / 2;
}

/// Enough bytes to hold a dictionary page's Thrift header: the header is a
/// handful of varints (type, sizes, crc, then num_values and encoding).
inline constexpr std::size_t kDictionaryHeaderReadBytes = 256;

namespace detail {

/// A cursor over Thrift compact-protocol bytes. Every read is bounds-checked;
/// running off the end sets `ok` to false and later reads return zero.
struct CompactReader {
    const std::uint8_t* at;
    const std::uint8_t* end;
    bool ok = true;

    auto byte() -> std::uint8_t {
        if (at == end) {
            ok = false;
            return 0;
        }
        return *at++;
    }
    auto varint() -> std::uint64_t {
        std::uint64_t value = 0;
        for (int shift = 0; shift < 64 && ok; shift += 7) {
            const std::uint8_t b = byte();
            value |= static_cast<std::uint64_t>(b & 0x7fU) << shift;
            if ((b & 0x80U) == 0) {
                return value;
            }
        }
        ok = false;
        return 0;
    }
    auto zigzag() -> std::int64_t {
        const std::uint64_t v = varint();
        return static_cast<std::int64_t>(v >> 1) ^ -static_cast<std::int64_t>(v & 1);
    }
    void skip_bytes(std::uint64_t n) {
        if (n > static_cast<std::uint64_t>(end - at)) {
            ok = false;
            at = end;
            return;
        }
        at += n;
    }
    // Skip one value of compact type `type`. `in_field` says whether a bool's
    // value already sat in the field header (fields) or takes a byte (elements).
    void skip(std::uint8_t type, bool in_field, int depth) {
        if (depth > 16) {
            ok = false;
            return;
        }
        switch (type) {
            case 1:
            case 2:
                if (!in_field) {
                    byte();
                }
                return;
            case 3:
                byte();
                return;
            case 4:
            case 5:
            case 6:
                varint();
                return;
            case 7:
                skip_bytes(8);
                return;
            case 8:
                skip_bytes(varint());
                return;
            case 9:
            case 10: {
                const std::uint8_t header = byte();
                std::uint64_t size = header >> 4U;
                if (size == 15) {
                    size = varint();
                }
                for (std::uint64_t i = 0; i < size && ok; ++i) {
                    skip(header & 0x0fU, false, depth + 1);
                }
                return;
            }
            case 11: {
                const std::uint64_t size = varint();
                if (size == 0) {
                    return;
                }
                const std::uint8_t types = byte();
                for (std::uint64_t i = 0; i < size && ok; ++i) {
                    skip(types >> 4U, false, depth + 1);
                    skip(types & 0x0fU, false, depth + 1);
                }
                return;
            }
            case 12:
                skip_struct(depth + 1);
                return;
            default:
                ok = false;
                return;
        }
    }
    void skip_struct(int depth) {
        std::int64_t id = 0;
        while (ok) {
            std::uint8_t type = 0;
            if (!next_field(id, type)) {
                return;
            }
            skip(type, true, depth);
        }
    }
    // Read a field header; false at the struct's stop byte (or on error).
    auto next_field(std::int64_t& id, std::uint8_t& type) -> bool {
        const std::uint8_t header = byte();
        if (!ok || header == 0) {
            return false;
        }
        type = header & 0x0fU;
        const std::uint8_t delta = header >> 4U;
        id = delta == 0 ? zigzag() : id + delta;
        return ok;
    }
};

}  // namespace detail

/// The entry count a dictionary page's header declares, parsed from the
/// Thrift compact-protocol `PageHeader` at the start of `bytes`: field 1 is
/// the page type (2 = DICTIONARY_PAGE) and field 7 the DictionaryPageHeader,
/// whose field 1 is num_values. This is the same count the page reader would
/// report, read without fetching or decompressing the page body -- deciding a
/// column's representation must not cost a read of `l_comment`'s strings.
/// nullopt when the bytes are not a complete dictionary page header.
[[nodiscard]] inline auto dictionary_header_entries(const std::uint8_t* bytes, std::size_t size)
    -> std::optional<std::int64_t> {
    detail::CompactReader in{.at = bytes, .end = bytes + size};
    std::optional<std::int64_t> page_type;
    std::optional<std::int64_t> entries;
    std::int64_t id = 0;
    std::uint8_t type = 0;
    while (in.next_field(id, type)) {
        if (id == 1 && type == 5) {
            page_type = in.zigzag();
        } else if (id == 7 && type == 12) {
            std::int64_t inner = 0;
            std::uint8_t inner_type = 0;
            while (in.next_field(inner, inner_type)) {
                if (inner == 1 && inner_type == 5) {
                    entries = in.zigzag();
                } else {
                    in.skip(inner_type, true, 1);
                }
            }
        } else {
            in.skip(type, true, 0);
        }
    }
    if (!in.ok || page_type != 2 || !entries.has_value() || *entries < 0) {
        return std::nullopt;
    }
    return entries;
}

}  // namespace ibex::parquet_dict
