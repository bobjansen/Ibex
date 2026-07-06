// sort.cpp — ordering and row-selection: LSD radix sort machinery,
// order_table (single- and multi-key, pre-sorted fast path), and grouped
// head/tail selection.
// Split out of interpreter.cpp; shared declarations live in interpreter_internal.hpp.

#include <ibex/runtime/interpreter.hpp>

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <limits>
#include <numeric>
#include <optional>
#include <pdqsort.h>
#include <robin_hood.h>
#include <string_view>
#include <type_traits>
#include <vector>

#if defined(__AVX2__) || defined(__BMI2__)
#include <immintrin.h>
#endif

#include "interpreter_internal.hpp"
#include "runtime_internal.hpp"

namespace ibex::runtime {

// LSD radix sort over pre-sign-flipped uint64 keys.
// Idx is the index type: uint32_t for tables ≤ UINT32_MAX rows, uint64_t otherwise.
// Keys must already be sign-flipped (int64 XOR 1<<63) so unsigned order == signed order.
// All 8 byte histograms are built in a single pass; passes where every element
// shares the same byte value are skipped (common for clustered timestamps).
// Stable LSD radix sort of the index array `idx` by `src_keys` (parallel arrays;
// idx is the payload carried alongside each key). `idx` is sorted in place and
// must already hold a valid permutation of [0, rows) — passing iota gives a sort
// from scratch, passing an existing order makes this a stable re-sort by a new
// key (the building block for multi-key LSD). Keys are consumed.
namespace {

template <typename Idx>

void radix_sort_by_key(std::vector<std::uint64_t> src_keys, std::vector<Idx>& idx,
                       std::size_t rows) {
    // Build all 8 byte-histograms in one sequential scan.
    std::array<std::array<std::size_t, 256>, 8> hists{};
    for (std::size_t i = 0; i < rows; ++i) {
        auto k = src_keys[i];
        for (std::size_t p = 0; p < 8; ++p)
            ++hists[p][(k >> (p * 8U)) & 0xFFU];
    }

    std::vector<std::uint64_t> dst_keys(rows);
    std::vector<Idx> dst_idx(rows);
    // Ping-pong between the caller's idx buffer and dst_idx; src_* point at the
    // buffer currently holding live data.
    std::vector<std::uint64_t>* src_k = &src_keys;
    std::vector<std::uint64_t>* dst_k = &dst_keys;
    std::vector<Idx>* src_i = &idx;
    std::vector<Idx>* dst_i = &dst_idx;

    std::array<std::size_t, 256> cnt;  //  NOLINT(cppcoreguidelines-pro-type-member-init)
    for (std::size_t pass = 0; pass < 8; ++pass) {
        const auto& h = hists[pass];
        // Skip pass if all elements have the same byte value.
        std::size_t non_zero = 0;
        for (auto c : h)
            if (c)
                ++non_zero;
        if (non_zero <= 1)
            continue;

        auto shift = pass * 8U;
        // Convert histogram to exclusive prefix-sum write positions.
        std::size_t total = 0;
        for (std::size_t b = 0; b < 256; ++b) {
            cnt[b] = total;
            total += h[b];
        }
        // Stable scatter: sequential reads, random writes.
        // Prefetch the destination cache line a few elements ahead.
        for (std::size_t i = 0; i < rows; ++i) {
#if defined(__GNUC__) || defined(__clang__)
            constexpr std::size_t kPrefetchDist = 8;
            if (i + kPrefetchDist < rows) {
                std::size_t pb = ((*src_k)[i + kPrefetchDist] >> shift) & 0xFFU;
                __builtin_prefetch(&(*dst_k)[cnt[pb]], 1, 1);
                __builtin_prefetch(&(*dst_i)[cnt[pb]], 1, 1);
            }
#endif
            std::size_t bucket = ((*src_k)[i] >> shift) & 0xFFU;
            (*dst_k)[cnt[bucket]] = (*src_k)[i];
            (*dst_i)[cnt[bucket]] = (*src_i)[i];
            ++cnt[bucket];
        }
        std::swap(src_k, dst_k);
        std::swap(src_i, dst_i);
    }
    // Ensure the sorted permutation ends up in the caller's idx buffer.
    if (src_i != &idx)
        idx = std::move(*src_i);
}

}  // namespace

namespace {

template <typename Idx>

auto radix_sort_impl(std::vector<std::uint64_t> src_keys, std::size_t rows) -> std::vector<Idx> {
    std::vector<Idx> idx(rows);
    std::iota(idx.begin(), idx.end(), Idx{0});
    radix_sort_by_key(std::move(src_keys), idx, rows);
    return idx;
}

}  // namespace

// Dispatch to 32-bit indices for tables that fit, 64-bit otherwise.
using SortIdx = std::variant<std::vector<std::uint32_t>, std::vector<std::uint64_t>>;
auto radix_sort_u64_asc(std::vector<std::uint64_t> keys, std::size_t rows) -> SortIdx {
    if (rows <= std::numeric_limits<std::uint32_t>::max())
        return radix_sort_impl<std::uint32_t>(std::move(keys), rows);
    return radix_sort_impl<std::uint64_t>(std::move(keys), rows);
}

// Stable multi-key sort by LSD radix: `codes[k]` holds one order-preserving u64
// per row for sort key k (key 0 most significant). Sorts least- to most-
// significant key, each pass stable, so the result equals a stable comparison
// sort on the same keys with ties broken by original row order. Each key is
// gathered into the current index order first so the radix scatter reads
// sequentially.
namespace {

template <typename Idx>

auto lsd_multi_radix(const std::vector<std::vector<std::uint64_t>>& codes, std::size_t rows)
    -> std::vector<Idx> {
    std::vector<Idx> idx(rows);
    std::iota(idx.begin(), idx.end(), Idx{0});
    for (std::size_t k = codes.size(); k-- > 0;) {
        const auto& code = codes[k];
        std::vector<std::uint64_t> gathered(rows);
        for (std::size_t i = 0; i < rows; ++i)
            gathered[i] = code[idx[i]];
        radix_sort_by_key(std::move(gathered), idx, rows);
    }
    return idx;
}

}  // namespace

auto order_table(const Table& input, const std::vector<ir::OrderKey>& keys)
    -> std::expected<Table, std::string> {
    std::size_t rows = input.rows();
    if (input.time_index.has_value()) {
        if (keys.size() != 1 || keys[0].name != *input.time_index || !keys[0].ascending) {
            return std::unexpected("order on TimeFrame must be by time index ascending");
        }
    }
    auto resolved_keys = ordering_keys_for_table(input, keys);
    if (rows <= 1 || input.columns.empty()) {
        Table output = input;
        output.ordering = resolved_keys;
        output.time_index = input.time_index;
        normalize_time_index(output);
        return output;
    }

    // Fast pre-sorted check for single ascending Timestamp/Date/Int key — avoids building
    // the 8 MB flat_keys[0].u64 vector when the input is already sorted (common TimeFrame case).
    if (resolved_keys.size() == 1 && resolved_keys[0].ascending) {
        const auto* column = input.find(resolved_keys[0].name);
        if (column != nullptr) {
            bool already_sorted = false;
            std::visit(
                [&](const auto& col) {
                    using ColT = std::decay_t<decltype(col)>;
                    if constexpr (std::is_same_v<ColT, Column<Timestamp>>) {
                        already_sorted = true;
                        for (std::size_t i = 1; i < rows; ++i) {
                            if (col[i].nanos < col[i - 1].nanos) {
                                already_sorted = false;
                                break;
                            }
                        }
                    } else if constexpr (std::is_same_v<ColT, Column<std::int64_t>>) {
                        already_sorted = true;
                        for (std::size_t i = 1; i < rows; ++i) {
                            if (col[i] < col[i - 1]) {
                                already_sorted = false;
                                break;
                            }
                        }
                    } else if constexpr (std::is_same_v<ColT, Column<Date>>) {
                        already_sorted = true;
                        for (std::size_t i = 1; i < rows; ++i) {
                            if (col[i].days < col[i - 1].days) {
                                already_sorted = false;
                                break;
                            }
                        }
                    }
                },
                *column);
            if (already_sorted) {
                Table output = input;
                output.ordering = std::move(resolved_keys);
                output.time_index = input.time_index;
                normalize_time_index(output);
                return output;
            }
        }
    }

    // Pre-extract each sort key into a flat typed array so the hot comparator
    // loop does plain vector indexing rather than per-comparison variant dispatch.
    // I64 keys are sign-flipped to uint64 at extraction time so that unsigned
    // comparison is equivalent to signed comparison — this lets radix_sort_u64_asc
    // consume the vector directly without an extra copy.
    constexpr std::uint64_t kSignFlip = std::uint64_t{1} << 63;
    enum class FlatKind : std::uint8_t { I64, F64, Str };
    struct FlatKey {
        FlatKind kind = FlatKind::I64;
        std::vector<std::uint64_t> u64;  // Int / Date.days / Timestamp.nanos, sign-flipped
        std::vector<double> f64;
        std::vector<std::string_view> str;  // views into original column storage
        bool ascending = true;
    };

    std::vector<FlatKey> flat_keys;
    flat_keys.reserve(resolved_keys.size());
    for (const auto& key : resolved_keys) {
        const auto* column = input.find(key.name);
        if (column == nullptr) {
            return std::unexpected("order column not found: " + key.name +
                                   " (available: " + format_columns(input) + ")");
        }
        FlatKey fk;
        fk.ascending = key.ascending;
        std::visit(
            [&](const auto& col) {
                using ColT = std::decay_t<decltype(col)>;
                if constexpr (std::is_same_v<ColT, Column<std::int64_t>>) {
                    fk.kind = FlatKind::I64;
                    fk.u64.reserve(rows);
                    for (auto v : col)
                        fk.u64.push_back(static_cast<std::uint64_t>(v) ^ kSignFlip);
                } else if constexpr (std::is_same_v<ColT, Column<double>>) {
                    fk.kind = FlatKind::F64;
                    fk.f64.assign(col.begin(), col.end());
                } else if constexpr (std::is_same_v<ColT, Column<Date>>) {
                    fk.kind = FlatKind::I64;
                    fk.u64.reserve(rows);
                    for (const auto& d : col)
                        fk.u64.push_back(static_cast<std::uint64_t>(d.days) ^ kSignFlip);
                } else if constexpr (std::is_same_v<ColT, Column<Timestamp>>) {
                    fk.kind = FlatKind::I64;
                    fk.u64.reserve(rows);
                    for (const auto& ts : col)
                        fk.u64.push_back(static_cast<std::uint64_t>(ts.nanos) ^ kSignFlip);
                } else if constexpr (std::is_same_v<ColT, Column<bool>>) {
                    fk.kind = FlatKind::I64;
                    fk.u64.reserve(rows);
                    for (std::size_t i = 0; i < rows; ++i)
                        fk.u64.push_back(static_cast<std::uint64_t>(col[i] ? 1 : 0) ^ kSignFlip);
                } else if constexpr (std::is_same_v<ColT, Column<std::string>>) {
                    fk.kind = FlatKind::Str;
                    fk.str.reserve(rows);
                    for (std::size_t i = 0; i < rows; ++i)
                        fk.str.push_back(col[i]);
                } else {
                    // Categorical: sort by dictionary value (string_view into shared dict)
                    fk.kind = FlatKind::Str;
                    fk.str.reserve(rows);
                    for (std::size_t i = 0; i < rows; ++i)
                        fk.str.push_back(col[i]);
                }
            },
            *column);
        flat_keys.push_back(std::move(fk));
    }

    // Fast path: single ascending I64 key — radix sort (pre-sorted case already handled above).
    if (flat_keys.size() == 1 && flat_keys[0].kind == FlatKind::I64 && flat_keys[0].ascending) {
        auto sort_result = radix_sort_u64_asc(std::move(flat_keys[0].u64), rows);
        return std::visit(
            [&]<typename Idx>(const std::vector<Idx>& idx) -> std::expected<Table, std::string> {
                return gather_rows(input, idx, &resolved_keys);
            },
            sort_result);
    }

    // Fast path: single ascending F64 key — map each double to an order-preserving
    // uint64 and radix sort, avoiding the comparison-based stable_sort.
    if (flat_keys.size() == 1 && flat_keys[0].kind == FlatKind::F64 && flat_keys[0].ascending) {
        std::vector<std::uint64_t> radix_keys(rows);
        const auto& f = flat_keys[0].f64;
        for (std::size_t i = 0; i < rows; ++i)
            radix_keys[i] = double_to_sortable_u64(f[i]);
        auto sort_result = radix_sort_u64_asc(std::move(radix_keys), rows);
        return std::visit(
            [&]<typename Idx>(const std::vector<Idx>& idx) -> std::expected<Table, std::string> {
                return gather_rows(input, idx, &resolved_keys);
            },
            sort_result);
    }

    // Ordinal-encode a string column to order-preserving u64 codes: dedup via
    // hash (O(rows)), sort the distinct values, map each row to its sorted rank.
    // Codes preserve string order, so radix on them == lexicographic sort.
    // Returns nullopt once the distinct count exceeds `cap` (bailing immediately,
    // so a high-cardinality reject is cheap) — the caller then prefers a
    // comparison sort, where the distinct-sort would cost as much as sorting all
    // rows anyway.
    auto ordinal_encode = [rows](const std::vector<std::string_view>& vals,
                                 std::size_t cap) -> std::optional<std::vector<std::uint64_t>> {
        robin_hood::unordered_map<std::string_view, std::uint64_t> code_of;
        std::vector<std::string_view> distinct;
        for (auto sv : vals) {
            if (code_of.emplace(sv, 0).second) {
                distinct.push_back(sv);
                if (distinct.size() > cap)
                    return std::nullopt;
            }
        }
        std::ranges::sort(distinct);
        for (std::uint64_t r = 0; r < distinct.size(); ++r)
            code_of[distinct[r]] = r;
        std::vector<std::uint64_t> code(rows);
        for (std::size_t i = 0; i < rows; ++i)
            code[i] = code_of[vals[i]];
        return code;
    };

    auto radix_gather =
        [&](std::vector<std::vector<std::uint64_t>>& codes) -> std::expected<Table, std::string> {
        if (rows <= std::numeric_limits<std::uint32_t>::max()) {
            auto idx = lsd_multi_radix<std::uint32_t>(codes, rows);
            return gather_rows(input, idx, &resolved_keys);
        }
        auto idx = lsd_multi_radix<std::uint64_t>(codes, rows);
        return gather_rows(input, idx, &resolved_keys);
    };

    // Invert order-preserving codes for a descending key so an ascending radix
    // yields descending order (equal codes stay equal → still stable).
    auto apply_descending = [](std::vector<std::uint64_t>& code, bool ascending) {
        if (!ascending)
            for (auto& c : code)
                c = ~c;
    };

    // Radix path for multi-key sorts and single descending numeric keys. Map each
    // key to an order-preserving u64 (strings via ordinal encoding, unconditional
    // here since the alternative for multi-key is itself a slow comparison sort)
    // and LSD-radix from the least- to the most-significant key. Single ascending
    // numeric keys already returned via the fast paths above.
    const bool use_radix_multi =
        flat_keys.size() >= 2 || (flat_keys.size() == 1 && flat_keys[0].kind != FlatKind::Str);
    if (use_radix_multi) {
        std::vector<std::vector<std::uint64_t>> codes;
        codes.reserve(flat_keys.size());
        for (auto& fk : flat_keys) {
            std::vector<std::uint64_t> code;
            switch (fk.kind) {
                case FlatKind::I64:
                    code = std::move(fk.u64);  // already sign-flipped to order-preserving u64
                    break;
                case FlatKind::F64:
                    code.resize(rows);
                    for (std::size_t i = 0; i < rows; ++i)
                        code[i] = double_to_sortable_u64(fk.f64[i]);
                    break;
                case FlatKind::Str:
                    code =
                        std::move(*ordinal_encode(fk.str, std::numeric_limits<std::size_t>::max()));
                    break;
            }
            apply_descending(code, fk.ascending);
            codes.push_back(std::move(code));
        }
        return radix_gather(codes);
    }

    // Single lone string key: ordinal-encode + radix when the column is low
    // cardinality (categorical/dictionary-like, the common case), where the
    // distinct-sort is far cheaper than sorting every row. High-cardinality
    // columns exceed the cap and fall through to the comparison sort below.
    if (flat_keys.size() == 1 && flat_keys[0].kind == FlatKind::Str) {
        constexpr std::size_t kOrdinalCap = std::size_t{1} << 16;
        if (auto code = ordinal_encode(flat_keys[0].str, kOrdinalCap)) {
            apply_descending(*code, flat_keys[0].ascending);
            std::vector<std::vector<std::uint64_t>> codes;
            codes.push_back(std::move(*code));
            return radix_gather(codes);
        }
    }

    // General path: a lone high-cardinality string key — comparison-based sort.
    // pdqsort is unstable, but the comparator's `lhs < rhs` tiebreak makes the
    // order total (no real ties), so the result matches a stable sort.
    auto compare_row = [&](std::size_t lhs, std::size_t rhs) -> bool {
        for (const auto& fk : flat_keys) {
            switch (fk.kind) {
                case FlatKind::I64: {
                    auto l = fk.u64[lhs];
                    auto r = fk.u64[rhs];
                    if (l != r)
                        return fk.ascending ? (l < r) : (l > r);
                    break;
                }
                case FlatKind::F64: {
                    auto l = fk.f64[lhs];
                    auto r = fk.f64[rhs];
                    if (l != r)
                        return fk.ascending ? (l < r) : (l > r);
                    break;
                }
                case FlatKind::Str: {
                    auto l = fk.str[lhs];
                    auto r = fk.str[rhs];
                    if (l != r)
                        return fk.ascending ? (l < r) : (l > r);
                    break;
                }
            }
        }
        return lhs < rhs;
    };
    std::vector<std::size_t> idx(rows);
    std::ranges::iota(idx, 0);
    pdqsort(idx.begin(), idx.end(), compare_row);
    return gather_rows(input, idx, &resolved_keys);
}

auto head_table(const Table& input, std::size_t count, const std::vector<ir::ColumnRef>& group_by)
    -> std::expected<Table, std::string> {
    if (count == 0) {
        Table output;
        for (const auto& entry : input.columns) {
            output.add_column(entry.name, make_empty_like(*entry.column));
        }
        output.ordering = input.ordering;
        output.time_index = input.time_index;
        normalize_time_index(output);
        return output;
    }

    const std::size_t rows = input.rows();
    if (rows <= count && group_by.empty()) {
        Table output = input;
        normalize_time_index(output);
        return output;
    }

    if (group_by.empty()) {
        std::vector<std::size_t> idx(std::min(rows, count));
        std::ranges::iota(idx, 0);
        return gather_rows(input, idx);
    }

    robin_hood::unordered_flat_map<Key, std::size_t, KeyHash, KeyEq> seen_counts;
    seen_counts.reserve(rows);
    std::vector<std::size_t> idx;
    idx.reserve(
        std::min(rows, count * std::max<std::size_t>(1, rows / std::max<std::size_t>(1, count))));

    for (std::size_t row = 0; row < rows; ++row) {
        Key key;
        key.values.reserve(group_by.size());
        for (const auto& ref : group_by) {
            const auto* column = input.find(ref.name);
            if (column == nullptr) {
                return std::unexpected("head group-by column not found: " + ref.name +
                                       " (available: " + format_columns(input) + ")");
            }
            key.values.push_back(scalar_from_column(*column, row));
        }
        auto& seen = seen_counts[key];
        if (seen >= count) {
            continue;
        }
        ++seen;
        idx.push_back(row);
    }

    return gather_rows(input, idx);
}

auto tail_table(const Table& input, std::size_t count, const std::vector<ir::ColumnRef>& group_by)
    -> std::expected<Table, std::string> {
    if (count == 0) {
        Table output;
        for (const auto& entry : input.columns) {
            output.add_column(entry.name, make_empty_like(*entry.column));
        }
        output.ordering = input.ordering;
        output.time_index = input.time_index;
        normalize_time_index(output);
        return output;
    }

    const std::size_t rows = input.rows();
    if (rows <= count && group_by.empty()) {
        Table output = input;
        normalize_time_index(output);
        return output;
    }

    if (group_by.empty()) {
        const std::size_t keep = std::min(rows, count);
        std::vector<std::size_t> idx(keep);
        const std::size_t start = rows - keep;
        std::ranges::iota(idx, start);
        return gather_rows(input, idx);
    }

    robin_hood::unordered_flat_map<Key, std::vector<std::size_t>, KeyHash, KeyEq> groups;
    groups.reserve(rows);
    std::vector<Key> order;
    order.reserve(rows);

    for (std::size_t row = 0; row < rows; ++row) {
        Key key;
        key.values.reserve(group_by.size());
        for (const auto& ref : group_by) {
            const auto* column = input.find(ref.name);
            if (column == nullptr) {
                return std::unexpected("tail group-by column not found: " + ref.name +
                                       " (available: " + format_columns(input) + ")");
            }
            key.values.push_back(scalar_from_column(*column, row));
        }
        auto [it, inserted] = groups.try_emplace(key);
        if (inserted) {
            order.push_back(key);
        }
        it->second.push_back(row);
    }

    std::vector<std::size_t> idx;
    idx.reserve(rows);
    for (const auto& key : order) {
        const auto& group_rows = groups.find(key)->second;
        const std::size_t keep = std::min(group_rows.size(), count);
        const std::size_t start = group_rows.size() - keep;
        idx.insert(idx.end(), group_rows.begin() + static_cast<std::ptrdiff_t>(start),
                   group_rows.end());
    }

    return gather_rows(input, idx);
}

}  // namespace ibex::runtime
