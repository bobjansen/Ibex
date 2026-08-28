// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#pragma once

#include <ibex/core/column.hpp>
#include <ibex/runtime/interpreter.hpp>

#include <algorithm>
#include <atomic>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <type_traits>
#include <variant>

#if defined(__x86_64__) || defined(_M_X64) || defined(__i386__) || defined(_M_IX86)
#include <immintrin.h>  // _pext_u64 under __BMI2__ (release builds enable it)
#endif

#include "kernel_types.hpp"

namespace ibex::runtime::kernel {

/// The OutputWriter for the fixed-width family (Phase 2 item 1): a pre-sized,
/// disjoint output range. The writer that did the count → prefix-sum owns the
/// buffer and hands each gather a window starting at `begin`; a worker-private
/// buffer is the same shape with `begin == 0`. Writes exactly `count` rows at
/// `data + begin` — never resizes, never reallocates, so parallel windows
/// never alias.
template <typename T>
struct OutputSpan {
    T* data = nullptr;
    std::size_t begin = 0;
    std::size_t count = 0;
};

/// The fixed-width gather kernel: copy `selection`'s surviving rows of `src`
/// into `out`, ascending. Dispatch over the selection shape is closed at
/// compile time (the plan's "closed dispatch at pipeline construction"); a
/// `RowRange` degenerates to one contiguous copy, `RowWordBlocks` iterates
/// the engine's native mask word-at-a-time — the exact loop
/// `gather_selection_into` ran before this kernel existed, so porting onto it
/// is behavior- and performance-neutral by construction.
template <typename T>
    requires std::is_trivially_copyable_v<T>
auto gather_selected(ColumnView<T> src, const Selection& selection, OutputSpan<T> out) noexcept
    -> void {
    std::visit(
        [&](const auto& sel) {
            using S = std::decay_t<decltype(sel)>;
            T* dp = out.data + out.begin;
            if constexpr (std::is_same_v<S, RowRange>) {
                if (out.count != 0) {
                    std::memcpy(dp, src.data() + sel.begin, out.count * sizeof(T));
                }
            } else if constexpr (std::is_same_v<S, RowIndices>) {
                for (std::size_t i = 0; i < sel.count; ++i) {
                    dp[i] = src.value(sel[i]);
                }
            } else if constexpr (std::is_same_v<S, RowBitmap>) {
                std::size_t j = 0;
                for (std::size_t r = 0; r < src.rows(); ++r) {
                    if (sel.test(r)) {
                        dp[j++] = src.value(r);
                    }
                }
            } else {  // RowWordBlocks
                std::size_t j = 0;
                for (std::size_t w = 0; w < sel.word_count; ++w) {
                    std::uint64_t bits = sel.words[w];
                    const std::size_t base = sel.row_base + (w * 64);
                    while (bits != 0) {
                        const auto bit = static_cast<std::size_t>(std::countr_zero(bits));
                        dp[j++] = src.value(base + bit);
                        bits &= (bits - 1);
                    }
                }
            }
        },
        selection);
}

// --- Bit-packed and variable-width representations -------------------------
//
// Moved verbatim from filter.cpp's anonymous namespace so the bool gather
// kernel and the two-phase filter share one definition (the I4 lesson: the
// hazard is a constant written out twice). These are the output-side half of
// the bool story; mask packing (pack_mask_word_*) stays with
// compute_filter_selection.

// values/mask mirror the _pext_u64(values, mask) intrinsic's own calling convention.
inline auto pack_selected_bool_bits(std::uint64_t values, std::uint64_t mask) noexcept
    -> std::uint64_t {
#ifdef __BMI2__
    return _pext_u64(values, mask);
#else
    std::uint64_t packed = 0;
    unsigned out_bit = 0;
    while (mask != 0) {
        const auto bit = static_cast<unsigned>(std::countr_zero(mask));
        packed |= ((values >> bit) & std::uint64_t{1}) << out_bit;
        ++out_bit;
        mask &= (mask - 1);
    }
    return packed;
#endif
}

/// The words of a bit-packed output that a single gather may share with another
/// gather running at the same time.
///
/// A gather writes a contiguous run of output bits, so it can only meet a
/// neighbour at the two ends of that run: every word strictly between them has
/// all 64 of its bits inside this run and is exclusively owned. Marking just
/// those two words is what keeps the atomics off the hot loop.
///
/// When a run is short enough to sit inside one word, `first == last` and that
/// single word is written atomically — which is also the case where three or
/// more gathers can meet in one word, and it needs no special handling because
/// OR is commutative and associative.
struct SharedBitWords {
    std::size_t first = 0;
    std::size_t last = 0;

    /// For a run of `count` bits starting at output bit `begin`. An empty run
    /// writes nothing, so its bounds are never consulted.
    [[nodiscard]] static auto of_run(std::size_t begin, std::size_t count) noexcept
        -> SharedBitWords {
        constexpr std::size_t kBitsPerWord = 64;
        return {.first = begin / kBitsPerWord,
                .last = (begin + (count == 0 ? 0 : count - 1)) / kBitsPerWord};
    }

    [[nodiscard]] auto contains(std::size_t word) const noexcept -> bool {
        return word == first || word == last;
    }
};

/// OR `bits` into output word `index`, atomically iff that word may be shared
/// with a concurrent gather.
///
/// Sound only because every bit-packed destination is zero-filled before any
/// gather runs and these writes only ever *set* bits — so the interleaving
/// cannot matter. A write that had to clear a bit could not use this, which is
/// why a validity gather skips its false bits instead of assigning them. A
/// plain store to an exclusively-owned word races with nothing: sharing a
/// cache line with an atomic write is a performance question, not a
/// correctness one.
inline void or_bits_into_word(std::uint64_t* words, std::size_t index, std::uint64_t bits,
                              SharedBitWords shared) noexcept {
    if (bits == 0) {
        return;
    }
    if (shared.contains(index)) {
#ifdef __cpp_lib_atomic_ref
        std::atomic_ref<std::uint64_t>(words[index]).fetch_or(bits, std::memory_order_relaxed);
#else
        // Apple's libc++ (macOS clang-werror leg) doesn't ship std::atomic_ref yet.
        // std::atomic<uint64_t> is a lock-free integral specialization and therefore
        // layout-compatible with a plain uint64_t, so a reinterpret_cast view gives
        // the same fetch-OR without needing atomic_ref.
        reinterpret_cast<std::atomic<std::uint64_t>*>(&words[index])
            ->fetch_or(bits, std::memory_order_relaxed);
#endif
    } else {
        words[index] |= bits;
    }
}

inline auto append_packed_bool_bits(std::uint64_t packed, std::size_t count,
                                    std::uint64_t* dst_words, std::size_t& out_bit,
                                    SharedBitWords shared) noexcept -> void {
    if (count == 0) {
        return;
    }
    constexpr std::size_t kBitsPerWord = 64;
    const std::size_t dst_word = out_bit / kBitsPerWord;
    const auto shift = static_cast<unsigned>(out_bit % kBitsPerWord);
    or_bits_into_word(dst_words, dst_word, packed << shift, shared);
    if (shift != 0 && count > kBitsPerWord - shift) {
        or_bits_into_word(dst_words, dst_word + 1, packed >> (kBitsPerWord - shift), shared);
    }
    out_bit += count;
}

/// Non-owning view of a bit-packed bool column. Owns nothing; hides the one
/// representation split that matters to a gather — internally-word-backed vs
/// external-byte-backed — behind `source_word(start_row)`: the 64 source bits
/// at `start_row`, straddling two words when the start is unaligned.
class BoolView {
   public:
    BoolView(const Column<bool>& col) noexcept : col_(&col) {}

    [[nodiscard]] auto rows() const noexcept -> std::size_t { return col_->size(); }
    [[nodiscard]] auto bit(std::size_t row) const noexcept -> bool { return (*col_)[row]; }

    /// The 64 source bits at rows [start_row, start_row + 64), LSB = start_row.
    /// Past-the-end bits read as zero (the gather only consults bits its
    /// selection actually selects, which are in range by construction).
    [[nodiscard]] auto source_word(std::size_t start_row) const noexcept -> std::uint64_t {
        if (!col_->is_external()) {
            const auto* __restrict src_words = col_->words_data();
            const std::size_t src_words_n = (rows() + 63) / 64;
            const std::size_t sw = start_row / 64;
            const auto shift = static_cast<unsigned>(start_row % 64);
            std::uint64_t bits = src_words[sw] >> shift;
            if (shift != 0 && sw + 1 < src_words_n) {
                bits |= src_words[sw + 1] << (64 - shift);
            }
            return bits;
        }
        const std::size_t count = std::min<std::size_t>(64, rows() - start_row);
        std::uint64_t bits = 0;
        for (std::size_t bit = 0; bit < count; ++bit) {
            bits |= static_cast<std::uint64_t>((*col_)[start_row + bit]) << bit;
        }
        return bits;
    }

   private:
    const Column<bool>* col_;
};

/// The bool OutputWriter: a zero-filled, pre-sized bit window of a packed
/// destination. Writes only ever SET bits (the `or_bits_into_word` rule), so
/// disjoint windows are safe without coordination and shared boundary words
/// are OR-ed atomically.
struct BoolOutputSpan {
    std::uint64_t* words = nullptr;
    std::size_t begin = 0;  // first output bit this span writes
    std::size_t count = 0;  // bits to write
};

/// Non-owning view of a validity bitmap.  As with BoolView, `source_word`
/// normalizes the owned-word and Arrow-compatible external-byte layouts.  A
/// validity gather is deliberately a set-only operation: its destination is
/// pre-sized and zero-filled, and false source bits are left untouched.
class ValidityView {
   public:
    ValidityView(const ValidityBitmap& bitmap) noexcept : bitmap_(&bitmap) {}

    [[nodiscard]] auto rows() const noexcept -> std::size_t { return bitmap_->size(); }
    [[nodiscard]] auto bit(std::size_t row) const noexcept -> bool { return (*bitmap_)[row]; }

    /// The 64 logical validity bits at `[start_row, start_row + 64)`, with
    /// out-of-range bits clear.  An external bitmap may begin at any bit in
    /// its byte buffer, so read that representation bytewise rather than
    /// pretending its first logical bit is word-aligned.
    [[nodiscard]] auto source_word(std::size_t start_row) const noexcept -> std::uint64_t {
        if (!bitmap_->is_external()) {
            const auto* __restrict src_words = bitmap_->words_data();
            const std::size_t sw = start_row / 64;
            const auto shift = static_cast<unsigned>(start_row % 64);
            std::uint64_t bits = src_words[sw] >> shift;
            if (shift != 0 && sw + 1 < bitmap_->word_count()) {
                bits |= src_words[sw + 1] << (64 - shift);
            }
            return bits;
        }
        const std::size_t count = std::min<std::size_t>(64, rows() - start_row);
        std::uint64_t bits = 0;
        for (std::size_t bit = 0; bit < count; ++bit) {
            bits |= static_cast<std::uint64_t>((*bitmap_)[start_row + bit]) << bit;
        }
        return bits;
    }

   private:
    const ValidityBitmap* bitmap_;
};

/// The bool gather kernel. `RowWordBlocks` is the hot shape (filter output);
/// the index/bitmap shapes fall back to bit-at-a-time sets, which nothing
/// hot uses yet — correctness first, they exist so the Selection contract is
/// total.
inline auto gather_selected_bool(BoolView src, const Selection& selection,
                                 BoolOutputSpan out) noexcept -> void {
    const SharedBitWords shared = SharedBitWords::of_run(out.begin, out.count);
    std::size_t out_bit = out.begin;
    std::visit(
        [&](const auto& sel) {
            using S = std::decay_t<decltype(sel)>;
            if constexpr (std::is_same_v<S, RowWordBlocks>) {
                for (std::size_t w = 0; w < sel.word_count; ++w) {
                    const std::uint64_t select = sel.words[w];
                    if (select == 0) {
                        continue;
                    }
                    const std::uint64_t src_bits = src.source_word(sel.row_base + (w * 64));
                    const std::uint64_t packed = pack_selected_bool_bits(src_bits, select);
                    append_packed_bool_bits(packed, static_cast<std::size_t>(std::popcount(select)),
                                            out.words, out_bit, shared);
                }
            } else {
                const auto each = [&](std::size_t row) {
                    if (src.bit(row)) {
                        or_bits_into_word(out.words, out_bit / 64,
                                          std::uint64_t{1} << (out_bit % 64), shared);
                    }
                    ++out_bit;
                };
                if constexpr (std::is_same_v<S, RowRange>) {
                    for (std::size_t r = sel.begin; r < sel.end; ++r) {
                        each(r);
                    }
                } else if constexpr (std::is_same_v<S, RowIndices>) {
                    for (std::size_t i = 0; i < sel.count; ++i) {
                        each(sel[i]);
                    }
                } else {  // RowBitmap
                    for (std::size_t r = 0; r < src.rows(); ++r) {
                        if (sel.test(r)) {
                            each(r);
                        }
                    }
                }
            }
        },
        selection);
}

/// Gather validity bits through `selection` into a zero-filled output window.
/// This is intentionally not an assignment kernel: leaving false bits clear
/// makes every write monotonic, so windows that meet in a destination word can
/// use the same atomic-OR boundary rule as bool gathers.
inline auto gather_selected_validity(ValidityView src, const Selection& selection,
                                     BoolOutputSpan out) noexcept -> void {
    const SharedBitWords shared = SharedBitWords::of_run(out.begin, out.count);
    std::size_t out_bit = out.begin;
    std::visit(
        [&](const auto& sel) {
            using S = std::decay_t<decltype(sel)>;
            if constexpr (std::is_same_v<S, RowWordBlocks>) {
                for (std::size_t w = 0; w < sel.word_count; ++w) {
                    const std::uint64_t select = sel.words[w];
                    if (select == 0) {
                        continue;
                    }
                    const std::uint64_t valid = src.source_word(sel.row_base + (w * 64));
                    append_packed_bool_bits(pack_selected_bool_bits(valid, select),
                                            static_cast<std::size_t>(std::popcount(select)),
                                            out.words, out_bit, shared);
                }
            } else {
                const auto each = [&](std::size_t row) {
                    if (src.bit(row)) {
                        or_bits_into_word(out.words, out_bit / 64,
                                          std::uint64_t{1} << (out_bit % 64), shared);
                    }
                    ++out_bit;
                };
                if constexpr (std::is_same_v<S, RowRange>) {
                    for (std::size_t r = sel.begin; r < sel.end; ++r) {
                        each(r);
                    }
                } else if constexpr (std::is_same_v<S, RowIndices>) {
                    for (std::size_t i = 0; i < sel.count; ++i) {
                        each(sel[i]);
                    }
                } else {  // RowBitmap
                    for (std::size_t r = 0; r < src.rows(); ++r) {
                        if (sel.test(r)) {
                            each(r);
                        }
                    }
                }
            }
        },
        selection);
}

/// Non-owning view of a string column: row `r`'s bytes are
/// `[chars + offsets[r], chars + offsets[r+1])`. Offsets are uint32.
struct StringView {
    const std::uint32_t* offsets = nullptr;  // rows + 1 entries
    const char* chars = nullptr;
    std::size_t rows = 0;

    [[nodiscard]] auto row_len(std::size_t row) const noexcept -> std::uint32_t {
        return offsets[row + 1] - offsets[row];
    }
};

/// The string OutputWriter: a pre-sized window of a `resize_for_gather`'d
/// destination. `char_base` is the destination byte offset where this window's
/// first row starts (zero for the serial whole-output gather; a prefix-summed
/// per-morsel offset in the parallel two-phase shape). Writes only the END
/// offset of each produced row — `offsets[begin]` is char_base's business,
/// set once by the presize pass or the previous window.
struct StringOutputSpan {
    std::uint32_t* offsets = nullptr;
    char* chars = nullptr;
    std::size_t begin = 0;        // first output row
    std::size_t count = 0;        // rows to write
    std::uint32_t char_base = 0;  // destination byte of output row `begin`
};

/// The string gather kernel: memcpy each selected row's slab, end offset per
/// row. `RowRange` could be one big memcpy + shifted offset copy; every other
/// shape walks the selection and copies row slabs — which is what the filter
/// arm always did, so the port is loop-identical.
inline auto gather_selected_strings(StringView src, const Selection& selection,
                                    StringOutputSpan out) noexcept -> void {
    std::visit(
        [&](const auto& sel) {
            using S = std::decay_t<decltype(sel)>;
            auto cur = out.char_base;
            std::size_t j = out.begin;
            const auto each = [&](std::size_t row) {
                const std::uint32_t start = src.offsets[row];
                const std::uint32_t len = src.offsets[row + 1] - start;
                if (len != 0) {
                    std::memcpy(out.chars + cur, src.chars + start, len);
                }
                cur += len;
                out.offsets[++j] = cur;
            };
            if constexpr (std::is_same_v<S, RowRange>) {
                for (std::size_t r = sel.begin; r < sel.end; ++r) {
                    each(r);
                }
            } else if constexpr (std::is_same_v<S, RowIndices>) {
                for (std::size_t i = 0; i < sel.count; ++i) {
                    each(sel[i]);
                }
            } else if constexpr (std::is_same_v<S, RowBitmap>) {
                for (std::size_t r = 0; r < src.rows; ++r) {
                    if (sel.test(r)) {
                        each(r);
                    }
                }
            } else {  // RowWordBlocks
                for (std::size_t w = 0; w < sel.word_count; ++w) {
                    std::uint64_t bits = sel.words[w];
                    const std::size_t base = sel.row_base + (w * 64);
                    while (bits != 0) {
                        const auto bit = static_cast<std::size_t>(std::countr_zero(bits));
                        each(base + bit);
                        bits &= (bits - 1);
                    }
                }
            }
        },
        selection);
}

}  // namespace ibex::runtime::kernel
