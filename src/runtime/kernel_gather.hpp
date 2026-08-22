// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#pragma once

#include <bit>
#include <cstddef>
#include <cstring>
#include <type_traits>
#include <variant>

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

}  // namespace ibex::runtime::kernel
