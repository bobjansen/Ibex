// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#pragma once

// Compiler-specific optimization annotations.  Keep these behind project
// macros so public Ibex headers remain consumable with MSVC as well as
// GCC-compatible compilers.
#if defined(_MSC_VER)
#define IBEX_ALWAYS_INLINE __forceinline
#define IBEX_NOINLINE __declspec(noinline)
#elif defined(__clang__) || defined(__GNUC__)
#define IBEX_ALWAYS_INLINE [[gnu::always_inline]]
#define IBEX_NOINLINE [[gnu::noinline]]
#else
#define IBEX_ALWAYS_INLINE
#define IBEX_NOINLINE
#endif

#if defined(_MSC_VER) && !defined(__clang__)
#include <intrin.h>
#endif

namespace ibex::detail {

/// A read-prefetch hint for the cache line holding `addr`, into every cache
/// level: GCC/Clang's `__builtin_prefetch` default, and MSVC's intrinsics for
/// the same instruction (`prefetcht0` on x86/x64, `prfm pldl1keep` on ARM64).
inline void prefetch(const void* addr) noexcept {
#if defined(__clang__) || defined(__GNUC__)
    __builtin_prefetch(addr);
#elif defined(_MSC_VER) && (defined(_M_X64) || defined(_M_IX86))
    _mm_prefetch(static_cast<const char*>(addr), _MM_HINT_T0);
#elif defined(_MSC_VER) && defined(_M_ARM64)
    __prefetch(addr);
#else
    (void)addr;
#endif
}

}  // namespace ibex::detail
