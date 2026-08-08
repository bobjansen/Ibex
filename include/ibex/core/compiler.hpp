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
