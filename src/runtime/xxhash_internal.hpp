// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen
#pragma once
// Keep upstream C implementation warnings out of the runtime's C++ warning policy.
#if defined(__GNUC__)
#pragma GCC system_header
#endif
#define XXH_INLINE_ALL
#include "vendor/xxhash/xxhash.h"
#undef XXH_INLINE_ALL
