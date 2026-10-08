// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#pragma once

#include <cmath>
#include <cstddef>

namespace ibex::runtime {

/// One Welford step: fold x into a running mean and M2 = Σ(x - mean)^2.
/// `count` is the number of values folded in, x included.
///
/// Every std() path goes through this so they agree bit for bit. The M2
/// update is an explicit fma: left as `m2 += a * b`, GCC's default
/// -ffp-contract=fast fuses it at some call sites and not others, depending on
/// inlining, and the paths then round differently. std::fma is correctly
/// rounded on every compiler, whether or not the target has an FMA unit.
inline void welford_add(double x, std::size_t count, double& mean, double& m2) noexcept {
    const double delta = x - mean;
    mean += delta / static_cast<double>(count);
    m2 = std::fma(delta, x - mean, m2);
}

}  // namespace ibex::runtime
