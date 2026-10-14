// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#pragma once

#include <algorithm>
#include <cmath>
#include <cstddef>
#include <optional>

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

/// The running co-moment of a pair (x, y): its count, both means, and the
/// centred sums Σ(x - mean_x)(y - mean_y), Σ(x - mean_x)², Σ(y - mean_y)² --
/// what cov() and corr() finish from. The same Welford-style update as
/// `welford_add`, and for the same reason its products are explicit fmas.
struct CoMoment {
    double n = 0.0;
    double mean_x = 0.0;
    double mean_y = 0.0;
    double cxy = 0.0;
    double m2x = 0.0;
    double m2y = 0.0;
};

inline void co_moment_add(CoMoment& m, double x, double y) noexcept {
    m.n += 1.0;
    const double dx = x - m.mean_x;
    const double dy = y - m.mean_y;
    m.mean_x += dx / m.n;
    m.mean_y += dy / m.n;
    m.cxy = std::fma(dx, y - m.mean_y, m.cxy);
    m.m2x = std::fma(dx, x - m.mean_x, m.m2x);
    m.m2y = std::fma(dy, y - m.mean_y, m.m2y);
}

/// Fold `b` into `a` (Chan et al.'s pairwise combination).
inline void co_moment_merge(CoMoment& a, const CoMoment& b) noexcept {
    if (b.n == 0.0) {
        return;
    }
    if (a.n == 0.0) {
        a = b;
        return;
    }
    const double n = a.n + b.n;
    const double dx = b.mean_x - a.mean_x;
    const double dy = b.mean_y - a.mean_y;
    const double w = a.n * b.n / n;
    a.cxy = std::fma(dx * dy, w, a.cxy + b.cxy);
    a.m2x = std::fma(dx * dx, w, a.m2x + b.m2x);
    a.m2y = std::fma(dy * dy, w, a.m2y + b.m2y);
    a.mean_x = std::fma(dx, b.n / n, a.mean_x);
    a.mean_y = std::fma(dy, b.n / n, a.mean_y);
    a.n = n;
}

/// Sample covariance (n - 1 denominator); none below two pairs.
[[nodiscard]] inline auto co_moment_cov(const CoMoment& m) noexcept -> std::optional<double> {
    if (m.n < 2.0) {
        return std::nullopt;
    }
    return m.cxy / (m.n - 1.0);
}

/// Pearson correlation; none below two pairs or when either side is constant
/// (its spread is zero, so the ratio is undefined -- SQL's CORR is null there
/// too). Clamped to [-1, 1] against rounding.
[[nodiscard]] inline auto co_moment_corr(const CoMoment& m) noexcept -> std::optional<double> {
    if (m.n < 2.0 || !(m.m2x > 0.0) || !(m.m2y > 0.0)) {
        return std::nullopt;
    }
    return std::clamp(m.cxy / std::sqrt(m.m2x * m.m2y), -1.0, 1.0);
}

}  // namespace ibex::runtime
