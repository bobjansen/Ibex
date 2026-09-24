// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#pragma once

#include <algorithm>
#include <array>
#include <bit>
#include <cmath>
#include <cstddef>
#include <cstdint>

namespace ibex::runtime {

/// HyperLogLog distinct-count estimate (Flajolet, Fusy, Gandouet, Meunier,
/// 2007), with linear counting for small cardinalities (Heule, Nunkesser,
/// Hall, 2013 use the same switch). 2^10 one-byte registers: 1 KiB, a standard
/// error of about 1.04 / sqrt(1024) = 3.3%.
///
/// Used to pre-size hash sets whose final size is not known up front (the
/// parallel `distinct`'s partitions): a set that starts empty and doubles its
/// way to 100k+ entries spends a large share of its time rehashing. An estimate
/// that is off by a few percent only means a little of that growth comes back;
/// it can never change an answer.
///
/// Takes WELL-MIXED 64-bit hashes (run a table hash through
/// `key_hash_finalize` first). The top 10 bits pick the register, and the
/// position of the first set bit in the rest is the observation, so both halves
/// must be uniformly distributed and independent of whatever bits a caller
/// uses for its own purposes (the partition mask uses the low bits).
class HyperLogLog {
   public:
    static constexpr unsigned kPrecision = 10;
    static constexpr std::size_t kRegisters = std::size_t{1} << kPrecision;

    void add(std::uint64_t hash) noexcept {
        const auto index = static_cast<std::size_t>(hash >> (64U - kPrecision));
        // The remaining 54 bits, with a guard bit below them so the count of
        // leading zeros is bounded even for an all-zero remainder.
        const std::uint64_t rest = (hash << kPrecision) | (std::uint64_t{1} << (kPrecision - 1));
        const auto rank = static_cast<std::uint8_t>(std::countl_zero(rest) + 1);
        registers_[index] = std::max(registers_[index], rank);
    }

    /// Fold another sketch in: the result is the sketch of the union.
    void merge(const HyperLogLog& other) noexcept {
        for (std::size_t i = 0; i < kRegisters; ++i) {
            registers_[i] = std::max(registers_[i], other.registers_[i]);
        }
    }

    [[nodiscard]] auto estimate() const noexcept -> double {
        constexpr auto m = static_cast<double>(kRegisters);
        constexpr double alpha = 0.7213 / (1.0 + (1.079 / m));
        double inverse_sum = 0.0;
        std::size_t zeros = 0;
        for (const std::uint8_t r : registers_) {
            inverse_sum += std::ldexp(1.0, -static_cast<int>(r));
            zeros += r == 0 ? 1U : 0U;
        }
        const double raw = alpha * m * m / inverse_sum;
        // Small range: while registers are still empty, counting them
        // ("linear counting") is far more accurate than the harmonic mean.
        if (raw <= 2.5 * m && zeros > 0) {
            return m * std::log(m / static_cast<double>(zeros));
        }
        return raw;
    }

    [[nodiscard]] auto operator==(const HyperLogLog&) const -> bool = default;

   private:
    std::array<std::uint8_t, kRegisters> registers_{};
};

}  // namespace ibex::runtime
