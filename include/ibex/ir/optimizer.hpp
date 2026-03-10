#pragma once

#include <ibex/ir/node.hpp>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace ibex::ir {

using EffectMask = std::uint32_t;

constexpr EffectMask kEffIoRead = 1U << 0;
constexpr EffectMask kEffIoWrite = 1U << 1;
constexpr EffectMask kEffNondet = 1U << 2;
constexpr EffectMask kEffState = 1U << 3;
constexpr EffectMask kEffBlocking = 1U << 4;
constexpr EffectMask kEffMayFail = 1U << 5;
constexpr EffectMask kEffAllCore =
    kEffIoRead | kEffIoWrite | kEffNondet | kEffState | kEffBlocking | kEffMayFail;

enum class ArgMode : std::uint8_t {
    Const,
    Mutable,
    Consume,
};

struct EffectSummary {
    EffectMask mask = 0;
    bool io_read_unscoped = false;
    bool io_write_unscoped = false;
    std::unordered_set<std::string> io_read_resources;
    std::unordered_set<std::string> io_write_resources;

    [[nodiscard]] auto is_pure() const noexcept -> bool { return mask == 0; }
};

struct CallableSummary {
    EffectSummary effects;
    std::vector<ArgMode> arg_modes;
};

struct OptimizationContext {
    std::unordered_map<std::string, CallableSummary> callee_summaries;
    CallableSummary unknown_callee = {
        .effects =
            EffectSummary{
                .mask = kEffAllCore,
                .io_read_unscoped = true,
                .io_write_unscoped = true,
                .io_read_resources = {},
                .io_write_resources = {},
            },
        .arg_modes = {},
    };
};

struct OptimizationStats {
    std::size_t removed_dead_preamble_calls = 0;
};

[[nodiscard]] auto is_elidable(const EffectSummary& effects) -> bool;
[[nodiscard]] auto can_cse(const CallableSummary& callable) -> bool;
[[nodiscard]] auto is_reorderable(const EffectSummary& lhs, const EffectSummary& rhs) -> bool;

class OptimizationPass {
   public:
    OptimizationPass() = default;
    OptimizationPass(const OptimizationPass&) = delete;
    OptimizationPass& operator=(const OptimizationPass&) = delete;
    OptimizationPass(OptimizationPass&&) = delete;
    OptimizationPass& operator=(OptimizationPass&&) = delete;
    virtual ~OptimizationPass() = default;
    virtual auto run(NodePtr root, const OptimizationContext& context,
                     OptimizationStats& stats) const -> NodePtr = 0;
};

class PassManager {
   public:
    void add_pass(std::unique_ptr<OptimizationPass> pass);
    [[nodiscard]] auto run(NodePtr root, const OptimizationContext& context,
                           OptimizationStats* stats = nullptr) const -> NodePtr;

   private:
    std::vector<std::unique_ptr<OptimizationPass>> passes_;
};

[[nodiscard]] auto make_default_pass_manager() -> PassManager;
[[nodiscard]] auto optimize_plan(NodePtr root, const OptimizationContext& context,
                                 OptimizationStats* stats = nullptr) -> NodePtr;

}  // namespace ibex::ir
