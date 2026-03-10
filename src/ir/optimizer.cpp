#include <ibex/ir/optimizer.hpp>

#include <algorithm>
#include <utility>

namespace ibex::ir {

namespace {

auto has_effect(EffectMask mask, EffectMask bit) -> bool {
    return (mask & bit) != 0;
}

auto has_non_io_effects(EffectMask mask) -> bool {
    return has_effect(mask, kEffNondet) || has_effect(mask, kEffState) ||
           has_effect(mask, kEffBlocking) || has_effect(mask, kEffMayFail);
}

auto resources_overlap(const std::unordered_set<std::string>& lhs,
                       const std::unordered_set<std::string>& rhs) -> bool {
    if (lhs.empty() || rhs.empty()) {
        return false;
    }
    if (lhs.size() < rhs.size()) {
        for (const auto& value : lhs) {
            if (rhs.contains(value)) {
                return true;
            }
        }
        return false;
    }
    for (const auto& value : rhs) {
        if (lhs.contains(value)) {
            return true;
        }
    }
    return false;
}

auto io_conflict(const EffectSummary& read_side, const EffectSummary& write_side) -> bool {
    if (!has_effect(read_side.mask, kEffIoRead) || !has_effect(write_side.mask, kEffIoWrite)) {
        return false;
    }
    if (read_side.io_read_unscoped || write_side.io_write_unscoped) {
        return true;
    }
    return resources_overlap(read_side.io_read_resources, write_side.io_write_resources);
}

auto write_conflict(const EffectSummary& lhs, const EffectSummary& rhs) -> bool {
    if (!has_effect(lhs.mask, kEffIoWrite) || !has_effect(rhs.mask, kEffIoWrite)) {
        return false;
    }
    if (lhs.io_write_unscoped || rhs.io_write_unscoped) {
        return true;
    }
    return resources_overlap(lhs.io_write_resources, rhs.io_write_resources);
}

class DeadPurePreamblePass final : public OptimizationPass {
   public:
    auto run(NodePtr root, const OptimizationContext& context, OptimizationStats& stats) const
        -> NodePtr override {
        if (!root || root->kind() != NodeKind::Program) {
            return root;
        }

        auto* program = dynamic_cast<ProgramNode*>(root.get());
        if (program == nullptr) {
            return root;
        }

        auto& preamble = program->mutable_preamble();
        preamble.erase(
            std::remove_if(preamble.begin(), preamble.end(),
                           [&](const NodePtr& node) {
                               if (!node || node->kind() != NodeKind::ExternCall) {
                                   return false;
                               }
                               const auto& call = static_cast<const ExternCallNode&>(*node);
                               const auto it = context.callee_summaries.find(call.callee());
                               const auto& summary = it != context.callee_summaries.end()
                                                         ? it->second
                                                         : context.unknown_callee;
                               if (!is_elidable(summary.effects)) {
                                   return false;
                               }
                               ++stats.removed_dead_preamble_calls;
                               return true;
                           }),
            preamble.end());

        if (preamble.empty() && program->mutable_main_node()) {
            return std::move(program->mutable_main_node());
        }
        return root;
    }
};

}  // namespace

auto is_elidable(const EffectSummary& effects) -> bool {
    return effects.is_pure();
}

auto can_cse(const CallableSummary& callable) -> bool {
    if (!is_elidable(callable.effects)) {
        return false;
    }
    return std::all_of(callable.arg_modes.begin(), callable.arg_modes.end(),
                       [](ArgMode mode) { return mode == ArgMode::Const; });
}

auto is_reorderable(const EffectSummary& lhs, const EffectSummary& rhs) -> bool {
    if (lhs.is_pure() || rhs.is_pure()) {
        // A pure call can be moved around any call that does not depend on
        // temporal effects.
        return !has_non_io_effects(lhs.mask | rhs.mask);
    }
    if (has_non_io_effects(lhs.mask) || has_non_io_effects(rhs.mask)) {
        return false;
    }

    if (write_conflict(lhs, rhs)) {
        return false;
    }
    if (io_conflict(lhs, rhs) || io_conflict(rhs, lhs)) {
        return false;
    }
    return true;
}

void PassManager::add_pass(std::unique_ptr<OptimizationPass> pass) {
    passes_.push_back(std::move(pass));
}

auto PassManager::run(NodePtr root, const OptimizationContext& context,
                      OptimizationStats* stats) const -> NodePtr {
    OptimizationStats local_stats;
    OptimizationStats& active_stats = stats != nullptr ? *stats : local_stats;

    for (const auto& pass : passes_) {
        root = pass->run(std::move(root), context, active_stats);
        if (!root) {
            break;
        }
    }
    return root;
}

auto make_default_pass_manager() -> PassManager {
    PassManager manager;
    manager.add_pass(std::make_unique<DeadPurePreamblePass>());
    return manager;
}

auto optimize_plan(NodePtr root, const OptimizationContext& context, OptimizationStats* stats)
    -> NodePtr {
    auto manager = make_default_pass_manager();
    return manager.run(std::move(root), context, stats);
}

}  // namespace ibex::ir
