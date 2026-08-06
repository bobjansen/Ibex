#include <ibex/ir/join_output.hpp>

#include <algorithm>
#include <unordered_set>

namespace ibex::ir {

auto plan_join_output(JoinKind kind, const std::vector<JoinKey>& keys,
                      std::span<const std::string_view> left_names,
                      std::span<const std::string_view> right_names)
    -> std::vector<JoinOutputColumn> {
    std::vector<JoinOutputColumn> plan;
    plan.reserve(left_names.size() + right_names.size());

    std::unordered_set<std::string> taken;
    taken.reserve(left_names.size() + right_names.size());

    for (std::size_t i = 0; i < left_names.size(); ++i) {
        plan.push_back(JoinOutputColumn{
            .side = JoinOutputSide::Left, .source_index = i, .name = std::string(left_names[i])});
        taken.insert(plan.back().name);
    }

    if (kind == JoinKind::Semi || kind == JoinKind::Anti) {
        return plan;
    }

    for (std::size_t i = 0; i < right_names.size(); ++i) {
        const std::string_view source = right_names[i];
        // A same-name key is one column in the output; a mapped key keeps both.
        const bool shared_key = std::ranges::any_of(
            keys, [&](const JoinKey& key) { return key.left == key.right && key.right == source; });
        if (shared_key) {
            continue;
        }
        std::string name(source);
        while (taken.contains(name)) {
            name += "_right";
        }
        plan.push_back(JoinOutputColumn{
            .side = JoinOutputSide::Right, .source_index = i, .name = std::move(name)});
        taken.insert(plan.back().name);
    }

    return plan;
}

}  // namespace ibex::ir
