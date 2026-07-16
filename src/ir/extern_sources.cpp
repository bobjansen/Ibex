#include <ibex/ir/extern_sources.hpp>

#include <cstddef>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <type_traits>
#include <utility>
#include <variant>

namespace ibex::ir {
namespace {

auto literal_key(const Literal& literal) -> std::string {
    return std::visit(
        [](const auto& value) -> std::string {
            using T = std::decay_t<decltype(value)>;
            if constexpr (std::is_same_v<T, std::int64_t>) {
                return "i:" + std::to_string(value);
            } else if constexpr (std::is_same_v<T, double>) {
                return "d:" + std::to_string(value);
            } else if constexpr (std::is_same_v<T, bool>) {
                return value ? "b:1" : "b:0";
            } else if constexpr (std::is_same_v<T, std::string>) {
                return "s:" + std::to_string(value.size()) + ":" + value;
            } else {
                // Date and Timestamp have stable value semantics but are not
                // reader arguments in the bundled I/O APIs.  Do not merge
                // them until their canonical textual representation is part
                // of the IR API.
                return {};
            }
        },
        literal.value);
}

auto call_key(const ExternCallNode& call) -> std::optional<std::string> {
    std::string key = call.callee();
    key += '(';
    for (const auto& arg : call.args()) {
        const auto* literal = std::get_if<Literal>(&arg.node);
        if (literal == nullptr) {
            return std::nullopt;
        }
        auto encoded = literal_key(*literal);
        if (encoded.empty()) {
            return std::nullopt;
        }
        key += encoded;
        key += ';';
    }
    key += ')';
    return key;
}

void rewrite(NodePtr& node, const std::set<std::string>& eligible,
             std::map<std::string, std::string>& names, std::vector<ExternSource>& sources) {
    if (node == nullptr) {
        return;
    }
    if (node->kind() == NodeKind::ExternCall) {
        const auto& call = static_cast<const ExternCallNode&>(*node);
        if (!eligible.contains(call.callee())) {
            return;
        }
        const auto key = call_key(call);
        if (!key.has_value()) {
            return;
        }
        auto [it, inserted] = names.emplace(*key, "__ibex_source_" + std::to_string(names.size()));
        if (inserted) {
            sources.push_back(ExternSource{
                .source_name = it->second, .callee = call.callee(), .args = call.args()});
        }
        node = std::make_unique<ScanNode>(node->id(), it->second);
        return;
    }

    if (node->kind() == NodeKind::Program) {
        auto& program = static_cast<ProgramNode&>(*node);
        for (auto& preamble : program.mutable_preamble()) {
            rewrite(preamble, eligible, names, sources);
        }
        rewrite(program.mutable_main_node(), eligible, names, sources);
    }
    for (auto& child : node->mutable_children()) {
        rewrite(child, eligible, names, sources);
    }
}

}  // namespace

auto hoist_extern_sources(NodePtr root, const std::set<std::string>& eligible_callees)
    -> std::pair<NodePtr, std::vector<ExternSource>> {
    std::map<std::string, std::string> names;
    std::vector<ExternSource> sources;
    rewrite(root, eligible_callees, names, sources);
    return {std::move(root), std::move(sources)};
}

}  // namespace ibex::ir
