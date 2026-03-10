#include <ibex/parser/effects.hpp>

#include <fmt/core.h>

#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <utility>

namespace ibex::parser {

namespace {

auto has_effect(EffectMask mask, EffectMask bit) -> bool {
    return (mask & bit) != 0;
}

auto make_effect_summary(EffectMask mask, bool io_read_unscoped = false,
                         bool io_write_unscoped = false) -> EffectSummary {
    return EffectSummary{
        .mask = mask,
        .io_read_unscoped = io_read_unscoped,
        .io_write_unscoped = io_write_unscoped,
        .io_read_resources = {},
        .io_write_resources = {},
    };
}

auto make_builtin_summary(EffectMask mask, bool io_read_unscoped = false,
                          bool io_write_unscoped = false) -> CallableSummary {
    return CallableSummary{
        .effects = make_effect_summary(mask, io_read_unscoped, io_write_unscoped),
        .param_modes = {},
        .is_extern = false,
    };
}

void merge_effect_summary(EffectSummary& dst, const EffectSummary& src) {
    dst.mask |= src.mask;
    dst.io_read_unscoped = dst.io_read_unscoped || src.io_read_unscoped;
    dst.io_write_unscoped = dst.io_write_unscoped || src.io_write_unscoped;
    dst.io_read_resources.insert(src.io_read_resources.begin(), src.io_read_resources.end());
    dst.io_write_resources.insert(src.io_write_resources.begin(), src.io_write_resources.end());
}

auto all_core_effects() -> EffectSummary {
    EffectSummary summary;
    summary.mask = kEffAllCore;
    summary.io_read_unscoped = true;
    summary.io_write_unscoped = true;
    return summary;
}

auto missing_effects(const EffectSummary& declared, const EffectSummary& inferred)
    -> EffectSummary {
    EffectSummary missing;

    constexpr EffectMask kNonIoMask = kEffNondet | kEffState | kEffBlocking | kEffMayFail;
    missing.mask |= (inferred.mask & kNonIoMask) & ~(declared.mask & kNonIoMask);

    const bool inferred_read = has_effect(inferred.mask, kEffIoRead);
    const bool declared_read = has_effect(declared.mask, kEffIoRead);
    if (inferred_read) {
        if (!declared_read) {
            missing.mask |= kEffIoRead;
            missing.io_read_unscoped = inferred.io_read_unscoped;
            missing.io_read_resources = inferred.io_read_resources;
        } else if (!declared.io_read_unscoped) {
            if (inferred.io_read_unscoped) {
                missing.mask |= kEffIoRead;
                missing.io_read_unscoped = true;
            }
            for (const auto& resource : inferred.io_read_resources) {
                if (!declared.io_read_resources.contains(resource)) {
                    missing.mask |= kEffIoRead;
                    missing.io_read_resources.insert(resource);
                }
            }
        }
    }

    const bool inferred_write = has_effect(inferred.mask, kEffIoWrite);
    const bool declared_write = has_effect(declared.mask, kEffIoWrite);
    if (inferred_write) {
        if (!declared_write) {
            missing.mask |= kEffIoWrite;
            missing.io_write_unscoped = inferred.io_write_unscoped;
            missing.io_write_resources = inferred.io_write_resources;
        } else if (!declared.io_write_unscoped) {
            if (inferred.io_write_unscoped) {
                missing.mask |= kEffIoWrite;
                missing.io_write_unscoped = true;
            }
            for (const auto& resource : inferred.io_write_resources) {
                if (!declared.io_write_resources.contains(resource)) {
                    missing.mask |= kEffIoWrite;
                    missing.io_write_resources.insert(resource);
                }
            }
        }
    }

    return missing;
}

auto effect_summaries_equal(const EffectSummary& lhs, const EffectSummary& rhs) -> bool {
    return lhs.mask == rhs.mask && lhs.io_read_unscoped == rhs.io_read_unscoped &&
           lhs.io_write_unscoped == rhs.io_write_unscoped &&
           lhs.io_read_resources == rhs.io_read_resources &&
           lhs.io_write_resources == rhs.io_write_resources;
}

auto builtin_summaries() -> const std::unordered_map<std::string, CallableSummary>& {
    static const std::unordered_map<std::string, CallableSummary> builtins = {
        {"print", make_builtin_summary(kEffIoWrite, false, true)},
        {"seed_rng", make_builtin_summary(kEffState)},
        {"rand_uniform", make_builtin_summary(kEffNondet)},
        {"rand_normal", make_builtin_summary(kEffNondet)},
        {"rand_student_t", make_builtin_summary(kEffNondet)},
        {"rand_gamma", make_builtin_summary(kEffNondet)},
        {"rand_exponential", make_builtin_summary(kEffNondet)},
        {"rand_bernoulli", make_builtin_summary(kEffNondet)},
        {"rand_poisson", make_builtin_summary(kEffNondet)},
        {"rand_int", make_builtin_summary(kEffNondet)},
    };
    return builtins;
}

class EffectAnalyzer {
   public:
    explicit EffectAnalyzer(const Program& program) : program_(program) {}

    auto run() -> std::expected<EffectAnalysis, ParseError> {
        collect_declarations();
        compute_user_function_effects();
        auto annotation_error = verify_annotations();
        if (annotation_error.has_value()) {
            return std::unexpected(*annotation_error);
        }
        return build_analysis();
    }

   private:
    void collect_declarations() {
        for (const auto& stmt : program_.statements) {
            if (const auto* fn = std::get_if<FunctionDecl>(&stmt)) {
                user_functions_.insert_or_assign(fn->name, fn);
                continue;
            }
            const auto* ext = std::get_if<ExternDecl>(&stmt);
            if (ext == nullptr) {
                continue;
            }
            CallableSummary summary;
            summary.effects = ext->effects.has_value() ? effect_summary_from_specs(ext->effects)
                                                       : all_core_effects();
            summary.is_extern = true;
            summary.param_modes.reserve(ext->params.size());
            for (const auto& param : ext->params) {
                summary.param_modes.push_back(param.effect);
            }
            externs_.insert_or_assign(ext->name, std::move(summary));
        }
    }

    void apply_call_effect(const std::string& callee, EffectSummary& direct,
                           std::unordered_set<std::string>& deps) const {
        if (user_functions_.contains(callee)) {
            deps.insert(callee);
            return;
        }
        if (auto it = externs_.find(callee); it != externs_.end()) {
            merge_effect_summary(direct, it->second.effects);
            return;
        }
        if (auto it = builtin_summaries().find(callee); it != builtin_summaries().end()) {
            merge_effect_summary(direct, it->second.effects);
        }
    }

    void collect_clause_effects(const Clause& clause, EffectSummary& direct,
                                std::unordered_set<std::string>& deps) const {
        std::visit(
            [&](const auto& c) {
                using T = std::decay_t<decltype(c)>;
                const auto collect_field_expr = [&](const Field& f) {
                    if (f.expr) {
                        collect_expr_effects(*f.expr, direct, deps);
                    }
                };
                if constexpr (std::is_same_v<T, FilterClause>) {
                    collect_expr_effects(*c.predicate, direct, deps);
                } else if constexpr (std::is_same_v<T, SelectClause>) {
                    for (const auto& f : c.fields) {
                        collect_field_expr(f);
                    }
                    for (const auto& tf : c.tuple_fields) {
                        collect_expr_effects(*tf.expr, direct, deps);
                    }
                } else if constexpr (std::is_same_v<T, DistinctClause>) {
                    for (const auto& f : c.fields) {
                        collect_field_expr(f);
                    }
                } else if constexpr (std::is_same_v<T, UpdateClause>) {
                    for (const auto& f : c.fields) {
                        collect_field_expr(f);
                    }
                    for (const auto& tf : c.tuple_fields) {
                        collect_expr_effects(*tf.expr, direct, deps);
                    }
                    if (c.merge_expr) {
                        collect_expr_effects(*c.merge_expr, direct, deps);
                    }
                } else if constexpr (std::is_same_v<T, RenameClause>) {
                    for (const auto& f : c.fields) {
                        collect_field_expr(f);
                    }
                } else if constexpr (std::is_same_v<T, ByClause>) {
                    for (const auto& f : c.keys) {
                        collect_field_expr(f);
                    }
                } else if constexpr (std::is_same_v<T, MeltClause>) {
                    for (const auto& f : c.id_fields) {
                        collect_field_expr(f);
                    }
                }
            },
            clause);
    }

    void collect_expr_effects(const Expr& expr, EffectSummary& direct,
                              std::unordered_set<std::string>& deps) const {
        std::visit(
            [&](const auto& node) {
                using T = std::decay_t<decltype(node)>;
                if constexpr (std::is_same_v<T, CallExpr>) {
                    apply_call_effect(node.callee, direct, deps);
                    for (const auto& arg : node.args) {
                        collect_expr_effects(*arg, direct, deps);
                    }
                    for (const auto& named : node.named_args) {
                        collect_expr_effects(*named.value, direct, deps);
                    }
                } else if constexpr (std::is_same_v<T, UnaryExpr>) {
                    collect_expr_effects(*node.expr, direct, deps);
                } else if constexpr (std::is_same_v<T, BinaryExpr>) {
                    collect_expr_effects(*node.left, direct, deps);
                    collect_expr_effects(*node.right, direct, deps);
                } else if constexpr (std::is_same_v<T, GroupExpr>) {
                    collect_expr_effects(*node.expr, direct, deps);
                } else if constexpr (std::is_same_v<T, BlockExpr>) {
                    if (node.base) {
                        collect_expr_effects(*node.base, direct, deps);
                    }
                    for (const auto& clause : node.clauses) {
                        collect_clause_effects(clause, direct, deps);
                    }
                } else if constexpr (std::is_same_v<T, JoinExpr>) {
                    collect_expr_effects(*node.left, direct, deps);
                    collect_expr_effects(*node.right, direct, deps);
                    if (node.predicate.has_value()) {
                        collect_expr_effects(*node.predicate.value(), direct, deps);
                    }
                } else if constexpr (std::is_same_v<T, StreamExpr>) {
                    collect_expr_effects(*node.source, direct, deps);
                    for (const auto& clause : node.transform) {
                        collect_clause_effects(clause, direct, deps);
                    }
                    apply_call_effect(node.sink_callee, direct, deps);
                    for (const auto& arg : node.sink_args) {
                        collect_expr_effects(*arg, direct, deps);
                    }
                } else if constexpr (std::is_same_v<T, ArrayLiteralExpr>) {
                    for (const auto& elem : node.elements) {
                        collect_expr_effects(*elem, direct, deps);
                    }
                } else if constexpr (std::is_same_v<T, TableExpr>) {
                    for (const auto& col : node.columns) {
                        collect_expr_effects(*col.expr, direct, deps);
                    }
                }
            },
            expr.node);
    }

    void compute_user_function_effects() {
        for (const auto& [name, fn] : user_functions_) {
            EffectSummary direct;
            std::unordered_set<std::string> deps;
            for (const auto& stmt : fn->body) {
                std::visit(
                    [&](const auto& s) {
                        using T = std::decay_t<decltype(s)>;
                        if constexpr (std::is_same_v<T, LetStmt>) {
                            collect_expr_effects(*s.value, direct, deps);
                        } else if constexpr (std::is_same_v<T, TupleLetStmt>) {
                            collect_expr_effects(*s.value, direct, deps);
                        } else if constexpr (std::is_same_v<T, ExprStmt>) {
                            collect_expr_effects(*s.expr, direct, deps);
                        }
                    },
                    stmt);
            }

            deps.erase(name);  // recursion is resolved by fixpoint.
            direct_effects_.insert_or_assign(name, direct);
            dep_graph_.insert_or_assign(name, std::move(deps));
            inferred_effects_.insert_or_assign(name, direct);
        }

        bool changed = true;
        while (changed) {
            changed = false;
            for (const auto& [name, _] : user_functions_) {
                (void)_;
                EffectSummary next = direct_effects_[name];
                if (auto dep_it = dep_graph_.find(name); dep_it != dep_graph_.end()) {
                    for (const auto& dep_name : dep_it->second) {
                        if (auto inferred_dep = inferred_effects_.find(dep_name);
                            inferred_dep != inferred_effects_.end()) {
                            merge_effect_summary(next, inferred_dep->second);
                        }
                    }
                }
                if (!effect_summaries_equal(next, inferred_effects_[name])) {
                    inferred_effects_[name] = std::move(next);
                    changed = true;
                }
            }
        }
    }

    auto verify_annotations() const -> std::optional<ParseError> {
        for (const auto& [name, fn] : user_functions_) {
            (void)name;
            if (!fn->effects.has_value()) {
                continue;
            }
            const EffectSummary declared = effect_summary_from_specs(fn->effects);
            const EffectSummary inferred = inferred_effects_.at(fn->name);
            const EffectSummary missing = missing_effects(declared, inferred);
            if (missing.is_pure()) {
                continue;
            }
            return ParseError{
                .message = fmt::format("function '{}' effect annotation missing: {}", fn->name,
                                       format_effect_summary(missing)),
                .line = fn->start_line,
                .column = 1,
            };
        }
        return std::nullopt;
    }

    auto build_analysis() const -> EffectAnalysis {
        EffectAnalysis analysis;
        analysis.externs = externs_;
        analysis.builtins = builtin_summaries();

        for (const auto& [name, fn] : user_functions_) {
            CallableSummary summary;
            summary.effects = inferred_effects_.at(name);
            summary.param_modes.reserve(fn->params.size());
            for (const auto& param : fn->params) {
                summary.param_modes.push_back(param.effect);
            }
            analysis.user_functions.insert_or_assign(name, std::move(summary));
        }

        return analysis;
    }

    const Program& program_;
    std::unordered_map<std::string, const FunctionDecl*> user_functions_;
    std::unordered_map<std::string, CallableSummary> externs_;
    std::unordered_map<std::string, EffectSummary> direct_effects_;
    std::unordered_map<std::string, std::unordered_set<std::string>> dep_graph_;
    std::unordered_map<std::string, EffectSummary> inferred_effects_;
};

}  // namespace

auto EffectAnalysis::find_callee(std::string_view name) const -> const CallableSummary* {
    if (auto it = user_functions.find(std::string(name)); it != user_functions.end()) {
        return &it->second;
    }
    if (auto it = externs.find(std::string(name)); it != externs.end()) {
        return &it->second;
    }
    if (auto it = builtins.find(std::string(name)); it != builtins.end()) {
        return &it->second;
    }
    return nullptr;
}

auto EffectAnalysis::merged_callee_summaries() const
    -> std::unordered_map<std::string, CallableSummary> {
    std::unordered_map<std::string, CallableSummary> out;
    out.reserve(user_functions.size() + externs.size() + builtins.size());
    out.insert(user_functions.begin(), user_functions.end());
    out.insert(externs.begin(), externs.end());
    out.insert(builtins.begin(), builtins.end());
    return out;
}

auto effect_kind_name(EffectKind kind) -> std::string_view {
    switch (kind) {
        case EffectKind::IoRead:
            return "io_read";
        case EffectKind::IoWrite:
            return "io_write";
        case EffectKind::Nondet:
            return "nondet";
        case EffectKind::State:
            return "state";
        case EffectKind::Blocking:
            return "blocking";
        case EffectKind::MayFail:
            return "may_fail";
    }
    return "unknown";
}

auto effect_kind_mask(EffectKind kind) -> EffectMask {
    switch (kind) {
        case EffectKind::IoRead:
            return kEffIoRead;
        case EffectKind::IoWrite:
            return kEffIoWrite;
        case EffectKind::Nondet:
            return kEffNondet;
        case EffectKind::State:
            return kEffState;
        case EffectKind::Blocking:
            return kEffBlocking;
        case EffectKind::MayFail:
            return kEffMayFail;
    }
    return 0;
}

auto effect_summary_from_specs(const std::optional<std::vector<EffectSpec>>& effects)
    -> EffectSummary {
    EffectSummary out;
    if (!effects.has_value()) {
        return out;
    }
    for (const auto& effect : *effects) {
        out.mask |= effect_kind_mask(effect.kind);
        if (effect.kind == EffectKind::IoRead) {
            if (effect.resource.has_value()) {
                out.io_read_resources.insert(*effect.resource);
            } else {
                out.io_read_unscoped = true;
            }
        } else if (effect.kind == EffectKind::IoWrite) {
            if (effect.resource.has_value()) {
                out.io_write_resources.insert(*effect.resource);
            } else {
                out.io_write_unscoped = true;
            }
        }
    }
    return out;
}

auto format_effect_summary(const EffectSummary& effects) -> std::string {
    std::vector<std::string> parts;

    if (has_effect(effects.mask, kEffIoRead)) {
        if (effects.io_read_unscoped) {
            parts.emplace_back("io_read");
        }
        for (const auto& resource : effects.io_read_resources) {
            parts.push_back("io_read(\"" + resource + "\")");
        }
    }
    if (has_effect(effects.mask, kEffIoWrite)) {
        if (effects.io_write_unscoped) {
            parts.emplace_back("io_write");
        }
        for (const auto& resource : effects.io_write_resources) {
            parts.push_back("io_write(\"" + resource + "\")");
        }
    }
    if (has_effect(effects.mask, kEffNondet)) {
        parts.emplace_back("nondet");
    }
    if (has_effect(effects.mask, kEffState)) {
        parts.emplace_back("state");
    }
    if (has_effect(effects.mask, kEffBlocking)) {
        parts.emplace_back("blocking");
    }
    if (has_effect(effects.mask, kEffMayFail)) {
        parts.emplace_back("may_fail");
    }
    if (parts.empty()) {
        return "<none>";
    }

    std::string out;
    for (std::size_t i = 0; i < parts.size(); ++i) {
        if (i != 0) {
            out.append(", ");
        }
        out.append(parts[i]);
    }
    return out;
}

auto analyze_effects(const Program& program) -> std::expected<EffectAnalysis, ParseError> {
    EffectAnalyzer analyzer(program);
    return analyzer.run();
}

}  // namespace ibex::parser
