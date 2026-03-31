#pragma once

#include <ibex/runtime/interpreter.hpp>

#include <expected>
#include <string>
#include <vector>

#include "runtime_internal.hpp"

namespace ibex::runtime {

using PredicateMaskEvaluator = std::expected<Mask, std::string> (*)(const ir::FilterExpr& predicate,
                                                                    const Table& table,
                                                                    const ScalarRegistry* scalars,
                                                                    std::size_t n);

[[nodiscard]] auto join_table_impl(const Table& left, const Table& right, ir::JoinKind kind,
                                   const std::vector<std::string>& keys,
                                   const ir::FilterExpr* predicate, const ScalarRegistry* scalars,
                                   PredicateMaskEvaluator mask_evaluator)
    -> std::expected<Table, std::string>;

}  // namespace ibex::runtime
