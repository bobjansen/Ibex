#pragma once

#include <ibex/runtime/extern_registry.hpp>
#include <ibex/runtime/interpreter.hpp>

#include <expected>
#include <string>
#include <vector>

namespace ibex::runtime {

[[nodiscard]] auto fit_model(const Table& input, const ir::ModelFormula& formula,
                             const std::string& method,
                             const std::vector<ir::ModelParamSpec>& params,
                             const ScalarRegistry* scalars, const ExternRegistry* externs)
    -> std::expected<ModelResult, std::string>;

}  // namespace ibex::runtime
