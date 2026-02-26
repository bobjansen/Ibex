#include "gen_tf_data.hpp"

#include <ibex/runtime/extern_registry.hpp>

extern "C" void ibex_register(ibex::runtime::ExternRegistry* registry) {
    registry->register_table("gen_tf_data", [](const ibex::runtime::ExternArgs& args) {
        if (args.size() != 1)
            throw std::invalid_argument("gen_tf_data: expected 1 argument (n: Int)");
        const auto* n_val = std::get_if<std::int64_t>(&args[0]);
        if (!n_val)
            throw std::invalid_argument("gen_tf_data: argument must be Int");
        return gen_tf_data(*n_val);
    });
}
