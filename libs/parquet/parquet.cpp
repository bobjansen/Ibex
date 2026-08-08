// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#include <ibex/parquet/backend.hpp>
#include <ibex/runtime/extern_registry.hpp>

extern "C" IBEX_PLUGIN_EXPORT void ibex_register(ibex::runtime::ExternRegistry* registry) {
    ibex::parquet::register_backend(*registry);
}
