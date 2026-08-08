// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#pragma once

namespace ibex::runtime {
class ExternRegistry;
}

namespace ibex::parquet {

/// Register Ibex's first-party Parquet reader, lazy source, chunked source,
/// and writer with a host registry. Registration is idempotent.
void register_backend(runtime::ExternRegistry& registry);

}  // namespace ibex::parquet
