// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#pragma once

#include <ibex/ir/join_output.hpp>
#include <ibex/ir/node.hpp>
#include <ibex/runtime/interpreter.hpp>
#include <ibex/runtime/lazy_table.hpp>
#include <ibex/runtime/operator.hpp>

#include <expected>
#include <functional>
#include <optional>
#include <string>

#include "physical_plan.hpp"

namespace ibex::runtime {

/// Copyable construction handle for one worker-private streaming join probe.
/// Attaching normally clones the pristine probe state for a worker; the
/// single-consumer path moves it directly into its operator. Both retain
/// shared ownership of the immutable hash build.
class JoinProbeFactory {
   public:
    using Attach = std::function<OperatorPtr(OperatorPtr, bool)>;

    JoinProbeFactory() = default;
    JoinProbeFactory(Attach attach, Attach attach_move, const ExecutionContext* exec)
        : attach_(std::move(attach)), attach_move_(std::move(attach_move)), exec_(exec) {}

    [[nodiscard]] auto attach(OperatorPtr child, bool preserve_empty_morsels = false) const
        -> OperatorPtr {
        return attach_(std::move(child), preserve_empty_morsels);
    }
    [[nodiscard]] auto attach_move(OperatorPtr child, bool preserve_empty_morsels = false)
        -> OperatorPtr {
        return attach_move_(std::move(child), preserve_empty_morsels);
    }
    [[nodiscard]] auto execution_context() const noexcept -> const ExecutionContext* {
        return exec_;
    }

   private:
    Attach attach_;
    Attach attach_move_;
    const ExecutionContext* exec_ = nullptr;
};

struct FusibleJoinProbe {
    Table probe_side;
    JoinProbeFactory probe;
};

struct DeferredProbeScan {
    const DeferredScan* scan = nullptr;
    const std::string* name = nullptr;
};

[[nodiscard]] auto deferred_probe_scan_of(const ir::Node& right, const ExecutionContext& exec)
    -> DeferredProbeScan;

/// Pipeline-owned adapter supplied by chunked.cpp. It may morselize an already
/// materialized probe side before attaching worker-private probes.
[[nodiscard]] auto make_join_probe_operator(OperatorPtr source,
                                            std::optional<Table> materialized_source,
                                            JoinProbeFactory probe)
    -> std::expected<OperatorPtr, std::string>;

[[nodiscard]] auto make_chunked_inner_join_operator(
    OperatorPtr left, Table right, const std::vector<ir::JoinKey>* keys,
    const ExecutionContext& exec, ir::JoinSuffixPolicy suffix = {},
    const std::vector<ir::OrderKey>* pending_order = nullptr,
    physical::JoinParallelism parallelism = {},
    std::optional<ir::JoinColumnMapping> columns = std::nullopt) -> OperatorPtr;

/// Streaming semi/anti join (single equi-key, `nulls never`). A separate
/// operator from the inner-join family by design — see the parent plan — but
/// lives in the same translation unit.
/// `right` is taken as an OPERATOR, not a materialized Table: the join reads
/// one column of it and never needs its rows contiguous, so materializing it
/// was a full serial copy of the right side for nothing.
[[nodiscard]] auto make_chunked_semi_anti_join_operator(OperatorPtr left, OperatorPtr right,
                                                        ir::JoinKind kind,
                                                        const std::vector<ir::JoinKey>* keys,
                                                        const ExecutionContext* exec)
    -> OperatorPtr;

[[nodiscard]] auto make_scheduled_chunked_inner_join_operator(
    OperatorPtr left, Table right, const std::vector<ir::JoinKey>* keys,
    const ExecutionContext& exec, ir::JoinSuffixPolicy suffix = {},
    const std::vector<ir::OrderKey>* pending_order = nullptr,
    physical::JoinParallelism parallelism = {},
    std::optional<ir::JoinColumnMapping> columns = std::nullopt)
    -> std::expected<OperatorPtr, std::string>;

[[nodiscard]] auto make_scheduled_deferred_inner_join_operator(
    OperatorPtr left, const ir::Node* right_node, const TableRegistry* registry,
    const ScalarRegistry* scalars, const ExternRegistry* externs, const ExecutionContext& exec,
    const std::vector<ir::JoinKey>* keys, const DeferredScan* probe, std::string probe_name,
    ir::JoinSuffixPolicy suffix = {}, const std::vector<ir::OrderKey>* pending_order = nullptr,
    physical::JoinParallelism parallelism = {},
    std::optional<ir::JoinColumnMapping> columns = std::nullopt)
    -> std::expected<OperatorPtr, std::string>;

[[nodiscard]] auto take_fusible_join_probe(OperatorPtr left, Table right,
                                           const std::vector<ir::JoinKey>* keys,
                                           const ExecutionContext& exec,
                                           ir::JoinSuffixPolicy suffix = {},
                                           const std::vector<ir::OrderKey>* pending_order = nullptr,
                                           physical::JoinParallelism parallelism = {})
    -> std::expected<std::optional<FusibleJoinProbe>, std::string>;

}  // namespace ibex::runtime
