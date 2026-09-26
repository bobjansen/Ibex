// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#pragma once

#include <ibex/ir/join_output.hpp>
#include <ibex/ir/node.hpp>
#include <ibex/runtime/interpreter.hpp>
#include <ibex/runtime/lazy_table.hpp>
#include <ibex/runtime/operator.hpp>

#include <cstddef>
#include <cstdint>
#include <expected>
#include <functional>
#include <optional>
#include <string>
#include <string_view>

#include "physical_plan.hpp"

namespace ibex::runtime {

/// Two fixed-width int keys packed into one struct, injective with no
/// knowledge of their domains -- same trick as the aggregate's own
/// `PairIntKey`. Shared by the two-key inner join's hash index and the
/// two-key semi/anti join's key set.
struct JoinPairKey {
    std::uint64_t a = 0;
    std::uint64_t b = 0;
    [[nodiscard]] auto operator==(const JoinPairKey&) const -> bool = default;
};
struct JoinPairKeyHash {
    auto operator()(const JoinPairKey& key) const noexcept -> std::size_t {
        std::uint64_t h = key.a * 0x9e3779b97f4a7c15ULL;
        h ^= key.b + 0x9e3779b97f4a7c15ULL + (h << 6) + (h >> 2);
        return static_cast<std::size_t>(h);
    }
};

/// One side's two Int64 key columns and their validity, or the error a join
/// reports for them. `side_name` is "left" or "right" only so the message
/// keeps naming the side the caller was asking about.
struct PairKeyColumns {
    const Column<std::int64_t>* col0 = nullptr;
    const Column<std::int64_t>* col1 = nullptr;
    const ValidityBitmap* v0 = nullptr;
    const ValidityBitmap* v1 = nullptr;
};

inline auto pair_key_columns(const Table& side, const std::string& name0, const std::string& name1,
                             std::string_view side_name)
    -> std::expected<PairKeyColumns, std::string> {
    const ColumnValue* key0 = side.find(name0);
    if (key0 == nullptr) {
        return std::unexpected("join key not found in " + std::string(side_name) +
                               " table: " + name0);
    }
    const ColumnValue* key1 = side.find(name1);
    if (key1 == nullptr) {
        return std::unexpected("join key not found in " + std::string(side_name) +
                               " table: " + name1);
    }
    PairKeyColumns out;
    out.col0 = std::get_if<Column<std::int64_t>>(key0);
    out.col1 = std::get_if<Column<std::int64_t>>(key1);
    if (out.col0 == nullptr || out.col1 == nullptr) {
        return std::unexpected("two-key join currently requires both keys to be Int64");
    }
    const auto* entry0 = side.find_entry(name0);
    const auto* entry1 = side.find_entry(name1);
    out.v0 = entry0 != nullptr && entry0->validity.has_value() ? &*entry0->validity : nullptr;
    out.v1 = entry1 != nullptr && entry1->validity.has_value() ? &*entry1->validity : nullptr;
    return out;
}

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
