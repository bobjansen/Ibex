#pragma once

#include <ibex/runtime/interpreter.hpp>
#include <ibex/runtime/operator.hpp>

#include <expected>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

namespace ibex::runtime {

/// Sentinel returned by a stream source to signal "receive timeout — no data
/// arrived but I am not done; keep listening."
///
/// Returning StreamTimeout instead of an empty Table lets the stream event
/// loop fire the wall-clock bucket flush and then call the source again.
///
/// ## Buffering responsibility
///
/// Ibex does NOT buffer data on behalf of the source.  The source plugin is
/// solely responsible for ensuring that messages arriving while StreamTimeout
/// is being processed are not lost.  Whether that guarantee holds depends
/// entirely on the underlying transport:
///
///   UDP sockets (SO_RCVTIMEO): the OS kernel buffers incoming datagrams in
///   the socket receive buffer (SO_RCVBUF) regardless of whether recvfrom()
///   is being called.  A packet that arrives while the event loop is handling
///   StreamTimeout will wait in the kernel buffer and be returned on the next
///   recvfrom() call.  No application-level buffering is needed.  Packets are
///   dropped only if SO_RCVBUF overflows — a property of UDP in general, not
///   specific to StreamTimeout.
///
///   In-process queues / custom transports: if the source reads from a
///   user-space queue without an independent producer thread, messages may
///   be lost while StreamTimeout is being processed.  The plugin author must
///   ensure the producer continues to run (e.g. via a separate thread or an
///   OS-level buffer) during the StreamTimeout window.
///
/// An empty Table (rows == 0) still signals end-of-stream (EOF).
struct StreamTimeout {};

/// Type-erased external function wrapper.
///
/// Stores C++ callables for interop with Ibex queries.
/// Functions are registered by name and can be looked up at runtime.
using ExternValue = std::variant<Table, ScalarValue, StreamTimeout>;
using ExternArgs = std::vector<ScalarValue>;
using ExternFn = std::function<std::expected<ExternValue, std::string>(const ExternArgs&)>;

/// Function signature for extern functions whose first argument is a DataFrame.
/// Used by write operations (e.g. write_csv, write_parquet).
using ExternTableConsumerFn =
    std::function<std::expected<ExternValue, std::string>(const Table&, const ExternArgs&)>;

/// Function signature for extern functions that produce a chunked table
/// source. Used by streaming readers (e.g. read_csv on the chunked path)
/// to return an operator that the interpreter can drain chunk by chunk.
using ExternChunkedTableFn =
    std::function<std::expected<OperatorPtr, std::string>(const ExternArgs&)>;

enum class ExternReturnKind : std::uint8_t {
    Scalar,
    Table,
};

/// A fitted model produced by a model plugin. `native` is an opaque,
/// plugin-owned object (e.g. a LightGBM booster) wrapped in a shared_ptr whose
/// deleter lives in the plugin, so it frees itself when the last reference
/// drops. The runtime never links or dereferences the underlying type — it only
/// stores the handle and hands it back to the plugin's `predict`.
struct FittedModel {
    std::shared_ptr<void> native;
    Table fitted;      ///< single column "fitted": in-sample predictions, input order
    Table importance;  ///< term | gain (may be empty)
    Table summary;     ///< free-form model summary, any schema/row count (e.g. cluster
                       ///< centroids, PCA loadings); surfaced via model_summary. May be empty.
};

/// Parsed model parameters handed to a model plugin's `fit`
/// (e.g. {"iterations", 300}, {"learning_rate", 0.03}).
using ModelParams = std::vector<std::pair<std::string, ScalarValue>>;

/// A model method implemented by a plugin: train, then predict on new data.
/// Registered by method name (the `method =` value), e.g. "lightgbm".
struct ModelOps {
    /// `design` holds the feature columns plus the response column named
    /// `response_col` (all Float64). Returns the fitted model.
    std::function<std::expected<FittedModel, std::string>(
        const Table& design, const std::string& response_col, const ModelParams& params)>
        fit;
    /// `native` is the FittedModel::native handle; `design` holds the feature
    /// columns in the same layout/order as training (no response column).
    /// Returns a single-column "prediction" table.
    std::function<std::expected<Table, std::string>(const void* native, const Table& design)>
        predict;
};

struct ExternFunction {
    ExternFn func;
    /// Set when the function's first argument is a DataFrame (e.g. write functions).
    ExternTableConsumerFn table_consumer_func;
    /// Set when the function produces a chunked table source that the
    /// interpreter can drain chunk by chunk. When both `func` and
    /// `chunked_table_func` are set, the interpreter prefers the chunked
    /// path.
    ExternChunkedTableFn chunked_table_func;
    ExternReturnKind kind = ExternReturnKind::Scalar;
    std::optional<ScalarKind> scalar_kind;
    /// True when the first argument is a DataFrame rather than a scalar.
    bool first_arg_is_table = false;
};

class ExternRegistry {
   public:
    ExternRegistry() = default;

    /// Register a scalar-returning extern function.
    void register_scalar(std::string name, ScalarKind kind, ExternFn func) {
        registry_.insert_or_assign(std::move(name), ExternFunction{
                                                        .func = std::move(func),
                                                        .table_consumer_func = {},
                                                        .chunked_table_func = {},
                                                        .kind = ExternReturnKind::Scalar,
                                                        .scalar_kind = kind,
                                                    });
    }

    /// Register a table-returning extern function.
    void register_table(std::string name, ExternFn func) {
        registry_.insert_or_assign(std::move(name), ExternFunction{.func = std::move(func),
                                                                   .table_consumer_func = {},
                                                                   .chunked_table_func = {},
                                                                   .kind = ExternReturnKind::Table,
                                                                   .scalar_kind = std::nullopt});
    }

    /// Register a chunked table source. The callback produces an operator
    /// that emits chunks on demand; the interpreter drains it into a
    /// materialized table at bind time today (steps 4+ will stream chunks
    /// further into downstream operators without materializing first).
    ///
    /// If a regular `register_table` entry already exists for this name,
    /// both are stored; the interpreter prefers the chunked path.
    void register_chunked_table(std::string name, ExternChunkedTableFn func) {
        auto it = registry_.find(name);
        if (it != registry_.end()) {
            it->second.chunked_table_func = std::move(func);
            it->second.kind = ExternReturnKind::Table;
            return;
        }
        ExternFunction ef;
        ef.chunked_table_func = std::move(func);
        ef.kind = ExternReturnKind::Table;
        registry_.insert_or_assign(std::move(name), std::move(ef));
    }

    /// Register a scalar-returning extern function whose first argument is a DataFrame.
    /// The registered function receives the DataFrame as a first argument, followed by the
    /// remaining scalar arguments.  Used for write operations such as write_csv and write_parquet.
    void register_scalar_table_consumer(std::string name, ScalarKind kind,
                                        ExternTableConsumerFn func) {
        ExternFunction ef;
        ef.table_consumer_func = std::move(func);
        ef.kind = ExternReturnKind::Scalar;
        ef.scalar_kind = kind;
        ef.first_arg_is_table = true;
        registry_.insert_or_assign(std::move(name), std::move(ef));
    }

    /// Look up a registered function by name.
    [[nodiscard]] auto find(const std::string& name) const -> const ExternFunction* {
        if (auto it = registry_.find(name); it != registry_.end()) {
            return &it->second;
        }
        return nullptr;
    }

    /// Check whether a function is registered.
    [[nodiscard]] auto contains(const std::string& name) const -> bool {
        return registry_.contains(name);
    }

    /// Number of registered functions.
    [[nodiscard]] auto size() const noexcept -> std::size_t { return registry_.size(); }

    /// Register a model method (the `method =` value, e.g. "lightgbm").
    void register_model(std::string name, ModelOps ops) {
        models_.insert_or_assign(std::move(name), std::move(ops));
    }

    /// Look up a registered model method by name.
    [[nodiscard]] auto find_model(const std::string& name) const -> const ModelOps* {
        if (auto it = models_.find(name); it != models_.end()) {
            return &it->second;
        }
        return nullptr;
    }

   private:
    std::unordered_map<std::string, ExternFunction> registry_;
    std::unordered_map<std::string, ModelOps> models_;
};

}  // namespace ibex::runtime
