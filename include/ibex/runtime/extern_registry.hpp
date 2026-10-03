// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Bob Jansen

#pragma once

// Every plugin's `ibex_register` entry point must be visible in the shared
// library's export table so the REPL's GetProcAddress/dlsym lookup can find
// it. `extern "C"` alone only fixes name mangling — on MSVC a symbol isn't
// exported at all unless explicitly marked dllexport (unlike GCC/Clang, which
// export by default), so without this a plugin DLL builds fine but the
// loader reports "has no ibex_register symbol".
#include <cstddef>
#include <variant>
#if defined(_WIN32)
#define IBEX_PLUGIN_EXPORT __declspec(dllexport)
#else
#define IBEX_PLUGIN_EXPORT
#endif

#include <ibex/runtime/interpreter.hpp>
#include <ibex/runtime/lazy_table.hpp>
#include <ibex/runtime/operator.hpp>
#include <ibex/runtime/rng.hpp>

#include <cstdint>
#include <expected>
#include <functional>
#include <memory>
#include <optional>
#include <robin_hood.h>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace ibex::runtime {

/// Function-pointer bridge to the host process's RNG singletons.
///
/// A plugin `.so` statically links its own copy of librng.a (the same way it
/// links the rest of libibex_runtime.a), so calling `ibex::runtime::fill_normal`
/// directly from plugin code would touch a *different* thread_local engine
/// than the one the REPL's `seed_rng` reseeds — same algorithm, disjoint state,
/// silently unreproducible. `ExternRegistry` is constructed once in the host
/// process, so its default member initializers below capture the addresses of
/// the host's own instantiations; a plugin that calls through this struct's
/// pointers therefore reaches the exact engine `seed_rng` controls, no matter
/// which shared object's code happens to invoke them.
struct RngBridge {
    void (*fill_uniform)(double*, std::size_t, double, double) noexcept = &runtime::fill_uniform;
    void (*fill_normal)(double*, std::size_t, double, double) noexcept = &runtime::fill_normal;
    void (*fill_exponential)(double*, std::size_t, double) noexcept = &runtime::fill_exponential;
    void (*fill_bernoulli)(std::int64_t*, std::size_t, double) noexcept = &runtime::fill_bernoulli;
    void (*fill_int)(std::int64_t*, std::size_t, std::int64_t,
                     std::uint64_t) noexcept = &runtime::fill_int;
};

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

/// An opaque, plugin-owned resource such as a database connection, declared in
/// Ibex with `extern type Name from "plugin.hpp";`. Ibex code can bind a
/// resource, pass it to extern functions and return it from them, but never
/// looks inside. The plugin that creates a resource is the only code that
/// casts it back to its concrete type (`ExternArgs::resource_as`), so the
/// dynamic_cast never crosses a shared-library boundary. Cleanup runs in the
/// destructor when the last reference drops; it must not throw.
class Resource {
   public:
    Resource() = default;
    Resource(const Resource&) = delete;
    Resource(Resource&&) = delete;
    auto operator=(const Resource&) -> Resource& = delete;
    auto operator=(Resource&&) -> Resource& = delete;
    virtual ~Resource() = default;

    /// The name the resource's `extern type` declaration gives it. The REPL
    /// checks it against the extern signature before every call.
    [[nodiscard]] virtual auto type_name() const noexcept -> std::string_view = 0;
};

using ResourcePtr = std::shared_ptr<Resource>;

/// Type-erased external function wrapper.
///
/// Stores C++ callables for interop with Ibex queries.
/// Functions are registered by name and can be looked up at runtime.
using ExternValue = std::variant<Table, ScalarValue, StreamTimeout, ResourcePtr>;

/// Arguments to an extern function, by position. Scalar arguments are the
/// vector's elements. A resource argument occupies a null scalar slot and is
/// read with `resource(i)` or `resource_as<T>(i)`; so does a table argument of
/// a resource function (one that also takes a resource), read with `table(i)`.
class ExternArgs : public std::vector<ScalarValue> {
   public:
    using std::vector<ScalarValue>::vector;

    void push_resource(ResourcePtr value) {
        resources_.emplace_back(size(), std::move(value));
        push_back(ScalarValue{});
    }

    /// The resource at position `index`, or null when that argument is a scalar.
    [[nodiscard]] auto resource(std::size_t index) const -> const ResourcePtr& {
        static const ResourcePtr kNone;
        for (const auto& [position, value] : resources_) {
            if (position == index) {
                return value;
            }
        }
        return kNone;
    }

    /// The resource at position `index` as the plugin's concrete type, or null
    /// when the argument is not a resource of that type.
    template <typename T>
    [[nodiscard]] auto resource_as(std::size_t index) const -> std::shared_ptr<T> {
        return std::dynamic_pointer_cast<T>(resource(index));
    }

    void push_table(std::shared_ptr<const Table> value) {
        tables_.emplace_back(size(), std::move(value));
        push_back(ScalarValue{});
    }

    /// The table at position `index`, or null when that argument is not a table.
    [[nodiscard]] auto table(std::size_t index) const -> std::shared_ptr<const Table> {
        for (const auto& [position, value] : tables_) {
            if (position == index) {
                return value;
            }
        }
        return nullptr;
    }

   private:
    std::vector<std::pair<std::size_t, ResourcePtr>> resources_;
    std::vector<std::pair<std::size_t, std::shared_ptr<const Table>>> tables_;
};
using ExternFn = std::function<std::expected<ExternValue, std::string>(const ExternArgs&)>;

/// Function signature for extern functions whose first argument is a DataFrame.
/// Used by write operations (e.g. csv::write, parquet::write).
using ExternTableConsumerFn =
    std::function<std::expected<ExternValue, std::string>(const Table&, const ExternArgs&)>;

/// Function signature for extern functions that produce a chunked table
/// source. Used by streaming readers (e.g. csv::read on the chunked path)
/// to return an operator that the interpreter can drain chunk by chunk.
using ExternChunkedTableFn =
    std::function<std::expected<OperatorPtr, std::string>(const ExternArgs&)>;

/// Function signature for extern functions that produce a lazily-decoded table
/// source (e.g. parquet::read). Returns a handle carrying the schema, from which
/// the interpreter materializes only the columns a query references.
using ExternLazyTableFn =
    std::function<std::expected<LazyTablePtr, std::string>(const ExternArgs&)>;

enum class ExternReturnKind : std::uint8_t {
    Scalar,
    Table,
    Resource,  ///< returns a ResourcePtr; callable only on the statement path
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
    /// Set when the source can decode its columns selectively. A `let` binding
    /// of such a function defers the read: it takes the schema now and lets each
    /// query pull only the columns it needs (projection pushdown).
    ExternLazyTableFn lazy_table_func;
    ExternReturnKind kind = ExternReturnKind::Scalar;
    std::optional<ScalarKind> scalar_kind;
    /// True when the first argument is a DataFrame rather than a scalar.
    bool first_arg_is_table = false;
};

class ExternRegistry {
   public:
    ExternRegistry() = default;

    /// Function pointers into the host process's own RNG engine (see
    /// RngBridge). Plugins that want reproducible-under-`seed_rng` random data
    /// should generate through `registry->rng()` rather than calling
    /// `ibex::runtime::fill_*` directly.
    [[nodiscard]] auto rng() const noexcept -> const RngBridge& { return rng_; }

    /// Register a scalar-returning extern function.
    void register_scalar(std::string name, ScalarKind kind, ExternFn func) {
        registry_.insert_or_assign(std::move(name), ExternFunction{
                                                        .func = std::move(func),
                                                        .table_consumer_func = {},
                                                        .chunked_table_func = {},
                                                        .lazy_table_func = {},
                                                        .kind = ExternReturnKind::Scalar,
                                                        .scalar_kind = kind,
                                                    });
    }

    /// Register a table-returning extern function.
    void register_table(std::string name, ExternFn func) {
        registry_.insert_or_assign(std::move(name), ExternFunction{.func = std::move(func),
                                                                   .table_consumer_func = {},
                                                                   .chunked_table_func = {},
                                                                   .lazy_table_func = {},
                                                                   .kind = ExternReturnKind::Table,
                                                                   .scalar_kind = std::nullopt});
    }

    /// Register an extern function that returns a resource (e.g. opens a
    /// connection). Functions that only take a resource register as usual
    /// with `register_scalar` or `register_table`.
    void register_resource(std::string name, ExternFn func) {
        registry_.insert_or_assign(std::move(name),
                                   ExternFunction{.func = std::move(func),
                                                  .table_consumer_func = {},
                                                  .chunked_table_func = {},
                                                  .lazy_table_func = {},
                                                  .kind = ExternReturnKind::Resource,
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

    /// Register a lazily-decoded table source. Binding one of these reads only
    /// the source's schema; each query then materializes just the columns it
    /// references. Stored alongside any existing `register_table` entry for the
    /// same name, which remains the fallback for callers that want the whole
    /// table at once.
    void register_lazy_table(std::string name, ExternLazyTableFn func) {
        auto it = registry_.find(name);
        if (it != registry_.end()) {
            it->second.lazy_table_func = std::move(func);
            it->second.kind = ExternReturnKind::Table;
            return;
        }
        ExternFunction ef;
        ef.lazy_table_func = std::move(func);
        ef.kind = ExternReturnKind::Table;
        registry_.insert_or_assign(std::move(name), std::move(ef));
    }

    /// Register a scalar-returning extern function whose first argument is a DataFrame.
    /// The registered function receives the DataFrame as a first argument, followed by the
    /// remaining scalar arguments.  Used for write operations such as csv::write and
    /// parquet::write.
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

    /// Mark a first-party or dynamically loaded library as registered.
    ///
    /// Import loaders use this to avoid loading a compatibility DSO when the
    /// embedding host already linked the same backend directly.
    void register_library(std::string name) { libraries_.insert(std::move(name)); }

    [[nodiscard]] auto contains_library(const std::string& name) const -> bool {
        return libraries_.contains(name);
    }

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
    robin_hood::unordered_map<std::string, ExternFunction> registry_;
    robin_hood::unordered_map<std::string, ModelOps> models_;
    robin_hood::unordered_set<std::string> libraries_;
    RngBridge rng_;
};

}  // namespace ibex::runtime
