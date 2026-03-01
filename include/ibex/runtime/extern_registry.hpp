#pragma once

#include <ibex/runtime/interpreter.hpp>

#include <expected>
#include <functional>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

namespace ibex::runtime {

/// Type-erased external function wrapper.
///
/// Stores C++ callables for interop with Ibex queries.
/// Functions are registered by name and can be looked up at runtime.
using ExternValue = std::variant<Table, ScalarValue>;
using ExternArgs = std::vector<ScalarValue>;
using ExternFn = std::function<std::expected<ExternValue, std::string>(const ExternArgs&)>;

/// Function signature for extern functions whose first argument is a DataFrame.
/// Used by write operations (e.g. write_csv, write_parquet).
using ExternTableConsumerFn =
    std::function<std::expected<ExternValue, std::string>(const Table&, const ExternArgs&)>;

enum class ExternReturnKind : std::uint8_t {
    Scalar,
    Table,
};

struct ExternFunction {
    ExternFn func;
    /// Set when the function's first argument is a DataFrame (e.g. write functions).
    ExternTableConsumerFn table_consumer_func;
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
        registry_.insert_or_assign(std::move(name), ExternFunction{.func = std::move(func),
                                                                   .kind = ExternReturnKind::Scalar,
                                                                   .scalar_kind = kind});
    }

    /// Register a table-returning extern function.
    void register_table(std::string name, ExternFn func) {
        registry_.insert_or_assign(std::move(name), ExternFunction{.func = std::move(func),
                                                                   .kind = ExternReturnKind::Table,
                                                                   .scalar_kind = std::nullopt});
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

   private:
    std::unordered_map<std::string, ExternFunction> registry_;
};

}  // namespace ibex::runtime
