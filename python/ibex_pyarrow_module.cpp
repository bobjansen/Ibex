#include <ibex/interop/arrow_c_data.hpp>
#include <ibex/parser/ast.hpp>
#include <ibex/parser/lower.hpp>
#include <ibex/parser/parser.hpp>
#include <ibex/runtime/extern_registry.hpp>
#include <ibex/runtime/interpreter.hpp>

#include <Python.h>

#include <datetime.h>

#include <dlfcn.h>
#include <expected>
#include <filesystem>
#include <fstream>
#include <memory>
#include <sstream>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

namespace {

enum class ImportedColumnKind {
    Bool,
    Int64,
    Float64,
    String,
};

class OwnedPyObject {
   public:
    OwnedPyObject() = default;
    explicit OwnedPyObject(PyObject* value) : value_(value) {}
    OwnedPyObject(const OwnedPyObject&) = delete;
    auto operator=(const OwnedPyObject&) -> OwnedPyObject& = delete;
    OwnedPyObject(OwnedPyObject&& other) noexcept : value_(std::exchange(other.value_, nullptr)) {}
    auto operator=(OwnedPyObject&& other) noexcept -> OwnedPyObject& {
        if (this == &other) {
            return *this;
        }
        Py_XDECREF(value_);
        value_ = std::exchange(other.value_, nullptr);
        return *this;
    }
    ~OwnedPyObject() { Py_XDECREF(value_); }

    [[nodiscard]] auto get() const noexcept -> PyObject* { return value_; }
    [[nodiscard]] explicit operator bool() const noexcept { return value_ != nullptr; }
    [[nodiscard]] auto release() noexcept -> PyObject* { return std::exchange(value_, nullptr); }

   private:
    PyObject* value_ = nullptr;
};

struct ExportedArrowCapsules {
    PyObject* schema_capsule = nullptr;
    PyObject* array_capsule = nullptr;

    ExportedArrowCapsules() = default;
    ExportedArrowCapsules(const ExportedArrowCapsules&) = delete;
    auto operator=(const ExportedArrowCapsules&) -> ExportedArrowCapsules& = delete;
    ExportedArrowCapsules(ExportedArrowCapsules&& other) noexcept
        : schema_capsule(std::exchange(other.schema_capsule, nullptr)),
          array_capsule(std::exchange(other.array_capsule, nullptr)) {}
    auto operator=(ExportedArrowCapsules&& other) noexcept -> ExportedArrowCapsules& {
        if (this == &other) {
            return *this;
        }
        Py_XDECREF(array_capsule);
        Py_XDECREF(schema_capsule);
        schema_capsule = std::exchange(other.schema_capsule, nullptr);
        array_capsule = std::exchange(other.array_capsule, nullptr);
        return *this;
    }

    ~ExportedArrowCapsules() {
        Py_XDECREF(array_capsule);
        Py_XDECREF(schema_capsule);
    }
};

struct SessionState {
    ibex::runtime::TableRegistry tables;
    std::unordered_set<std::string> table_externs;
    std::unordered_set<std::string> sink_externs;
    ibex::runtime::ExternRegistry externs;
    std::unordered_set<std::string> loaded_plugins;
    std::vector<std::string> plugin_paths;
};

auto set_python_error_from_message(PyObject* exc_type, const std::string& message) -> PyObject* {
    PyErr_SetString(exc_type, message.c_str());
    return nullptr;
}

auto format_ibex_pyarrow_error(std::string_view stage, const std::string& message) -> std::string {
    return "ibex_pyarrow " + std::string(stage) + ": " + message;
}

auto current_python_error_message() -> std::string {
    PyObject* type_raw = nullptr;
    PyObject* value_raw = nullptr;
    PyObject* traceback_raw = nullptr;
    PyErr_Fetch(&type_raw, &value_raw, &traceback_raw);
    PyErr_NormalizeException(&type_raw, &value_raw, &traceback_raw);
    OwnedPyObject type(type_raw);
    OwnedPyObject value(value_raw);
    OwnedPyObject traceback(traceback_raw);
    if (!value) {
        return "unknown Python error";
    }
    OwnedPyObject text(PyObject_Str(value.get()));
    if (!text.get()) {
        PyErr_Clear();
        return "unknown Python error";
    }
    const char* utf8 = PyUnicode_AsUTF8(text.get());
    if (utf8 == nullptr) {
        PyErr_Clear();
        return "unknown Python error";
    }
    return utf8;
}

auto capsule_destructor_name(const char* expected_name) -> const char* {
    return expected_name;
}

void schema_capsule_destructor(PyObject* capsule) {
    auto* schema = static_cast<ArrowSchema*>(
        PyCapsule_GetPointer(capsule, capsule_destructor_name("arrow_schema")));
    if (schema == nullptr) {
        PyErr_Clear();
        return;
    }
    ibex::interop::release_arrow_schema(schema);
    delete schema;
}

void array_capsule_destructor(PyObject* capsule) {
    auto* array = static_cast<ArrowArray*>(
        PyCapsule_GetPointer(capsule, capsule_destructor_name("arrow_array")));
    if (array == nullptr) {
        PyErr_Clear();
        return;
    }
    ibex::interop::release_arrow_array(array);
    delete array;
}

auto wrap_arrow_export(std::shared_ptr<const ibex::runtime::Table> table)
    -> std::expected<ExportedArrowCapsules, std::string> {
    auto schema = std::make_unique<ArrowSchema>();
    auto array = std::make_unique<ArrowArray>();
    auto exported =
        ibex::interop::export_table_to_arrow(std::move(table), array.get(), schema.get());
    if (!exported.has_value()) {
        return std::unexpected(exported.error());
    }

    ExportedArrowCapsules capsules;
    capsules.schema_capsule =
        PyCapsule_New(schema.release(), "arrow_schema", schema_capsule_destructor);
    if (capsules.schema_capsule == nullptr) {
        return std::unexpected("failed to create Arrow schema capsule");
    }

    capsules.array_capsule =
        PyCapsule_New(array.release(), "arrow_array", array_capsule_destructor);
    if (capsules.array_capsule == nullptr) {
        return std::unexpected("failed to create Arrow array capsule");
    }

    return capsules;
}

auto import_pyarrow_table(PyObject* schema_capsule, PyObject* array_capsule) -> PyObject* {
    PyObject* pyarrow = PyImport_ImportModule("pyarrow");
    if (pyarrow == nullptr) {
        return nullptr;
    }

    PyObject* record_batch_type = PyObject_GetAttrString(pyarrow, "RecordBatch");
    PyObject* table_type = PyObject_GetAttrString(pyarrow, "Table");
    Py_DECREF(pyarrow);
    if (record_batch_type == nullptr || table_type == nullptr) {
        Py_XDECREF(record_batch_type);
        Py_XDECREF(table_type);
        return nullptr;
    }

    PyObject* import_capsules = PyObject_GetAttrString(record_batch_type, "_import_from_c_capsule");
    if (import_capsules == nullptr) {
        Py_DECREF(record_batch_type);
        Py_DECREF(table_type);
        return nullptr;
    }

    PyObject* batch =
        PyObject_CallFunctionObjArgs(import_capsules, schema_capsule, array_capsule, nullptr);
    Py_DECREF(import_capsules);
    Py_DECREF(record_batch_type);
    if (batch == nullptr) {
        Py_DECREF(table_type);
        return nullptr;
    }

    PyObject* batches = PyList_New(1);
    if (batches == nullptr) {
        Py_DECREF(batch);
        Py_DECREF(table_type);
        return nullptr;
    }
    PyList_SET_ITEM(batches, 0, batch);

    PyObject* table = PyObject_CallMethod(table_type, "from_batches", "O", batches);
    Py_DECREF(table_type);
    if (table == nullptr) {
        Py_DECREF(batches);
        return nullptr;
    }
    Py_DECREF(batches);
    return table;
}

auto plugin_stem(const std::string& source_path) -> std::string {
    std::filesystem::path p(source_path);
    return p.stem().string();
}

void session_capsule_destructor(PyObject* capsule) {
    auto* session =
        static_cast<SessionState*>(PyCapsule_GetPointer(capsule, "ibex_pyarrow.session"));
    if (session == nullptr) {
        PyErr_Clear();
        return;
    }
    delete session;
}

enum class PluginLoadStatus : std::uint8_t { Loaded, NotFound, LoadError };

struct PluginLoadResult {
    PluginLoadStatus status;
    std::string message;
};

auto try_load_plugin(const std::string& stem, const std::vector<std::string>& search_paths,
                     std::unordered_set<std::string>& loaded_plugins,
                     ibex::runtime::ExternRegistry& externs) -> PluginLoadResult {
    if (loaded_plugins.contains(stem)) {
        return {PluginLoadStatus::Loaded, ""};
    }

    const std::string filename = stem + ".so";
    std::string last_error;
    std::string last_candidate;
    for (const auto& dir : search_paths) {
        auto full_path = std::filesystem::path(dir) / filename;
        void* handle = dlopen(full_path.c_str(), RTLD_NOW | RTLD_LOCAL);
        if (handle == nullptr) {
            if (std::filesystem::exists(full_path)) {
                if (const char* err = dlerror()) {
                    last_error = err;
                }
                last_candidate = full_path.string();
            }
            continue;
        }

        using RegisterFn = void (*)(ibex::runtime::ExternRegistry*);
        auto* fn = reinterpret_cast<RegisterFn>(dlsym(handle, "ibex_register"));
        if (fn == nullptr) {
            dlclose(handle);
            last_candidate = full_path.string();
            last_error = "missing ibex_register symbol";
            continue;
        }

        fn(&externs);
        loaded_plugins.insert(stem);
        return {PluginLoadStatus::Loaded, ""};
    }

    if (!last_candidate.empty()) {
        return {PluginLoadStatus::LoadError,
                "failed to load '" + last_candidate +
                    "': " + (last_error.empty() ? "unknown error" : last_error)};
    }
    return {PluginLoadStatus::NotFound, ""};
}

auto load_source_plugins(const ibex::parser::Program& program,
                         const std::vector<std::string>& plugin_search_paths,
                         ibex::runtime::ExternRegistry& externs)
    -> std::expected<void, std::string> {
    std::unordered_set<std::string> loaded_plugins;
    for (const auto& stmt : program.statements) {
        if (auto* decl = std::get_if<ibex::parser::ExternDecl>(&stmt)) {
            if (decl->source_path.empty()) {
                continue;
            }
            auto stem = plugin_stem(decl->source_path);
            auto result = try_load_plugin(stem, plugin_search_paths, loaded_plugins, externs);
            if (result.status == PluginLoadStatus::NotFound) {
                return std::unexpected("could not find plugin '" + stem + ".so' in search path");
            }
            if (result.status == PluginLoadStatus::LoadError) {
                return std::unexpected(result.message);
            }
            continue;
        }

        if (std::holds_alternative<ibex::parser::ImportDecl>(stmt)) {
            return std::unexpected(
                "ibex_pyarrow does not yet support import declarations; use explicit extern fn "
                "declarations with plugin_paths");
        }
    }
    return {};
}

auto register_extern_decl(const ibex::parser::ExternDecl& decl, SessionState& session)
    -> std::expected<void, std::string> {
    if (decl.return_type.kind == ibex::parser::Type::Kind::DataFrame ||
        decl.return_type.kind == ibex::parser::Type::Kind::TimeFrame) {
        session.table_externs.insert(decl.name);
    }
    if (!decl.params.empty() && decl.params[0].type.kind == ibex::parser::Type::Kind::DataFrame) {
        session.sink_externs.insert(decl.name);
    }
    if (decl.source_path.empty()) {
        return {};
    }
    auto stem = plugin_stem(decl.source_path);
    auto result =
        try_load_plugin(stem, session.plugin_paths, session.loaded_plugins, session.externs);
    if (result.status == PluginLoadStatus::NotFound) {
        return std::unexpected("could not find plugin '" + stem + ".so' in search path");
    }
    if (result.status == PluginLoadStatus::LoadError) {
        return std::unexpected(result.message);
    }
    return {};
}

auto session_from_capsule(PyObject* capsule) -> std::expected<SessionState*, std::string> {
    if (capsule == nullptr || capsule == Py_None) {
        return std::unexpected("'session' is required");
    }
    if (!PyCapsule_CheckExact(capsule)) {
        return std::unexpected("'session' must be an ibex_pyarrow session");
    }
    auto* session =
        static_cast<SessionState*>(PyCapsule_GetPointer(capsule, "ibex_pyarrow.session"));
    if (session == nullptr) {
        auto message = current_python_error_message();
        PyErr_Clear();
        return std::unexpected(message.empty() ? "invalid ibex_pyarrow session" : message);
    }
    return session;
}

auto eval_table_impl(const std::string& source, const ibex::runtime::TableRegistry& registry,
                     const ibex::runtime::ScalarRegistry& scalars,
                     const std::vector<std::string>& plugin_search_paths)
    -> std::expected<std::shared_ptr<const ibex::runtime::Table>, std::string> {
    auto parsed = ibex::parser::parse(source);
    if (!parsed.has_value()) {
        return std::unexpected(format_ibex_pyarrow_error("parse error", parsed.error().format()));
    }

    ibex::runtime::ExternRegistry externs;
    if (!plugin_search_paths.empty()) {
        auto loaded = load_source_plugins(*parsed, plugin_search_paths, externs);
        if (!loaded.has_value()) {
            return std::unexpected(format_ibex_pyarrow_error("plugin load error", loaded.error()));
        }
    }

    auto lowered = ibex::parser::lower(*parsed);
    if (!lowered.has_value()) {
        return std::unexpected(
            format_ibex_pyarrow_error("lowering error", lowered.error().message));
    }

    auto evaluated = ibex::runtime::interpret(*lowered.value(), registry, &scalars,
                                              plugin_search_paths.empty() ? nullptr : &externs);
    if (!evaluated.has_value()) {
        return std::unexpected(format_ibex_pyarrow_error("runtime error", evaluated.error()));
    }

    return std::make_shared<ibex::runtime::Table>(std::move(*evaluated));
}

auto merge_registries(const ibex::runtime::TableRegistry& base,
                      const ibex::runtime::TableRegistry& extra) -> ibex::runtime::TableRegistry {
    ibex::runtime::TableRegistry merged = base;
    for (const auto& [name, table] : extra) {
        merged.insert_or_assign(name, table);
    }
    return merged;
}

auto merge_scalars(const ibex::runtime::ScalarRegistry& base,
                   const ibex::runtime::ScalarRegistry& extra) -> ibex::runtime::ScalarRegistry {
    ibex::runtime::ScalarRegistry merged = base;
    for (const auto& [name, value] : extra) {
        merged.insert_or_assign(name, value);
    }
    return merged;
}

auto eval_table_in_session(SessionState& session, const std::string& source,
                           const ibex::runtime::TableRegistry& extra_tables,
                           const ibex::runtime::ScalarRegistry& extra_scalars)
    -> std::expected<std::shared_ptr<const ibex::runtime::Table>, std::string> {
    auto parsed = ibex::parser::parse(source);
    if (!parsed.has_value()) {
        return std::unexpected(format_ibex_pyarrow_error("parse error", parsed.error().format()));
    }

    std::shared_ptr<const ibex::runtime::Table> last_table;
    for (const auto& stmt : parsed->statements) {
        if (const auto* decl = std::get_if<ibex::parser::ExternDecl>(&stmt)) {
            auto registered = register_extern_decl(*decl, session);
            if (!registered.has_value()) {
                return std::unexpected(
                    format_ibex_pyarrow_error("plugin load error", registered.error()));
            }
            continue;
        }
        if (std::holds_alternative<ibex::parser::ImportDecl>(stmt)) {
            return std::unexpected(format_ibex_pyarrow_error(
                "session error",
                "import declarations are not supported in ibex_pyarrow sessions; use explicit "
                "extern fn declarations"));
        }
        if (std::holds_alternative<ibex::parser::FunctionDecl>(stmt)) {
            return std::unexpected(format_ibex_pyarrow_error(
                "session error",
                "function declarations are not supported in ibex_pyarrow sessions"));
        }
        if (std::holds_alternative<ibex::parser::TupleLetStmt>(stmt)) {
            return std::unexpected(format_ibex_pyarrow_error(
                "session error", "tuple let bindings are not supported in ibex_pyarrow sessions"));
        }

        ibex::parser::LowerContext context;
        context.table_externs = session.table_externs;
        context.sink_externs = session.sink_externs;

        auto runtime_registry = merge_registries(session.tables, extra_tables);
        auto runtime_scalars = merge_scalars({}, extra_scalars);

        if (const auto* let_stmt = std::get_if<ibex::parser::LetStmt>(&stmt)) {
            auto lowered = ibex::parser::lower_expr(*let_stmt->value, context);
            if (!lowered.has_value()) {
                return std::unexpected(format_ibex_pyarrow_error(
                    "lowering error",
                    "ibex_pyarrow sessions currently support only table-valued let bindings: " +
                        lowered.error().message));
            }
            auto evaluated = ibex::runtime::interpret(*lowered.value(), runtime_registry,
                                                      &runtime_scalars, &session.externs);
            if (!evaluated.has_value()) {
                return std::unexpected(
                    format_ibex_pyarrow_error("runtime error", evaluated.error()));
            }
            session.tables.insert_or_assign(let_stmt->name, std::move(*evaluated));
            continue;
        }

        const auto& expr_stmt = std::get<ibex::parser::ExprStmt>(stmt);
        auto lowered = ibex::parser::lower_expr(*expr_stmt.expr, context);
        if (!lowered.has_value()) {
            return std::unexpected(format_ibex_pyarrow_error(
                "lowering error",
                "ibex_pyarrow sessions currently support only table-valued expressions: " +
                    lowered.error().message));
        }
        auto evaluated = ibex::runtime::interpret(*lowered.value(), runtime_registry,
                                                  &runtime_scalars, &session.externs);
        if (!evaluated.has_value()) {
            return std::unexpected(format_ibex_pyarrow_error("runtime error", evaluated.error()));
        }
        last_table = std::make_shared<ibex::runtime::Table>(std::move(*evaluated));
    }

    return last_table;
}

auto read_text_file(const std::string& path) -> std::expected<std::string, std::string> {
    std::ifstream in(path);
    if (!in.is_open()) {
        return std::unexpected(
            format_ibex_pyarrow_error("file error", "failed to open Ibex file '" + path + "'"));
    }

    std::ostringstream buffer;
    buffer << in.rdbuf();
    if (!in.good() && !in.eof()) {
        return std::unexpected(
            format_ibex_pyarrow_error("file error", "failed to read Ibex file '" + path + "'"));
    }
    return buffer.str();
}

auto to_utf8_string(PyObject* obj, const char* context) -> std::expected<std::string, std::string> {
    if (!PyUnicode_Check(obj)) {
        return std::unexpected(std::string(context) + " must be a Python string");
    }
    const char* utf8 = PyUnicode_AsUTF8(obj);
    if (utf8 == nullptr) {
        auto message = current_python_error_message();
        PyErr_Clear();
        return std::unexpected(message);
    }
    return std::string(utf8);
}

auto parse_plugin_paths(PyObject* plugin_paths_obj)
    -> std::expected<std::vector<std::string>, std::string> {
    std::vector<std::string> paths;
    if (plugin_paths_obj == nullptr || plugin_paths_obj == Py_None) {
        return paths;
    }

    OwnedPyObject fast(
        PySequence_Fast(plugin_paths_obj, "'plugin_paths' must be a sequence of strings"));
    if (!fast) {
        auto message = current_python_error_message();
        PyErr_Clear();
        return std::unexpected(message);
    }

    const auto size = PySequence_Fast_GET_SIZE(fast.get());
    PyObject** items = PySequence_Fast_ITEMS(fast.get());
    paths.reserve(static_cast<std::size_t>(size));
    for (Py_ssize_t i = 0; i < size; ++i) {
        auto path = to_utf8_string(items[i], "plugin path");
        if (!path.has_value()) {
            return std::unexpected(path.error());
        }
        paths.push_back(*path);
    }
    return paths;
}

auto date_from_ymd(int year, int month, int day) -> ibex::Date {
    using namespace std::chrono;
    const auto ymd =
        year_month_day{std::chrono::year{year}, std::chrono::month{static_cast<unsigned>(month)},
                       std::chrono::day{static_cast<unsigned>(day)}};
    const auto sys = sys_days{ymd};
    return ibex::Date{static_cast<std::int32_t>(sys.time_since_epoch().count())};
}

auto timestamp_from_python_datetime(PyObject* value)
    -> std::expected<ibex::Timestamp, std::string> {
    using namespace std::chrono;
    const auto y = PyDateTime_GET_YEAR(value);
    const auto m = PyDateTime_GET_MONTH(value);
    const auto d = PyDateTime_GET_DAY(value);
    const auto hh = PyDateTime_DATE_GET_HOUR(value);
    const auto mm = PyDateTime_DATE_GET_MINUTE(value);
    const auto ss = PyDateTime_DATE_GET_SECOND(value);
    const auto us = PyDateTime_DATE_GET_MICROSECOND(value);

    const auto day_point =
        sys_days{year{y} / month{static_cast<unsigned>(m)} / day{static_cast<unsigned>(d)}};
    auto total = duration_cast<nanoseconds>(day_point.time_since_epoch()) + hours{hh} +
                 minutes{mm} + seconds{ss} + microseconds{us};

    OwnedPyObject offset(PyObject_CallMethod(value, "utcoffset", nullptr));
    if (!offset.get()) {
        auto message = current_python_error_message();
        PyErr_Clear();
        return std::unexpected(message);
    }
    if (offset.get() != Py_None) {
        const auto delta_days = PyDateTime_DELTA_GET_DAYS(offset.get());
        const auto delta_seconds = PyDateTime_DELTA_GET_SECONDS(offset.get());
        const auto delta_micros = PyDateTime_DELTA_GET_MICROSECONDS(offset.get());
        total -= days{delta_days} + seconds{delta_seconds} + microseconds{delta_micros};
    }

    return ibex::Timestamp{total.count()};
}

auto build_scalar_registry_from_python(PyObject* scalars_obj)
    -> std::expected<ibex::runtime::ScalarRegistry, std::string> {
    ibex::runtime::ScalarRegistry registry;
    if (scalars_obj == nullptr || scalars_obj == Py_None) {
        return registry;
    }
    if (!PyDict_Check(scalars_obj)) {
        return std::unexpected("'scalars' must be a dict from name to Python scalar");
    }

    Py_ssize_t pos = 0;
    PyObject* key = nullptr;
    PyObject* value = nullptr;
    while (PyDict_Next(scalars_obj, &pos, &key, &value) != 0) {
        auto name = to_utf8_string(key, "scalar binding name");
        if (!name.has_value()) {
            return std::unexpected(name.error());
        }
        if (PyBool_Check(value)) {
            registry.emplace(*name, value == Py_True);
            continue;
        }
        if (PyLong_Check(value)) {
            long long as_i64 = PyLong_AsLongLong(value);
            if (PyErr_Occurred()) {
                auto message = current_python_error_message();
                PyErr_Clear();
                return std::unexpected("scalar '" + *name + "': " + message);
            }
            registry.emplace(*name, static_cast<std::int64_t>(as_i64));
            continue;
        }
        if (PyFloat_Check(value)) {
            double as_f64 = PyFloat_AsDouble(value);
            if (PyErr_Occurred()) {
                auto message = current_python_error_message();
                PyErr_Clear();
                return std::unexpected("scalar '" + *name + "': " + message);
            }
            registry.emplace(*name, as_f64);
            continue;
        }
        if (PyDateTime_Check(value)) {
            auto ts = timestamp_from_python_datetime(value);
            if (!ts.has_value()) {
                return std::unexpected("scalar '" + *name + "': " + ts.error());
            }
            registry.emplace(*name, *ts);
            continue;
        }
        if (PyDate_Check(value)) {
            registry.emplace(*name,
                             date_from_ymd(PyDateTime_GET_YEAR(value), PyDateTime_GET_MONTH(value),
                                           PyDateTime_GET_DAY(value)));
            continue;
        }
        if (PyUnicode_Check(value)) {
            auto text = to_utf8_string(value, "scalar binding value");
            if (!text.has_value()) {
                return std::unexpected("scalar '" + *name + "': " + text.error());
            }
            registry.emplace(*name, *text);
            continue;
        }
        OwnedPyObject repr(PyObject_Repr(value));
        const char* utf8 = repr ? PyUnicode_AsUTF8(repr.get()) : nullptr;
        return std::unexpected("scalar '" + *name + "': unsupported Python scalar type " +
                               std::string(utf8 != nullptr ? utf8 : "<unprintable>"));
    }
    return registry;
}

auto classify_scalar(PyObject* value) -> std::expected<ImportedColumnKind, std::string> {
    if (PyBool_Check(value)) {
        return ImportedColumnKind::Bool;
    }
    if (PyLong_Check(value)) {
        return ImportedColumnKind::Int64;
    }
    if (PyFloat_Check(value)) {
        return ImportedColumnKind::Float64;
    }
    if (PyUnicode_Check(value)) {
        return ImportedColumnKind::String;
    }
    OwnedPyObject repr(PyObject_Repr(value));
    const char* utf8 = repr ? PyUnicode_AsUTF8(repr.get()) : nullptr;
    return std::unexpected(std::string("unsupported Python scalar type in input table: ") +
                           (utf8 != nullptr ? utf8 : "<unprintable>"));
}

auto infer_column_kind(PyObject* sequence, const std::string& column_name)
    -> std::expected<ImportedColumnKind, std::string> {
    OwnedPyObject fast(PySequence_Fast(sequence, "column values must be a sequence"));
    if (!fast) {
        auto message = current_python_error_message();
        PyErr_Clear();
        return std::unexpected(message);
    }

    const auto size = PySequence_Fast_GET_SIZE(fast.get());
    PyObject** items = PySequence_Fast_ITEMS(fast.get());
    std::optional<ImportedColumnKind> kind;
    for (Py_ssize_t i = 0; i < size; ++i) {
        PyObject* item = items[i];
        if (item == Py_None) {
            continue;
        }
        auto classified = classify_scalar(item);
        if (!classified.has_value()) {
            return std::unexpected("column '" + column_name + "': " + classified.error());
        }
        if (!kind.has_value()) {
            kind = *classified;
            continue;
        }
        if (*kind == ImportedColumnKind::Int64 && *classified == ImportedColumnKind::Float64) {
            kind = ImportedColumnKind::Float64;
            continue;
        }
        if (*kind == ImportedColumnKind::Float64 && *classified == ImportedColumnKind::Int64) {
            continue;
        }
        if (*kind != *classified) {
            return std::unexpected("column '" + column_name +
                                   "' has mixed incompatible Python types");
        }
    }
    if (!kind.has_value()) {
        return std::unexpected("column '" + column_name +
                               "' contains only nulls; type inference failed");
    }
    return *kind;
}

auto build_column_from_sequence(const std::string& column_name, PyObject* sequence)
    -> std::expected<
        std::pair<ibex::runtime::ColumnValue, std::optional<ibex::runtime::ValidityBitmap>>,
        std::string> {
    OwnedPyObject fast(PySequence_Fast(sequence, "column values must be a sequence"));
    if (!fast) {
        auto message = current_python_error_message();
        PyErr_Clear();
        return std::unexpected(message);
    }

    auto inferred_kind = infer_column_kind(sequence, column_name);
    if (!inferred_kind.has_value()) {
        return std::unexpected(inferred_kind.error());
    }

    const auto size = PySequence_Fast_GET_SIZE(fast.get());
    PyObject** items = PySequence_Fast_ITEMS(fast.get());
    bool has_nulls = false;

    auto build_validity = [&](ibex::runtime::ValidityBitmap& validity, Py_ssize_t i, bool valid) {
        if (!has_nulls && !valid) {
            has_nulls = true;
            validity.assign(static_cast<std::size_t>(size), true);
        }
        if (has_nulls) {
            validity.set(static_cast<std::size_t>(i), valid);
        }
    };

    switch (*inferred_kind) {
        case ImportedColumnKind::Bool: {
            ibex::Column<bool> column;
            column.reserve(static_cast<std::size_t>(size));
            ibex::runtime::ValidityBitmap validity;
            for (Py_ssize_t i = 0; i < size; ++i) {
                PyObject* item = items[i];
                if (item == Py_None) {
                    column.push_back(false);
                    build_validity(validity, i, false);
                    continue;
                }
                if (!PyBool_Check(item)) {
                    return std::unexpected("column '" + column_name +
                                           "' expected bool values after type inference");
                }
                column.push_back(item == Py_True);
                build_validity(validity, i, true);
            }
            return std::pair{ibex::runtime::ColumnValue{std::move(column)},
                             has_nulls
                                 ? std::optional<ibex::runtime::ValidityBitmap>(std::move(validity))
                                 : std::nullopt};
        }
        case ImportedColumnKind::Int64: {
            ibex::Column<std::int64_t> column;
            column.reserve(static_cast<std::size_t>(size));
            ibex::runtime::ValidityBitmap validity;
            for (Py_ssize_t i = 0; i < size; ++i) {
                PyObject* item = items[i];
                if (item == Py_None) {
                    column.push_back(0);
                    build_validity(validity, i, false);
                    continue;
                }
                long long value = PyLong_AsLongLong(item);
                if (PyErr_Occurred()) {
                    auto message = current_python_error_message();
                    PyErr_Clear();
                    return std::unexpected("column '" + column_name + "': " + message);
                }
                column.push_back(static_cast<std::int64_t>(value));
                build_validity(validity, i, true);
            }
            return std::pair{ibex::runtime::ColumnValue{std::move(column)},
                             has_nulls
                                 ? std::optional<ibex::runtime::ValidityBitmap>(std::move(validity))
                                 : std::nullopt};
        }
        case ImportedColumnKind::Float64: {
            ibex::Column<double> column;
            column.reserve(static_cast<std::size_t>(size));
            ibex::runtime::ValidityBitmap validity;
            for (Py_ssize_t i = 0; i < size; ++i) {
                PyObject* item = items[i];
                if (item == Py_None) {
                    column.push_back(0.0);
                    build_validity(validity, i, false);
                    continue;
                }
                double value = PyFloat_Check(item) ? PyFloat_AsDouble(item)
                                                   : static_cast<double>(PyLong_AsLongLong(item));
                if (PyErr_Occurred()) {
                    auto message = current_python_error_message();
                    PyErr_Clear();
                    return std::unexpected("column '" + column_name + "': " + message);
                }
                column.push_back(value);
                build_validity(validity, i, true);
            }
            return std::pair{ibex::runtime::ColumnValue{std::move(column)},
                             has_nulls
                                 ? std::optional<ibex::runtime::ValidityBitmap>(std::move(validity))
                                 : std::nullopt};
        }
        case ImportedColumnKind::String: {
            ibex::Column<std::string> column;
            column.reserve(static_cast<std::size_t>(size));
            ibex::runtime::ValidityBitmap validity;
            for (Py_ssize_t i = 0; i < size; ++i) {
                PyObject* item = items[i];
                if (item == Py_None) {
                    column.push_back("");
                    build_validity(validity, i, false);
                    continue;
                }
                auto value = to_utf8_string(item, "string column value");
                if (!value.has_value()) {
                    return std::unexpected("column '" + column_name + "': " + value.error());
                }
                column.push_back(*value);
                build_validity(validity, i, true);
            }
            return std::pair{ibex::runtime::ColumnValue{std::move(column)},
                             has_nulls
                                 ? std::optional<ibex::runtime::ValidityBitmap>(std::move(validity))
                                 : std::nullopt};
        }
    }

    return std::unexpected("unreachable imported column kind");
}

auto normalize_table_like_to_column_dict(PyObject* table_like)
    -> std::expected<OwnedPyObject, std::string> {
    if (PyDict_Check(table_like)) {
        Py_INCREF(table_like);
        return OwnedPyObject(table_like);
    }

    if (PyObject_HasAttrString(table_like, "to_pydict")) {
        OwnedPyObject dict(PyObject_CallMethod(table_like, "to_pydict", nullptr));
        if (!dict) {
            auto message = current_python_error_message();
            PyErr_Clear();
            return std::unexpected(message);
        }
        if (!PyDict_Check(dict.get())) {
            return std::unexpected("table-like object's to_pydict() did not return a dict");
        }
        return dict;
    }

    if (PyObject_HasAttrString(table_like, "to_dict") &&
        PyObject_HasAttrString(table_like, "columns")) {
        OwnedPyObject dict(PyObject_CallMethod(table_like, "to_dict", "s", "list"));
        if (!dict) {
            auto message = current_python_error_message();
            PyErr_Clear();
            return std::unexpected(message);
        }
        if (!PyDict_Check(dict.get())) {
            return std::unexpected("table-like object's to_dict('list') did not return a dict");
        }
        return dict;
    }

    return std::unexpected(
        "unsupported table binding object; expected dict, pyarrow table-like, or pandas DataFrame");
}

auto build_runtime_table_from_python(PyObject* table_like)
    -> std::expected<ibex::runtime::Table, std::string> {
    auto maybe_dict = normalize_table_like_to_column_dict(table_like);
    if (!maybe_dict.has_value()) {
        return std::unexpected(maybe_dict.error());
    }
    OwnedPyObject dict = std::move(*maybe_dict);

    ibex::runtime::Table table;
    Py_ssize_t pos = 0;
    PyObject* key = nullptr;
    PyObject* value = nullptr;
    std::optional<std::size_t> row_count;
    while (PyDict_Next(dict.get(), &pos, &key, &value) != 0) {
        auto name = to_utf8_string(key, "table binding column name");
        if (!name.has_value()) {
            return std::unexpected(name.error());
        }

        OwnedPyObject fast(PySequence_Fast(value, "table binding column must be a sequence"));
        if (!fast) {
            auto message = current_python_error_message();
            PyErr_Clear();
            return std::unexpected("column '" + *name + "': " + message);
        }

        const auto size = static_cast<std::size_t>(PySequence_Fast_GET_SIZE(fast.get()));
        if (!row_count.has_value()) {
            row_count = size;
        } else if (*row_count != size) {
            return std::unexpected("table binding columns must all have the same length");
        }

        auto built = build_column_from_sequence(*name, value);
        if (!built.has_value()) {
            return std::unexpected(built.error());
        }
        auto& [column, validity] = *built;
        if (validity.has_value()) {
            table.add_column(*name, std::move(column), std::move(*validity));
        } else {
            table.add_column(*name, std::move(column));
        }
    }
    return table;
}

auto build_table_registry_from_python(PyObject* tables_obj)
    -> std::expected<ibex::runtime::TableRegistry, std::string> {
    ibex::runtime::TableRegistry registry;
    if (tables_obj == nullptr || tables_obj == Py_None) {
        return registry;
    }
    if (!PyDict_Check(tables_obj)) {
        return std::unexpected("'tables' must be a dict from name to table-like object");
    }

    Py_ssize_t pos = 0;
    PyObject* key = nullptr;
    PyObject* value = nullptr;
    while (PyDict_Next(tables_obj, &pos, &key, &value) != 0) {
        auto name = to_utf8_string(key, "table binding name");
        if (!name.has_value()) {
            return std::unexpected(name.error());
        }
        auto table = build_runtime_table_from_python(value);
        if (!table.has_value()) {
            return std::unexpected("while importing table '" + *name + "': " + table.error());
        }
        registry.emplace(*name, std::move(*table));
    }
    return registry;
}

PyObject* export_result_table_or_none(const std::shared_ptr<const ibex::runtime::Table>& table) {
    if (!table) {
        Py_RETURN_NONE;
    }

    auto capsules = wrap_arrow_export(table);
    if (!capsules.has_value()) {
        return set_python_error_from_message(PyExc_RuntimeError, capsules.error());
    }

    PyObject* py_table = import_pyarrow_table(capsules->schema_capsule, capsules->array_capsule);
    if (py_table == nullptr) {
        return nullptr;
    }
    return py_table;
}

PyObject* py_create_session(PyObject* /*self*/, PyObject* args, PyObject* kwargs) {
    static const char* kwlist[] = {"plugin_paths", nullptr};
    PyObject* plugin_paths_obj = Py_None;
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "|O", const_cast<char**>(kwlist),
                                     &plugin_paths_obj)) {
        return nullptr;
    }

    auto plugin_paths = parse_plugin_paths(plugin_paths_obj);
    if (!plugin_paths.has_value()) {
        return set_python_error_from_message(PyExc_RuntimeError, plugin_paths.error());
    }

    auto* session = new SessionState();
    session->plugin_paths = std::move(*plugin_paths);
    return PyCapsule_New(session, "ibex_pyarrow.session", session_capsule_destructor);
}

PyObject* py_reset_session(PyObject* /*self*/, PyObject* args, PyObject* kwargs) {
    static const char* kwlist[] = {"session", nullptr};
    PyObject* session_obj = Py_None;
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "O", const_cast<char**>(kwlist), &session_obj)) {
        return nullptr;
    }

    auto session = session_from_capsule(session_obj);
    if (!session.has_value()) {
        return set_python_error_from_message(PyExc_RuntimeError, session.error());
    }

    (*session)->tables.clear();
    (*session)->table_externs.clear();
    (*session)->sink_externs.clear();
    (*session)->externs = ibex::runtime::ExternRegistry{};
    (*session)->loaded_plugins.clear();
    Py_RETURN_NONE;
}

PyObject* py_eval_table(PyObject* /*self*/, PyObject* args, PyObject* kwargs) {
    static const char* kwlist[] = {"source", "tables", "scalars", "plugin_paths", nullptr};
    const char* source = nullptr;
    PyObject* tables_obj = Py_None;
    PyObject* scalars_obj = Py_None;
    PyObject* plugin_paths_obj = Py_None;
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "s|OOO", const_cast<char**>(kwlist), &source,
                                     &tables_obj, &scalars_obj, &plugin_paths_obj)) {
        return nullptr;
    }

    auto registry = build_table_registry_from_python(tables_obj);
    if (!registry.has_value()) {
        return set_python_error_from_message(
            PyExc_RuntimeError, format_ibex_pyarrow_error("table import error", registry.error()));
    }

    auto scalars = build_scalar_registry_from_python(scalars_obj);
    if (!scalars.has_value()) {
        return set_python_error_from_message(
            PyExc_RuntimeError, format_ibex_pyarrow_error("scalar import error", scalars.error()));
    }

    auto plugin_paths = parse_plugin_paths(plugin_paths_obj);
    if (!plugin_paths.has_value()) {
        return set_python_error_from_message(
            PyExc_RuntimeError,
            format_ibex_pyarrow_error("plugin path error", plugin_paths.error()));
    }

    auto evaluated = eval_table_impl(source, *registry, *scalars, *plugin_paths);
    if (!evaluated.has_value()) {
        return set_python_error_from_message(PyExc_RuntimeError, evaluated.error());
    }

    return export_result_table_or_none(*evaluated);
}

PyObject* py_eval_file(PyObject* /*self*/, PyObject* args, PyObject* kwargs) {
    static const char* kwlist[] = {"path", "tables", "scalars", "plugin_paths", nullptr};
    const char* path = nullptr;
    PyObject* tables_obj = Py_None;
    PyObject* scalars_obj = Py_None;
    PyObject* plugin_paths_obj = Py_None;
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "s|OOO", const_cast<char**>(kwlist), &path,
                                     &tables_obj, &scalars_obj, &plugin_paths_obj)) {
        return nullptr;
    }

    auto source = read_text_file(path);
    if (!source.has_value()) {
        return set_python_error_from_message(PyExc_RuntimeError, source.error());
    }

    auto registry = build_table_registry_from_python(tables_obj);
    if (!registry.has_value()) {
        return set_python_error_from_message(
            PyExc_RuntimeError, format_ibex_pyarrow_error("table import error", registry.error()));
    }

    auto scalars = build_scalar_registry_from_python(scalars_obj);
    if (!scalars.has_value()) {
        return set_python_error_from_message(
            PyExc_RuntimeError, format_ibex_pyarrow_error("scalar import error", scalars.error()));
    }

    auto plugin_paths = parse_plugin_paths(plugin_paths_obj);
    if (!plugin_paths.has_value()) {
        return set_python_error_from_message(
            PyExc_RuntimeError,
            format_ibex_pyarrow_error("plugin path error", plugin_paths.error()));
    }

    auto evaluated = eval_table_impl(*source, *registry, *scalars, *plugin_paths);
    if (!evaluated.has_value()) {
        return set_python_error_from_message(PyExc_RuntimeError, evaluated.error());
    }

    return export_result_table_or_none(*evaluated);
}

PyObject* py_session_eval_table(PyObject* /*self*/, PyObject* args, PyObject* kwargs) {
    static const char* kwlist[] = {"session", "source", "tables", "scalars", nullptr};
    PyObject* session_obj = Py_None;
    const char* source = nullptr;
    PyObject* tables_obj = Py_None;
    PyObject* scalars_obj = Py_None;
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "Os|OO", const_cast<char**>(kwlist),
                                     &session_obj, &source, &tables_obj, &scalars_obj)) {
        return nullptr;
    }

    auto session = session_from_capsule(session_obj);
    if (!session.has_value()) {
        return set_python_error_from_message(PyExc_RuntimeError, session.error());
    }

    auto extra_tables = build_table_registry_from_python(tables_obj);
    if (!extra_tables.has_value()) {
        return set_python_error_from_message(
            PyExc_RuntimeError,
            format_ibex_pyarrow_error("table import error", extra_tables.error()));
    }

    auto extra_scalars = build_scalar_registry_from_python(scalars_obj);
    if (!extra_scalars.has_value()) {
        return set_python_error_from_message(
            PyExc_RuntimeError,
            format_ibex_pyarrow_error("scalar import error", extra_scalars.error()));
    }

    auto evaluated = eval_table_in_session(**session, source, *extra_tables, *extra_scalars);
    if (!evaluated.has_value()) {
        return set_python_error_from_message(PyExc_RuntimeError, evaluated.error());
    }

    return export_result_table_or_none(*evaluated);
}

PyObject* py_session_eval_file(PyObject* /*self*/, PyObject* args, PyObject* kwargs) {
    static const char* kwlist[] = {"session", "path", "tables", "scalars", nullptr};
    PyObject* session_obj = Py_None;
    const char* path = nullptr;
    PyObject* tables_obj = Py_None;
    PyObject* scalars_obj = Py_None;
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "Os|OO", const_cast<char**>(kwlist),
                                     &session_obj, &path, &tables_obj, &scalars_obj)) {
        return nullptr;
    }

    auto session = session_from_capsule(session_obj);
    if (!session.has_value()) {
        return set_python_error_from_message(PyExc_RuntimeError, session.error());
    }

    auto source = read_text_file(path);
    if (!source.has_value()) {
        return set_python_error_from_message(PyExc_RuntimeError, source.error());
    }

    auto extra_tables = build_table_registry_from_python(tables_obj);
    if (!extra_tables.has_value()) {
        return set_python_error_from_message(
            PyExc_RuntimeError,
            format_ibex_pyarrow_error("table import error", extra_tables.error()));
    }

    auto extra_scalars = build_scalar_registry_from_python(scalars_obj);
    if (!extra_scalars.has_value()) {
        return set_python_error_from_message(
            PyExc_RuntimeError,
            format_ibex_pyarrow_error("scalar import error", extra_scalars.error()));
    }

    auto evaluated = eval_table_in_session(**session, *source, *extra_tables, *extra_scalars);
    if (!evaluated.has_value()) {
        return set_python_error_from_message(PyExc_RuntimeError, evaluated.error());
    }

    return export_result_table_or_none(*evaluated);
}

#if defined(__clang__) && __has_warning("-Wcast-function-type-mismatch")
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wcast-function-type-mismatch"
#endif
PyMethodDef kModuleMethods[] = {
    {"create_session", reinterpret_cast<PyCFunction>(py_create_session),
     METH_VARARGS | METH_KEYWORDS,
     PyDoc_STR("Create a persistent Ibex session for repeated evaluation.")},
    {"reset_session", reinterpret_cast<PyCFunction>(py_reset_session), METH_VARARGS | METH_KEYWORDS,
     PyDoc_STR("Reset a persistent Ibex session.")},
    {"eval_table", reinterpret_cast<PyCFunction>(py_eval_table), METH_VARARGS | METH_KEYWORDS,
     PyDoc_STR("Evaluate an Ibex source string and return a pyarrow.Table.")},
    {"eval_file", reinterpret_cast<PyCFunction>(py_eval_file), METH_VARARGS | METH_KEYWORDS,
     PyDoc_STR("Evaluate an Ibex file and return a pyarrow.Table.")},
    {"session_eval_table", reinterpret_cast<PyCFunction>(py_session_eval_table),
     METH_VARARGS | METH_KEYWORDS,
     PyDoc_STR("Evaluate an Ibex source string within a persistent session and return a "
               "pyarrow.Table or None.")},
    {"session_eval_file", reinterpret_cast<PyCFunction>(py_session_eval_file),
     METH_VARARGS | METH_KEYWORDS,
     PyDoc_STR(
         "Evaluate an Ibex file within a persistent session and return a pyarrow.Table or None.")},
    {nullptr, nullptr, 0, nullptr},
};
#if defined(__clang__) && __has_warning("-Wcast-function-type-mismatch")
#pragma clang diagnostic pop
#endif

PyModuleDef kModuleDef = {
    PyModuleDef_HEAD_INIT,
    "ibex_pyarrow",
    "PyArrow bridge for evaluating Ibex tables.",
    -1,
    kModuleMethods,
    nullptr,
    nullptr,
    nullptr,
    nullptr,
};

}  // namespace

PyMODINIT_FUNC PyInit_ibex_pyarrow() {
    PyDateTime_IMPORT;
    return PyModule_Create(&kModuleDef);
}
