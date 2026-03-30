#include <ibex/interop/arrow_c_data.hpp>
#include <ibex/parser/lower.hpp>
#include <ibex/parser/parser.hpp>
#include <ibex/runtime/interpreter.hpp>

#include <expected>
#include <fstream>
#include <memory>
#include <Python.h>
#include <sstream>
#include <string>
#include <utility>

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

auto set_python_error_from_message(PyObject* exc_type, const std::string& message) -> PyObject* {
    PyErr_SetString(exc_type, message.c_str());
    return nullptr;
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

auto eval_table_impl(const std::string& source, const ibex::runtime::TableRegistry& registry)
    -> std::expected<std::shared_ptr<const ibex::runtime::Table>, std::string> {
    auto parsed = ibex::parser::parse(source);
    if (!parsed.has_value()) {
        return std::unexpected(parsed.error().format());
    }

    auto lowered = ibex::parser::lower(*parsed);
    if (!lowered.has_value()) {
        return std::unexpected(lowered.error().message);
    }

    auto evaluated = ibex::runtime::interpret(*lowered.value(), registry);
    if (!evaluated.has_value()) {
        return std::unexpected(evaluated.error());
    }

    return std::make_shared<ibex::runtime::Table>(std::move(*evaluated));
}

auto read_text_file(const std::string& path) -> std::expected<std::string, std::string> {
    std::ifstream in(path);
    if (!in.is_open()) {
        return std::unexpected("failed to open ibex file: " + path);
    }

    std::ostringstream buffer;
    buffer << in.rdbuf();
    if (!in.good() && !in.eof()) {
        return std::unexpected("failed to read ibex file: " + path);
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

PyObject* py_eval_table(PyObject* /*self*/, PyObject* args, PyObject* kwargs) {
    static const char* kwlist[] = {"source", "tables", nullptr};
    const char* source = nullptr;
    PyObject* tables_obj = Py_None;
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "s|O", const_cast<char**>(kwlist), &source,
                                     &tables_obj)) {
        return nullptr;
    }

    auto registry = build_table_registry_from_python(tables_obj);
    if (!registry.has_value()) {
        return set_python_error_from_message(PyExc_RuntimeError, registry.error());
    }

    auto evaluated = eval_table_impl(source, *registry);
    if (!evaluated.has_value()) {
        return set_python_error_from_message(PyExc_RuntimeError, evaluated.error());
    }

    auto capsules = wrap_arrow_export(std::move(*evaluated));
    if (!capsules.has_value()) {
        return set_python_error_from_message(PyExc_RuntimeError, capsules.error());
    }

    PyObject* table = import_pyarrow_table(capsules->schema_capsule, capsules->array_capsule);
    if (table == nullptr) {
        return nullptr;
    }

    return table;
}

PyObject* py_eval_file(PyObject* /*self*/, PyObject* args, PyObject* kwargs) {
    static const char* kwlist[] = {"path", "tables", nullptr};
    const char* path = nullptr;
    PyObject* tables_obj = Py_None;
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "s|O", const_cast<char**>(kwlist), &path,
                                     &tables_obj)) {
        return nullptr;
    }

    auto source = read_text_file(path);
    if (!source.has_value()) {
        return set_python_error_from_message(PyExc_RuntimeError, source.error());
    }

    auto registry = build_table_registry_from_python(tables_obj);
    if (!registry.has_value()) {
        return set_python_error_from_message(PyExc_RuntimeError, registry.error());
    }

    auto evaluated = eval_table_impl(*source, *registry);
    if (!evaluated.has_value()) {
        return set_python_error_from_message(PyExc_RuntimeError, evaluated.error());
    }

    auto capsules = wrap_arrow_export(std::move(*evaluated));
    if (!capsules.has_value()) {
        return set_python_error_from_message(PyExc_RuntimeError, capsules.error());
    }

    PyObject* table = import_pyarrow_table(capsules->schema_capsule, capsules->array_capsule);
    if (table == nullptr) {
        return nullptr;
    }

    return table;
}

#if defined(__clang__)
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wcast-function-type-mismatch"
#endif
PyMethodDef kModuleMethods[] = {
    {"eval_table", reinterpret_cast<PyCFunction>(py_eval_table), METH_VARARGS | METH_KEYWORDS,
     PyDoc_STR("Evaluate an Ibex source string and return a pyarrow.Table.")},
    {"eval_file", reinterpret_cast<PyCFunction>(py_eval_file), METH_VARARGS | METH_KEYWORDS,
     PyDoc_STR("Evaluate an Ibex file and return a pyarrow.Table.")},
    {nullptr, nullptr, 0, nullptr},
};
#if defined(__clang__)
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
    return PyModule_Create(&kModuleDef);
}
