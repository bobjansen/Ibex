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

auto eval_table_impl(const std::string& source)
    -> std::expected<std::shared_ptr<const ibex::runtime::Table>, std::string> {
    auto parsed = ibex::parser::parse(source);
    if (!parsed.has_value()) {
        return std::unexpected(parsed.error().format());
    }

    auto lowered = ibex::parser::lower(*parsed);
    if (!lowered.has_value()) {
        return std::unexpected(lowered.error().message);
    }

    ibex::runtime::TableRegistry registry;
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

PyObject* py_eval_table(PyObject* /*self*/, PyObject* args, PyObject* kwargs) {
    static const char* kwlist[] = {"source", nullptr};
    const char* source = nullptr;
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "s", const_cast<char**>(kwlist), &source)) {
        return nullptr;
    }

    auto evaluated = eval_table_impl(source);
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
    static const char* kwlist[] = {"path", nullptr};
    const char* path = nullptr;
    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "s", const_cast<char**>(kwlist), &path)) {
        return nullptr;
    }

    auto source = read_text_file(path);
    if (!source.has_value()) {
        return set_python_error_from_message(PyExc_RuntimeError, source.error());
    }

    auto evaluated = eval_table_impl(*source);
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
