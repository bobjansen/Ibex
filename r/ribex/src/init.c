#define R_NO_REMAP

#include <R_ext/Rdynload.h>
#include <Rinternals.h>

SEXP ribex_c_eval_file(SEXP path_sexp, SEXP plugin_paths_sexp);
SEXP ribex_c_eval_ibex(SEXP query_sexp, SEXP plugin_paths_sexp);

static const R_CallMethodDef call_methods[] = {
    {"ribex_c_eval_file", (DL_FUNC)&ribex_c_eval_file, 2},
    {"ribex_c_eval_ibex", (DL_FUNC)&ribex_c_eval_ibex, 2},
    {NULL, NULL, 0},
};

void R_init_ribex(DllInfo* dll) {
    R_registerRoutines(dll, NULL, call_methods, NULL, NULL);
    R_useDynamicSymbols(dll, FALSE);
}
