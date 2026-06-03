#pragma once

// Real Microsoft LightGBM model plugin. Registers the "lightgbm" model method
// (ExternRegistry::register_model) implementing fit + predict over a gradient-
// boosted tree booster. The booster is kept alive in a self-freeing handle
// (FittedModel::native) so model_predict can reuse it on new data.
//
// All implementation lives in lightgbm.cpp; the plugin entry point is the
// extern "C" ibex_register hook.
