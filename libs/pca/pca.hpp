#pragma once

// Principal Component Analysis plugin (pure C++, Jacobi symmetric eigensolver).
// A third model plugin, chosen to stress a new axis of the register_model
// abstraction: a *transform* whose fit and predict both produce MULTIPLE output
// columns (the k principal-component scores), unlike the single-column output of
// the lightgbm and kmeans plugins.
//
//   df[model { ~ x1 + x2 + x3, method = pca, k = 2 }]
//     result / model_fitted -> { pc1, pc2 }   (in-sample scores)
//     model_summary         -> loadings (component x feature)
//     model_predict(m, nd)  -> { pc1, pc2 }   (scores for new rows)
//
// All implementation lives in pca.cpp; the entry point is ibex_register.
