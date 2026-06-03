#pragma once

// K-means clustering plugin (pure C++, no external dependency). Serves as a
// second model plugin to validate that ExternRegistry::register_model
// generalizes beyond supervised regression to an unsupervised model.
//
// Registers the "kmeans" model method: fit runs Lloyd's algorithm and keeps the
// centroids alive in a self-freeing FittedModel::native handle; predict assigns
// each new row to its nearest centroid. Invoke with a response-less formula:
//
//   df[model { ~ x1 + x2, method = kmeans, k = 3 }]
//
// All implementation lives in kmeans.cpp; the entry point is ibex_register.
