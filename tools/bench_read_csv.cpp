#include <CLI/CLI.hpp>

#include <chrono>
#include <iomanip>
#include <iostream>
#include <limits>
#include <string>

#include "../libs/csv/csv.hpp"

int main(int argc, char** argv) {
    CLI::App app{"Benchmark read_csv() in isolation"};

    std::string input = "examples/measurements.txt";
    std::string nulls;
    std::string delimiter = ",";
    bool has_header = true;
    std::string schema;
    int warmup = 1;
    int iters = 5;

    app.add_option("--input", input, "CSV file to read");
    app.add_option("--nulls", nulls, "Comma-separated null tokens");
    app.add_option("--delimiter", delimiter, "Single-character CSV delimiter");
    app.add_flag("--no-header", has_header, "Treat input as headerless")->default_val(false);
    app.add_option("--schema", schema, "Schema hint, e.g. 'cat,f64' or 'name:cat,temp:f64'");
    app.add_option("--warmup", warmup, "Warmup iterations")->check(CLI::NonNegativeNumber);
    app.add_option("--iters", iters, "Measured iterations")->check(CLI::PositiveNumber);

    CLI11_PARSE(app, argc, argv);

    has_header = !app.get_option("--no-header")->as<bool>();

    for (int i = 0; i < warmup; ++i) {
        (void)read_csv(input, nulls, delimiter, has_header, schema);
    }

    double total_ms = 0.0;
    double min_ms = std::numeric_limits<double>::infinity();
    double max_ms = 0.0;
    std::size_t rows = 0;
    std::size_t cols = 0;

    for (int i = 0; i < iters; ++i) {
        const auto start = std::chrono::steady_clock::now();
        auto table = read_csv(input, nulls, delimiter, has_header, schema);
        const auto end = std::chrono::steady_clock::now();
        const double elapsed_ms = std::chrono::duration<double, std::milli>(end - start).count();
        total_ms += elapsed_ms;
        min_ms = std::min(min_ms, elapsed_ms);
        max_ms = std::max(max_ms, elapsed_ms);
        rows = table.rows();
        cols = table.columns.size();
    }

    std::cout << "framework\tavg_ms\tmin_ms\tmax_ms\titers\trows\tcols\n";
    std::cout << "read_csv\t" << std::fixed << std::setprecision(3) << (total_ms / iters) << '\t'
              << min_ms << '\t' << max_ms << '\t' << iters << '\t' << rows << '\t' << cols << '\n';

    return 0;
}
