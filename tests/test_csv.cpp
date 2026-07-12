#include <catch2/catch_approx.hpp>
#include <catch2/catch_test_macros.hpp>

#include <csv.hpp>
#include <filesystem>
#include <fstream>
#include <string>

namespace {

auto write_csv(const std::filesystem::path& path, const char* content) {
    std::ofstream out(path);
    out << content;
}

auto tmp(const char* name) -> std::filesystem::path {
    return std::filesystem::temp_directory_path() / name;
}

auto get_string_at(const ibex::runtime::Table& table, const char* name, std::size_t row)
    -> std::string {
    if (const auto* col = std::get_if<ibex::Column<std::string>>(table.find(name))) {
        return std::string((*col)[row]);
    }
    if (const auto* col = std::get_if<ibex::Column<ibex::Categorical>>(table.find(name))) {
        return std::string((*col)[row]);
    }
    return {};
}

auto is_null_at(const ibex::runtime::Table& table, const char* name, std::size_t row) -> bool {
    const auto* entry = table.find_entry(name);
    REQUIRE(entry != nullptr);
    return ibex::runtime::is_null(*entry, row);
}

}  // namespace

TEST_CASE("Read simple CSV - int and string columns") {
    auto path = tmp("ibex_test_simple.csv");
    write_csv(path, "price,symbol\n10,A\n20,B\n30,A\n");

    auto table = read_csv(path.string());
    const auto* prices = std::get_if<ibex::Column<std::int64_t>>(table.find("price"));
    REQUIRE(prices != nullptr);
    REQUIRE(prices->size() == 3);
    REQUIRE((*prices)[0] == 10);
    REQUIRE((*prices)[1] == 20);
    REQUIRE((*prices)[2] == 30);
    REQUIRE(get_string_at(table, "symbol", 0) == "A");
    REQUIRE(get_string_at(table, "symbol", 1) == "B");
    REQUIRE(get_string_at(table, "symbol", 2) == "A");
}

TEST_CASE("Read CSV - categorical promotion preserves single code per distinct value",
          "[csv][categorical]") {
    // Reproduces a bug where the categorical-inference path stored
    // `std::string_view` keys in its dedup map, with the views pointing into
    // `dict.back()`. As `dict` grew it could reallocate, invalidating every
    // existing key view; subsequent rows that should have hashed to an
    // existing entry instead created fresh dictionary entries with new codes.
    // Symptom downstream: `select { ... } by symbol` returned more groups than
    // distinct symbols. This test guards a many-distinct-key CSV that's small
    // enough to stay under the 10% categorical-promotion ratio.
    auto path = tmp("ibex_test_categorical_dedup.csv");
    constexpr std::size_t kSymbols = 252;
    constexpr std::size_t kRepeats = 200;
    {
        std::ofstream out(path);
        out << "symbol,price\n";
        std::uint64_t lcg = 0x9E3779B97F4A7C15ULL;
        for (std::size_t i = 0; i < kSymbols * kRepeats; ++i) {
            lcg = (lcg * 6364136223846793005ULL) + 1442695040888963407ULL;
            const std::size_t s = static_cast<std::size_t>(lcg >> 32) % kSymbols;
            char sym[4] = {
                static_cast<char>('A' + ((s / (26 * 26)) % 26)),
                static_cast<char>('A' + ((s / 26) % 26)),
                static_cast<char>('A' + (s % 26)),
                '\0',
            };
            out << sym << "," << s << "\n";
        }
    }

    auto table = read_csv(path.string());
    const auto* sym_col = std::get_if<ibex::Column<ibex::Categorical>>(table.find("symbol"));
    REQUIRE(sym_col != nullptr);
    REQUIRE(sym_col->size() == kSymbols * kRepeats);

    // The dictionary must contain exactly the kSymbols distinct values; if the
    // bug is present the dictionary swells past kSymbols because re-encountered
    // values get fresh codes after each `dict` reallocation.
    REQUIRE(sym_col->dictionary().size() == kSymbols);

    // Every code must round-trip through the dictionary back to the same
    // 3-character symbol that was written for that row.
    std::uint64_t lcg = 0x9E3779B97F4A7C15ULL;
    for (std::size_t i = 0; i < kSymbols * kRepeats; ++i) {
        lcg = (lcg * 6364136223846793005ULL) + 1442695040888963407ULL;
        const std::size_t s = static_cast<std::size_t>(lcg >> 32) % kSymbols;
        const std::string expected = {
            static_cast<char>('A' + ((s / (26 * 26)) % 26)),
            static_cast<char>('A' + ((s / 26) % 26)),
            static_cast<char>('A' + (s % 26)),
        };
        REQUIRE((*sym_col)[i] == expected);
    }
}

TEST_CASE("Read CSV - schema-hinted categorical preserves single code per distinct value",
          "[csv][categorical][schema]") {
    auto path = tmp("ibex_test_categorical_schema.csv");
    constexpr std::size_t kSymbols = 252;
    constexpr std::size_t kRepeats = 200;
    {
        std::ofstream out(path);
        out << "symbol,price\n";
        std::uint64_t lcg = 0x9E3779B97F4A7C15ULL;
        for (std::size_t i = 0; i < kSymbols * kRepeats; ++i) {
            lcg = (lcg * 6364136223846793005ULL) + 1442695040888963407ULL;
            const std::size_t s = static_cast<std::size_t>(lcg >> 32) % kSymbols;
            char sym[4] = {
                static_cast<char>('A' + ((s / (26 * 26)) % 26)),
                static_cast<char>('A' + ((s / 26) % 26)),
                static_cast<char>('A' + (s % 26)),
                '\0',
            };
            out << sym << "," << s << "\n";
        }
    }

    auto table = read_csv(path.string(), /*null_spec=*/"", /*delimiter=*/",",
                          /*has_header=*/true, /*schema=*/"symbol:cat,price:int");
    const auto* sym_col = std::get_if<ibex::Column<ibex::Categorical>>(table.find("symbol"));
    REQUIRE(sym_col != nullptr);
    REQUIRE(sym_col->size() == kSymbols * kRepeats);
    REQUIRE(sym_col->dictionary().size() == kSymbols);
}

TEST_CASE("Read CSV - float64 column detection") {
    auto path = tmp("ibex_test_float.csv");
    write_csv(path, "price,qty\n1.5,10\n2.25,20\n0.5,5\n");

    auto table = read_csv(path.string());
    const auto* prices = std::get_if<ibex::Column<double>>(table.find("price"));
    const auto* qtys = std::get_if<ibex::Column<std::int64_t>>(table.find("qty"));
    REQUIRE(prices != nullptr);
    REQUIRE(qtys != nullptr);
    REQUIRE(prices->size() == 3);
    REQUIRE((*prices)[0] == Catch::Approx(1.5));
    REQUIRE((*prices)[1] == Catch::Approx(2.25));
    REQUIRE((*prices)[2] == Catch::Approx(0.5));
    REQUIRE((*qtys)[0] == 10);
}

TEST_CASE("Read CSV - large int64 values") {
    auto path = tmp("ibex_test_int64.csv");
    // Value exceeds int32 range
    write_csv(path, "ts\n9999999999\n1000000000\n");

    auto table = read_csv(path.string());
    const auto* ts = std::get_if<ibex::Column<std::int64_t>>(table.find("ts"));
    REQUIRE(ts != nullptr);
    REQUIRE((*ts)[0] == 9999999999LL);
    REQUIRE((*ts)[1] == 1000000000LL);
}

TEST_CASE("Read CSV - single column") {
    auto path = tmp("ibex_test_single.csv");
    write_csv(path, "value\n1\n2\n3\n");

    auto table = read_csv(path.string());
    REQUIRE(table.rows() == 3);
    const auto* col = std::get_if<ibex::Column<std::int64_t>>(table.find("value"));
    REQUIRE(col != nullptr);
    REQUIRE((*col)[0] == 1);
    REQUIRE((*col)[2] == 3);
}

TEST_CASE("Read CSV - mixed numeric/non-numeric falls back to string") {
    auto path = tmp("ibex_test_mixed.csv");
    // "price" column has a non-numeric value -> whole column must become string
    write_csv(path, "price\n10\n20\nN/A\n30\n");

    auto table = read_csv(path.string());
    REQUIRE(table.rows() == 4);
    REQUIRE(get_string_at(table, "price", 0) == "10");
    REQUIRE(get_string_at(table, "price", 2) == "N/A");
}

TEST_CASE("Read CSV - single data row") {
    auto path = tmp("ibex_test_onerow.csv");
    write_csv(path, "a,b\n42,hello\n");

    auto table = read_csv(path.string());
    REQUIRE(table.rows() == 1);
    const auto* a = std::get_if<ibex::Column<std::int64_t>>(table.find("a"));
    REQUIRE(a != nullptr);
    REQUIRE((*a)[0] == 42);
    REQUIRE(get_string_at(table, "b", 0) == "hello");
}

TEST_CASE("Read CSV - trailing comma produces empty last field") {
    auto path = tmp("ibex_test_trailing.csv");
    write_csv(path, "a,b,c\n1,2,\n3,4,\n");

    auto table = read_csv(path.string());
    REQUIRE(table.rows() == 2);
    REQUIRE(get_string_at(table, "c", 0) == "");
    REQUIRE(get_string_at(table, "c", 1) == "");
}

TEST_CASE("Read CSV - RFC 4180 quoted fields with embedded commas") {
    auto path = tmp("ibex_test_quoted.csv");
    // "name" column has commas inside quotes - old parser would split incorrectly
    write_csv(path, "id,name,score\n1,\"Smith, John\",95\n2,\"Doe, Jane\",87\n");

    auto table = read_csv(path.string());
    REQUIRE(table.rows() == 2);
    const auto* ids = std::get_if<ibex::Column<std::int64_t>>(table.find("id"));
    const auto* scores = std::get_if<ibex::Column<std::int64_t>>(table.find("score"));
    REQUIRE(ids != nullptr);
    REQUIRE(scores != nullptr);
    REQUIRE(get_string_at(table, "name", 0) == "Smith, John");
    REQUIRE(get_string_at(table, "name", 1) == "Doe, Jane");
    REQUIRE((*ids)[0] == 1);
    REQUIRE((*scores)[0] == 95);
}

TEST_CASE("Read CSV - RFC 4180 escaped quotes inside quoted field") {
    auto path = tmp("ibex_test_escaped_quotes.csv");
    // "" inside a quoted field represents a literal quote character
    write_csv(path, "msg\n\"say \"\"hello\"\"\"\n\"world\"\n");

    auto table = read_csv(path.string());
    REQUIRE(table.rows() == 2);
    REQUIRE(get_string_at(table, "msg", 0) == "say \"hello\"");
    REQUIRE(get_string_at(table, "msg", 1) == "world");
}

TEST_CASE("Read CSV - nullable parsing via null spec") {
    auto path = tmp("ibex_test_nullable_parse.csv");
    write_csv(path, "price,qty,note\n10,1,ok\n,2,NA\n30,NA,\n");

    auto table = read_csv(path.string(), "<empty>,NA");
    REQUIRE(table.rows() == 3);

    const auto* prices = std::get_if<ibex::Column<std::int64_t>>(table.find("price"));
    const auto* qtys = std::get_if<ibex::Column<std::int64_t>>(table.find("qty"));
    const auto* notes = std::get_if<ibex::Column<std::string>>(table.find("note"));
    REQUIRE(prices != nullptr);
    REQUIRE(qtys != nullptr);
    REQUIRE(notes != nullptr);

    REQUIRE((*prices)[0] == 10);
    REQUIRE((*prices)[1] == 0);  // null payload
    REQUIRE((*prices)[2] == 30);
    REQUIRE_FALSE(is_null_at(table, "price", 0));
    REQUIRE(is_null_at(table, "price", 1));
    REQUIRE_FALSE(is_null_at(table, "price", 2));

    REQUIRE((*qtys)[0] == 1);
    REQUIRE((*qtys)[1] == 2);
    REQUIRE((*qtys)[2] == 0);  // null payload
    REQUIRE_FALSE(is_null_at(table, "qty", 0));
    REQUIRE_FALSE(is_null_at(table, "qty", 1));
    REQUIRE(is_null_at(table, "qty", 2));

    REQUIRE((*notes)[0] == "ok");
    REQUIRE((*notes)[1] == "");  // null payload
    REQUIRE((*notes)[2] == "");  // null payload
    REQUIRE_FALSE(is_null_at(table, "note", 0));
    REQUIRE(is_null_at(table, "note", 1));
    REQUIRE(is_null_at(table, "note", 2));
}

TEST_CASE("Read CSV - custom delimiter preserves quoted commas and numeric inference") {
    auto path = tmp("ibex_test_semicolon.csv");
    write_csv(path, "station;temp\n\"Washington, D.C.\";12.5\nAmsterdam;-1.0\n");

    auto table = read_csv(path.string(), "", ";");
    REQUIRE(table.rows() == 2);
    REQUIRE(get_string_at(table, "station", 0) == "Washington, D.C.");
    REQUIRE(get_string_at(table, "station", 1) == "Amsterdam");

    const auto* temp = std::get_if<ibex::Column<double>>(table.find("temp"));
    REQUIRE(temp != nullptr);
    REQUIRE((*temp)[0] == Catch::Approx(12.5));
    REQUIRE((*temp)[1] == Catch::Approx(-1.0));
}

TEST_CASE("Read CSV - no-header mode numbers columns and infers types") {
    auto path = tmp("ibex_test_no_header_semicolon.csv");
    write_csv(path, "\"Washington, D.C.\";12.5\nAmsterdam;-1.0\n");

    auto table = read_csv(path.string(), "", ";", false);
    REQUIRE(table.rows() == 2);
    REQUIRE(get_string_at(table, "col1", 0) == "Washington, D.C.");
    REQUIRE(get_string_at(table, "col1", 1) == "Amsterdam");

    const auto* temp = std::get_if<ibex::Column<double>>(table.find("col2"));
    REQUIRE(temp != nullptr);
    REQUIRE((*temp)[0] == Catch::Approx(12.5));
    REQUIRE((*temp)[1] == Catch::Approx(-1.0));
}

TEST_CASE("Read CSV - schema hint forces categorical and double types") {
    auto path = tmp("ibex_test_schema_hint.csv");
    write_csv(path, "Amsterdam;12.5\nBerlin;-1.0\nAmsterdam;3.25\n");

    auto table = read_csv(path.string(), "", ";", false, "cat,f64");
    REQUIRE(table.rows() == 3);
    const auto* stations = std::get_if<ibex::Column<ibex::Categorical>>(table.find("col1"));
    REQUIRE(stations != nullptr);
    REQUIRE(stations->size() == 3);
    REQUIRE(std::string((*stations)[0]) == "Amsterdam");
    REQUIRE(std::string((*stations)[2]) == "Amsterdam");

    const auto* temp = std::get_if<ibex::Column<double>>(table.find("col2"));
    REQUIRE(temp != nullptr);
    REQUIRE((*temp)[0] == Catch::Approx(12.5));
    REQUIRE((*temp)[1] == Catch::Approx(-1.0));
    REQUIRE((*temp)[2] == Catch::Approx(3.25));
}

TEST_CASE("Read CSV - schema hint named columns override inference") {
    auto path = tmp("ibex_test_schema_hint_named.csv");
    write_csv(path, "id,code\n1,100\n2,200\n3,300\n");

    auto table = read_csv(path.string(), "", ",", true, "code:str");
    const auto* codes = std::get_if<ibex::Column<std::string>>(table.find("code"));
    REQUIRE(codes != nullptr);
    REQUIRE((*codes)[0] == "100");
    REQUIRE((*codes)[2] == "300");
    // id has no hint — falls through to inference and becomes int64.
    const auto* ids = std::get_if<ibex::Column<std::int64_t>>(table.find("id"));
    REQUIRE(ids != nullptr);
    REQUIRE((*ids)[1] == 2);
}

TEST_CASE("Read CSV - schema hint parse failure throws") {
    auto path = tmp("ibex_test_schema_hint_fail.csv");
    write_csv(path, "x\nnot_a_number\n");

    REQUIRE_THROWS_AS(read_csv(path.string(), "", ",", true, "f64"), std::runtime_error);
}

TEST_CASE("Read CSV - schema hint 'date' parses YYYY-MM-DD into days-since-epoch") {
    auto path = tmp("ibex_test_schema_hint_date.csv");
    write_csv(path, "id,d\n1,1970-01-01\n2,1996-03-13\n3,2026-07-12\n");

    auto table = read_csv(path.string(), "", ",", true, "d:date");
    const auto* dates = std::get_if<ibex::Column<ibex::Date>>(table.find("d"));
    REQUIRE(dates != nullptr);
    REQUIRE((*dates)[0].days == 0);
    REQUIRE((*dates)[1].days == 9568);   // matches parser.cpp's date"1996-03-13" literal
    REQUIRE((*dates)[2].days == 20646);  // matches parser.cpp's date"2026-07-12" literal
}

TEST_CASE("Read CSV - schema hint 'date' parse failure throws") {
    auto path = tmp("ibex_test_schema_hint_date_fail.csv");
    write_csv(path, "d\nnot-a-date\n");

    REQUIRE_THROWS_AS(read_csv(path.string(), "", ",", true, "date"), std::runtime_error);
}

TEST_CASE(
    "ChunkedCsvSourceOperator - pipe-delimited, no-header, trailing-delimiter rows with a "
    "date column (matches TPC-H dbgen .tbl layout)") {
    auto path = tmp("ibex_test_chunked_tbl.csv");
    // dbgen emits a trailing '|' on every row; the last field is therefore empty.
    write_csv(path, "1|17|1996-03-13|\n2|36|1996-04-12|\n3|8|1996-01-29|\n");

    ChunkedCsvSourceOperator op(
        path.string(), {"id", "qty", "shipdate", "trailing"},
        {CsvColumnKind::Int, CsvColumnKind::Int, CsvColumnKind::Date, CsvColumnKind::String}, '|',
        /*rows_per_chunk=*/64);

    auto first = op.next();
    REQUIRE(first.has_value());
    REQUIRE(first.value().has_value());
    auto& chunk = *first.value();
    REQUIRE(chunk.rows() == 3);

    auto find_col = [&](const char* name) -> const ibex::runtime::ColumnValue* {
        for (const auto& entry : chunk.columns) {
            if (entry.name == name) {
                return entry.column.get();
            }
        }
        return nullptr;
    };

    const auto* id_col = std::get_if<ibex::Column<std::int64_t>>(find_col("id"));
    REQUIRE(id_col != nullptr);
    REQUIRE((*id_col)[0] == 1);
    REQUIRE((*id_col)[2] == 3);

    const auto* date_col = std::get_if<ibex::Column<ibex::Date>>(find_col("shipdate"));
    REQUIRE(date_col != nullptr);
    REQUIRE((*date_col)[0].days == 9568);  // 1996-03-13

    const auto* trailing_col = std::get_if<ibex::Column<std::string>>(find_col("trailing"));
    REQUIRE(trailing_col != nullptr);
    REQUIRE((*trailing_col)[0].empty());

    auto second = op.next();
    REQUIRE(second.has_value());
    REQUIRE_FALSE(second.value().has_value());
}

// ---------------------------------------------------------------------------
// write_csv tests
// ---------------------------------------------------------------------------

TEST_CASE("Write CSV - int and string columns round-trip") {
    auto path = tmp("ibex_test_write_simple.csv");
    write_csv(path, "price,symbol\n10,A\n20,B\n30,A\n");

    auto original = read_csv(path.string());
    auto out_path = tmp("ibex_test_write_simple_out.csv");
    auto rows_written = write_csv(original, out_path.string());
    REQUIRE(rows_written == 3);

    auto reread = read_csv(out_path.string());
    REQUIRE(reread.rows() == 3);
    const auto* prices = std::get_if<ibex::Column<std::int64_t>>(reread.find("price"));
    REQUIRE(prices != nullptr);
    REQUIRE((*prices)[0] == 10);
    REQUIRE((*prices)[1] == 20);
    REQUIRE((*prices)[2] == 30);
    REQUIRE(get_string_at(reread, "symbol", 0) == "A");
    REQUIRE(get_string_at(reread, "symbol", 1) == "B");
    REQUIRE(get_string_at(reread, "symbol", 2) == "A");
}

TEST_CASE("Write CSV - double column round-trip") {
    auto path = tmp("ibex_test_write_double.csv");
    write_csv(path, "x,y\n1.5,2.25\n0.5,3.0\n");

    auto original = read_csv(path.string());
    auto out_path = tmp("ibex_test_write_double_out.csv");
    auto rows_written = write_csv(original, out_path.string());
    REQUIRE(rows_written == 2);

    auto reread = read_csv(out_path.string());
    const auto* x = std::get_if<ibex::Column<double>>(reread.find("x"));
    const auto* y = std::get_if<ibex::Column<double>>(reread.find("y"));
    REQUIRE(x != nullptr);
    REQUIRE(y != nullptr);
    REQUIRE((*x)[0] == Catch::Approx(1.5));
    REQUIRE((*x)[1] == Catch::Approx(0.5));
    REQUIRE((*y)[0] == Catch::Approx(2.25));
    REQUIRE((*y)[1] == Catch::Approx(3.0));
}

TEST_CASE("Write CSV - double column preserves full precision for large-magnitude values") {
    // `out << double` truncates to the stream's default 6-significant-digit
    // precision (e.g. 3083843.06 -> "3.08384e+06"), which silently corrupts
    // any sum/aggregate large enough to exceed 6 digits -- exactly the shape
    // of TPC-H revenue totals. write_csv must round-trip exactly instead.
    ibex::runtime::Table table;
    table.add_column("revenue", ibex::Column<double>({3083843.06, 55908965222.827362}));

    auto out_path = tmp("ibex_test_write_double_precision.csv");
    write_csv(table, out_path.string());

    auto reread = read_csv(out_path.string());
    const auto* revenue = std::get_if<ibex::Column<double>>(reread.find("revenue"));
    REQUIRE(revenue != nullptr);
    REQUIRE((*revenue)[0] == Catch::Approx(3083843.06));
    REQUIRE((*revenue)[1] == Catch::Approx(55908965222.827362));
}

TEST_CASE("Write CSV - fields with commas are quoted") {
    // Build a table that has a string column containing commas.
    ibex::runtime::Table table;
    table.add_column("name", ibex::Column<std::string>({"Smith, John", "Doe, Jane"}));
    table.add_column("score", ibex::Column<std::int64_t>({95, 87}));

    auto out_path = tmp("ibex_test_write_quoted.csv");
    auto rows_written = write_csv(table, out_path.string());
    REQUIRE(rows_written == 2);

    // Read back and verify the commas survived the round-trip.
    auto reread = read_csv(out_path.string());
    REQUIRE(reread.rows() == 2);
    REQUIRE(get_string_at(reread, "name", 0) == "Smith, John");
    REQUIRE(get_string_at(reread, "name", 1) == "Doe, Jane");
    const auto* scores = std::get_if<ibex::Column<std::int64_t>>(reread.find("score"));
    REQUIRE(scores != nullptr);
    REQUIRE((*scores)[0] == 95);
    REQUIRE((*scores)[1] == 87);
}

TEST_CASE("Write CSV - fields with double-quotes are escaped") {
    ibex::runtime::Table table;
    table.add_column("msg", ibex::Column<std::string>({"say \"hello\"", "world"}));

    auto out_path = tmp("ibex_test_write_escaped.csv");
    write_csv(table, out_path.string());

    auto reread = read_csv(out_path.string());
    REQUIRE(get_string_at(reread, "msg", 0) == "say \"hello\"");
    REQUIRE(get_string_at(reread, "msg", 1) == "world");
}

TEST_CASE("Write CSV - null values written as empty fields") {
    auto src_path = tmp("ibex_test_write_nulls_src.csv");
    write_csv(src_path, "price,note\n10,ok\n,NA\n30,\n");

    auto original = read_csv(src_path.string(), "<empty>,NA");
    auto out_path = tmp("ibex_test_write_nulls_out.csv");
    auto rows_written = write_csv(original, out_path.string());
    REQUIRE(rows_written == 3);

    // Re-read with the same null spec to verify round-trip.
    auto reread = read_csv(out_path.string(), "<empty>,NA");
    REQUIRE(reread.rows() == 3);
    REQUIRE_FALSE(is_null_at(reread, "price", 0));
    REQUIRE(is_null_at(reread, "price", 1));
    REQUIRE_FALSE(is_null_at(reread, "price", 2));
    REQUIRE_FALSE(is_null_at(reread, "note", 0));
    REQUIRE(is_null_at(reread, "note", 1));
    REQUIRE(is_null_at(reread, "note", 2));
}

TEST_CASE("Write CSV - empty table writes only header") {
    ibex::runtime::Table table;
    table.add_column("a", ibex::Column<std::int64_t>{});
    table.add_column("b", ibex::Column<std::string>{});

    auto out_path = tmp("ibex_test_write_empty.csv");
    auto rows_written = write_csv(table, out_path.string());
    REQUIRE(rows_written == 0);

    // File should contain only the header line.
    std::ifstream f(out_path);
    std::string line;
    REQUIRE(std::getline(f, line));
    REQUIRE(line == "a,b");
    REQUIRE_FALSE(std::getline(f, line));  // no data rows
}
