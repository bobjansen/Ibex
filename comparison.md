# Five small tasks, five languages

Five common data-shape questions, asked of the same dataset, written in
**Ibex**, **Pandas**, **Polars**, **DuckDB SQL**, and **R**. No installation
beyond each language's own runtime — every snippet pulls the data straight
from a public HTTPS URL, so you can paste any block into a REPL and see the
result immediately.

**Dataset.** January 2024 yellow-cab trips from the NYC Taxi & Limousine
Commission — about 3 million rows, 50 MB Parquet, served as a public
ZSTD-compressed file from cloudfront. The zone lookup is a separate CSV from
the same host.

```
https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet
https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv
```

**Reading these snippets.** Each task shows Ibex first, then the Pandas
equivalent inline. The Polars, DuckDB, and R versions are tucked into
collapsible sections — click to expand. Every snippet is a leaf
expression, written for an interactive REPL where the trailing value
auto-prints (Ibex `;`, Python's `>>>`, the DuckDB CLI, R's `>`). All Ibex
outputs shown below are real — captured from running the same script
against the live URL.

> **Heads up — Pandas + CloudFront.** Cloudfront occasionally returns HTTP
> 403 to Python's default `urllib` user-agent after many requests in a row.
> If `pd.read_parquet(URL)` fails with a 403, either download the file
> once with `curl -A 'Mozilla/5.0' …` and point `pd.read_parquet` at the
> local copy, or use `pyarrow.parquet.read_table(URL).to_pandas()` which
> uses Arrow's HTTP client. Polars (via `object_store`), DuckDB (via its
> `httpfs` extension), and Ibex (via libcurl) don't hit this.

---

## Task 1 — Trip count and average fare by pickup hour

How does the city's taxi load vary across the 24 hours of the day?

### Ibex
```ibex
import "parquet";

let trips = read_parquet(
  "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet");

trips[head 5];

trips[select { trips = count(), avg_fare = mean(fare_amount) },
      by { hour = hour(tpep_pickup_datetime) },
      order { hour asc }];
```

```
rows: 24
+------+--------+----------+
| hour | trips  | avg_fare |
+------+--------+----------+
| 0    |  79094 | 19.20266 |
| 1    |  53627 | 17.52730 |
| 2    |  37517 | 16.48288 |
| 3    |  24811 | 18.15013 |
| 4    |  16742 | 22.51865 |
| 5    |  18764 | 26.61992 |
| 6    |  41429 | 21.65040 |
| 7    |  83719 | 18.53918 |
| 8    | 117209 | 17.65468 |
| 9    | 128970 | 17.70842 |
+------+--------+----------+
... (14 more rows)
```

### Pandas
```python
import pandas as pd

trips = pd.read_parquet(
    "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet")

print(trips.head())

(trips.assign(hour=trips["tpep_pickup_datetime"].dt.hour)
      .groupby("hour", as_index=False)
      .agg(trips=("tpep_pickup_datetime", "size"),
           avg_fare=("fare_amount", "mean"))
      .sort_values("hour"))
```

<details>
<summary><b>Polars</b></summary>

```python
import polars as pl

trips = pl.scan_parquet(
    "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet")

(trips.group_by(hour=pl.col("tpep_pickup_datetime").dt.hour())
      .agg(trips=pl.len(), avg_fare=pl.col("fare_amount").mean())
      .sort("hour")
      .collect())
```
</details>

<details>
<summary><b>DuckDB SQL</b></summary>

```sql
INSTALL httpfs; LOAD httpfs;

SELECT hour(tpep_pickup_datetime) AS hour,
       count(*)                   AS trips,
       avg(fare_amount)           AS avg_fare
FROM 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet'
GROUP BY hour
ORDER BY hour;
```
</details>

<details>
<summary><b>R (arrow + dplyr)</b></summary>

```r
# R's arrow reads naive Parquet timestamps as UTC and lubridate's hour()
# returns the hour in the system-local timezone — pin to UTC first so hour()
# matches the values stored in the file.
Sys.setenv(TZ = "UTC")
library(arrow); library(dplyr); library(lubridate)

trips <- read_parquet(
  "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet")

trips |>
  mutate(hour = hour(tpep_pickup_datetime)) |>
  group_by(hour) |>
  summarise(trips = n(), avg_fare = mean(fare_amount)) |>
  arrange(hour)
```
</details>

---

## Task 2a — Top 10 pickup zones by revenue

Which pickup zones brought in the most fare revenue?

### Ibex
```ibex
let zones = trips[select { revenue = sum(total_amount), n = count() },
                  by PULocationID];

zones[order { revenue desc }, head 10];
```

```
rows: 10
+--------------+------------+--------+
| PULocationID | revenue    | n      |
+--------------+------------+--------+
| 132          | 1.112193e7 | 145240 |   ← JFK Airport
| 138          | 5820970    |  89533 |   ← LaGuardia
| 161          | 3369045    | 143471 |   ← Midtown Center
| 230          | 2793052    | 106324 |
| 237          | 2776195    | 142708 |
| 236          | 2729559    | 136465 |
| 186          | 2471024    | 104523 |
| 162          | 2441728    | 106717 |
| 142          | 2185566    | 104080 |
| 163          | 1980377    |  85692 |
+--------------+------------+--------+
```

### Pandas
```python
(trips.groupby("PULocationID", as_index=False)
      .agg(revenue=("total_amount", "sum"), n=("total_amount", "size"))
      .sort_values("revenue", ascending=False)
      .head(10))
```

<details>
<summary><b>Polars</b></summary>

```python
(trips.group_by("PULocationID")
      .agg(revenue=pl.col("total_amount").sum(), n=pl.len())
      .sort("revenue", descending=True)
      .head(10)
      .collect())
```
</details>

<details>
<summary><b>DuckDB SQL</b></summary>

```sql
SELECT PULocationID,
       sum(total_amount) AS revenue,
       count(*)          AS n
FROM 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet'
GROUP BY PULocationID
ORDER BY revenue DESC
LIMIT 10;
```
</details>

<details>
<summary><b>R (arrow + dplyr)</b></summary>

```r
trips |>
  group_by(PULocationID) |>
  summarise(revenue = sum(total_amount), n = n()) |>
  arrange(desc(revenue)) |>
  head(10)
```
</details>

---

## Task 2b — Top 3 busiest hours **per zone**

Same idea, but with the awkward twist that every analyst hits eventually:
*top-N inside each group*. SQL needs a window function; Pandas pushes you
into `groupby().apply(lambda x: x.nlargest(...))`; Ibex says `head 3, by zone`.

### Ibex
```ibex
let by_zh = trips[select { n = count() },
                  by { zone = PULocationID,
                       h    = hour(tpep_pickup_datetime) }];

by_zh[order { n desc }, head 3, by zone];
```

```
rows: ~780  (3 per zone × 261 zones)
+------+----+-----+
| zone | h  | n   |
+------+----+-----+
|  1   | 17 |  37 |
|  1   | 14 |  35 |
|  1   | 15 |  33 |
|  2   |  5 |   1 |
|  2   | 22 |   1 |
|  2   | 23 |   1 |
|  3   |  6 |  10 |
|  3   | 12 |   9 |
|  3   |  8 |   9 |
...
```

### Pandas
```python
by_zh = (trips.assign(h=trips["tpep_pickup_datetime"].dt.hour)
              .groupby(["PULocationID", "h"], as_index=False)
              .size().rename(columns={"size": "n", "PULocationID": "zone"}))

(by_zh.sort_values("n", ascending=False)
      .groupby("zone", as_index=False)
      .head(3))
```

<details>
<summary><b>Polars</b></summary>

```python
(trips.group_by(zone="PULocationID",
                h=pl.col("tpep_pickup_datetime").dt.hour())
      .agg(n=pl.len())
      .sort("n", descending=True)
      .group_by("zone").head(3)
      .collect())
```
</details>

<details>
<summary><b>DuckDB SQL</b></summary>

```sql
WITH counted AS (
  SELECT PULocationID AS zone,
         hour(tpep_pickup_datetime) AS h,
         count(*) AS n,
         row_number() OVER (PARTITION BY PULocationID
                            ORDER BY count(*) DESC) AS rk
  FROM 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet'
  GROUP BY zone, h
)
SELECT zone, h, n FROM counted WHERE rk <= 3;
```
</details>

<details>
<summary><b>R (arrow + dplyr)</b></summary>

```r
Sys.setenv(TZ = "UTC")   # see Task 1 R note

trips |>
  mutate(h = hour(tpep_pickup_datetime), zone = PULocationID) |>
  count(zone, h, name = "n") |>
  group_by(zone) |>
  slice_max(n, n = 3, with_ties = FALSE)
```
</details>

---

## Task 3 — Rolling 1-hour average fare at JFK pickups

For every JFK pickup, what was the average fare and trip count in the
previous hour? This is the canonical streaming-window shape — Ibex needs
the table promoted to a `TimeFrame`, then a `window` clause does the rest.

### Ibex
```ibex
let jfk     = trips[filter PULocationID == 132];
let jfk_ts  = as_timeframe(jfk, "tpep_pickup_datetime");

let smoothed = jfk_ts[window 1h,
                      update { rolling_avg_fare = rolling_mean(fare_amount),
                               rolling_trips    = rolling_count() }];

smoothed[select { tpep_pickup_datetime, fare_amount,
                  rolling_avg_fare, rolling_trips },
         head 10];
```

```
rows: 10
+-------------------------------+-------------+------------------+---------------+
| tpep_pickup_datetime          | fare_amount | rolling_avg_fare | rolling_trips |
+-------------------------------+-------------+------------------+---------------+
| 2024-01-01 00:00:05.000000000 | 70.0        | 70.00000         | 1             |
| 2024-01-01 00:00:25.000000000 | 70.0        | 70.00000         | 2             |
| 2024-01-01 00:00:26.000000000 | 51.3        | 63.76667         | 3             |
| 2024-01-01 00:00:36.000000000 | 26.1        | 54.35000         | 4             |
| 2024-01-01 00:00:44.000000000 | 68.1        | 57.10000         | 5             |
| 2024-01-01 00:01:37.000000000 | 99.6        | 64.18333         | 6             |
| 2024-01-01 00:02:22.000000000 | 70.0        | 65.01429         | 7             |
| 2024-01-01 00:02:27.000000000 | 10.0        | 58.13750         | 8             |
| 2024-01-01 00:03:19.000000000 | 70.0        | 59.45556         | 9             |
| 2024-01-01 00:03:51.000000000 | 33.1        | 56.82000         | 10            |
+-------------------------------+-------------+------------------+---------------+
```

### Pandas
```python
jfk = (trips[trips["PULocationID"] == 132]
       .sort_values("tpep_pickup_datetime")
       .set_index("tpep_pickup_datetime"))

roll = jfk["fare_amount"].rolling("1h")
jfk = jfk.assign(rolling_avg_fare=roll.mean(),
                 rolling_trips=roll.count())

jfk[["fare_amount", "rolling_avg_fare", "rolling_trips"]].head(10)
```

<details>
<summary><b>Polars</b></summary>

```python
(trips.filter(pl.col("PULocationID") == 132)
      .sort("tpep_pickup_datetime")
      .rolling(index_column="tpep_pickup_datetime", period="1h")
      .agg(fare_amount=pl.col("fare_amount").last(),
           rolling_avg_fare=pl.col("fare_amount").mean(),
           rolling_trips=pl.len())
      .head(10)
      .collect())
```
</details>

<details>
<summary><b>DuckDB SQL</b></summary>

```sql
SELECT tpep_pickup_datetime, fare_amount,
       avg(fare_amount) OVER w AS rolling_avg_fare,
       count(*)         OVER w AS rolling_trips
FROM 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet'
WHERE PULocationID = 132
WINDOW w AS (ORDER BY tpep_pickup_datetime
             RANGE BETWEEN INTERVAL 1 HOUR PRECEDING AND CURRENT ROW)
ORDER BY tpep_pickup_datetime
LIMIT 10;
```
</details>

<details>
<summary><b>R (arrow + dplyr + slider)</b></summary>

```r
Sys.setenv(TZ = "UTC")   # see Task 1 R note
library(slider); library(lubridate)

trips |>
  filter(PULocationID == 132) |>
  arrange(tpep_pickup_datetime) |>
  mutate(rolling_avg_fare = slide_index_mean(fare_amount, tpep_pickup_datetime,
                                             before = hours(1)),
         rolling_trips    = slide_index_dbl(fare_amount, tpep_pickup_datetime,
                                            ~ length(.x), .before = hours(1))) |>
  head(10)
```
</details>

---

## Task 4 — Outlier-trimmed average tip rate by hour

A naive `mean(tip_amount / fare_amount)` is a mess: zero-fare rows give
infinite ratios, refunds give negatives, and the occasional 5000% tip
(probably a typo) swamps the mean. Strip those first, then average per
pickup hour.

### Ibex
```ibex
let clean = trips[filter fare_amount > 5.0
                      && tip_amount  >= 0.0
                      && tip_amount  <  fare_amount];

clean[select { n = count(),
               avg_tip_rate = mean(tip_amount / fare_amount) },
      by { hour = hour(tpep_pickup_datetime) },
      order { hour asc }];
```

```
rows: 24
+------+--------+--------------+
| hour | n      | avg_tip_rate |
+------+--------+--------------+
|  0   |  75951 | 0.1922065    |
|  1   |  51345 | 0.1882492    |
|  2   |  35676 | 0.1882201    |
|  3   |  23479 | 0.1773079    |
|  4   |  15749 | 0.1531954    |
|  5   |  17893 | 0.1585829    |
|  6   |  39982 | 0.1642494    |
|  7   |  81288 | 0.1864541    |
|  8   | 113806 | 0.1926733    |
|  9   | 125007 | 0.1959586    |
+------+--------+--------------+
... (14 more rows)
```

### Pandas
```python
clean = trips[(trips["fare_amount"] > 5) &
              (trips["tip_amount"]  >= 0) &
              (trips["tip_amount"]  <  trips["fare_amount"])].copy()

clean["tip_rate"] = clean["tip_amount"] / clean["fare_amount"]
clean["hour"]     = clean["tpep_pickup_datetime"].dt.hour

(clean.groupby("hour", as_index=False)
      .agg(n=("tip_rate", "size"), avg_tip_rate=("tip_rate", "mean"))
      .sort_values("hour"))
```

<details>
<summary><b>Polars</b></summary>

```python
(trips.filter((pl.col("fare_amount") > 5) &
              (pl.col("tip_amount")  >= 0) &
              (pl.col("tip_amount")  <  pl.col("fare_amount")))
      .group_by(hour=pl.col("tpep_pickup_datetime").dt.hour())
      .agg(n=pl.len(),
           avg_tip_rate=(pl.col("tip_amount") / pl.col("fare_amount")).mean())
      .sort("hour")
      .collect())
```
</details>

<details>
<summary><b>DuckDB SQL</b></summary>

```sql
SELECT hour(tpep_pickup_datetime)        AS hour,
       count(*)                           AS n,
       avg(tip_amount / fare_amount)      AS avg_tip_rate
FROM 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet'
WHERE fare_amount > 5
  AND tip_amount  >= 0
  AND tip_amount  <  fare_amount
GROUP BY hour
ORDER BY hour;
```
</details>

<details>
<summary><b>R (arrow + dplyr)</b></summary>

```r
Sys.setenv(TZ = "UTC")   # see Task 1 R note

trips |>
  filter(fare_amount > 5, tip_amount >= 0, tip_amount < fare_amount) |>
  mutate(hour     = hour(tpep_pickup_datetime),
         tip_rate = tip_amount / fare_amount) |>
  group_by(hour) |>
  summarise(n = n(), avg_tip_rate = mean(tip_rate)) |>
  arrange(hour)
```
</details>

---

## Task 5 — Revenue by borough (join + group)

Locations are encoded as integer zone IDs; borough names live in a second
file. Join the two and roll up to borough level.

### Ibex
```ibex
import "csv";

let zones  = read_csv(
  "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv");

let joined = trips join zones on PULocationID == LocationID;

joined[select { revenue = sum(total_amount), n = count() },
       by Borough,
       order { revenue desc }];
```

```
rows: 8
+-----------------+------------+---------+
| Borough         | revenue    | n       |
+-----------------+------------+---------+
| "Manhattan"     | 5.924753e7 | 2646948 |
| "Queens"        | 1.863979e7 |  273128 |
| "Brooklyn"      |   828808.4 |   25258 |
| "Unknown"       |   304152.4 |   10360 |
| "Bronx"         |   246496.4 |    6905 |
| "N/A"           |   154900.9 |    1658 |
| "EWR"           |    30738.0 |     295 |
| "Staten Island" |     3966.4 |      72 |
+-----------------+------------+---------+
```

### Pandas
```python
# keep_default_na=False so the literal string "N/A" (a real Borough value)
# isn't silently turned into a NaN and dropped at groupby time.
zones = pd.read_csv(
    "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv",
    keep_default_na=False)

joined = trips.merge(zones, left_on="PULocationID", right_on="LocationID",
                     how="inner")

(joined.groupby("Borough", as_index=False)
       .agg(revenue=("total_amount", "sum"), n=("total_amount", "size"))
       .sort_values("revenue", ascending=False))
```

<details>
<summary><b>Polars</b></summary>

```python
zones = pl.scan_csv(
    "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv")

(trips.join(zones, left_on="PULocationID", right_on="LocationID")
      .group_by("Borough")
      .agg(revenue=pl.col("total_amount").sum(), n=pl.len())
      .sort("revenue", descending=True)
      .collect())
```
</details>

<details>
<summary><b>DuckDB SQL</b></summary>

```sql
SELECT z.Borough,
       sum(t.total_amount) AS revenue,
       count(*)            AS n
FROM 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet' t
JOIN read_csv_auto('https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv') z
  ON t.PULocationID = z.LocationID
GROUP BY z.Borough
ORDER BY revenue DESC;
```
</details>

<details>
<summary><b>R (arrow + dplyr)</b></summary>

```r
zones <- read_csv_arrow(
  "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv")

trips |>
  inner_join(zones, by = c("PULocationID" = "LocationID")) |>
  group_by(Borough) |>
  summarise(revenue = sum(total_amount), n = n()) |>
  arrange(desc(revenue))
```
</details>

---

## Try it yourself

Ibex needs only a build of the REPL and its parquet/csv plugins:

```bash
cmake -B build-release -G Ninja -DCMAKE_BUILD_TYPE=Release
cmake --build build-release --target ibex ibex_parquet_plugin ibex_csv_plugin
IBEX_LIBRARY_PATH=build-release/tools ./build-release/tools/ibex
```

Paste any of the Ibex blocks above. Everything else (the Parquet file, the
zone lookup CSV) is fetched over HTTPS — no AWS credentials, no environment
setup, no Docker.

The other languages each take their normal install (`pip install pandas
pyarrow polars duckdb`, `install.packages(c("arrow", "dplyr", "lubridate",
"slider"))`); the snippets above are otherwise self-contained too.
