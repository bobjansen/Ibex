## Benchmark

Scale benchmark snapshot on **4,000,000 rows** (warmup=2, iters=15).

| query                        |     ibex |   polars | polars-st |    duckdb | duckdb-st | datafusion | datafusion-st | clickhouse | clickhouse-st |     sqlite |    pandas | data.table |     dplyr |
| ---------------------------- | -------: | -------: | --------: | --------: | --------: | ---------: | ------------: | ---------: | ------------: | ---------: | --------: | ---------: | --------: |
| mean_by_symbol               |  28.0 ms |  33.0 ms |   65.2 ms |   9.20 ms |   40.0 ms |    4.76 ms |       26.2 ms |    9.66 ms |       54.7 ms |  1660.3 ms |   68.5 ms |    25.1 ms |   46.9 ms |
| ohlc_by_symbol               |  33.0 ms |  24.6 ms |   74.3 ms |   13.4 ms |   58.8 ms |    9.33 ms |       63.6 ms |    7.42 ms |       56.6 ms |          - |   87.7 ms |    25.1 ms |   51.4 ms |
| update_price_x2              |  3.26 ms |  2.70 ms |   3.27 ms |  118.6 ms |  156.4 ms |    4.28 ms |       5.99 ms |    2.99 ms |       3.70 ms |  2140.8 ms |   3.53 ms |    27.9 ms |   5.73 ms |
| cumsum_price                 |  3.11 ms |  13.0 ms |   12.7 ms |  207.5 ms |  692.4 ms |   520.6 ms |      487.3 ms |    81.3 ms |       80.8 ms |  5003.1 ms |   12.8 ms |    24.9 ms |   9.20 ms |
| cumprod_price                |  3.58 ms |  13.6 ms |   12.7 ms |         - |         - |          - |             - |          - |             - |          - |   12.7 ms |   343.2 ms |  347.1 ms |
| rand_uniform                 |  2.60 ms |  8.29 ms |   8.20 ms |  119.6 ms |  188.9 ms |    5.68 ms |       13.8 ms |    3.29 ms |       4.29 ms |          - |   9.64 ms |    28.7 ms |   24.9 ms |
| rand_normal                  |  11.4 ms |  31.8 ms |   30.1 ms |         - |         - |          - |             - |          - |             - |          - |   35.1 ms |    79.5 ms |   76.7 ms |
| rand_int                     |  4.04 ms |  8.11 ms |   7.88 ms |         - |         - |          - |             - |          - |             - |          - |   10.1 ms |    62.3 ms |   62.3 ms |
| rand_bernoulli               |  2.87 ms |  30.7 ms |   29.5 ms |         - |         - |          - |             - |          - |             - |          - |   32.7 ms |    59.7 ms |   59.3 ms |
| fill_null                    |  3.97 ms |  3.11 ms |   2.65 ms |   14.6 ms |   29.3 ms |    8.44 ms |       28.3 ms |    4.18 ms |       5.27 ms |  1143.2 ms |   7.05 ms |    5.20 ms |   14.7 ms |
| fill_forward                 |  3.71 ms |  8.95 ms |   8.73 ms |   58.0 ms |  225.4 ms |          - |             - |          - |             - |          - |   7.56 ms |    5.87 ms |   13.7 ms |
| fill_backward                |  7.10 ms |  8.78 ms |   8.71 ms |   63.2 ms |  233.8 ms |          - |             - |          - |             - |          - |   8.41 ms |    10.4 ms |   14.8 ms |
| null_left_join               |  75.4 ms |  30.9 ms |   80.1 ms |  189.1 ms |  294.2 ms |    17.2 ms |      114.8 ms |    24.7 ms |      147.9 ms |  3096.3 ms |  321.5 ms |   162.6 ms |  163.2 ms |
| null_semi_join               |  35.3 ms |  23.3 ms |  104.0 ms |   54.4 ms |  116.2 ms |    13.5 ms |       85.6 ms |    19.7 ms |       63.2 ms | 18769.1 ms |  253.1 ms |    36.5 ms |   82.3 ms |
| null_anti_join               |  35.1 ms |  26.3 ms |  103.1 ms |   56.8 ms |  158.8 ms |    14.0 ms |       86.1 ms |    18.3 ms |       62.5 ms | 18825.9 ms |   74.6 ms |    58.7 ms |  110.8 ms |
| null_cross_join_small        |  2.50 ms | 0.659 ms |  0.786 ms |   7.59 ms |   7.46 ms |   0.879 ms |       1.09 ms |    1.60 ms |       2.54 ms |    83.4 ms |   4.70 ms |    3.13 ms |   53.2 ms |
| filter_simple                |  11.8 ms |  7.38 ms |   12.8 ms |   59.8 ms |   95.7 ms |    7.73 ms |       19.3 ms |    5.84 ms |       19.9 ms |  1227.2 ms |   21.8 ms |    29.3 ms |   24.5 ms |
| filter_and                   |  7.12 ms |  4.71 ms |   8.18 ms |   14.4 ms |   42.1 ms |    3.87 ms |       11.6 ms |    8.38 ms |       29.0 ms |   424.2 ms |   19.3 ms |    28.9 ms |   37.1 ms |
| filter_arith                 |  16.2 ms |  8.25 ms |   16.7 ms |   77.9 ms |  122.7 ms |    8.96 ms |       25.5 ms |    8.67 ms |       23.4 ms |  1612.9 ms |   33.8 ms |    40.1 ms |   31.9 ms |
| filter_or                    |  8.17 ms |  4.57 ms |   8.16 ms |   17.1 ms |   61.9 ms |    4.16 ms |       12.7 ms |    6.14 ms |       15.2 ms |   464.6 ms |   15.1 ms |    25.7 ms |   35.5 ms |
| count_by_symbol_day          |  1.35 ms |  12.8 ms |   20.3 ms |   4.86 ms |   8.62 ms |    3.53 ms |       7.75 ms |    4.58 ms |       11.1 ms |   420.1 ms |   24.6 ms |    4.33 ms |   17.6 ms |
| mean_by_symbol_day           |  1.55 ms |  13.2 ms |   22.0 ms |   4.56 ms |   9.26 ms |    3.92 ms |       8.30 ms |    4.93 ms |       13.0 ms |   503.8 ms |   27.1 ms |    4.67 ms |   20.9 ms |
| ohlc_by_symbol_day           |  2.33 ms |  13.6 ms |   22.9 ms |   5.86 ms |   15.6 ms |    9.65 ms |       27.1 ms |    5.60 ms |       12.6 ms |          - |   31.9 ms |    5.33 ms |   39.9 ms |
| sum_by_user                  |  54.4 ms |  50.6 ms |  221.7 ms |   28.5 ms |   94.1 ms |    38.0 ms |       56.8 ms |    80.9 ms |       81.4 ms |  2057.8 ms |  112.0 ms |    45.6 ms |  313.6 ms |
| filter_events                |  12.2 ms |  7.05 ms |   13.4 ms |   78.7 ms |  109.5 ms |    9.65 ms |       24.5 ms |    8.44 ms |       21.0 ms |  1157.5 ms |   26.0 ms |    33.4 ms |   31.1 ms |
| melt_wide_to_long            |  53.3 ms |  41.5 ms |   52.5 ms | 1030.5 ms | 1581.7 ms |    24.4 ms |       24.5 ms |    10.6 ms |       22.7 ms | 11266.4 ms |  216.1 ms |   223.3 ms |  358.1 ms |
| dcast_long_to_wide           | 908.4 ms | 676.3 ms | 1605.1 ms |  308.1 ms | 1284.0 ms |   228.2 ms |      628.5 ms |   513.6 ms |     2086.1 ms | 16761.9 ms | 4309.7 ms |  1221.4 ms | 1884.6 ms |
| dcast_long_to_wide_int_pivot | 801.8 ms |        - |         - |         - |         - |          - |             - |          - |             - |          - |         - |          - |         - |
| dcast_long_to_wide_cat_pivot | 897.5 ms |        - |         - |         - |         - |          - |             - |          - |             - |          - |         - |          - |         - |

_Generated by `benchmarking/run_scale_suite.sh --to-readme` from `benchmarking/results/scales.tsv`._
