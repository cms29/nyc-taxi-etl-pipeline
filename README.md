# NYC Taxi ETL Pipeline — DuckDB + Python

A production-grade ETL pipeline that processes 3 million rows of real NYC taxi data locally using DuckDB and Python — replicating the same architecture used in AWS (S3 + Glue + Athena) without any cloud cost.

---

## Architecture

```
Raw Parquet File
      │
      ▼
[1. Extract]          Read 3,066,766 rows from NYC TLC dataset
      │
      ▼
[2. Transform]        Clean bad rows, add derived columns
      │               (trip_duration_mins, fare_per_mile, tip_category)
      ▼
[3. Load]             Write Hive-style partitioned Parquet
      │               year=2023/month=01/day=01/data.parquet
      ▼
[4. Quality Check]    Validate row counts, null checks, thresholds
      │
      ▼
[5. Analyse]          SQL queries on partitioned data (like Athena)
```

### AWS equivalent mapping

| This project        | AWS production equivalent     |
|---------------------|-------------------------------|
| Raw Parquet file    | S3 raw layer                  |
| DuckDB ETL          | AWS Glue PySpark job          |
| Partitioned folders | S3 Hive partitions            |
| SQL on partitions   | Amazon Athena                 |
| Quality checks      | AWS Glue Data Quality         |
| Timestamped logs    | Amazon CloudWatch logs        |

---

## Key results

| Metric            | Value              |
|-------------------|--------------------|
| Raw rows ingested  | 3,066,766          |
| Clean rows output  | 2,884,165          |
| Bad rows removed   | 182,601 (5.96%)    |
| Total revenue      | $78,908,689        |
| Avg fare           | $18.55             |
| Avg trip distance  | 3.47 mi            |
| Avg trip duration  | 15.8 min           |
| Pipeline runtime   | ~5 seconds         |

---

## Business insights from the data

**Busiest hours**
- 6pm is the busiest hour with 203,600 trips
- Rush hours (2–7pm) drive the highest volume
- Early morning (4–5am) earns the most per mile at $15.07/mi — likely airport runs

**Trip breakdown**
- 62.5% of trips are medium distance (1–5 miles)
- Long trips (>5 miles) earn 6x more per ride ($48 avg vs $7.42)
- Revenue trended upward through January, peaking around Jan 26–28

**Tipping behaviour**
- 61.5% of riders tip generously (>20% of fare)
- Only 21.7% leave no tip
- Vendor 1 has a higher avg tip rate (22.76%) vs Vendor 2 (20.63%)

**Data quality flags** (out of 2.88M clean rows)
- 2,984 trips over 3 hours
- 4,683 trips under 1 minute
- 7,955 trips with unusually high fare per mile
- 36 trips over 100 miles (likely data entry errors)

---

## Project structure

```
taxi-project/
├── 1_download.py       # Download NYC taxi Parquet file
├── 2_explore.py        # Explore schema, nulls, stats
├── 3_clean.py          # Remove bad rows, add derived columns
├── 4_partition.py      # Write Hive-style partitions + perf test
├── 5_analyse.py        # 7 SQL queries (window functions, ranking)
├── 6_pipeline.py       # Full ETL pipeline with logging + quality checks
├── taxi_clean.parquet  # Cleaned output file
└── taxi_partitioned/   # Partitioned Parquet folders
    └── pickup_year=2023/
        └── pickup_month=1/
            └── pickup_day=1/
                └── data.parquet
```

---

## How to run

**Requirements**
- Python 3.8+
- pip

**Install dependencies**
```bash
pip install duckdb pandas pyarrow requests
```

**Run scripts in order**
```bash
python 1_download.py    # Downloads ~47MB NYC taxi data
python 2_explore.py     # Explore the raw data
python 3_clean.py       # Clean and transform
python 4_partition.py   # Partition like S3 Hive structure
python 5_analyse.py     # Run 7 SQL analytical queries
python 6_pipeline.py    # Run full pipeline end-to-end
```

**Expected pipeline output**
```
01:57:57 | INFO | NYC TAXI ETL PIPELINE STARTED
01:57:57 | INFO | EXTRACT: 3,066,766 rows loaded
01:57:57 | INFO | TRANSFORM: Removed old partition folder
01:58:01 | INFO | TRANSFORM: 2,884,165 clean rows written (182,601 removed)
01:58:01 | INFO | Quality check PASSED: 94.0% rows retained
01:58:01 | INFO | Quality check PASSED: no nulls in critical columns
01:58:01 | INFO | Trips        : 2,884,165
01:58:01 | INFO | Revenue      : $78,908,689.22
01:58:01 | INFO | Avg fare     : $18.55
01:58:01 | INFO | Avg distance : 3.47 mi
01:58:01 | INFO | Avg duration : 15.8 min
01:58:02 | INFO | PIPELINE COMPLETE in 4.86s
```

---

## SQL techniques used

- `GROUP BY` with aggregations (`COUNT`, `SUM`, `AVG`)
- Window functions: `AVG() OVER (ORDER BY ... ROWS BETWEEN ...)`
- Ranking: `RANK() OVER (ORDER BY ...)`
- Percentage of total: `COUNT(*) * 100.0 / SUM(COUNT(*)) OVER ()`
- Conditional aggregation: `SUM(CASE WHEN ... THEN 1 ELSE 0 END)`
- `PERCENTILE_CONT` for median and percentile calculations
- `NULLIF` to avoid division by zero
- `DATEDIFF` for duration calculations

---

## What I learned

- How ETL pipelines work: extract → validate → transform → load → quality check
- Why partitioning matters: single-day queries on partitioned data are significantly faster than full scans — at TB scale on AWS Athena this reduces cost by 90%+
- How to write production-grade Python with logging, error handling, and quality thresholds
- SQL window functions for rolling averages, ranking, and percentage calculations
- How dirty real-world data is: 5.96% of rows had invalid values (negative fares, zero distances, passengers = 0)
- The local DuckDB code maps directly to AWS Glue (PySpark) — only the file paths change when moving to S3

---

## Dataset

NYC TLC Yellow Taxi Trip Records — January 2023  
Source: [NYC Taxi and Limousine Commission](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)  
Format: Parquet | Size: ~47MB | Rows: 3,066,766

---

## Next steps

- [ ] Deploy to AWS: swap local paths for `s3://` URIs, run on AWS Glue
- [ ] Add Apache Airflow DAG to schedule daily runs
- [ ] Load into PostgreSQL (local Redshift equivalent)
- [ ] Add Great Expectations for automated data quality validation
- [ ] Extend to multiple months and test partition performance at larger scale

---

