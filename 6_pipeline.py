import duckdb, logging, time, os, sys, shutil

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%H:%M:%S"
)
log = logging.getLogger("taxi_pipeline")

RAW   = "yellow_tripdata_2023-01.parquet"
CLEAN = "taxi_clean.parquet"
OUT   = "taxi_partitioned"


def extract():
    log.info("EXTRACT: Reading raw parquet file...")
    con = duckdb.connect()
    count = con.sql(f"SELECT COUNT(*) FROM '{RAW}'").fetchone()[0]
    log.info(f"EXTRACT: {count:,} rows loaded from {RAW}")
    return con, count


def transform(con, raw_count):
    log.info("TRANSFORM: Cleaning data and adding derived columns...")
    if os.path.exists(OUT):
        shutil.rmtree(OUT)
        log.info("TRANSFORM: Removed old partition folder")
    os.makedirs(OUT, exist_ok=True)
    con.sql(f"""
        COPY (
            SELECT
                VendorID,
                tpep_pickup_datetime,
                tpep_dropoff_datetime,
                CAST(passenger_count AS INTEGER)    AS passenger_count,
                ROUND(trip_distance, 2)             AS trip_distance,
                ROUND(fare_amount,   2)             AS fare_amount,
                ROUND(tip_amount,    2)             AS tip_amount,
                ROUND(total_amount,  2)             AS total_amount,
                YEAR(tpep_pickup_datetime)          AS pickup_year,
                MONTH(tpep_pickup_datetime)         AS pickup_month,
                DAY(tpep_pickup_datetime)           AS pickup_day,
                HOUR(tpep_pickup_datetime)          AS pickup_hour,
                DATEDIFF('minute',
                    tpep_pickup_datetime,
                    tpep_dropoff_datetime)          AS trip_duration_mins,
                ROUND(fare_amount /
                    NULLIF(trip_distance, 0), 2)    AS fare_per_mile,
                CASE
                    WHEN tip_amount = 0                  THEN 'no_tip'
                    WHEN tip_amount < fare_amount * 0.1  THEN 'low_tip'
                    WHEN tip_amount < fare_amount * 0.2  THEN 'standard_tip'
                    ELSE                                      'generous_tip'
                END                                 AS tip_category
            FROM '{RAW}'
            WHERE trip_distance   > 0
              AND fare_amount     > 0
              AND passenger_count > 0
              AND tpep_pickup_datetime < tpep_dropoff_datetime
        ) TO '{OUT}'
        (FORMAT PARQUET,
         PARTITION_BY (pickup_year, pickup_month, pickup_day))
    """)
    clean = con.sql(f"SELECT COUNT(*) FROM '{OUT}/**/*.parquet'").fetchone()[0]
    removed = raw_count - clean
    log.info(f"TRANSFORM: {clean:,} clean rows written  ({removed:,} removed)")
    return clean


def quality_check(con, raw_count, clean_count):
    log.info("QUALITY CHECK: Validating output...")
    pct_kept = clean_count / raw_count * 100

    if pct_kept < 90:
        log.warning(f"Only {pct_kept:.1f}% rows kept — check source data!")
    else:
        log.info(f"Quality check PASSED: {pct_kept:.1f}% rows retained")

    nulls = con.sql(f"""
        SELECT COUNT(*) FROM '{OUT}/**/*.parquet'
        WHERE fare_amount IS NULL OR trip_distance IS NULL
    """).fetchone()[0]

    if nulls > 0:
        log.error(f"Quality check FAILED: {nulls} nulls found in output!")
        sys.exit(1)
    else:
        log.info("Quality check PASSED: no nulls in critical columns")


def summarise(con):
    log.info("SUMMARY: Key metrics from pipeline output")
    result = con.sql(f"""
        SELECT
            COUNT(*)                         AS total_trips,
            ROUND(SUM(total_amount), 2)      AS total_revenue,
            ROUND(AVG(fare_amount),  2)      AS avg_fare,
            ROUND(AVG(trip_distance),2)      AS avg_distance_mi,
            ROUND(AVG(trip_duration_mins),1) AS avg_duration_mins
        FROM '{OUT}/**/*.parquet'
    """).fetchone()
    log.info(f"  Trips        : {result[0]:,}")
    log.info(f"  Revenue      : ${result[1]:,.2f}")
    log.info(f"  Avg fare     : ${result[2]}")
    log.info(f"  Avg distance : {result[3]} mi")
    log.info(f"  Avg duration : {result[4]} min")


def run():
    log.info("=" * 55)
    log.info("NYC TAXI ETL PIPELINE STARTED")
    log.info("=" * 55)
    start = time.time()

    con, raw_count  = extract()
    clean_count     = transform(con, raw_count)
    quality_check(con, raw_count, clean_count)
    summarise(con)

    elapsed = round(time.time() - start, 2)
    log.info("=" * 55)
    log.info(f"PIPELINE COMPLETE in {elapsed}s")
    log.info("=" * 55)


if __name__ == "__main__":
    run()
