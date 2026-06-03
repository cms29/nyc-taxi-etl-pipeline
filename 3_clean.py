import duckdb

con   = duckdb.connect()
RAW   = "yellow_tripdata_2023-01.parquet"
CLEAN = "taxi_clean.parquet"

# Count bad rows before cleaning
print("Checking data quality...")
bad = con.sql(f"""
    SELECT COUNT(*) AS bad_rows FROM '{RAW}'
    WHERE trip_distance   <= 0
       OR fare_amount     <= 0
       OR passenger_count IS NULL
       OR passenger_count  = 0
       OR tpep_pickup_datetime >= tpep_dropoff_datetime
""").fetchone()[0]

total = con.sql(f"SELECT COUNT(*) FROM '{RAW}'").fetchone()[0]
print(f"Total rows : {total:>10,}")
print(f"Bad rows   : {bad:>10,}  ({bad/total*100:.1f}%)")
print(f"Clean rows : {total-bad:>10,}")

# Write clean file with derived columns
print("\nCleaning and transforming...")
con.sql(f"""
    COPY (
        SELECT
            VendorID,
            tpep_pickup_datetime,
            tpep_dropoff_datetime,
            CAST(passenger_count AS INTEGER)        AS passenger_count,
            ROUND(trip_distance, 2)                 AS trip_distance,
            ROUND(fare_amount,   2)                 AS fare_amount,
            ROUND(tip_amount,    2)                 AS tip_amount,
            ROUND(total_amount,  2)                 AS total_amount,

            -- Partition columns (replicates S3 Hive partitions)
            YEAR(tpep_pickup_datetime)              AS pickup_year,
            MONTH(tpep_pickup_datetime)             AS pickup_month,
            DAY(tpep_pickup_datetime)               AS pickup_day,
            HOUR(tpep_pickup_datetime)              AS pickup_hour,

            -- Derived metrics (replicates Glue transform logic)
            DATEDIFF('minute',
                tpep_pickup_datetime,
                tpep_dropoff_datetime)              AS trip_duration_mins,
            ROUND(fare_amount /
                NULLIF(trip_distance, 0), 2)        AS fare_per_mile,
            CASE
                WHEN tip_amount = 0                      THEN 'no_tip'
                WHEN tip_amount < fare_amount * 0.1      THEN 'low_tip'
                WHEN tip_amount < fare_amount * 0.2      THEN 'standard_tip'
                ELSE                                          'generous_tip'
            END                                     AS tip_category

        FROM '{RAW}'
        WHERE trip_distance   > 0
          AND fare_amount     > 0
          AND passenger_count > 0
          AND tpep_pickup_datetime < tpep_dropoff_datetime
    ) TO '{CLEAN}' (FORMAT PARQUET)
""")

clean_count = con.sql(f"SELECT COUNT(*) FROM '{CLEAN}'").fetchone()[0]
print(f"\nClean file written: {CLEAN}")
print(f"Rows saved: {clean_count:,}")
print("Done!")
