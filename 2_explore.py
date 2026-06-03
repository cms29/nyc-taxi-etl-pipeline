import duckdb

con = duckdb.connect()
RAW = "yellow_tripdata_2023-01.parquet"

print("=" * 50)
print("STEP 1: How many rows?")
print("=" * 50)
con.sql(f"SELECT COUNT(*) AS total_rows FROM '{RAW}'").show()

print("=" * 50)
print("STEP 2: Column names and data types")
print("=" * 50)
con.sql(f"DESCRIBE SELECT * FROM '{RAW}'").show()

print("=" * 50)
print("STEP 3: First 3 rows (sample)")
print("=" * 50)
con.sql(f"""
    SELECT VendorID,
           tpep_pickup_datetime,
           tpep_dropoff_datetime,
           passenger_count,
           trip_distance,
           fare_amount,
           tip_amount,
           total_amount
    FROM '{RAW}' LIMIT 3
""").show()

print("=" * 50)
print("STEP 4: Null check - missing values per column")
print("=" * 50)
con.sql(f"""
    SELECT
        COUNT(*) - COUNT(VendorID)              AS null_vendor,
        COUNT(*) - COUNT(tpep_pickup_datetime)  AS null_pickup,
        COUNT(*) - COUNT(passenger_count)       AS null_passengers,
        COUNT(*) - COUNT(trip_distance)         AS null_distance,
        COUNT(*) - COUNT(fare_amount)           AS null_fare
    FROM '{RAW}'
""").show()

print("=" * 50)
print("STEP 5: Basic stats - min, max, average")
print("=" * 50)
con.sql(f"""
    SELECT
        ROUND(MIN(fare_amount),2)   AS min_fare,
        ROUND(MAX(fare_amount),2)   AS max_fare,
        ROUND(AVG(fare_amount),2)   AS avg_fare,
        ROUND(MIN(trip_distance),2) AS min_dist,
        ROUND(MAX(trip_distance),2) AS max_dist,
        ROUND(AVG(trip_distance),2) AS avg_dist
    FROM '{RAW}'
""").show()

print("=" * 50)
print("STEP 6: Date range of the data")
print("=" * 50)
con.sql(f"""
    SELECT
        MIN(tpep_pickup_datetime) AS earliest_trip,
        MAX(tpep_pickup_datetime) AS latest_trip
    FROM '{RAW}'
""").show()
