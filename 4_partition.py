import duckdb, os, time

con   = duckdb.connect()
CLEAN = "taxi_clean.parquet"
OUT   = "taxi_partitioned"

print("Deleting old partition folder if exists...")
import shutil
if os.path.exists(OUT):
    shutil.rmtree(OUT)
    print("Old folder deleted.")

print("Writing partitioned Parquet files...")
print("(This may take 30-60 seconds)")

os.makedirs(OUT, exist_ok=True)
con.sql(f"""
    COPY (SELECT * FROM '{CLEAN}')
    TO '{OUT}'
    (FORMAT PARQUET,
     PARTITION_BY (pickup_year, pickup_month, pickup_day))
""")
print("Partitions written!\n")

print("Folder structure created:")
count = 0
for root, dirs, files in os.walk(OUT):
    level = root.replace(OUT, "").count(os.sep)
    indent = "  " * level
    folder = os.path.basename(root)
    if level <= 3:
        print(f"{indent}{folder}/")
    if files and level == 3:
        fsize = os.path.getsize(os.path.join(root, files[0])) / 1024
        print(f"{indent}  {files[0]}  ({fsize:.0f} KB)")
        count += 1
        if count >= 3:
            print("  ... (31 day partitions total)")
            break

import time
print("\n" + "="*50)
print("PERFORMANCE TEST: Full scan vs 1 partition")
print("="*50)

t = time.time()
r1 = con.sql(f"""
    SELECT COUNT(*) AS trips, ROUND(AVG(fare_amount),2) AS avg_fare
    FROM '{CLEAN}'
    WHERE tpep_pickup_datetime >= '2023-01-15'
      AND tpep_pickup_datetime  < '2023-01-16'
""").fetchone()
full_time = round(time.time() - t, 3)

t = time.time()
r2 = con.sql(f"""
    SELECT COUNT(*) AS trips, ROUND(AVG(fare_amount),2) AS avg_fare
    FROM '{OUT}/**/*.parquet'
    WHERE pickup_year=2023 AND pickup_month=1 AND pickup_day=15
""").fetchone()
part_time = round(time.time() - t, 3)

print(f"\nFull file scan : {full_time}s -> trips={r1[0]:,} avg_fare=${r1[1]}")
print(f"Partition scan : {part_time}s -> trips={r2[0]:,} avg_fare=${r2[1]}")
print("\nDone! Move to 5_analyse.py next.")