import duckdb

con = duckdb.connect()
P   = "taxi_partitioned/**/*.parquet"

print("=" * 50)
print("Q1: Busiest hours of day")
print("=" * 50)
con.sql(f"""
    SELECT pickup_hour               AS hour,
           COUNT(*)                  AS trips,
           ROUND(AVG(fare_amount),2) AS avg_fare
    FROM '{P}'
    GROUP BY pickup_hour
    ORDER BY trips DESC
    LIMIT 6
""").show()

print("=" * 50)
print("Q2: Revenue and tip % by vendor")
print("=" * 50)
con.sql(f"""
    SELECT VendorID,
           COUNT(*)                                        AS total_trips,
           ROUND(SUM(total_amount), 2)                    AS total_revenue,
           ROUND(AVG(tip_amount / NULLIF(fare_amount,0))
                 * 100, 2)                                AS avg_tip_pct
    FROM '{P}'
    GROUP BY VendorID
    ORDER BY total_revenue DESC
""").show()

print("=" * 50)
print("Q3: Trip category breakdown")
print("=" * 50)
con.sql(f"""
    SELECT
        CASE
            WHEN trip_distance < 1 THEN 'short  <1mi'
            WHEN trip_distance < 5 THEN 'medium 1-5mi'
            ELSE                        'long   >5mi'
        END                              AS category,
        COUNT(*)                         AS trips,
        ROUND(AVG(fare_amount),2)        AS avg_fare,
        ROUND(AVG(trip_duration_mins),1) AS avg_mins
    FROM '{P}'
    GROUP BY category
    ORDER BY trips DESC
""").show()

print("=" * 50)
print("Q4: Daily revenue + 7-day rolling average (window function)")
print("=" * 50)
con.sql(f"""
    WITH daily AS (
        SELECT pickup_day,
               ROUND(SUM(total_amount),2) AS revenue,
               COUNT(*)                   AS trips
        FROM '{P}'
        GROUP BY pickup_day
    )
    SELECT
        pickup_day,
        trips,
        revenue,
        ROUND(AVG(revenue) OVER (
            ORDER BY pickup_day
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ), 2)                            AS rolling_7d_avg
    FROM daily
    ORDER BY pickup_day
""").show()

print("=" * 50)
print("Q5: Tip category distribution with percentage")
print("=" * 50)
con.sql(f"""
    SELECT tip_category,
           COUNT(*)                              AS trips,
           ROUND(COUNT(*) * 100.0 /
               SUM(COUNT(*)) OVER (), 2)         AS pct_of_total
    FROM '{P}'
    GROUP BY tip_category
    ORDER BY trips DESC
""").show()

print("=" * 50)
print("Q6: Top 5 most profitable hours (RANK window function)")
print("=" * 50)
con.sql(f"""
    SELECT pickup_hour,
           COUNT(*)                        AS trips,
           ROUND(AVG(fare_per_mile),2)     AS avg_fare_per_mile,
           ROUND(AVG(trip_duration_mins),1) AS avg_trip_mins,
           RANK() OVER (ORDER BY AVG(fare_per_mile) DESC) AS rank
    FROM '{P}'
    WHERE trip_distance > 0
    GROUP BY pickup_hour
    ORDER BY rank
    LIMIT 5
""").show()

print("=" * 50)
print("Q7: Data quality audit")
print("=" * 50)
con.sql(f"""
    SELECT
        SUM(CASE WHEN trip_duration_mins > 180 THEN 1 ELSE 0 END) AS trips_over_3hrs,
        SUM(CASE WHEN trip_duration_mins < 1   THEN 1 ELSE 0 END) AS trips_under_1min,
        SUM(CASE WHEN fare_per_mile > 50       THEN 1 ELSE 0 END) AS high_fare_per_mile,
        SUM(CASE WHEN trip_distance > 100      THEN 1 ELSE 0 END) AS very_long_trips,
        COUNT(*)                                                    AS total_rows
    FROM '{P}'
""").show()
