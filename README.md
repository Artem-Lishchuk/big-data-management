

# CORRECTNESS

## Row counts

Raw input rows:        7052769 <br>
After cleaning rows:   5591610 <br>
After dedup rows:      5590943 <br>
Final output rows:     5590943 <br>

## EXAMPLE OF BAD ROWS:

<img width="934" height="142" alt="image" src="https://github.com/user-attachments/assets/bc036cd1-29b4-4df1-a1d5-c00cb47fe2ad" />

## RULES FOR BAD ROWS:

bad_trips = df.filter(
        (F.col("tpep_pickup_datetime").isNull())
        | (F.col("tpep_dropoff_datetime").isNull())
        | (F.col("PULocationID").isNull())
        | (F.col("DOLocationID").isNull())
        | (F.col("passenger_count").isNull() | (F.col("passenger_count") < 0))
        | (F.col("trip_distance").isNull() | (F.col("trip_distance") < 0))
        | (F.col("fare_amount").isNull() | (F.col("fare_amount") < 0))
        | (F.col("tip_amount").isNull() | (F.col("tip_amount") < 0))
    )


# PERFORMANCE

Total runtime for the full job when no previous runs done: 88s

<img width="491" height="253" alt="image" src="https://github.com/user-attachments/assets/9d88365e-6f86-4719-960b-d76ce5b81cd3" />

<img width="1870" height="155" alt="image" src="https://github.com/user-attachments/assets/9b5c4950-1df7-4c1b-a877-ad3fb2019abc" />

<img width="1839" height="435" alt="image" src="https://github.com/user-attachments/assets/84640ff6-4f68-4671-b34f-a7f5452c9872" />


## OPTIMIZATION CHOICES

As the write output jobs were the slowest, when there was an output file existing, the cleaned and deduplicated dataframe was cached and materialized before writing, reducing runtime for the writing operation by 20 s, and increasing the operation for dedup by around 6 seconds, giving a total decrease in operation around 14 s. 


# PROJECT SPECIFIC TASK

Scenario
After each run, compute the top 5 pickup zones by total trip count across all processed data and write to data/outbox/top_zones.parquet with columns: zone, borough, trip_count. Must reflect the full output (recomputed each run, not incrementally appended).

For this, the enriched parque file was read back in so it matches all historical data, the lookup file was read in and these were joined and grouped by zones with all trips aggregated.

top_zones = (
        df.groupBy("pickup_location_id", "pickup_zone_name")
        .agg(F.count("*").alias("trip_count"))
        .orderBy(F.col("trip_count").desc())
        .limit(5)
        .join(zones.select("LocationID", "Borough"), 
            F.col("pickup_location_id") == F.col("LocationID"), "left")
        .withColumnRenamed("pickup_zone_name", "zone")
        .withColumnRenamed("Borough", "borough")
        .select("zone", "borough", "trip_count")
    )

Example result:

<img width="611" height="227" alt="image" src="https://github.com/user-attachments/assets/982bd6fd-0e07-407b-9ce0-a8e4c9b5d1dd" />



