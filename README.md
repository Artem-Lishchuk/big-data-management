

CORRECTNESS

Raw input rows:        7052769
After cleaning rows:   5591610
After dedup rows:      5590943
Final output rows:     5590943

EXAMPLE OF BAD ROWS:

+--------+--------------------+---------------------+---------------+-------------+----------+------------------+------------+------------+------------+-----------+-----+-------+----------+------------+---------------------+------------+
|VendorID|tpep_pickup_datetime|tpep_dropoff_datetime|passenger_count|trip_distance|RatecodeID|store_and_fwd_flag|PULocationID|DOLocationID|payment_type|fare_amount|extra|mta_tax|tip_amount|tolls_amount|improvement_surcharge|total_amount|                                                 |
+--------+--------------------+---------------------+---------------+-------------+----------+------------------+------------+------------+------------+-----------+-----+-------+----------+------------+---------------------+------------+
|2       |2025-01-01 00:01:41 |2025-01-01 00:07:14  |1              |0.71         |1         |N                 |79          |107         |2           |-7.2       |-1.0 |-0.5   |3.66      |0.0         |-1.0                 |-8.54       |
|2       |2025-01-01 00:55:54 |2025-01-01 01:00:38  |1              |0.69         |1         |N                 |137         |233         |4           |-6.5       |-1.0 |-0.5   |0.0       |0.0         |-1.0                 |-11.5       |
|2       |2025-01-01 00:56:12 |2025-01-01 01:15:00  |1              |0.97         |1         |N                 |161         |170         |4           |-16.3      |-1.0 |-0.5   |0.0       |0.0         |-1.0                 |-21.3       |
+--------+--------------------+---------------------+---------------+-------------+----------+------------------+------------+------------+------------+-----------+-----+-------+----------+------------+---------------------+------------+

RULES FOR BAD ROWS:

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


PERFORMANCE

Total runtime for the full job when no previous runs done: 88s

<img width="491" height="253" alt="image" src="https://github.com/user-attachments/assets/9d88365e-6f86-4719-960b-d76ce5b81cd3" />

<img width="1870" height="155" alt="image" src="https://github.com/user-attachments/assets/9b5c4950-1df7-4c1b-a877-ad3fb2019abc" />

<img width="1839" height="435" alt="image" src="https://github.com/user-attachments/assets/84640ff6-4f68-4671-b34f-a7f5452c9872" />


OPTIMIZATION CHOICES

As the write output jobs were the slowest, when there was an output file existing, the cleaned and deduplicated dataframe was cached and materialized before writing, reducing runtime for the writing operation by 20 s, and increasing the operation for dedup by around 6 seconds, giving a total decrease in operation around 14 s. 







