# Streaming correctness

<img width="785" height="434" alt="image" src="https://github.com/user-attachments/assets/39fd57c8-2fa7-4d98-9251-498b8474c89c" />

<img width="523" height="213" alt="image" src="https://github.com/user-attachments/assets/8c899c00-c922-40fe-9f12-93ded74dfa67" />

# Lakehouse design

## Medallion layer description

### Bronze — bronze_raw_events

Schema:

key, value (BINARY), topic, partition, offset, timestamp, timestampType

---

### Silver — silver_trips

Schema:

VendorID, pickup_datetime, dropoff_datetime, trip_duration_min,
pickup_date, PULocationID, pickup_zone, pickup_borough,
DOLocationID, dropoff_zone, dropoff_borough, passenger_count,
trip_distance, fare_amount, tip_amount, tolls_amount...


The binary column value has been decoded, parsed and enriched. Key differences from bronze:
- Binary → typed columns (timestamps, doubles, bigints)
- Zone IDs joined to pickup_zone, pickup_borough
- Derived column trip_duration_min calculated from pickup/dropoff times
- Bad/null records filtered out

---

### Gold tables

#### gold_trips_per_hour:

pickup_date, pickup_hour, trip_count

Silver rows collapsed into hourly trip counts, one row per hour.

#### gold_revenue_per_zone:

pickup_date, pickup_location_id, pickup_borough, pickup_zone, total_revenue

Silver rows aggregated into total revenue per zone per day.

#### gold_average_fare_per_zone:

pickup_date, pickup_location_id, pickup_borough, pickup_zone, average_fare

Silver rows aggregated into average fare per zone per day.

---

### Gold partitioning strategy

All gold (and silver) tables are partitioned by pickup_date, organizing data into daily partitions. This ensures each batch job writes only to its own date partition without affecting historical data.

### Snapshot history
<img width="505" height="107" alt="image" src="https://github.com/user-attachments/assets/3b338427-e942-4543-bfa8-c475af463643" />


<img width="599" height="545" alt="image" src="https://github.com/user-attachments/assets/ad0c8920-9fa7-4413-9a38-799748172ff0" />


# Bronze Trigger Benchmark

The Bronze trigger benchmark was added to the bottom of `work/index.ipynb` and executed from the notebook.

Benchmark setup:

- Same input for every run: first 360 rows from `data/yellow_tripdata_2025-01.parquet`
- Replay rate: 10 Kafka events per second
- Sink: separate Iceberg Bronze benchmark table per trigger configuration
- Measured end-to-end time: from the start of Kafka replay until all 360 rows were visible in the Bronze table
- Measured batch count: number of Iceberg commits that added records

## Results

| Trigger | Output files | Batches processed | End-to-end time |
| --- | ---: | ---: | ---: |
| `5 seconds` | 16 | 8 | 38.33 s |
| `30 seconds` | 4 | 2 | 48.98 s |
| `1 minute` | 4 | 2 | 88.27 s |

## Trade-off

More frequent triggers reduce latency because Spark commits smaller chunks of data sooner. In this benchmark, the `5 seconds` trigger finished fastest, but it also created the most files and the most batches. That is the classic low-latency trade-off: better freshness, higher metadata overhead, and more small-file pressure in Iceberg.

Less frequent triggers reduce file churn by grouping more input into each micro-batch. The `30 seconds` trigger cut the file count from 16 to 4 and reduced the batch count from 8 to 2, but latency increased because data waited longer before each commit.

The `1 minute` trigger did not reduce files further for this specific workload, because the 360-row replay still crossed two committed micro-batches in practice. Even so, it had the highest latency by a wide margin. This shows that a larger trigger interval does not guarantee fewer files if the input duration and trigger boundaries still produce multiple commits, but it does increase how long downstream consumers wait for results.

For this workload, `30 seconds` is the better balance than `1 minute`: it kept the low file count while avoiding the large latency penalty of the slowest trigger. The `5 seconds` trigger only makes sense if lower latency is more important than Bronze-layer file growth.
