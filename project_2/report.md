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

# Custom scenario

[
  {
    "trigger": "5 seconds",
    "rows": 360,
    "batches": 9,
    "output_files": 18,
    "end_to_end_seconds": 41.05
  },
  {
    "trigger": "30 seconds",
    "rows": 360,
    "batches": 3,
    "output_files": 6,
    "end_to_end_seconds": 62.52
  },
  {
    "trigger": "1 minute",
    "rows": 360,
    "batches": 1,
    "output_files": 2,
    "end_to_end_seconds": 57.49
  }
]

More frequent triggers write data sooner but produce more small files, hurting query performance. Less frequent triggers produce fewer, larger files that are faster to query, but data waits longer before landing. The 30s trigger is the best balance with 3 times fewer files than 5s and acceptable latency.






Each layer reduces row count and increases semantic meaning — bronze is widest in raw bytes, gold is smallest in rows but most meaningful for analysis.
