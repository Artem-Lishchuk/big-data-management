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