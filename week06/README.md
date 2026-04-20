# Week 06 Practice: Pipeline Orchestration with Apache Airflow

Hands-on practice orchestrating the CDC pipeline from Week 05 with Apache Airflow — scheduling DAG runs, adding reliability via retries and SLAs, testing failure recovery, and using `BranchPythonOperator` to skip work when there is nothing to process.

## Learning outcomes

By the end of this session you should be able to:

- Explain the anatomy of an Airflow **DAG**: `start_date`, `schedule_interval`, `catchup`, `max_active_runs`
- Use `HttpSensor` to guard pipeline tasks behind a health-check endpoint
- Build a task dependency graph with `>>` and `BranchPythonOperator`
- Configure **retries**, `retry_exponential_backoff`, and **SLA** callbacks
- Trigger DAG runs and inspect results via the **Airflow REST API**
- Perform a **backfill** (reprocess historical windows) and verify **idempotency**
- Simulate a connector failure and watch Airflow retry and recover
- Skip downstream work with `BranchPythonOperator` when no new events exist

## Requirements

- Docker Desktop installed and running (4 GB+ RAM recommended)
- Week 05 practice completed (creates `public.customers` table used here)

## Quick start

```bash
# Copy credentials file (only needed once)
cp .env.example .env

docker compose up -d
```

Wait ~60 seconds for all services to stabilise (Airflow takes the longest), then open:

- **Jupyter:** [http://localhost:8889](http://localhost:8889) — token: `admin`
- **Airflow UI:** [http://localhost:8081](http://localhost:8081) — admin / admin
- **Kafka Connect:** [http://localhost:8084](http://localhost:8084)

Navigate to `work/week_06_practice.ipynb` and run the cells in order.

```bash
docker compose down -v   # when done (-v removes volumes for a clean restart)
```

## Architecture

```
┌──────────────────────────────────────────────────────────────────────────────┐
│  Docker Compose network                                                      │
│                                                                              │
│  ┌──────────────┐  WAL   ┌──────────────────────┐                           │
│  │  postgres    │───────►│  connect             │                           │
│  │  (source DB) │        │  (Debezium / Kafka   │                           │
│  │  :5432       │        │   Connect)  :8083    │                           │
│  └──────────────┘        └──────────┬───────────┘                           │
│                                     │ CDC events                            │
│                                     ▼                                       │
│  ┌──────────────────────────────────────────┐                               │
│  │  kafka  (KRaft)  :9092                   │                               │
│  │  Topic: dbserver1.public.customers       │                               │
│  └───────────────┬──────────────────────────┘                               │
│                  │ consumed by Airflow tasks                                 │
│  ┌───────────────▼──────────────────────────────────────────────────────┐   │
│  │  airflow  :8080                                                       │   │
│  │  DAG: cdc_pipeline  (@hourly, catchup=False, max_active_runs=1)       │   │
│  │                                                                       │   │
│  │  health_check (HttpSensor)                                            │   │
│  │       │                                                               │   │
│  │  check_new_events (BranchPythonOperator)                              │   │
│  │      /               \                                                │   │
│  │  bronze_cdc        skip_merge (EmptyOperator)                         │   │
│  │      │                                                                │   │
│  │  silver_cdc  → silver_customers_af in PostgreSQL                      │   │
│  └───────────────────────────────────────────────────────────────────────┘   │
│                                                                              │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │  jupyter  :8888/:4040   (notebook drives the whole practice)        │    │
│  │  • Writes DAG to dags/cdc_pipeline.py (shared mount with Airflow)   │    │
│  │  • Calls Airflow REST API to trigger + monitor runs                 │    │
│  │  Notebook: work/week_06_practice.ipynb                              │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
└──────────────────────────────────────────────────────────────────────────────┘
       ▲            ▲              ▲
  :8888/:4040    :8080          :8083
    Jupyter     Airflow UI    Connect API
```

**What's new compared to Week 05?**

| Component | Week 05 | Week 06 |
|-----------|---------|---------|
| Kafka + Debezium + PostgreSQL | ✓ | ✓ (reused) |
| PySpark + Iceberg | ✓ | — (not needed) |
| MinIO | ✓ | — (not needed) |
| Apache Airflow | — | ✓ **new** |
| Bronze/Silver storage | Iceberg on MinIO | JSON checkpoint + PostgreSQL table |

Week 06 simplifies storage to focus entirely on orchestration concepts. The Bronze
layer is a JSON checkpoint file; the Silver layer is a PostgreSQL table updated via
`ON CONFLICT` upsert — both trivially idempotent.

## Notebook structure

| Part | Topic | What you'll do |
|------|-------|----------------|
| **1** | HttpSensor | Create the Airflow Connection object, manually test the endpoint, write the full DAG file to `dags/` |
| **2** | DAG structure | Wait for Airflow file scan, unpause the DAG, inspect the task graph via REST API |
| **3** | Scheduling, retries & SLA | Trigger the first run, poll to completion, inspect task states and Silver table |
| **4** | Backfill & idempotency | Make PG changes, trigger runs, reset stored offset, prove same Silver result |
| **5** | Failure test | Delete the connector, observe sensor retrying, re-register, watch recovery |
| **6** | BranchPythonOperator | Trigger with no new events → `skip_merge` path; then add a row and take the full path |
| **Exercises** | Two self-guided tasks | Add a `validate` task; inspect XCom values across runs |

## File layout

```
week06/
├── compose.yml                 ← this file — 5-service stack
├── .env.example                ← copy to .env before first run
├── dags/                       ← shared with Airflow (/opt/airflow/dags)
│   ├── cdc_pipeline.py         ← written by the notebook (Part 2)
│   ├── cdc_state.json          ← stored Kafka offset (written by bronze_cdc task)
│   └── bronze_YYYYMMDD.json    ← CDC checkpoint per logical date
└── work/
    └── week_06_practice.ipynb  ← the practice notebook
```

## Useful commands

```bash
# Check all containers and their health status
docker compose ps

# Stream Airflow logs (scheduler + webserver)
docker compose logs airflow --follow --tail 50

# Stream Debezium logs
docker compose logs connect --tail 50

# Inspect Kafka topics
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --list

# Consume CDC events live (Ctrl+C to stop)
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic dbserver1.public.customers \
  --from-beginning

# Connect to PostgreSQL
docker compose exec postgres psql -U cdc_user -d sourcedb

# Trigger a DAG run from the CLI (alternative to notebook)
docker compose exec airflow airflow dags trigger cdc_pipeline

# List all DAG runs
docker compose exec airflow airflow dags list-runs -d cdc_pipeline
```

## Reference

- [Apache Airflow — DAG Concepts](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/dags.html)
- [Airflow REST API Reference](https://airflow.apache.org/docs/apache-airflow/stable/stable-rest-api-ref.html)
- [HttpSensor](https://airflow.apache.org/docs/apache-airflow-providers-http/stable/sensors/http.html)
- [BranchPythonOperator](https://airflow.apache.org/docs/apache-airflow/stable/howto/operator/python.html#branch-python-operator)
- [Debezium PostgreSQL Connector](https://debezium.io/documentation/reference/stable/connectors/postgresql.html)
- *Designing Data-Intensive Applications*, 2nd Ed. — Ch. 10 (Batch Processing), Ch. 12 (Future of Data Systems)
