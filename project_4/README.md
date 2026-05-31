# Runbook — run & verify the pipeline

Quick, copy-pasteable. For the conceptual overview see `README.md`.

## Run

```bash
make clean && make up        # first time, or after migration changes
make pull-models             # one-time: pulls qwen2.5:3b into Ollama (~2 GB)
make airflow-install         # only after editing pyproject.toml
```

First `make up` may time out on the Airflow healthcheck — that's normal,
the `pip install -e /opt/project` is still running. Watch it finish with:

```bash
docker compose logs -f airflow
```

## Verify a run

After the DAG goes all-green, open psql and run:

```sql
-- 1. Run record
SELECT run_id, status, limit_param, started_at, ended_at
  FROM pipeline_runs ORDER BY started_at DESC LIMIT 1;     -- status='success'

-- 2. Row counts
SELECT COUNT(*) FROM screens_metadata;                     -- 5
SELECT embedding_kind, COUNT(*)
  FROM screens_embeddings GROUP BY embedding_kind;         -- image=5, text=5

-- 3. DoD: no nulls in required columns
SELECT COUNT(*) FROM screens_metadata   WHERE run_id IS NULL;             -- 0
SELECT COUNT(*) FROM screens_embeddings WHERE run_id IS NULL;             -- 0
SELECT COUNT(*) FROM screens_metadata   WHERE source_fingerprint IS NULL; -- 0
SELECT COUNT(*) FROM screens_embeddings WHERE source_fingerprint IS NULL; -- 0

-- 4. Audit passed
SELECT audit_name, passed FROM audit_results;              -- both true

-- 5. Eval ran
SELECT run_id, n_queries, recall_at_5 FROM screens_eval;

-- 6. Metrics
SELECT COUNT(*) FROM pipeline_metrics
  WHERE run_id = (SELECT run_id FROM pipeline_runs ORDER BY started_at DESC LIMIT 1);

-- 7. LLM extractions
SELECT screen_id, confidence, extraction_payload->>'title' AS title
  FROM screens_metadata;
SELECT COUNT(*) FROM screens_review_queue;                 -- 0..5
```

**MinIO:** <http://localhost:9001> (minioadmin / minioadmin) → bucket
`rico-raw` → 5 PNG + 5 JSON under `screens/`.

**Slack mock:** `project_4/logs/slack/<YYYY-MM-DD>.log` — one
`run_started` and one `run_finished` line per run.


```


