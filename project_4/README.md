# Runbook — run & verify the pipeline

Quick, copy-pasteable. For the conceptual overview see `README.md`.

## Run

```powershell
make clean && make up        # first time, or after migration changes
make pull-models             # one-time: pulls qwen2.5:3b into Ollama (~2 GB)
make airflow-install         # only after editing pyproject.toml
```

First `make up` may time out on the Airflow healthcheck — that's normal,
the `pip install -e /opt/project` is still running. Watch it finish with:

```powershell
docker compose logs -f airflow
```

When you see the webserver banner, open <http://localhost:8080>
(admin / admin) → trigger `project_4_dag` with config `{"limit": 5}`.

## Useful shells

```powershell
docker compose exec postgres psql -U rico -d rico   # SQL
docker compose exec ollama ollama list              # which LLMs are loaded
docker compose exec airflow bash                    # poke around Airflow container
docker compose logs -f <service>                    # tail any container
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

## Verify idempotency

Trigger the DAG again with the same `{"limit": 5}`. Re-run the row-count
queries above — numbers must be **identical**. Only `pipeline_runs` and
`pipeline_metrics` grow.

## Verify audit halt

Force a duplicate, then trigger the DAG:

```sql
ALTER TABLE screens_embeddings DROP CONSTRAINT screens_embeddings_pkey;
INSERT INTO screens_embeddings
  (screen_id, run_id, model_name, model_version, embedding_kind, vector, source_fingerprint)
SELECT screen_id, run_id, model_name, model_version, embedding_kind, vector, 'forced-dup'
  FROM screens_embeddings LIMIT 1;
```

Expected: `audit` red, `eval` skipped, `finalize` green,
`pipeline_runs.status='failed'`, `audit_results.passed=false`,
extra `audit_failed` line in `logs/slack/*.log`.

Cleanup:

```sql
DELETE FROM screens_embeddings WHERE source_fingerprint = 'forced-dup';
ALTER TABLE screens_embeddings
  ADD PRIMARY KEY (screen_id, model_name, model_version, embedding_kind);
```


