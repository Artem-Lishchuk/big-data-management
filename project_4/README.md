# Project 4 — RICO Pipeline

Airflow DAG that ingests RICO screenshots, embeds them (CLIP + SBERT),
extracts structured metadata (Ollama), audits the result, and computes
recall@5.

```
init → ingest → parse → [embed_image, embed_text, llm_extract] → audit → eval → finalize
```

## Run

```bash
make clean && make up        # first time, or after a migration change
make pull-models             # one-time: pulls qwen2.5:3b (~2 GB)
make airflow-install         # only after editing pyproject.toml
```

First `make up` may time out on the Airflow healthcheck — `pip install -e
/opt/project` is still running. Watch with `docker compose logs -f airflow`.

Open <http://localhost:8080> (admin / admin) → trigger `project_4_dag`
with config `{"limit": 5}`. `LIMIT` controls how many screens to ingest
(dev = 5, demo = 50).

## Verify

In psql (`docker compose exec postgres psql -U rico -d rico`):

```sql
-- run record
SELECT run_id, status FROM pipeline_runs ORDER BY started_at DESC LIMIT 1;   -- success

-- row counts
SELECT COUNT(*) FROM screens_metadata;                                       -- 5
SELECT embedding_kind, COUNT(*) FROM screens_embeddings GROUP BY embedding_kind;  -- 5,5

-- DoD: required columns are non-null
SELECT COUNT(*) FROM screens_metadata   WHERE run_id IS NULL OR source_fingerprint IS NULL;  -- 0
SELECT COUNT(*) FROM screens_embeddings WHERE run_id IS NULL OR source_fingerprint IS NULL;  -- 0

-- audit + eval
SELECT audit_name, passed FROM audit_results;                                -- both true
SELECT run_id, recall_at_5, n_queries FROM screens_eval;

-- metrics
SELECT COUNT(*) FROM pipeline_metrics
  WHERE run_id = (SELECT run_id FROM pipeline_runs ORDER BY started_at DESC LIMIT 1);

-- llm extractions
SELECT screen_id, confidence, extraction_payload->>'title' AS title FROM screens_metadata;
```

**MinIO:** <http://localhost:9001> (minioadmin / minioadmin) → bucket
`rico-raw` has 5 PNG + 5 JSON under `screens/`.

**End-of-run summary:** `finalize` task log shows one boxed block listing
every metric for the run.

**Slack mock:** `logs/slack/<YYYY-MM-DD>.log` — `run_started` +
`run_finished` per run, plus `audit_failed` on halt. Set
`SLACK_WEBHOOK_URL` to post to real Slack.

### Idempotency

Re-trigger the DAG with the same `{"limit": 5}`. Row counts in
`screens_metadata` / `screens_embeddings` must be **identical**; only
`pipeline_runs` and `pipeline_metrics` grow.

### Audit halt

Force a duplicate, trigger the DAG, expect `audit` red + `eval` skipped +
`pipeline_runs.status='failed'`:

```sql
ALTER TABLE screens_embeddings DROP CONSTRAINT screens_embeddings_pkey;
INSERT INTO screens_embeddings
  (screen_id, run_id, model_name, model_version, embedding_kind, vector, source_fingerprint)
SELECT screen_id, run_id, model_name, model_version, embedding_kind, vector, 'forced-dup'
  FROM screens_embeddings LIMIT 1;
-- after the failed run, clean up:
DELETE FROM screens_embeddings WHERE source_fingerprint = 'forced-dup';
ALTER TABLE screens_embeddings
  ADD PRIMARY KEY (screen_id, model_name, model_version, embedding_kind);
```

## Metrics

Stored in `pipeline_metrics (run_id, metric_name, model_version, embedding_kind, task_id, metric_value)`.

| metric_name                                | meaning                                          |
| ------------------------------------------ | ------------------------------------------------ |
| `metadata.row_count`                       | rows in `screens_metadata` for this run          |
| `metadata.extraction_payload_non_null_pct` | % rows where LLM extract succeeded               |
| `metadata.confidence_gte_0_5_pct`          | % rows with LLM confidence ≥ 0.5                 |
| `metadata.review_queue_pct`                | % rows sent to `screens_review_queue`            |
| `embeddings.row_count`                     | rows per (model_version, embedding_kind)         |
| `embeddings.avg_vector_dim`                | mean vector dim (constant: 512 / 384)            |
| `embeddings.zero_norm_pct`                 | % zero-norm vectors (expect 0)                   |
| `sanity.distinct_app_package_count`        | distinct `app_package` values                    |
| `sanity.distinct_category_count`           | distinct `category` values                       |
| `task.duration_seconds`                    | per-task duration (`task_id` dim)                |
| `task.rows_in` / `task.rows_out`           | per-task rows read / written                     |
| `task.retries`                             | per-task retry count                             |
| `run.duration_seconds`                     | sum of task durations                            |
| `eval.recall_at_5`                         | self-test recall@5 (see caveat below)            |

**Recall@5 caveat.** Eval queries the same `screens_embeddings` rows it
just produced — a tautology, expected ≈1.0. Verifies the embed → store →
pgvector-query path end-to-end. See `notebook.ipynb` §7 for the
disjoint-holdout (honest) version.

## Interpreting an audit failure

Audit checks two invariants:

1. No duplicate `(screen_id, model_name, model_version, embedding_kind)`
   in `screens_embeddings`.
2. No duplicate `screen_id` in `screens_metadata` for the current `run_id`.

On failure the `audit` task prints every duplicate in full, writes
`audit_results.passed=false` with the offending keys in `details`, sends
a Slack `audit_failed` notification, and raises — so `eval` is skipped
and `finalize` marks the run failed.

Debug: open the `audit` task log, copy the listed keys, query the table.
Usual causes: (a) recent change to an `ON CONFLICT` path, (b) manual SQL
that bypassed idempotency.

