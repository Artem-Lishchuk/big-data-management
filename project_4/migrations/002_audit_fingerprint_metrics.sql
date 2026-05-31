-- Follow-up migration to align the schema with the PDF spec:
--   1. source_fingerprint on screens_metadata and screens_embeddings
--   2. audit_results table
--   3. widen pipeline_metrics CHECK to admit pipeline-health metrics
--      and an optional task_id dimension column
--   4. eval_recall_at_5 admitted as a metric name (also written to screens_eval)

\c rico

-- 1. source_fingerprint columns -------------------------------------------
ALTER TABLE screens_metadata    ADD COLUMN IF NOT EXISTS source_fingerprint TEXT;
ALTER TABLE screens_embeddings  ADD COLUMN IF NOT EXISTS source_fingerprint TEXT;

-- 2. audit_results --------------------------------------------------------
CREATE TABLE IF NOT EXISTS audit_results (
    run_id      UUID NOT NULL REFERENCES pipeline_runs(run_id),
    audit_name  TEXT NOT NULL,
    passed      BOOLEAN NOT NULL,
    details     JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (run_id, audit_name)
);

-- 3. pipeline_metrics: widen CHECK + add task_id dimension ---------------
ALTER TABLE pipeline_metrics ADD COLUMN IF NOT EXISTS task_id TEXT NOT NULL DEFAULT '';

-- Drop and recreate the metric_name CHECK with the expanded whitelist.
DO $$
DECLARE
    cname TEXT;
BEGIN
    SELECT conname INTO cname
    FROM pg_constraint
    WHERE conrelid = 'pipeline_metrics'::regclass
      AND contype = 'c'
      AND pg_get_constraintdef(oid) ILIKE '%metric_name%';
    IF cname IS NOT NULL THEN
        EXECUTE format('ALTER TABLE pipeline_metrics DROP CONSTRAINT %I', cname);
    END IF;
END$$;

ALTER TABLE pipeline_metrics
    ADD CONSTRAINT pipeline_metrics_metric_name_check
    CHECK (metric_name IN (
        -- data quality (original set)
        'metadata.row_count',
        'metadata.extraction_payload_non_null_pct',
        'metadata.confidence_gte_0_5_pct',
        'metadata.review_queue_pct',
        'embeddings.row_count',
        'embeddings.avg_vector_dim',
        'embeddings.zero_norm_pct',
        'sanity.distinct_app_package_count',
        'sanity.distinct_category_count',
        -- pipeline health
        'task.duration_seconds',
        'task.rows_in',
        'task.rows_out',
        'task.retries',
        'run.duration_seconds',
        -- eval
        'eval.recall_at_5'
    ));

-- The original PK was (run_id, metric_name, model_version, embedding_kind);
-- task_id needs to be part of the key so per-task rows coexist.
ALTER TABLE pipeline_metrics DROP CONSTRAINT IF EXISTS pipeline_metrics_pkey;
ALTER TABLE pipeline_metrics
    ADD CONSTRAINT pipeline_metrics_pkey
    PRIMARY KEY (run_id, metric_name, model_version, embedding_kind, task_id);
