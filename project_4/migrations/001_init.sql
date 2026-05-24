-- Runs on first initialization of the postgres data volume.
-- The default database (POSTGRES_DB=rico) is created by the entrypoint
-- before this script runs.

-- Airflow stores its metadata in a separate database on the same instance.
SELECT 'CREATE DATABASE airflow'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'airflow')\gexec

\c rico

CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    dag_run_id      TEXT NOT NULL,
    started_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    ended_at        TIMESTAMPTZ,
    status          TEXT NOT NULL,
    limit_param     INTEGER,
    git_sha         TEXT,
    clip_version    TEXT,
    sbert_version   TEXT,
    llm_model       TEXT,
    prompt_version  TEXT
);

CREATE TABLE IF NOT EXISTS screens_metadata (
    screen_id           BIGINT PRIMARY KEY,
    run_id              UUID REFERENCES pipeline_runs(run_id),
    app_package         TEXT,
    category            TEXT,
    png_path            TEXT NOT NULL,
    hierarchy_json_path TEXT NOT NULL,
    extraction_payload  JSONB,
    prompt_version      TEXT,
    confidence          DOUBLE PRECISION,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS screens_embeddings (
    screen_id      BIGINT NOT NULL,
    run_id         UUID REFERENCES pipeline_runs(run_id),
    model_name     TEXT NOT NULL,
    model_version  TEXT NOT NULL,
    embedding_kind TEXT NOT NULL CHECK (embedding_kind IN ('image', 'text')),
    vector         vector NOT NULL,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (screen_id, model_name, model_version, embedding_kind)
);

CREATE TABLE IF NOT EXISTS screens_review_queue (
    id          BIGSERIAL PRIMARY KEY,
    run_id      UUID REFERENCES pipeline_runs(run_id),
    screen_id   BIGINT NOT NULL,
    reason      TEXT NOT NULL,
    raw_output  TEXT,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS screens_eval (
    id                       BIGSERIAL PRIMARY KEY,
    embedding_model_version  TEXT NOT NULL,
    n_queries                INTEGER NOT NULL,
    recall_at_5              DOUBLE PRECISION NOT NULL,
    created_at               TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS pipeline_metrics (
    run_id          UUID NOT NULL REFERENCES pipeline_runs(run_id),
    metric_name     TEXT NOT NULL CHECK (metric_name IN (
        'metadata.row_count',
        'metadata.extraction_payload_non_null_pct',
        'metadata.confidence_gte_0_5_pct',
        'metadata.review_queue_pct',
        'embeddings.row_count',
        'embeddings.avg_vector_dim',
        'embeddings.zero_norm_pct',
        'sanity.distinct_app_package_count',
        'sanity.distinct_category_count'
    )),
    model_version   TEXT NOT NULL DEFAULT '',
    embedding_kind  TEXT NOT NULL DEFAULT '' CHECK (embedding_kind IN ('', 'image', 'text')),
    metric_value    DOUBLE PRECISION NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (run_id, metric_name, model_version, embedding_kind)
);