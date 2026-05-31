import uuid

import psycopg

from rico_pipeline.config import (
    clip_version,
    git_sha,
    ollama_model,
    postgres_dsn,
    prompt_version,
    sbert_version,
)
from rico_pipeline.slack import notify_run_started


def init_run(**context):
    dag_run = context["dag_run"]
    dag_run_id = dag_run.run_id
    run_uuid = uuid.uuid4()
    limit = context["params"].get("limit")

    with psycopg.connect(postgres_dsn()) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO pipeline_runs (
                    run_id, dag_run_id, status, limit_param,
                    git_sha, clip_version, sbert_version,
                    llm_model, prompt_version
                ) VALUES (%s, %s, 'running', %s, %s, %s, %s, %s, %s)
                """,
                (
                    run_uuid,
                    dag_run_id,
                    limit,
                    git_sha(),
                    clip_version(),
                    sbert_version(),
                    ollama_model(),
                    f"v{prompt_version()}",
                ),
            )
        conn.commit()

    trigger = getattr(dag_run, "run_type", "manual") or "manual"
    notify_run_started(str(run_uuid), limit, trigger)
    return str(run_uuid)
