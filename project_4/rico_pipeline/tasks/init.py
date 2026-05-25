import uuid
import psycopg

from rico_pipeline.config import ollama_model, postgres_dsn


def init_run(**context):
    dag_run_id = context["dag_run"].run_id
    run_uuid = uuid.uuid4()
    limit = context["params"].get("limit")

    with psycopg.connect(postgres_dsn()) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO pipeline_runs (
                    run_id, dag_run_id, status, limit_param,
                    git_sha, llm_model, prompt_version
                ) VALUES (%s, %s, 'running', %s, %s, %s, %s)
                """,
                (
                    run_uuid,
                    dag_run_id,
                    limit,
                    "git-sha",
                    ollama_model(),
                    "v1",
                ),
            )
        conn.commit()

    return str(run_uuid)
