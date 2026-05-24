import os
import uuid
import psycopg

def init_run(**context):
    dag_run_id = context["dag_run"].run_id
    run_uuid = uuid.uuid4()
    limit = context["params"].get("limit")

    dsn = os.environ["POSTGRES_DSN"]

    with psycopg.connect(dsn) as conn:
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
                    os.environ.get("OLLAMA_MODEL"),
                    "v1"
                ),
            )
        conn.commit()
    
    return str(run_uuid)