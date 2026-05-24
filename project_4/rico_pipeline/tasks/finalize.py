import psycopg
import os
from rico_pipeline import utils

def finalize_run(**context):
    run_uuid = utils.get_pipeline_run_id(context)
    dag_run = context["dag_run"]
    ingest_state = dag_run.get_task_instance("ingest").state
    status = "success" if ingest_state == "success" else "failed"

    with psycopg.connect(os.environ["POSTGRES_DSN"]) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE pipeline_runs
                SET status = %s, ended_at = NOW()
                WHERE run_id = %s
                """,
                (status, run_uuid),
            )
        conn.commit()