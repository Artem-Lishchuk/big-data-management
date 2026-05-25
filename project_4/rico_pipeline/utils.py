import psycopg

from rico_pipeline.config import postgres_dsn


def get_pipeline_run_id(context) -> str:
    dag_run_id = context["dag_run"].run_id
    with psycopg.connect(postgres_dsn()) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT run_id FROM pipeline_runs WHERE dag_run_id = %s",
                (dag_run_id,),
            )
            row = cur.fetchone()
    if not row:
        raise RuntimeError(f"No pipeline_runs row for dag_run_id={dag_run_id}")
    return str(row[0])