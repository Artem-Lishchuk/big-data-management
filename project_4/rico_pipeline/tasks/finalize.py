import psycopg

from rico_pipeline import utils
from rico_pipeline.config import postgres_dsn


def finalize_run(**context):
    run_uuid = utils.get_pipeline_run_id(context)
    dag_run = context["dag_run"]
    dag = context.get("dag")
    
    task_ids = [t.task_id for t in dag.tasks]

    states: list[str | None] = []

    for task_id in task_ids:
        if task_id == "finalize":
            continue

        ti = dag_run.get_task_instance(task_id)
        states.append(ti.state if ti is not None else None)
        status = "success" if all(s == "success" for s in states) else "failed"

    with psycopg.connect(postgres_dsn()) as conn:
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