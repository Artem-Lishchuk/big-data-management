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


def list_screens_for_run(run_id: str) -> list[tuple[int, str]]:
    with psycopg.connect(postgres_dsn()) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT screen_id, hierarchy_json_path
                FROM screens_metadata
                WHERE run_id = %s
                ORDER BY screen_id
                """,
                (run_id,),
            )
            rows = cur.fetchall()
    return [(int(screen_id), hierarchy_json_path) for screen_id, hierarchy_json_path in rows]

def get_text_representation_by_screen_id(screen_id: int) -> str:
    with psycopg.connect(postgres_dsn()) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT text_representation
                FROM screens_metadata
                WHERE screen_id = %s
                """,
                (screen_id,),
            )
            row = cur.fetchone()
    return str(row[0])