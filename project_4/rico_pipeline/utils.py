import hashlib

import psycopg

from rico_pipeline.config import postgres_dsn


def sha256_hex(data: bytes | str) -> str:
    """SHA-256 hex digest. Accepts bytes or str (utf-8 encoded)."""
    if isinstance(data, str):
        data = data.encode("utf-8")
    return hashlib.sha256(data).hexdigest()


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
    return [(int(sid), path) for sid, path in rows]


def get_text_representations_for_run(run_id: str) -> dict[int, str]:
    """Single-query batch fetch of text_representation keyed by screen_id."""
    with psycopg.connect(postgres_dsn()) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT screen_id, COALESCE(text_representation, '')
                FROM screens_metadata
                WHERE run_id = %s
                ORDER BY screen_id
                """,
                (run_id,),
            )
            rows = cur.fetchall()
    return {int(sid): txt for sid, txt in rows}
