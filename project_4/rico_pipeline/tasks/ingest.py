import itertools
import time
from io import BytesIO

import psycopg
from datasets import load_dataset

from rico_pipeline.config import minio_bucket, postgres_dsn, s3_client
from rico_pipeline.utils import get_pipeline_run_id, sha256_hex


def _ingest_screen(sid: int, row: dict, *, run_id: str, s3, bucket: str, cur) -> int:
    """Returns 1 if a new row was inserted, 0 if it already existed."""
    png_key = f"screens/{sid}.png"
    hier_key = f"screens/{sid}.json"

    png_buf = BytesIO()
    row["image"].save(png_buf, format="PNG")
    png_bytes = png_buf.getvalue()
    fingerprint = sha256_hex(png_bytes)

    s3.put_object(Bucket=bucket, Key=png_key, Body=png_bytes)
    s3.put_object(
        Bucket=bucket,
        Key=hier_key,
        Body=row["view_hierarchy"].encode("utf-8"),
    )

    cur.execute(
        """
        INSERT INTO screens_metadata (
            screen_id, run_id, app_package, category,
            png_path, hierarchy_json_path, source_fingerprint
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (screen_id) DO NOTHING
        """,
        (
            sid, run_id, row["app_package_name"], row["category"],
            png_key, hier_key, fingerprint,
        ),
    )
    return cur.rowcount  # 1 = inserted, 0 = conflict


def run_ingest(**context):
    t0 = time.monotonic()
    limit = context["params"].get("limit")
    run_id = get_pipeline_run_id(context)

    ds = load_dataset(
        "rootsautomation/RICO-Screen2Words",
        split="train",
        streaming=True,
        trust_remote_code=True,
    )
    raw_rows: dict[int, dict] = {}
    for row in itertools.islice(ds, limit):
        sid = int(row["screenId"])
        raw_rows[sid] = row

    s3 = s3_client()
    bucket = minio_bucket()

    inserted = 0
    with psycopg.connect(postgres_dsn()) as conn, conn.cursor() as cur:
        for sid, row in raw_rows.items():
            inserted += _ingest_screen(
                sid, row, run_id=run_id, s3=s3, bucket=bucket, cur=cur,
            )
        # Make sure THIS run owns all the screens it processed, even if the
        # underlying row was inserted by a previous run.  Required so audit /
        # downstream tasks find this run's screens.
        cur.execute(
            """
            UPDATE screens_metadata
               SET run_id = %s, updated_at = NOW()
             WHERE screen_id = ANY(%s) AND run_id IS DISTINCT FROM %s
            """,
            (run_id, list(raw_rows.keys()), run_id),
        )
        conn.commit()

    return {
        "rows_in": len(raw_rows),
        "rows_out": inserted,
        "duration_s": time.monotonic() - t0,
    }
