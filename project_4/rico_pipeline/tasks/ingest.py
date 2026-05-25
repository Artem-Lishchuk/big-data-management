import itertools
from io import BytesIO
import psycopg
from datasets import load_dataset

from rico_pipeline.config import minio_bucket, postgres_dsn, s3_client
from rico_pipeline.utils import get_pipeline_run_id


def _ingest_screen(sid: int, row: dict, *, run_id: str, s3, bucket: str, cur) -> None:
    png_key = f"screens/{sid}.png"
    hier_key = f"screens/{sid}.json"

    png_buf = BytesIO()
    row["image"].save(png_buf, format="PNG")
    png_bytes = png_buf.getvalue()

    s3.put_object(Bucket=bucket, Key=png_key, Body=png_bytes)
    s3.put_object(Bucket=bucket, Key=hier_key, Body=row["view_hierarchy"].encode("utf-8"))

    cur.execute(
        """
        INSERT INTO screens_metadata (
            screen_id, run_id, app_package, category, png_path, hierarchy_json_path
        )
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (screen_id) DO UPDATE SET
            run_id = EXCLUDED.run_id,
            app_package = EXCLUDED.app_package,
            category = EXCLUDED.category,
            png_path = EXCLUDED.png_path,
            hierarchy_json_path = EXCLUDED.hierarchy_json_path,
            updated_at = NOW()
        """,
        (sid, run_id, row["app_package_name"], row["category"], png_key, hier_key),
    )

def run_ingest(**context):
    dag_run_id = context["dag_run"].run_id
    limit = context["params"].get("limit")

    print(f"Running ingest for DAG run {dag_run_id}, limit={limit}")

    ds = load_dataset("rootsautomation/RICO-Screen2Words", split="train", streaming=True, trust_remote_code=True)
    raw_rows: dict[int, dict] = {}
    for row in itertools.islice(ds, limit):
        sid = int(row["screenId"])
        raw_rows[sid] = row
    print(f"collected {len(raw_rows)} rows: {sorted(raw_rows)}")

    run_id = get_pipeline_run_id(context)
    s3 = s3_client()
    bucket = minio_bucket()

    with psycopg.connect(postgres_dsn()) as conn, conn.cursor() as cur:
        for sid, row in raw_rows.items():
            _ingest_screen(sid, row, run_id=run_id, s3=s3, bucket=bucket, cur=cur)
        conn.commit()

    return {"screens_ingested": len(raw_rows)}