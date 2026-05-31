"""
Audit task — runs after embeds + LLM extract, before eval.

Checks:
  1. No duplicate (screen_id, model_name, model_version, embedding_kind) in
     screens_embeddings (across the whole table — duplicates anywhere are bad).
  2. No duplicate screen_id in screens_metadata for the current run.

On failure: log every duplicate key found, write a row to audit_results with
passed=false, send a Slack notification, and raise so eval is skipped and the
run is marked failed.
"""

from __future__ import annotations

import json
import time

import psycopg

from rico_pipeline.config import postgres_dsn
from rico_pipeline.slack import notify_audit_failed
from rico_pipeline.utils import get_pipeline_run_id


DUP_EMBEDDINGS_SQL = """
    SELECT screen_id, model_name, model_version, embedding_kind, COUNT(*) AS n
      FROM screens_embeddings
     GROUP BY screen_id, model_name, model_version, embedding_kind
    HAVING COUNT(*) > 1
"""

DUP_METADATA_SQL = """
    SELECT screen_id, COUNT(*) AS n
      FROM screens_metadata
     WHERE run_id = %s
     GROUP BY screen_id
    HAVING COUNT(*) > 1
"""

UPSERT_AUDIT_SQL = """
    INSERT INTO audit_results (run_id, audit_name, passed, details)
    VALUES (%s, %s, %s, %s::jsonb)
    ON CONFLICT (run_id, audit_name)
    DO UPDATE SET passed = EXCLUDED.passed,
                  details = EXCLUDED.details,
                  created_at = NOW()
"""


def run_audit(**context):
    t0 = time.monotonic()
    run_id = get_pipeline_run_id(context)

    with psycopg.connect(postgres_dsn()) as conn, conn.cursor() as cur:
        cur.execute(DUP_EMBEDDINGS_SQL)
        dup_emb = [
            {
                "screen_id": int(r[0]),
                "model_name": r[1],
                "model_version": r[2],
                "embedding_kind": r[3],
                "count": int(r[4]),
            }
            for r in cur.fetchall()
        ]

        cur.execute(DUP_METADATA_SQL, (run_id,))
        dup_meta = [
            {"screen_id": int(r[0]), "count": int(r[1])} for r in cur.fetchall()
        ]

        emb_passed = len(dup_emb) == 0
        meta_passed = len(dup_meta) == 0

        cur.execute(
            UPSERT_AUDIT_SQL,
            (
                run_id,
                "screens_embeddings.no_duplicate_key",
                emb_passed,
                json.dumps({"duplicates": dup_emb}),
            ),
        )
        cur.execute(
            UPSERT_AUDIT_SQL,
            (
                run_id,
                "screens_metadata.no_duplicate_screen_id_for_run",
                meta_passed,
                json.dumps({"duplicates": dup_meta}),
            ),
        )
        conn.commit()

    # Log every duplicate found in full, per the PDF.
    if dup_emb:
        print(f"[audit] {len(dup_emb)} duplicate embedding keys:")
        for d in dup_emb:
            print(f"    {d}")
    if dup_meta:
        print(f"[audit] {len(dup_meta)} duplicate metadata screen_ids for run {run_id}:")
        for d in dup_meta:
            print(f"    {d}")

    duration = time.monotonic() - t0
    if not (emb_passed and meta_passed):
        notify_audit_failed(run_id, dup_emb + dup_meta)
        raise RuntimeError(
            f"Audit failed: {len(dup_emb)} duplicate embedding keys, "
            f"{len(dup_meta)} duplicate metadata screen_ids."
        )

    print(f"[audit] passed in {duration:.2f}s")
    return {"rows_in": 0, "rows_out": 0, "duration_s": duration}
