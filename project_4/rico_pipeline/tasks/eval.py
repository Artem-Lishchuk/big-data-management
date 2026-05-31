"""
Eval task — recall@5 self-test.

For each screen processed in this run, query screens_embeddings (text kind)
by vector similarity and check whether the screen's own id appears in the
top-5 results.  This is acknowledged as a tautology (the query vector IS in
the index) — see PDF "Eval Task".  The number is still useful as a sanity
check that the embedding+pgvector path works end-to-end.

Persists:
  * screens_eval (n_queries, recall_at_5, embedding_model_version)
  * pipeline_metrics (eval.recall_at_5)
"""

from __future__ import annotations

import random
import time

import psycopg
from pgvector.psycopg import register_vector

from rico_pipeline.config import postgres_dsn, sbert_client, sbert_version
from rico_pipeline.utils import (
    get_pipeline_run_id,
    get_text_representations_for_run,
)


NEAREST_SQL = """
    SELECT screen_id
      FROM screens_embeddings
     WHERE embedding_kind = 'text'
       AND model_version = %s
     ORDER BY vector <-> %s::vector
     LIMIT 5
"""

INSERT_EVAL_SQL = """
    INSERT INTO screens_eval (embedding_model_version, n_queries, recall_at_5)
    VALUES (%s, %s, %s)
"""

INSERT_METRIC_SQL = """
    INSERT INTO pipeline_metrics (run_id, metric_name, model_version, embedding_kind, task_id, metric_value)
    VALUES (%s, %s, %s, %s, %s, %s)
    ON CONFLICT (run_id, metric_name, model_version, embedding_kind, task_id)
    DO UPDATE SET metric_value = EXCLUDED.metric_value, created_at = NOW()
"""


def run_eval(**context):
    t0 = time.monotonic()
    run_id = get_pipeline_run_id(context)
    texts_by_sid = get_text_representations_for_run(run_id)
    sids = sorted(texts_by_sid.keys())
    model_version = sbert_version()

    if not sids:
        print("[eval] no screens for run; skipping")
        return {"recall_at_5": 0.0, "n_queries": 0, "duration_s": 0.0}

    # Holdout: 20% sample, min 1, max all.
    rng = random.Random(42)
    sample_size = max(1, int(round(len(sids) * 0.2)))
    sample_size = min(sample_size, len(sids))
    holdout = rng.sample(sids, sample_size)

    sbert = sbert_client()
    hits = 0
    detail: list[tuple[int, list[int]]] = []

    with psycopg.connect(postgres_dsn()) as conn:
        register_vector(conn)
        with conn.cursor() as cur:
            for sid in holdout:
                qvec = sbert.encode(
                    [texts_by_sid[sid]], normalize_embeddings=True
                ).astype("float32")[0]
                cur.execute(NEAREST_SQL, (model_version, qvec))
                top = [int(r[0]) for r in cur.fetchall()]
                detail.append((sid, top))
                if sid in top:
                    hits += 1

            recall = hits / len(holdout)
            cur.execute(
                INSERT_EVAL_SQL, (model_version, len(holdout), recall)
            )
            cur.execute(
                INSERT_METRIC_SQL,
                (run_id, "eval.recall_at_5", model_version, "text", "eval", recall),
            )
        conn.commit()

    print(f"[eval] recall@5 (self-test) = {recall:.3f} over {len(holdout)} holdout screens")
    for expected, top in detail:
        hit = "HIT " if expected in top else "MISS"
        print(f"   {hit}  expected={expected}  top5={top}")

    return {
        "recall_at_5": recall,
        "n_queries": len(holdout),
        "duration_s": time.monotonic() - t0,
    }
