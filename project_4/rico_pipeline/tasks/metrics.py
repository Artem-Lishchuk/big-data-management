"""
Metric collection helpers.

* collect_data_quality_metrics(run_id): scans current run's tables and writes
  data-quality metrics into pipeline_metrics.
* collect_pipeline_health_metrics(context, run_id): walks task instances in
  this DAG run and writes per-task duration, retries, and (via XCom) rows_in
  and rows_out into pipeline_metrics.
* build_summary(run_id): returns a single human-readable block for the
  end-of-run log + Slack message.
"""

from __future__ import annotations

import time
from typing import Any

import psycopg

from rico_pipeline.config import postgres_dsn


UPSERT_METRIC_SQL = """
    INSERT INTO pipeline_metrics
        (run_id, metric_name, model_version, embedding_kind, task_id, metric_value)
    VALUES (%s, %s, %s, %s, %s, %s)
    ON CONFLICT (run_id, metric_name, model_version, embedding_kind, task_id)
    DO UPDATE SET metric_value = EXCLUDED.metric_value, created_at = NOW()
"""


def _upsert(cur, run_id, name, value, *, model_version="", embedding_kind="", task_id=""):
    cur.execute(
        UPSERT_METRIC_SQL,
        (run_id, name, model_version, embedding_kind, task_id, float(value)),
    )


def collect_data_quality_metrics(run_id: str) -> dict[str, float]:
    out: dict[str, float] = {}
    with psycopg.connect(postgres_dsn()) as conn, conn.cursor() as cur:
        # screens_metadata stats
        cur.execute(
            """
            SELECT
              COUNT(*),
              COUNT(*) FILTER (WHERE extraction_payload IS NOT NULL),
              COUNT(*) FILTER (WHERE confidence IS NOT NULL AND confidence >= 0.5),
              COUNT(DISTINCT app_package),
              COUNT(DISTINCT category)
              FROM screens_metadata WHERE run_id = %s
            """,
            (run_id,),
        )
        total, non_null_payload, conf_ge_05, app_pkgs, cats = cur.fetchone()
        total = int(total)
        pct = lambda n: (100.0 * n / total) if total else 0.0
        out["metadata.row_count"] = total
        out["metadata.extraction_payload_non_null_pct"] = pct(non_null_payload)
        out["metadata.confidence_gte_0_5_pct"] = pct(conf_ge_05)
        out["sanity.distinct_app_package_count"] = int(app_pkgs)
        out["sanity.distinct_category_count"] = int(cats)
        _upsert(cur, run_id, "metadata.row_count", total)
        _upsert(cur, run_id, "metadata.extraction_payload_non_null_pct", out["metadata.extraction_payload_non_null_pct"])
        _upsert(cur, run_id, "metadata.confidence_gte_0_5_pct", out["metadata.confidence_gte_0_5_pct"])
        _upsert(cur, run_id, "sanity.distinct_app_package_count", app_pkgs)
        _upsert(cur, run_id, "sanity.distinct_category_count", cats)

        # review queue %
        cur.execute(
            "SELECT COUNT(*) FROM screens_review_queue WHERE run_id = %s", (run_id,)
        )
        review_n = int(cur.fetchone()[0])
        out["metadata.review_queue_pct"] = pct(review_n)
        _upsert(cur, run_id, "metadata.review_queue_pct", out["metadata.review_queue_pct"])

        # embeddings per (model_version, embedding_kind)
        cur.execute(
            """
            SELECT model_version, embedding_kind, COUNT(*) AS n,
                   AVG(vector_dims(vector))::float AS avg_dim,
                   100.0 * SUM(CASE WHEN l2_norm(vector) = 0 THEN 1 ELSE 0 END)::float
                       / NULLIF(COUNT(*), 0) AS zero_norm_pct
              FROM screens_embeddings
             WHERE run_id = %s
             GROUP BY model_version, embedding_kind
            """,
            (run_id,),
        )
        emb_rows = cur.fetchall()
        for mv, ek, n, avg_dim, zero_pct in emb_rows:
            _upsert(cur, run_id, "embeddings.row_count", n, model_version=mv, embedding_kind=ek)
            _upsert(cur, run_id, "embeddings.avg_vector_dim", avg_dim or 0.0,
                    model_version=mv, embedding_kind=ek)
            _upsert(cur, run_id, "embeddings.zero_norm_pct", zero_pct or 0.0,
                    model_version=mv, embedding_kind=ek)
            out[f"embeddings.row_count[{ek}/{mv}]"] = int(n)
            out[f"embeddings.avg_vector_dim[{ek}/{mv}]"] = float(avg_dim or 0.0)
            out[f"embeddings.zero_norm_pct[{ek}/{mv}]"] = float(zero_pct or 0.0)
        conn.commit()
    return out


def collect_pipeline_health_metrics(context: dict[str, Any], run_id: str) -> dict[str, float]:
    """Walk task instances of this DAG run; emit per-task duration / retries
    and total run duration. rows_in/rows_out come from task XCom return dict.
    """
    dag_run = context["dag_run"]
    dag = context.get("dag")
    if dag is None:
        return {}

    task_ids = [t.task_id for t in dag.tasks if t.task_id != "finalize"]
    health: dict[str, float] = {}

    with psycopg.connect(postgres_dsn()) as conn, conn.cursor() as cur:
        total_duration = 0.0
        for tid in task_ids:
            ti = dag_run.get_task_instance(tid)
            if ti is None:
                continue
            # duration: prefer Airflow's recorded duration; fall back to 0.
            duration = float(getattr(ti, "duration", None) or 0.0)
            retries = int(getattr(ti, "try_number", 1) or 1) - 1
            total_duration += duration

            _upsert(cur, run_id, "task.duration_seconds", duration, task_id=tid)
            _upsert(cur, run_id, "task.retries", retries, task_id=tid)
            health[f"task.duration_seconds[{tid}]"] = duration
            health[f"task.retries[{tid}]"] = retries

            # rows_in / rows_out from XCom return value if dict-shaped.
            try:
                xcom = ti.xcom_pull(task_ids=tid, key="return_value")
            except Exception:
                xcom = None
            if isinstance(xcom, dict):
                if "rows_in" in xcom:
                    _upsert(cur, run_id, "task.rows_in", xcom["rows_in"], task_id=tid)
                    health[f"task.rows_in[{tid}]"] = xcom["rows_in"]
                if "rows_out" in xcom:
                    _upsert(cur, run_id, "task.rows_out", xcom["rows_out"], task_id=tid)
                    health[f"task.rows_out[{tid}]"] = xcom["rows_out"]

        _upsert(cur, run_id, "run.duration_seconds", total_duration)
        health["run.duration_seconds"] = total_duration
        conn.commit()
    return health


def build_summary(run_id: str) -> str:
    """Single human-readable block. Read straight from pipeline_metrics."""
    with psycopg.connect(postgres_dsn()) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT metric_name, model_version, embedding_kind, task_id, metric_value
              FROM pipeline_metrics
             WHERE run_id = %s
             ORDER BY metric_name, task_id, embedding_kind, model_version
            """,
            (run_id,),
        )
        rows = cur.fetchall()
        cur.execute(
            "SELECT recall_at_5, n_queries FROM screens_eval ORDER BY id DESC LIMIT 1"
        )
        latest_eval = cur.fetchone()

    lines = ["", "=" * 64, f"RUN SUMMARY  run_id={run_id}", "=" * 64]
    for name, mv, ek, tid, val in rows:
        dim = ""
        bits = []
        if tid: bits.append(f"task={tid}")
        if ek:  bits.append(f"kind={ek}")
        if mv:  bits.append(f"model={mv}")
        if bits: dim = " (" + ", ".join(bits) + ")"
        lines.append(f"  {name}{dim}: {val:.3f}")
    if latest_eval:
        lines.append(f"  -> latest screens_eval.recall_at_5={latest_eval[0]:.3f} over n={latest_eval[1]}")
    lines.append("=" * 64)
    return "\n".join(lines)
