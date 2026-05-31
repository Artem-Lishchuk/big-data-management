"""
Finalize task — runs with trigger_rule='all_done', so it executes even if
upstream tasks failed (in particular when audit halts the pipeline).

Responsibilities:
  1. Compute the final run status from upstream task states.
  2. Collect data-quality + pipeline-health metrics into pipeline_metrics.
  3. Update pipeline_runs.status / ended_at.
  4. Log a human-readable end-of-run summary.
  5. Send the final Slack notification.
"""

from __future__ import annotations

import time

import psycopg

from rico_pipeline.config import postgres_dsn
from rico_pipeline.slack import notify_run_finished
from rico_pipeline.tasks.metrics import (
    build_summary,
    collect_data_quality_metrics,
    collect_pipeline_health_metrics,
)
from rico_pipeline.utils import get_pipeline_run_id


def finalize_run(**context):
    t0 = time.monotonic()
    run_id = get_pipeline_run_id(context)
    dag_run = context["dag_run"]
    dag = context.get("dag")

    # ---- determine final status -----------------------------------------
    status = "success"
    if dag is not None:
        for t in dag.tasks:
            if t.task_id == "finalize":
                continue
            ti = dag_run.get_task_instance(t.task_id)
            if ti is None:
                continue
            state = ti.state
            if state in ("failed", "upstream_failed"):
                status = "failed"
                break
            if state == "skipped":
                # Skipped is fine *unless* it's a critical task; if eval is
                # skipped because audit failed, the audit failure already set
                # status='failed' above. Treat lone skips as success.
                continue

    # ---- collect metrics (best-effort) ----------------------------------
    try:
        dq = collect_data_quality_metrics(run_id)
    except Exception as exc:
        print(f"[finalize] data-quality metric collection failed: {exc!r}")
        dq = {}
    try:
        health = collect_pipeline_health_metrics(context, run_id)
    except Exception as exc:
        print(f"[finalize] health metric collection failed: {exc!r}")
        health = {}

    # ---- update pipeline_runs -------------------------------------------
    with psycopg.connect(postgres_dsn()) as conn, conn.cursor() as cur:
        cur.execute(
            """
            UPDATE pipeline_runs
               SET status = %s, ended_at = NOW()
             WHERE run_id = %s
            """,
            (status, run_id),
        )
        cur.execute(
            "SELECT EXTRACT(EPOCH FROM (ended_at - started_at)) FROM pipeline_runs WHERE run_id = %s",
            (run_id,),
        )
        duration_s = float(cur.fetchone()[0] or 0.0)
        conn.commit()

    # ---- summary + Slack -------------------------------------------------
    summary = build_summary(run_id)
    print(summary)

    one_line = (
        f"status={status} "
        f"rows={int(dq.get('metadata.row_count', 0))} "
        f"conf>=0.5%={dq.get('metadata.confidence_gte_0_5_pct', 0):.1f} "
        f"review%={dq.get('metadata.review_queue_pct', 0):.1f}"
    )
    notify_run_finished(run_id, status, duration_s, one_line)

    print(f"[finalize] done in {time.monotonic() - t0:.2f}s")
    return {"status": status, "duration_s": duration_s}
