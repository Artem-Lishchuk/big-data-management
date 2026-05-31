"""
Slack notification helper.

Behaviour:
  * If $SLACK_WEBHOOK_URL is set to a non-empty value, POST a JSON payload
    to it (Slack-compatible "text" field).
  * Otherwise, write the message as a line to logs/slack/<YYYY-MM-DD>.log
    inside the project root. The folder is created on first use.

Either way, the message is also printed so it shows in the Airflow task log.
Every call is wrapped in try/except — Slack failure must never fail the run.
"""

from __future__ import annotations

import json
import os
from datetime import datetime
from pathlib import Path
from typing import Any

import requests

# Project root = parent of this file's parent (rico_pipeline/).
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
_LOG_DIR = _PROJECT_ROOT / "logs" / "slack"


def _format(kind: str, text: str, fields: dict[str, Any]) -> str:
    ts = datetime.utcnow().isoformat(timespec="seconds") + "Z"
    field_str = " ".join(f"{k}={v}" for k, v in fields.items()) if fields else ""
    return f"[{ts}] [{kind}] {text} {field_str}".rstrip()


def _write_to_log(line: str) -> None:
    _LOG_DIR.mkdir(parents=True, exist_ok=True)
    fname = datetime.utcnow().strftime("%Y-%m-%d") + ".log"
    with (_LOG_DIR / fname).open("a", encoding="utf-8") as f:
        f.write(line + "\n")


def notify(kind: str, text: str, **fields: Any) -> None:
    """Send a Slack notification (or mock to a log file).

    kind: short tag e.g. "run_started", "audit_failed", "run_finished".
    text: human-readable headline.
    fields: extra structured context appended to the message.
    """
    line = _format(kind, text, fields)
    try:
        print(f"[slack] {line}", flush=True)
    except Exception:
        pass

    webhook = os.environ.get("SLACK_WEBHOOK_URL", "").strip()
    try:
        if webhook:
            payload = {"text": line, "kind": kind, **fields}
            requests.post(webhook, json=payload, timeout=5)
        else:
            _write_to_log(line)
    except Exception as exc:  # noqa: BLE001
        # Never propagate Slack failures.
        try:
            _write_to_log(f"[slack-error] {exc!r} :: {line}")
        except Exception:
            pass


def notify_run_started(run_id: str, limit: int | None, trigger: str) -> None:
    notify("run_started", "Pipeline run started",
           run_id=run_id, limit=limit, trigger=trigger)


def notify_audit_failed(run_id: str, duplicates: list[dict], task_log_url: str = "") -> None:
    notify(
        "audit_failed",
        "Audit failed: duplicate keys detected",
        run_id=run_id,
        duplicate_count=len(duplicates),
        duplicates=json.dumps(duplicates)[:500],
        task_log_url=task_log_url or "n/a",
    )


def notify_run_finished(run_id: str, status: str, duration_s: float, summary: str) -> None:
    notify(
        "run_finished",
        f"Pipeline run {status}",
        run_id=run_id,
        duration_s=round(duration_s, 2),
        summary=summary,
    )
