import json
import time

import psycopg
import requests

from rico_pipeline.config import (
    min_confidence,
    ollama_model,
    ollama_url,
    postgres_dsn,
    prompt,
    prompt_version,
)
from rico_pipeline.utils import (
    get_pipeline_run_id,
    get_text_representations_for_run,
)


UPDATE_EXTRACTION_SQL = """
    UPDATE screens_metadata
       SET extraction_payload = %s::jsonb,
           prompt_version     = %s,
           confidence         = %s,
           updated_at         = NOW()
     WHERE screen_id = %s AND run_id = %s
"""

INSERT_REVIEW_SQL = """
    INSERT INTO screens_review_queue (run_id, screen_id, reason, raw_output)
    VALUES (%s, %s, %s, %s)
"""


def run_llm_extract(**context):
    t0 = time.monotonic()
    run_id = get_pipeline_run_id(context)
    texts_by_sid = get_text_representations_for_run(run_id)
    sids = sorted(texts_by_sid.keys())

    raw_prompt = prompt()
    url = ollama_url()
    model = ollama_model()
    conf_threshold = min_confidence()
    pv = f"v{prompt_version()}"

    rows_out = 0
    reviewed = 0
    with psycopg.connect(postgres_dsn()) as conn, conn.cursor() as cur:
        for sid in sids:
            filled = raw_prompt.replace("{hierarchy_text}", texts_by_sid[sid])
            raw_output = extract_one(filled, url, model)
            payload, reason = _parse_raw_payload(raw_output, conf_threshold)

            if reason:
                cur.execute(INSERT_REVIEW_SQL, (run_id, sid, reason, raw_output))
                reviewed += 1
            else:
                body = {k: v for k, v in payload.items() if k != "confidence"}
                confidence = float(payload.get("confidence", 0.0))
                cur.execute(
                    UPDATE_EXTRACTION_SQL,
                    (json.dumps(body), pv, confidence, sid, run_id),
                )
                rows_out += 1
        conn.commit()

    return {
        "rows_in": len(sids),
        "rows_out": rows_out,
        "reviewed": reviewed,
        "duration_s": time.monotonic() - t0,
    }


def extract_one(filled_prompt: str, url: str, model: str) -> str:
    response = requests.post(
        f"{url}/api/generate",
        json={"model": model, "prompt": filled_prompt, "stream": False},
        timeout=120,
    )
    response.raise_for_status()
    return response.json()["response"]


def _parse_raw_payload(raw: str, conf_threshold: float):
    text = raw.strip()

    if text.startswith("```"):
        text = text.removeprefix("```json").removeprefix("```").removesuffix("```").strip()

    try:
        data = json.loads(text)
    except Exception:
        return None, "invalid_json"

    if not isinstance(data, dict):
        return None, "invalid_schema"

    title = data.get("title", "")
    if not isinstance(title, str):
        return None, "invalid_schema"

    elements = data.get("elements", [])
    if not isinstance(elements, list):
        return None, "invalid_schema"

    normalized_elements = []
    for el in elements:
        if not isinstance(el, dict):
            continue
        et, txt = el.get("type"), el.get("text")
        if isinstance(et, str) and isinstance(txt, str):
            normalized_elements.append({"type": et, "text": txt})

    try:
        confidence = float(data.get("confidence", 0.0))
    except Exception:
        return None, "invalid_schema"

    if not (0.0 <= confidence <= 1.0):
        return None, "invalid_schema"

    payload = {
        "title": title,
        "elements": normalized_elements,
        "confidence": confidence,
    }

    if confidence < conf_threshold:
        return None, "low_confidence"

    return payload, None
