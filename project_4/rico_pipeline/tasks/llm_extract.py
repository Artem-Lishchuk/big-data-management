import requests
import json
import psycopg
from rico_pipeline.utils import (
    get_pipeline_run_id
    , list_screens_for_run
    , get_text_representation_by_screen_id
)

from rico_pipeline.config import (
    prompt
    , ollama_url
    , ollama_model
    , min_confidence
    , postgres_dsn
    , prompt_version
)

def run_llm_extract(**context):
    run_id = get_pipeline_run_id(context)
    screens = list_screens_for_run(run_id)
    sids = [sid for sid, _ in screens]

    raw_prompt = prompt()
    OLLAMA_URL = ollama_url()
    OLLAMA_MODEL = ollama_model()
    conf_threshold = min_confidence()
    PROMPT_VERSION = f"v{prompt_version()}"

    UPDATE_EXTRACTION_SQL = """
    UPDATE screens_metadata
    SET extraction_payload = %s::jsonb,
        prompt_version     = %s,
        confidence         = %s,
        updated_at         = NOW()
    WHERE screen_id = %s
    """

    INSERT_REVIEW_SQL = """
    INSERT INTO screens_review_queue (run_id, screen_id, reason, raw_output)
    VALUES (%s, %s, %s, %s)
    """

    with psycopg.connect(postgres_dsn()) as conn:
        with conn.cursor() as cur:
            for sid in sids:
                text_representation = get_text_representation_by_screen_id(sid)
                filled_prompt = raw_prompt.replace("{hierarchy_text}", text_representation)
                raw_output = extract_one(filled_prompt, OLLAMA_URL, OLLAMA_MODEL)
                payload, reason = _parse_raw_payload(raw_output, conf_threshold)

                if reason:
                    cur.execute(INSERT_REVIEW_SQL, (run_id, sid, reason, raw_output))
                else:
                    body = {k: v for k, v in payload.items() if k != "confidence"}
                    confidence = float(payload.get("confidence", 0.0))
                    cur.execute(
                        UPDATE_EXTRACTION_SQL,
                        (json.dumps(body), PROMPT_VERSION, confidence, sid),
                    )
        conn.commit()

def extract_one(prompt, OLLAMA_URL, OLLAMA_MODEL):
    response = requests.post(
        f"{OLLAMA_URL}/api/generate",
        json={"model": OLLAMA_MODEL, "prompt": prompt, "stream": False},
        timeout=120,
    )
    response.raise_for_status()
    raw = response.json()["response"]
    return raw

def _parse_raw_payload(raw, conf_threshold):
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
        type, txt = el.get("type"), el.get("text")
        if isinstance(type, str) and isinstance(txt, str):
            normalized_elements.append({"type": type, "text": txt})

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