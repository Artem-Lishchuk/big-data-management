import json
import psycopg

from rico_pipeline.config import minio_bucket, postgres_dsn, s3_client
from rico_pipeline.utils import get_pipeline_run_id, list_screens_for_run


def parse_hierarchy(raw_json: str) -> list[tuple[str, str, tuple[int, int, int, int]]]:
    try:
        tree = json.loads(raw_json)
    except json.JSONDecodeError:
        return []

    root = tree.get("activity", {}).get("root", tree) if isinstance(tree, dict) else None

    elements: list[tuple[str, str, tuple[int, int, int, int]]] = []
    stack = [root]
    while stack:
        node = stack.pop()
        if not isinstance(node, dict):
            continue
        text = (node.get("text") or "").strip()
        cls = (node.get("class") or "").strip()
        if text or cls:
            element_type = cls.rsplit(".", 1)[-1] if cls else ""
            raw_bounds = node.get("bounds") or [0, 0, 0, 0]
            bounds = tuple(int(b) for b in raw_bounds) if len(raw_bounds) == 4 else (0, 0, 0, 0)
            elements.append((element_type, text, bounds))
        children = node.get("children")
        if isinstance(children, list):
            stack.extend(reversed(children))
    return elements

def text_representation(elements: list[tuple[str, str, tuple[int, int, int, int]]]) -> str:
    with_text = [e for e in elements if e[1]]
    in_order = sorted(with_text, key=lambda e: (e[2][1], e[2][0]))
    return " ".join(text for _, text, _ in in_order)

def run_parse(**context):
    run_id = get_pipeline_run_id(context)
    s3 = s3_client()
    bucket = minio_bucket()
    screens = list_screens_for_run(run_id)

    with psycopg.connect(postgres_dsn()) as conn, conn.cursor() as cur:
        for screen_id, hierarchy_json_path in screens:
            raw_json = (
                s3.get_object(Bucket=bucket, Key=hierarchy_json_path)["Body"]
                .read()
                .decode("utf-8")
            )
            text_rep = text_representation(parse_hierarchy(raw_json))

            cur.execute(
                """
                UPDATE screens_metadata
                SET text_representation = %s, updated_at = NOW()
                WHERE screen_id = %s AND run_id = %s
                """,
                (text_rep, screen_id, run_id),
            )
            preview = text_rep if len(text_rep) <= 80 else text_rep[:77] + "..."
            print(f"  screen {screen_id:>3}: {len(text_rep)} chars  {preview!r}")

        conn.commit()