import time

import psycopg
from pgvector.psycopg import register_vector

from rico_pipeline.config import postgres_dsn, sbert_client, sbert_version
from rico_pipeline.utils import (
    get_pipeline_run_id,
    get_text_representations_for_run,
    sha256_hex,
)


def run_embed_text(**context):
    t0 = time.monotonic()
    run_id = get_pipeline_run_id(context)
    texts_by_sid = get_text_representations_for_run(run_id)
    sids = sorted(texts_by_sid.keys())
    corpus = [texts_by_sid[sid] for sid in sids]
    fingerprints = [sha256_hex(t) for t in corpus]

    if not sids:
        return {"rows_in": 0, "rows_out": 0, "duration_s": time.monotonic() - t0}

    model_version = sbert_version()
    sbert = sbert_client()
    text_vectors_np = sbert.encode(corpus, normalize_embeddings=True).astype("float32")
    assert text_vectors_np.shape[1] == 384

    INSERT_SQL = """
        INSERT INTO screens_embeddings
            (screen_id, run_id, model_name, model_version,
             embedding_kind, vector, source_fingerprint)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (screen_id, model_name, model_version, embedding_kind)
        DO NOTHING
    """

    rows_out = 0
    with psycopg.connect(postgres_dsn()) as conn:
        register_vector(conn)
        with conn.cursor() as cur:
            for sid, vec, fp in zip(sids, text_vectors_np, fingerprints, strict=True):
                cur.execute(
                    INSERT_SQL,
                    (sid, run_id, "sentence-transformers",
                     model_version, "text", vec, fp),
                )
                rows_out += cur.rowcount
        conn.commit()

    return {
        "rows_in": len(sids),
        "rows_out": rows_out,
        "duration_s": time.monotonic() - t0,
    }
