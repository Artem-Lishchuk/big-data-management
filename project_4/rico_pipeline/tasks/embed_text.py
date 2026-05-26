import psycopg
from pgvector.psycopg import register_vector
from rico_pipeline.config import (
    sbert_client,
    sbert_version,
    postgres_dsn,
)
from rico_pipeline.utils import get_pipeline_run_id, list_screens_for_run, get_text_representation_by_screen_id

def run_embed_text(**context):
    run_id = get_pipeline_run_id(context)
    screens = list_screens_for_run(run_id)
    sids = [sid for sid, _ in screens]

    model_version = sbert_version()
    sbert = sbert_client()

    corpus = [get_text_representation_by_screen_id(sid) for sid in sids]
    text_vectors_np = sbert.encode(corpus, normalize_embeddings=True).astype("float32")
    assert text_vectors_np.shape[1] == 384

    INSERT_SQL = """
    INSERT INTO screens_embeddings
    (screen_id, run_id, model_name, model_version, embedding_kind, vector)
    VALUES (%s, %s, %s, %s, %s, %s)
    ON CONFLICT (screen_id, model_name, model_version, embedding_kind)
    DO UPDATE SET
        run_id = EXCLUDED.run_id,
        vector = EXCLUDED.vector
    """

    with psycopg.connect(postgres_dsn()) as conn:
        register_vector(conn)
        with conn.cursor() as cur:
            rows = [
                (sid, run_id, "sentence-transformers", model_version, "text", vec)
                for sid, vec in zip(sids, text_vectors_np, strict=True)
            ]
            cur.executemany(INSERT_SQL, rows)
        conn.commit()