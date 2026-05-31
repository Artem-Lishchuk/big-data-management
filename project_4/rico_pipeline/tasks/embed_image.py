import time
from io import BytesIO

import psycopg
import torch
from PIL import Image
from pgvector.psycopg import register_vector

from rico_pipeline.config import (
    clip_client,
    clip_version,
    minio_bucket,
    postgres_dsn,
    s3_client,
)
from rico_pipeline.utils import get_pipeline_run_id, list_screens_for_run, sha256_hex


def run_embed_image(**context):
    t0 = time.monotonic()
    run_id = get_pipeline_run_id(context)
    screens = list_screens_for_run(run_id)
    sids = [sid for sid, _ in screens]

    if not sids:
        return {"rows_in": 0, "rows_out": 0, "duration_s": time.monotonic() - t0}

    s3 = s3_client()
    bucket = minio_bucket()
    model_version = clip_version()
    clip_model, clip_preprocess, _ = clip_client()
    clip_model.eval()

    fingerprints: list[str] = []
    batch = []
    for sid in sids:
        blob = s3.get_object(Bucket=bucket, Key=f"screens/{sid}.png")["Body"].read()
        fingerprints.append(sha256_hex(blob))
        img = Image.open(BytesIO(blob)).convert("RGB")
        batch.append(clip_preprocess(img))

    images_tensor = torch.stack(batch)
    device = next(clip_model.parameters()).device
    images_tensor = images_tensor.to(device)
    with torch.no_grad():
        image_vectors = clip_model.encode_image(images_tensor)
        image_vectors = image_vectors / image_vectors.norm(dim=-1, keepdim=True)
        image_vectors_np = image_vectors.cpu().numpy().astype("float32")
    assert image_vectors_np.shape[1] == 512

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
            for sid, vec, fp in zip(sids, image_vectors_np, fingerprints, strict=True):
                cur.execute(
                    INSERT_SQL,
                    (sid, run_id, "open-clip", model_version, "image", vec, fp),
                )
                rows_out += cur.rowcount
        conn.commit()

    return {
        "rows_in": len(sids),
        "rows_out": rows_out,
        "duration_s": time.monotonic() - t0,
    }
