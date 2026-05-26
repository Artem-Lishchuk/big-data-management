import os
import boto3
import open_clip

def _env(name: str, default: str) -> str:
    return os.environ.get(name, default)

def postgres_dsn() -> str:
    return _env("POSTGRES_DSN", "postgresql://rico:rico@localhost:5432/rico")

def minio_endpoint() -> str:
    return _env("MINIO_ENDPOINT", _env("MINIO_URL", "http://localhost:9000"))

def minio_access_key() -> str:
    return _env("MINIO_ACCESS_KEY", _env("MINIO_KEY", "minioadmin"))

def minio_secret_key() -> str:
    return _env("MINIO_SECRET_KEY", _env("MINIO_SECRET", "minioadmin"))

def minio_bucket() -> str:
    return _env("MINIO_BUCKET", "rico-raw")

def ollama_url() -> str:
    return _env("OLLAMA_URL", "http://localhost:11434")

def ollama_model() -> str:
    return _env("OLLAMA_MODEL", "qwen2.5:3b")

def clip_version() -> str:
    return _env("CLIP_VERSION", "open-clip-ViT-B-32-laion2b-s34b-b79k")

def sbert_version() -> str:
    return _env("SBERT_VERSION", "sentence-transformers/all-MiniLM-L6-v2")

def git_sha() -> str:
    return _env("GIT_SHA", "unknown")

def s3_client():
    return boto3.client(
        "s3",
        endpoint_url=minio_endpoint(),
        aws_access_key_id=minio_access_key(),
        aws_secret_access_key=minio_secret_key(),
    )

def clip_client():
    clip_model, _, clip_preprocess = open_clip.create_model_and_transforms(
        _env("CLIP_ARCH", _env("CLIP_ARCH", "ViT-B-32"))
        , pretrained=_env("CLIP_PRETRAINED", _env("CLIP_PRETRAINED", "laion2b_s34b_b79k"))
    )
    clip_tokenizer = open_clip.get_tokenizer( _env("CLIP_ARCH", _env("CLIP_ARCH", "ViT-B-32")))
    return clip_model, clip_preprocess, clip_tokenizer