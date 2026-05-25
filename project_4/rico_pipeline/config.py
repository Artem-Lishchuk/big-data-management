import os
import boto3


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


def s3_client():
    return boto3.client(
        "s3",
        endpoint_url=minio_endpoint(),
        aws_access_key_id=minio_access_key(),
        aws_secret_access_key=minio_secret_key(),
    )
