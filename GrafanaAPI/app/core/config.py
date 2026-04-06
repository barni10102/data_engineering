"""Application configuration loaded from environment variables.

Defaults are chosen for local Docker-based development; production values
should be provided explicitly via environment variables.
"""

import os
from dotenv import load_dotenv

load_dotenv(override=False)

# Redis cache connection settings.
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD")

# PostgreSQL DWH connection settings.
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")

# MinIO object storage settings used by raw ETL landing.
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ROOT_USER = os.getenv("MINIO_ROOT_USER")
MINIO_ROOT_PASSWORD = os.getenv("MINIO_ROOT_PASSWORD")
MINIO_RAW_BUCKET = os.getenv("MINIO_RAW_BUCKET")

# Token for external Kaggle-based data pulls.
KAGGLE_API_TOKEN = os.getenv("KAGGLE_API_TOKEN")
