"""MinIO client factory used by extract and transform tasks."""

from minio import Minio
from app.core import config


def get_minio_client():
    """Create a MinIO client configured for the local object-storage endpoint."""
    return Minio(
        endpoint=config.MINIO_ENDPOINT,
        access_key=config.MINIO_ROOT_USER,
        secret_key=config.MINIO_ROOT_PASSWORD,
        # Local MinIO is typically exposed over HTTP inside the compose network.
        secure=False
    )