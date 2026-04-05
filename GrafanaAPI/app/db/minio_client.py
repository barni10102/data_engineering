from minio import Minio
from app.core import config

def get_minio_client():
    return Minio(
        endpoint=config.MINIO_ENDPOINT,
        access_key=config.MINIO_ROOT_USER,
        secret_key=config.MINIO_ROOT_PASSWORD,
        secure=False
    )