import psycopg2
from psycopg2.extras import RealDictCursor

from app.core import config


def get_connection():
    conn = psycopg2.connect(
        host=config.POSTGRES_HOST,
        port=config.POSTGRES_PORT,
        dbname=config.POSTGRES_DB,
        user=config.POSTGRES_USER,
        password=config.POSTGRES_PASSWORD,
        cursor_factory=RealDictCursor,
    )
    return conn
