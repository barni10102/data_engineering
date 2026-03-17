import psycopg
from psycopg.rows import dict_row

from app.core import config


def get_postgres_connection():
    conn = psycopg.connect(
        host=config.POSTGRES_HOST,
        port=config.POSTGRES_PORT,
        dbname=config.POSTGRES_DB,
        user=config.POSTGRES_USER,
        password=config.POSTGRES_PASSWORD,
        row_factory=dict_row,
    )
    return conn