"""PostgreSQL connection helpers shared across API and ETL modules."""

import psycopg
from psycopg.rows import dict_row

from app.core import config


def get_postgres_connection():
    """Create a new PostgreSQL connection with dict-style row access."""
    conn = psycopg.connect(
        host=config.POSTGRES_HOST,
        port=config.POSTGRES_PORT,
        dbname=config.POSTGRES_DB,
        user=config.POSTGRES_USER,
        password=config.POSTGRES_PASSWORD,
        # Return rows as mapping objects so callers can use named columns safely.
        row_factory=dict_row,
    )
    return conn