import duckdb

from app.core import config

def get_duckdb_connection():
    conn = duckdb.connect(database=':memory:')

    conn.execute("INSTALL httpfs; LOAD httpfs;")
    conn.execute(f"""
        SET s3_endpoint='minio:9000';
        SET s3_access_key_id='{config.MINIO_ROOT_USER}';
        SET s3_secret_access_key='{config.MINIO_ROOT_PASSWORD}';
        SET s3_use_ssl=false;
        SET s3_url_style='path';
    """)
    return conn