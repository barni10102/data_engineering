from prefect import task, get_run_logger
from app.db.duckdb_client import get_duckdb_connection
from app.core import config


@task(retries=2, retry_delay_seconds=10)
def load_sp500_to_postgres(parquet_paths: dict):
    logger = get_run_logger()

    cleaned_parquet = parquet_paths["cleaned_data"]
    agg_parquet = parquet_paths["sector_stats"]

    logger.info("Starting DuckDB Load to Postgres...")

    conn = get_duckdb_connection()

    conn.execute("INSTALL postgres; LOAD postgres;")

    pg_conn_str = (
        f"host={config.POSTGRES_HOST} "
        f"port={config.POSTGRES_PORT} "
        f"dbname={config.POSTGRES_DB} "
        f"user={config.POSTGRES_USER} "
        f"password={config.POSTGRES_PASSWORD}"
    )

    conn.execute(f"ATTACH '{pg_conn_str}' AS pg (TYPE POSTGRES);")

    logger.info("Upserting into config.stock_universe...")

    conn.execute(f"""
            INSERT INTO pg.config.stock_universe (symbol, name, marketcap)
            SELECT symbol, shortname, marketcap 
            FROM read_parquet('{cleaned_parquet}')
            ON CONFLICT (symbol) DO UPDATE SET
                name = EXCLUDED.name,
                marketcap = EXCLUDED.marketcap;
        """)

    logger.info("Upserting into config.sector_stats...")

    conn.execute(f"""
            INSERT INTO pg.config.sector_stats (sector, company_count, avg_marketcap, total_weight, updated_at)
            SELECT 
                sector, 
                company_count, 
                avg_marketcap, 
                total_weight,
                CURRENT_TIMESTAMP
            FROM read_parquet('{agg_parquet}')
            ON CONFLICT (sector) DO UPDATE SET
                company_count = EXCLUDED.company_count,
                avg_marketcap = EXCLUDED.avg_marketcap,
                total_weight = EXCLUDED.total_weight,
                updated_at = CURRENT_TIMESTAMP;
        """)

    logger.info("Successfully loaded all Parquet data into PostgreSQL!")
    conn.close()