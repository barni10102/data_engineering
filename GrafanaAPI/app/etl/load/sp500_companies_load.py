import pandas as pd

from prefect import task, get_run_logger
from app.db.postgres import get_postgres_connection


@task(retries=2, retry_delay_seconds=10)
def load_sp500_to_postgres(data: tuple[pd.DataFrame, pd.DataFrame] | None):
    logger = get_run_logger()

    if data is None:
        logger.warning("No data received from transform step. Skipping load.")
        return

    universe_df, sector_stats_df = data
    logger.info("Starting Pandas Load to Postgres...")

    universe_values = [tuple(x) for x in universe_df.to_numpy()]
    stats_values = [tuple(x) for x in sector_stats_df.to_numpy()]

    conn = get_postgres_connection()

    try:
        with conn.transaction():
            with conn.cursor() as cur:
                logger.info("Ensuring schema and tables exist...")

                cur.execute("""
                        CREATE SCHEMA IF NOT EXISTS config;

                        CREATE TABLE IF NOT EXISTS config.stock_universe (
                            symbol      text PRIMARY KEY,
                            name        text,
                            marketcap   numeric
                        );
                        
                        CREATE TABLE IF NOT EXISTS config.sector_stats (
                            sector          text PRIMARY KEY,
                            company_count   integer,
                            avg_marketcap   numeric,
                            total_weight    numeric,
                            updated_at      timestamp DEFAULT now()
                        );
                    """)

                logger.info("Upserting data into config.stock_universe...")
                cur.executemany(
                    """
                    INSERT INTO config.stock_universe (symbol, name, marketcap)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (symbol) DO UPDATE SET
                        name      = EXCLUDED.name,
                        marketcap = EXCLUDED.marketcap;
                    """,
                    universe_values
                )

                logger.info("Upserting data into config.sector_stats...")
                cur.executemany(
                    """
                    INSERT INTO config.sector_stats (sector, company_count, avg_marketcap, total_weight)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (sector) DO UPDATE SET
                        company_count = EXCLUDED.company_count,
                        avg_marketcap = EXCLUDED.avg_marketcap,
                        total_weight  = EXCLUDED.total_weight,
                        updated_at    = now();
                    """,
                    stats_values
                )

        logger.info("Successfully loaded all Pandas data into PostgreSQL!")
    except Exception as e:
        logger.error(f"Database load failed: {e}")
        raise
    finally:
        conn.close()
