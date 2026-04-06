"""Idempotent bootstrap helpers for creating DWH schema objects."""

import logging
from app.db.postgres import get_postgres_connection

logger = logging.getLogger(__name__)

def ensure_dwh_schema_exists():
    """Create or validate core DWH schema objects required by load tasks.

    The function is idempotent and safe to run before each load cycle.
    """
    # Idempotent bootstrap: safe to call from every load task before writing facts.
    conn = get_postgres_connection()
    try:
        with conn.transaction():
            with conn.cursor() as cur:
                logger.info("Ensuring DWH schema, tables, and materialized views exist...")

                cur.execute("""
                    CREATE SCHEMA IF NOT EXISTS dwh;

                    CREATE TABLE IF NOT EXISTS dwh.asset_dim (
                        asset_id    serial PRIMARY KEY,
                        asset_type  text NOT NULL CHECK (asset_type IN ('crypto', 'stock')),
                        symbol      text NOT NULL,
                        name        text,
                        UNIQUE(asset_type, symbol)
                    );
                    
                    CREATE TABLE IF NOT EXISTS dwh.date_dim (
                        date_id     date PRIMARY KEY,
                        year        integer,
                        month       integer,
                        day         integer,
                        is_weekend  boolean
                    );

                    CREATE TABLE IF NOT EXISTS dwh.intraday_price_fact (
                        asset_id    int         NOT NULL REFERENCES dwh.asset_dim(asset_id),
                        date_id     date        NOT NULL REFERENCES dwh.date_dim(date_id),
                        snapshot_ts timestamptz NOT NULL,
                        close_price numeric,
                        volume      numeric,
                        volume_usd  numeric,
                        return_24h  numeric,
                        PRIMARY KEY (asset_id, snapshot_ts)
                    );
                    
                    CREATE MATERIALIZED VIEW IF NOT EXISTS dwh.daily_price_fact AS
                    WITH last_snap AS (
                        -- Keep one daily closing snapshot per asset (latest intraday point in UTC day).
                        SELECT DISTINCT ON (asset_id, (snapshot_ts AT TIME ZONE 'UTC')::date)
                            asset_id,
                            (snapshot_ts AT TIME ZONE 'UTC')::date AS price_date,
                            snapshot_ts,
                            close_price,
                            volume,
                            volume_usd,
                            return_24h
                        FROM dwh.intraday_price_fact
                        ORDER BY asset_id,
                                 (snapshot_ts AT TIME ZONE 'UTC')::date,
                                 snapshot_ts DESC
                    )
                    SELECT
                        asset_id,
                        price_date,
                        close_price,
                        volume,
                        volume_usd,
                        return_24h AS return_1d
                    FROM last_snap;
                    
                    CREATE UNIQUE INDEX IF NOT EXISTS idx_daily_price_unique 
                    ON dwh.daily_price_fact (price_date, asset_id);
                """)

                logger.info("DWH Schema setup completed.")
    except Exception as e:
        logger.error(f"Failed to setup DWH schema: {e}")
        raise
    finally:
        conn.close()