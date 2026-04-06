"""Load S&P500 transformed reference datasets into current and historical tables."""

import pandas as pd
from datetime import datetime, timezone

from prefect import task, get_run_logger
from app.db.postgres import get_postgres_connection


@task(retries=2, retry_delay_seconds=10)
def load_sp500_to_postgres(data: tuple[pd.DataFrame, pd.DataFrame] | None):
    """Load S&P500 reference data into current tables and SCD Type 2 history."""
    logger = get_run_logger()

    if data is None:
        logger.warning("No data received from transform step. Skipping load.")
        return

    universe_df, sector_stats_df = data
    logger.info("Starting Pandas Load to Postgres...")

    required_universe_cols = {"symbol", "name", "sector", "marketcap", "weight"}
    missing_universe_cols = required_universe_cols - set(universe_df.columns)
    if missing_universe_cols:
        raise ValueError(f"Missing required columns in universe_df: {sorted(missing_universe_cols)}")

    universe_df = universe_df.copy()
    universe_df = universe_df.dropna(subset=["symbol"])
    universe_df["symbol"] = universe_df["symbol"].astype(str).str.strip().str.upper()
    universe_df = universe_df[universe_df["symbol"] != ""]

    universe_values = [
        tuple(x)
        for x in universe_df[["symbol", "name", "sector", "marketcap", "weight"]].to_numpy()
    ]
    stats_values = [tuple(x) for x in sector_stats_df.to_numpy()]

    conn = get_postgres_connection()

    try:
        with conn.transaction():
            with conn.cursor() as cur:
                logger.info("Ensuring schema and tables exist...")

                cur.execute(
                    """
                    CREATE SCHEMA IF NOT EXISTS config;

                    CREATE TABLE IF NOT EXISTS config.stock_universe (
                        symbol      text PRIMARY KEY,
                        name        text,
                        sector      text,
                        marketcap   numeric,
                        weight      numeric
                    );

                    CREATE TABLE IF NOT EXISTS config.stock_universe_hist (
                        -- SCD2 history table keeps full attribute change timeline per symbol.
                        company_hist_id  bigserial PRIMARY KEY,
                        symbol           text NOT NULL,
                        name             text,
                        sector           text,
                        marketcap        numeric,
                        weight           numeric,
                        valid_from       timestamptz NOT NULL,
                        valid_to         timestamptz NOT NULL DEFAULT '9999-12-31 00:00:00+00',
                        is_current       boolean NOT NULL DEFAULT true,
                        updated_at       timestamptz NOT NULL DEFAULT now()
                    );

                    CREATE TABLE IF NOT EXISTS config.sector_stats (
                        sector          text PRIMARY KEY,
                        company_count   integer,
                        avg_marketcap   numeric,
                        total_weight    numeric,
                        updated_at      timestamp DEFAULT now()
                    );
                    """
                )

                cur.execute(
                    """
                    ALTER TABLE config.stock_universe
                        ADD COLUMN IF NOT EXISTS sector text,
                        ADD COLUMN IF NOT EXISTS weight numeric;
                    """
                )

                cur.execute(
                    """
                    -- Enforce only one active SCD2 row per symbol.
                    CREATE UNIQUE INDEX IF NOT EXISTS uq_stock_universe_hist_current
                    ON config.stock_universe_hist(symbol)
                    WHERE is_current;

                    CREATE INDEX IF NOT EXISTS ix_stock_universe_hist_symbol_valid_from
                    ON config.stock_universe_hist(symbol, valid_from DESC);
                    """
                )

                logger.info("Upserting data into config.stock_universe (current snapshot)...")
                cur.executemany(
                    """
                    INSERT INTO config.stock_universe (symbol, name, sector, marketcap, weight)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (symbol) DO UPDATE SET
                        name      = EXCLUDED.name,
                        sector    = EXCLUDED.sector,
                        marketcap = EXCLUDED.marketcap,
                        weight    = EXCLUDED.weight;
                    """,
                    universe_values,
                )

                logger.info("Applying SCD Type 2 into config.stock_universe_hist...")

                cur.execute(
                    """
                    CREATE TEMP TABLE tmp_stock_universe (
                        symbol      text,
                        name        text,
                        sector      text,
                        marketcap   numeric,
                        weight      numeric
                    ) ON COMMIT DROP;
                    """
                )

                if universe_values:
                    cur.executemany(
                        """
                        INSERT INTO tmp_stock_universe (symbol, name, sector, marketcap, weight)
                        VALUES (%s, %s, %s, %s, %s);
                        """,
                        universe_values,
                    )

                now_ts = datetime.now(timezone.utc)

                cur.execute(
                    """
                    -- Step 1: close currently active rows when any tracked attribute changed.
                    UPDATE config.stock_universe_hist h
                    SET valid_to = %s,
                        is_current = false,
                        updated_at = now()
                    FROM tmp_stock_universe s
                    WHERE h.symbol = s.symbol
                      AND h.is_current = true
                      AND (
                        h.name IS DISTINCT FROM s.name OR
                        h.sector IS DISTINCT FROM s.sector OR
                        h.marketcap IS DISTINCT FROM s.marketcap OR
                        h.weight IS DISTINCT FROM s.weight
                      );
                    """,
                    (now_ts,),
                )

                cur.execute(
                    """
                    -- Step 2: insert new active version for new or changed symbols.
                    INSERT INTO config.stock_universe_hist (
                        symbol, name, sector, marketcap, weight, valid_from, valid_to, is_current, updated_at
                    )
                    SELECT
                        s.symbol, s.name, s.sector, s.marketcap, s.weight,
                        %s, '9999-12-31 00:00:00+00', true, now()
                    FROM tmp_stock_universe s
                    LEFT JOIN config.stock_universe_hist h
                      ON h.symbol = s.symbol
                     AND h.is_current = true
                    WHERE h.symbol IS NULL
                       OR h.name IS DISTINCT FROM s.name
                       OR h.sector IS DISTINCT FROM s.sector
                       OR h.marketcap IS DISTINCT FROM s.marketcap
                       OR h.weight IS DISTINCT FROM s.weight;
                    """,
                    (now_ts,),
                )

                cur.execute(
                    """
                    -- Step 3: close symbols that disappeared from the latest snapshot.
                    UPDATE config.stock_universe_hist h
                    SET valid_to = %s,
                        is_current = false,
                        updated_at = now()
                    WHERE h.is_current = true
                      AND NOT EXISTS (
                          SELECT 1
                          FROM tmp_stock_universe s
                          WHERE s.symbol = h.symbol
                      );
                    """,
                    (now_ts,),
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
                    stats_values,
                )

        logger.info("Successfully loaded all Pandas data into PostgreSQL (current + SCD2 history).")
    except Exception as e:
        logger.error(f"Database load failed: {e}")
        raise
    finally:
        conn.close()