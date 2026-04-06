"""Utility task for refreshing the daily materialized view after ETL loads."""

import logging
from prefect import task

from app.db.postgres import get_postgres_connection

logger = logging.getLogger(__name__)


@task(name="Refresh Daily Materialized View", retries=2)
def refresh_daily_view():
    """Refresh the daily materialized view used by analytics and top-movers queries.

    Tries concurrent refresh first to keep reads available, then falls back to a regular
    refresh when concurrent mode is not possible.
    """
    logger.info("Refreshing dwh.daily_price_fact materialized view...")

    conn = get_postgres_connection()
    try:
        # Concurrent refresh keeps the view readable while refresh is running.
        # PostgreSQL requires autocommit for REFRESH MATERIALIZED VIEW CONCURRENTLY.
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("REFRESH MATERIALIZED VIEW CONCURRENTLY dwh.daily_price_fact;")
        logger.info("Materialized view refreshed successfully.")
    except Exception as e:
        logger.error(f"Failed to refresh materialized view: {e}")
        try:
            # Fallback for first-run/lock/index edge cases where CONCURRENTLY is not possible.
            with conn.cursor() as cur:
                cur.execute("REFRESH MATERIALIZED VIEW dwh.daily_price_fact;")
        except:
            pass
    finally:
        conn.close()