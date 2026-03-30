import logging
from prefect import task

from app.db.postgres import get_postgres_connection

logger = logging.getLogger(__name__)


@task(name="Refresh Daily Materialized View", retries=2)
def refresh_daily_view():
    logger.info("Refreshing dwh.daily_price_fact materialized view...")

    conn = get_postgres_connection()
    try:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("REFRESH MATERIALIZED VIEW CONCURRENTLY dwh.daily_price_fact;")
        logger.info("Materialized view refreshed successfully.")
    except Exception as e:
        logger.error(f"Failed to refresh materialized view: {e}")
        try:
            with conn.cursor() as cur:
                cur.execute("REFRESH MATERIALIZED VIEW dwh.daily_price_fact;")
        except:
            pass
    finally:
        conn.close()