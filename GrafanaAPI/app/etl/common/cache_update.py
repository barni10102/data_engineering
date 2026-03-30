import json

from prefect import task, get_run_logger
from psycopg.rows import dict_row
from app.db.redis_client import redis_client
from app.db.postgres import get_postgres_connection

CACHE_TTL_SECONDS = 360  # 6 minutes

@task(retries=2, retry_delay_seconds=10)
def update_top_movers_cache(asset_type: str):
    logger = get_run_logger()

    allowed_types = ['crypto', 'stock']
    if asset_type not in allowed_types:
        error_msg = f"Invalid asset_type '{asset_type}'. Must be one of {allowed_types}."
        logger.error(error_msg)
        raise ValueError(error_msg)

    logger.info(f"Updating Top Movers cache for '{asset_type}' in Redis...")

    queries = {
        "crypto": {
            "key": "asset:top_movers:crypto",
            "sql": """
                WITH last_date AS (
                    SELECT MAX(f.price_date) AS d
                    FROM dwh.daily_price_fact f
                    JOIN dwh.asset_dim ad ON ad.asset_id = f.asset_id
                    WHERE ad.asset_type = 'crypto'
                )
                SELECT ad.asset_type, ad.symbol, ad.name, f.return_1d, f.volume_usd
                FROM dwh.daily_price_fact f
                JOIN dwh.asset_dim ad ON ad.asset_id = f.asset_id
                CROSS JOIN last_date ld
                WHERE f.price_date = ld.d
                  AND ad.asset_type = 'crypto'
                  AND f.return_1d IS NOT NULL
                ORDER BY f.return_1d DESC
                LIMIT 10;
            """,
        },
        "stock": {
            "key": "asset:top_movers:stock",
            "sql": """
                WITH last_date AS (
                    SELECT MAX(f.price_date) AS d
                    FROM dwh.daily_price_fact f
                    JOIN dwh.asset_dim ad ON ad.asset_id = f.asset_id
                    WHERE ad.asset_type = 'stock'
                )
                SELECT ad.asset_type, ad.symbol, ad.name, f.return_1d, f.volume_usd
                FROM dwh.daily_price_fact f
                JOIN dwh.asset_dim ad ON ad.asset_id = f.asset_id
                CROSS JOIN last_date ld
                WHERE f.price_date = ld.d
                  AND ad.asset_type = 'stock'
                  AND f.return_1d IS NOT NULL
                ORDER BY f.return_1d DESC
                LIMIT 10;
            """,
        },
        "all": {
            "key": "asset:top_movers:all",
            "sql": """
                WITH last_dates AS (
                    SELECT ad.asset_type, MAX(f.price_date) AS d
                    FROM dwh.daily_price_fact f
                    JOIN dwh.asset_dim ad ON ad.asset_id = f.asset_id
                    WHERE ad.asset_type IN ('crypto', 'stock')
                    GROUP BY ad.asset_type
                )
                SELECT ad.asset_type, ad.symbol, ad.name, f.return_1d, f.volume_usd
                FROM dwh.daily_price_fact f
                JOIN dwh.asset_dim ad ON ad.asset_id = f.asset_id
                JOIN last_dates ld
                  ON ld.asset_type = ad.asset_type
                 AND ld.d = f.price_date
                WHERE ad.asset_type IN ('crypto', 'stock')
                  AND f.return_1d IS NOT NULL
                ORDER BY f.return_1d DESC
                LIMIT 20;
            """,
        },
    }

    tasks_to_run = [asset_type, "all"]

    conn = get_postgres_connection()
    try:
        with conn.cursor(row_factory=dict_row) as cur:
            for category in tasks_to_run:
                data = queries[category]
                logger.info(f"Calculating top movers for: {category}...")

                cur.execute(data["sql"])
                results = cur.fetchall()

                if results:
                    redis_json = json.dumps(results, default=float, ensure_ascii=False)
                    if category == "stock":
                        redis_client.set(data["key"], redis_json)
                    else:
                        redis_client.set(data["key"], redis_json, ex=CACHE_TTL_SECONDS)
                    logger.info(f"Successfully cached {len(results)} items to Redis key: {data['key']}")
                else:
                    logger.warning(f"No results found for {category} top movers.")
    except Exception as e:
        logger.error(f"Failed to calculate and cache top movers: {e}")
        raise
    finally:
        conn.close()