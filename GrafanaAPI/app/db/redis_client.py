"""Redis client singleton used by API read paths and ETL cache updates."""

import redis
from app.core import config

redis_client = redis.Redis(
    host=config.REDIS_HOST,
    port=config.REDIS_PORT,
    password=config.REDIS_PASSWORD,
    # Keep string payloads as decoded text so json.loads works without manual decode.
    decode_responses=True,
)
