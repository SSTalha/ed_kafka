import logging

import redis.asyncio as aioredis

from config import settings

logger = logging.getLogger(__name__)

_redis_client: aioredis.Redis | None = None


async def connect_redis() -> aioredis.Redis:
    global _redis_client
    try:
        _redis_client = await aioredis.from_url(
            settings.REDIS_URL,
            socket_connect_timeout=5,
            socket_timeout=5,
        )
        await _redis_client.ping()
        logger.info("Redis connected")
        return _redis_client
    except Exception as e:
        logger.error(f"Redis connection failed: {e}")
        raise


async def disconnect_redis() -> None:
    global _redis_client
    if _redis_client:
        await _redis_client.aclose()
        logger.info("Redis disconnected")


def get_redis() -> aioredis.Redis:
    if _redis_client is None:
        raise RuntimeError("Redis not initialized. Call connect_redis() first.")
    return _redis_client