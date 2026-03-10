import logging

from beanie import init_beanie
from motor.motor_asyncio import AsyncIOMotorClient

from config import settings
from models.user_traffic import UserTraffic

logger = logging.getLogger(__name__)

_mongo_client: AsyncIOMotorClient | None = None


async def connect_db() -> None:
    global _mongo_client
    try:
        _mongo_client = AsyncIOMotorClient(settings.MONGO_URI)
        await init_beanie(
            database=_mongo_client[settings.MONGO_DB_NAME],
            document_models=[UserTraffic]
        )
        logger.info(f"MongoDB connected — database: {settings.MONGO_DB_NAME}")
    except Exception as e:
        logger.error(f"MongoDB connection failed: {e}")
        raise


async def disconnect_db() -> None:
    global _mongo_client
    if _mongo_client:
        _mongo_client.close()
        logger.info("MongoDB disconnected")