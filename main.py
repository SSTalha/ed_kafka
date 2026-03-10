import asyncio
import logging
from contextlib import asynccontextmanager

from db.mongodb import connect_db, disconnect_db
from db.redis import connect_redis, disconnect_redis
from services.bloom_filter import bloom
from consumer import consume

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan():
    # --- Startup ---
    logger.info("Starting up...")

    await connect_db()
    await connect_redis()
    await bloom.initialize()
    await bloom.check_and_repopulate_if_empty()

    logger.info("Startup complete")

    yield  # Consumer runs here

    # --- Shutdown ---
    logger.info("Shutting down...")
    await disconnect_redis()
    await disconnect_db()
    logger.info("Shutdown complete")


async def main() -> None:
    async with lifespan():
        try:
            await consume()
        except Exception as e:
            logger.error(f"Consumer crashed: {e}")
            raise


if __name__ == "__main__":
    asyncio.run(main())