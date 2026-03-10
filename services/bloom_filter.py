import json
import logging

from config import settings
from db.redis import get_redis

logger = logging.getLogger(__name__)


class BloomFilterService:

    @property
    def _redis(self):
        return get_redis()

    async def initialize(self) -> None:
        """
        Create the bloom filter if it doesn't exist.
        Called once on startup after Redis is connected.
        """
        try:
            await self._redis.execute_command(
                "BF.RESERVE",
                settings.BLOOM_FILTER_KEY,
                settings.BLOOM_FILTER_ERROR_RATE,
                settings.BLOOM_FILTER_CAPACITY,
            )
            logger.info(
                f"Bloom filter created — "
                f"capacity: {settings.BLOOM_FILTER_CAPACITY:,}, "
                f"error rate: {settings.BLOOM_FILTER_ERROR_RATE}"
            )
        except Exception:
            # BF.RESERVE throws if key already exists — that's fine
            logger.info("Bloom filter already exists, skipping creation")


    async def repopulate_from_db(self) -> None:
        """
        Called on startup when the bloom filter is empty (e.g. after Redis cleared).
        Streams all known IPs from MongoDB in chunks to avoid loading millions into memory.
        """
        logger.info("Bloom filter is empty — repopulating from DB...")

        processed = 0
        chunk_size = 10_000

        async for batch in self._stream_ips_from_db(chunk_size):
            if batch:
                await self._redis.execute_command(
                    "BF.MADD", settings.BLOOM_FILTER_KEY, *batch
                )
                processed += len(batch)
                logger.info(f"Repopulated {processed:,} IPs so far...")

        logger.info(f"Bloom filter repopulation complete — total: {processed:,} IPs")


    async def _stream_ips_from_db(self, chunk_size: int):
        """Yield batches of unique IPs from MongoDB."""
        from models.user_traffic import UserTraffic

        skip = 0
        while True:
            batch = await UserTraffic.find(
                projection={"ip_address": 1, "_id": 0}
            ).skip(skip).limit(chunk_size).to_list()

            if not batch:
                break

            yield [doc.ip_address for doc in batch]
            skip += chunk_size


    async def check_and_repopulate_if_empty(self) -> None:
        """Check if bloom filter is empty and repopulate from DB if so."""
        try:
            info = await self._redis.execute_command(
                "BF.INFO", settings.BLOOM_FILTER_KEY
            )
            info_dict = dict(zip(info[::2], info[1::2]))
            items_inserted = int(info_dict.get(b"Number of items inserted", 0))

            if items_inserted == 0:
                # Check if DB actually has anything before streaming
                count = await UserTraffic.count()
                if count == 0:
                    logger.info("Fresh setup — DB is empty, skipping repopulation")
                    return

                logger.info(f"Bloom filter empty but DB has {count:,} IPs — repopulating")
                await self.repopulate_from_db()
                
            else:
                logger.info(f"Bloom filter has {items_inserted:,} IPs — skipping repopulation")
        except Exception:
            # Filter doesn't exist yet — fresh setup, nothing to repopulate
            logger.info("Fresh bloom filter — no repopulation needed")


    async def exists(self, ip: str) -> bool:
        try:
            result = await self._redis.execute_command(
                "BF.EXISTS", settings.BLOOM_FILTER_KEY, ip
            )
            return bool(result)
        except Exception as e:
            logger.error(f"Bloom filter exists check failed for {ip}: {e}")
            return False  # Treat as unseen — safe fallback, may cause duplicate geo lookup


    async def add_many(self, ips: list[str]) -> None:
        if not ips:
            return
        try:
            await self._redis.execute_command(
                "BF.MADD", settings.BLOOM_FILTER_KEY, *ips
            )
        except Exception as e:
            logger.error(f"Bloom filter add_many failed: {e}")
            raise


    async def get_cached_geo(self, ip: str) -> dict | None:
        try:
            data = await self._redis.get(f"geo:{ip}")
            return json.loads(data) if data else None
        except Exception as e:
            logger.error(f"Redis geo cache read failed for {ip}: {e}")
            return None


    async def cache_geo(self, ip: str, geo: dict) -> None:
        try:
            await self._redis.set(f"geo:{ip}", json.dumps(geo))
        except Exception as e:
            logger.error(f"Redis geo cache write failed for {ip}: {e}")


    async def cache_geo_many(self, geo_map: dict[str, dict]) -> None:
        """Cache multiple geo results in a single Redis pipeline."""
        if not geo_map:
            return
        try:
            async with self._redis.pipeline(transaction=False) as pipe:
                for ip, geo in geo_map.items():
                    pipe.set(f"geo:{ip}", json.dumps(geo))
                await pipe.execute()
        except Exception as e:
            logger.error(f"Redis pipeline geo cache write failed: {e}")


bloom = BloomFilterService()