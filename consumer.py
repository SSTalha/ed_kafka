import asyncio
import json
import logging
from datetime import datetime, timezone

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

from config import settings
from db.mongodb import connect_db, disconnect_db
from db.redis import connect_redis, disconnect_redis
from models.user_traffic import UserTraffic, GeoLocation
from services.bloom_filter import bloom
from services.geo_service import fetch_geo_batch

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger(__name__)


async def process_batch(messages: list[dict], producer: AIOKafkaProducer) -> None:
    if not messages:
        return

    logger.info(f"Processing batch of {len(messages)} messages")

    # --- Step 1: Unique IPs from this batch ---
    unique_ips = {msg["ip"] for msg in messages}

    # --- Step 2: Filter already-seen IPs via bloom filter ---
    new_ips = [ip for ip in unique_ips if not await bloom.exists(ip)]
    logger.info(f"Unique IPs: {len(unique_ips)}, New to lookup: {len(new_ips)}")

    # --- Step 3: Fetch geo only for new IPs ---
    geo_map: dict[str, dict] = {}
    failed_ips: list[str] = []

    if new_ips:
        try:
            geo_map, failed_ips = await fetch_geo_batch(new_ips)
        except Exception as e:
            logger.error(f"Geo service failure: {e}. Sending batch to dead letter queue.")
            await _send_to_dlq(producer, messages)
            raise

        if geo_map:
            await bloom.cache_geo_many(geo_map)

        resolved_ips = [ip for ip in new_ips if ip in geo_map]
        if resolved_ips:
            await bloom.add_many(resolved_ips)

        if failed_ips:
            failed_messages = [m for m in messages if m["ip"] in failed_ips]
            logger.warning(f"{len(failed_ips)} IPs failed geo lookup — sending to DLQ")
            await _send_to_dlq(producer, failed_messages)

    # --- Step 4: Fill geo for already-known IPs from Redis cache ---
    for ip in unique_ips:
        if ip not in geo_map:
            cached = await bloom.get_cached_geo(ip)
            geo_map[ip] = cached if cached is not None else {}

    # --- Step 5: Build and bulk insert — deduplicate at user+ip level ---
    failed_set = set(failed_ips)
    seen_combinations: set[tuple] = set()
    docs = []

    for msg in messages:
        ip = msg.get("ip")
        user_id = msg.get("user_id")

        if not ip or not user_id:
            logger.warning(f"Skipping malformed message: {msg}")
            continue

        if ip in failed_set:
            continue

        combo = (user_id, ip)
        if combo in seen_combinations:
            continue  # Duplicate user+ip in this batch — skip
        seen_combinations.add(combo)

        docs.append(UserTraffic(
            user_id=user_id,
            ip_address=ip,
            geo_location=GeoLocation(**geo_map.get(ip, {})),
            created_at=datetime.now(timezone.utc),
        ))

    if docs:
        try:
            # ordered=False — skips duplicates at DB level, continues inserting rest
            await UserTraffic.insert_many(docs, ordered=False)
            logger.info(f"Inserted {len(docs)} records into MongoDB")
        except Exception as e:
            if "duplicate key" in str(e).lower():
                logger.info("Some duplicate user+ip combinations skipped by DB index")
            else:
                logger.error(f"MongoDB insert failed: {e}. Sending batch to DLQ.")
                await _send_to_dlq(producer, messages)
                raise


async def _send_to_dlq(producer: AIOKafkaProducer, messages: list[dict]) -> None:
    """Send failed messages to the dead letter topic for later reprocessing."""
    for msg in messages:
        try:
            await producer.send(settings.KAFKA_DEAD_LETTER_TOPIC, msg)
        except Exception as e:
            logger.error(f"Failed to send message to DLQ: {e} — message: {msg}")


async def consume() -> None:
    await connect_db()
    await connect_redis()

    await bloom.initialize()
    await bloom.check_and_repopulate_if_empty()

    producer = AIOKafkaProducer(
        bootstrap_servers=settings.KAFKA_BROKER_URL,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    consumer = AIOKafkaConsumer(
        settings.KAFKA_TOPIC,
        settings.KAFKA_DEAD_LETTER_TOPIC,
        bootstrap_servers=settings.KAFKA_BROKER_URL,
        group_id=settings.KAFKA_CONSUMER_GROUP_ID,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        retry_backoff_ms=2000,
        metadata_max_age_ms=30000,
        request_timeout_ms=30000,
    )

    await producer.start()
    await consumer.start()
    logger.info(f"Kafka consumer started — topic: {settings.KAFKA_TOPIC}")

    try:
        batch: list[dict] = []
        last_flush = asyncio.get_event_loop().time()

        async for msg in consumer:
            batch.append(msg.value)

            time_elapsed = asyncio.get_event_loop().time() - last_flush
            should_flush = (
                len(batch) >= settings.BATCH_SIZE
                or time_elapsed >= settings.BATCH_TIMEOUT_SECONDS
            )

            if should_flush:
                try:
                    await process_batch(batch, producer)
                    await consumer.commit()
                    logger.info("Kafka offset committed")
                except Exception as e:
                    logger.error(f"Batch processing failed, offset NOT committed: {e}")
                finally:
                    batch = []
                    last_flush = asyncio.get_event_loop().time()

    except Exception as e:
        logger.error(f"Consumer loop error: {e}")
        raise
    finally:
        logger.info("Shutting down...")
        await consumer.stop()
        await producer.stop()
        await disconnect_redis()
        await disconnect_db()


if __name__ == "__main__":
    asyncio.run(consume())