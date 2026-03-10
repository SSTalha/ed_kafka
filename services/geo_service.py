import asyncio
import logging

import httpx

logger = logging.getLogger(__name__)

BATCH_URL = "http://ip-api.com/batch"
FIELDS = "status,message,query,country,regionName,city,lat,lon,isp,timezone"
MAX_BATCH_SIZE = 100    # ip-api.com hard limit per request
MAX_RETRIES = 2


async def _post_chunk(
    client: httpx.AsyncClient,
    chunk: list[str],
) -> tuple[dict[str, dict], list[str]]:
    """
    POST a single chunk of IPs to ip-api.com.
    Returns (geo_map, failed_ips).
    """
    payload = [{"query": ip} for ip in chunk]
    geo_map: dict[str, dict] = {}
    failed_ips: list[str] = []

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = await client.post(
                BATCH_URL,
                json=payload,
                params={"fields": FIELDS},
            )

            rl_remaining = int(response.headers.get("X-Rl", 1))
            rl_ttl = int(response.headers.get("X-Ttl", 60))

            if response.status_code == 429:
                logger.warning(f"Rate limit hit — sleeping {rl_ttl}s (attempt {attempt})")
                await asyncio.sleep(rl_ttl)
                continue  # Retry after sleep

            response.raise_for_status()

            for item in response.json():
                ip = item.get("query")
                if not ip:
                    continue
                if item.get("status") == "success":
                    geo_map[ip] = {
                        "country":  item.get("country"),
                        "region":   item.get("regionName"),
                        "city":     item.get("city"),
                        "lat":      item.get("lat"),
                        "lon":      item.get("lon"),
                        "isp":      item.get("isp"),
                        "timezone": item.get("timezone"),
                    }
                else:
                    # ip-api returned status=fail for this IP (e.g. private/reserved range)
                    logger.debug(f"Geo lookup returned non-success for {ip}: {item.get('message')}")
                    geo_map[ip] = {}  # Store empty — do not retry private IPs

            # Proactively wait if rate limit window is exhausted
            if rl_remaining == 0:
                logger.info(f"Rate limit window exhausted — waiting {rl_ttl}s")
                await asyncio.sleep(rl_ttl)

            return geo_map, failed_ips  # Success

        except httpx.HTTPStatusError as e:
            logger.error(f"HTTP error on attempt {attempt} for chunk: {e}")
        except httpx.RequestError as e:
            logger.error(f"Request error on attempt {attempt} for chunk: {e}")
        except Exception as e:
            logger.error(f"Unexpected error on attempt {attempt} for chunk: {e}")

        if attempt < MAX_RETRIES:
            await asyncio.sleep(2 ** attempt)  # Exponential backoff: 2s, 4s

    # All retries exhausted — mark entire chunk as failed
    logger.error(f"All {MAX_RETRIES} attempts failed for chunk of {len(chunk)} IPs")
    failed_ips = chunk
    return geo_map, failed_ips


async def fetch_geo_batch(ips: list[str]) -> tuple[dict[str, dict], list[str]]:
    """
    Fetch geo data for a list of IPs using ip-api.com batch endpoint.

    Returns:
        geo_map:    { ip: geo_dict } for successfully resolved IPs
        failed_ips: list of IPs that could not be resolved after retries
    """
    all_geo: dict[str, dict] = {}
    all_failed: list[str] = []

    chunks = [ips[i:i + MAX_BATCH_SIZE] for i in range(0, len(ips), MAX_BATCH_SIZE)]
    logger.info(f"Fetching geo for {len(ips)} IPs in {len(chunks)} chunk(s)")

    async with httpx.AsyncClient(timeout=30.0) as client:
        for chunk in chunks:
            geo_map, failed_ips = await _post_chunk(client, chunk)
            all_geo.update(geo_map)
            all_failed.extend(failed_ips)

    logger.info(
        f"Geo fetch complete — "
        f"resolved: {len(all_geo)}, failed: {len(all_failed)}"
    )
    return all_geo, all_failed