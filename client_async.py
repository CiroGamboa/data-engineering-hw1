"""
Async client for the rate-limited orders API.
Fetches items 1..1000, handles 429/5xx/timeouts with retries, writes items_async.csv.
"""
import asyncio
import csv
import logging

import httpx
from aiolimiter import AsyncLimiter

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
BASE_URL = "http://127.0.0.1:8000"
ITEM_IDS = range(1, 1001)
OUTPUT_CSV = "items_async.csv"
RATE_LIMIT_CALLS = 18
RATE_LIMIT_PERIOD = 1
TIMEOUT = 2.0
MAX_RETRIES = 5
SEMAPHORE_LIMIT = 50

CSV_FIELDS = [
    "order_id",
    "account_id",
    "company",
    "status",
    "currency",
    "subtotal",
    "tax",
    "total",
    "created_at",
]

logger = logging.getLogger(__name__)


def _row_from_order(data: dict) -> dict:
    """Extract flat CSV row from Order JSON (drop contact, lines, source)."""
    return {k: data[k] for k in CSV_FIELDS if k in data}


async def fetch_one(
    client: httpx.AsyncClient,
    item_id: int,
    limiter: AsyncLimiter,
    semaphore: asyncio.Semaphore,
) -> dict | None:
    """
    Fetch one item by ID with retries.
    - 429: await sleep(Retry-After), retry.
    - 5xx / timeout: await sleep(1), retry (bounded).
    - 4xx (other than 429): no retry, return None.
    """
    url = f"{BASE_URL}/item/{item_id}"
    last_exc = None
    for attempt in range(MAX_RETRIES):
        async with semaphore:
            async with limiter:
                try:
                    resp = await client.get(url, timeout=TIMEOUT)
                    if resp.status_code == 200:
                        return _row_from_order(resp.json())
                    if resp.status_code == 429:
                        retry_after = int(resp.headers.get("Retry-After", 1))
                        logger.warning(
                            "429 for item_id=%s, retrying after %s s",
                            item_id,
                            retry_after,
                        )
                        await asyncio.sleep(retry_after)
                        continue
                    if 500 <= resp.status_code < 600:
                        logger.warning(
                            "5xx (%s) for item_id=%s, retrying in 1 s",
                            resp.status_code,
                            item_id,
                        )
                        await asyncio.sleep(1)
                        continue
                    # 4xx other than 429
                    logger.error(
                        "Non-retryable 4xx (%s) for item_id=%s",
                        resp.status_code,
                        item_id,
                    )
                    return None
                except (httpx.TimeoutException, httpx.TransportError) as e:
                    last_exc = e
                    logger.warning(
                        "Timeout/transport error for item_id=%s: %s; retrying in 1 s",
                        item_id,
                        e,
                    )
                    await asyncio.sleep(1)
    logger.error(
        "Exhausted retries for item_id=%s after %s attempts (last: %s)",
        item_id,
        MAX_RETRIES,
        last_exc,
    )
    return None


async def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )

    limiter = AsyncLimiter(RATE_LIMIT_CALLS, RATE_LIMIT_PERIOD)
    semaphore = asyncio.Semaphore(SEMAPHORE_LIMIT)
    results: dict[int, dict] = {}
    failed_ids: set[int] = set()
    max_rounds = 3

    async with httpx.AsyncClient() as client:
        for round_no in range(max_rounds):
            ids_to_fetch = (
                sorted(failed_ids) if round_no > 0 else list(ITEM_IDS)
            )
            if not ids_to_fetch and len(results) >= 1000:
                break
            if round_no > 0 and not ids_to_fetch:
                break

            tasks = [
                fetch_one(client, i, limiter, semaphore) for i in ids_to_fetch
            ]
            outcome = await asyncio.gather(*tasks, return_exceptions=True)
            for i, out in zip(ids_to_fetch, outcome):
                if isinstance(out, Exception):
                    logger.exception("Unexpected error for item_id=%s", i)
                    failed_ids.add(i)
                elif out is not None:
                    results[out["order_id"]] = out
                else:
                    failed_ids.add(i)

            failed_ids = set(ids_to_fetch) - set(results.keys())
            if len(results) >= 1000:
                break
            if not failed_ids:
                break

    rows = [results[k] for k in sorted(results.keys())[:1000]]

    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        writer.writeheader()
        writer.writerows(rows)

    logger.info("Wrote %s rows to %s", len(rows), OUTPUT_CSV)
    if failed_ids and len(rows) < 1000:
        logger.warning(
            "Missing %s items after retries: %s", len(failed_ids), failed_ids
        )


if __name__ == "__main__":
    asyncio.run(main())
