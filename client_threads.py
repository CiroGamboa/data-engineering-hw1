"""
Threaded (sync) client for the rate-limited orders API.
Fetches items 1..1000, handles 429/5xx/timeouts with retries, writes items_threads.csv.
"""
import csv
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import httpx
from ratelimit import limits, sleep_and_retry

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
BASE_URL = "http://127.0.0.1:8000"
ITEM_IDS = range(1, 1001)
OUTPUT_CSV = "items_threads.csv"
RATE_LIMIT_CALLS = 18
RATE_LIMIT_PERIOD = 1
TIMEOUT = 2.0
MAX_RETRIES = 5
MAX_WORKERS = 10

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


# ---------------------------------------------------------------------------
# Rate-limited single request (shared by all threads)
# ---------------------------------------------------------------------------
@sleep_and_retry
@limits(calls=RATE_LIMIT_CALLS, period=RATE_LIMIT_PERIOD)
def _do_request(client: httpx.Client, url: str) -> httpx.Response:
    return client.get(url, timeout=TIMEOUT)


def _row_from_order(data: dict) -> dict:
    """Extract flat CSV row from Order JSON (drop contact, lines, source)."""
    return {k: data[k] for k in CSV_FIELDS if k in data}


def fetch_one(client: httpx.Client, item_id: int) -> dict | None:
    """
    Fetch one item by ID with retries.
    - 429: sleep Retry-After, retry.
    - 5xx / timeout: sleep 1s, retry (bounded).
    - 4xx (other than 429): no retry, return None.
    """
    url = f"{BASE_URL}/item/{item_id}"
    last_exc = None
    for attempt in range(MAX_RETRIES):
        try:
            resp = _do_request(client, url)
            if resp.status_code == 200:
                return _row_from_order(resp.json())
            if resp.status_code == 429:
                retry_after = int(resp.headers.get("Retry-After", 1))
                logger.warning(
                    "429 for item_id=%s, retrying after %s s",
                    item_id,
                    retry_after,
                )
                time.sleep(retry_after)
                continue
            if 500 <= resp.status_code < 600:
                logger.warning(
                    "5xx (%s) for item_id=%s, retrying in 1 s",
                    resp.status_code,
                    item_id,
                )
                time.sleep(1)
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
            time.sleep(1)
    logger.error(
        "Exhausted retries for item_id=%s after %s attempts (last: %s)",
        item_id,
        MAX_RETRIES,
        last_exc,
    )
    return None


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )

    results: dict[int, dict] = {}  # order_id -> row
    failed_ids: set[int] = set()
    max_rounds = 3  # retry failed IDs up to this many rounds

    with httpx.Client() as client:
        for round_no in range(max_rounds):
            ids_to_fetch = failed_ids if round_no > 0 else set(ITEM_IDS)
            if not ids_to_fetch and len(results) >= 1000:
                break
            if round_no > 0 and not ids_to_fetch:
                break
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                future_to_id = {
                    executor.submit(fetch_one, client, i): i for i in ids_to_fetch
                }
                for future in as_completed(future_to_id):
                    item_id = future_to_id[future]
                    try:
                        row = future.result()
                        if row is not None:
                            results[row["order_id"]] = row
                        else:
                            failed_ids.add(item_id)
                    except Exception as e:
                        logger.exception("Unexpected error for item_id=%s", item_id)
                        failed_ids.add(item_id)

            failed_ids = ids_to_fetch - set(results.keys())
            if len(results) >= 1000:
                break
            if not failed_ids:
                break

    # Build sorted rows (by order_id) up to 1000
    rows = [results[k] for k in sorted(results.keys())[:1000]]

    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        writer.writeheader()
        writer.writerows(rows)

    logger.info("Wrote %s rows to %s", len(rows), OUTPUT_CSV)
    if failed_ids and len(rows) < 1000:
        logger.warning("Missing %s items after retries: %s", len(failed_ids), failed_ids)


if __name__ == "__main__":
    main()
