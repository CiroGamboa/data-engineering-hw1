# Data Engineering HW1 — Faulty API Clients

Two Python clients (threaded and async) that fetch 1,000 orders from a rate-limited, occasionally failing API, with retries and CSV export.

## What’s in this repo

- **client_threads.py** — Sync client using `ThreadPoolExecutor`, `httpx`, and `ratelimit`
- **client_async.py** — Async client using `asyncio`, `httpx.AsyncClient`, and `aiolimiter`
- **orders_server-0.1.0.tar.gz** — Provided API server package (install and run separately)
- **pyproject.toml** — Project metadata and client dependencies

## Quick start

### 1. Virtual environment

```bash
python -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
```

### 2. Install the API server

The server is required before running the clients. From this directory:

```bash
pip install --index-url https://pypi.org/simple/ ./orders_server-0.1.0.tar.gz
```

If you hit Faker compatibility issues, pin an older version:

```bash
pip install --index-url https://pypi.org/simple/ 'faker>=37.6.0,<39'
```

### 3. Install client dependencies

```bash
pip install -e .
# or: uv sync
```

### 4. Run the server (in a separate terminal)

```bash
source .venv/bin/activate
orders_server
```

Server runs at **http://127.0.0.1:8000** (Swagger at `/docs`).

### 5. Run the clients

**Threaded client** (writes `items_threads.csv`):

```bash
python client_threads.py
```

**Async client** (writes `items_async.csv`):

```bash
python client_async.py
```

### Generated output

Sample CSV output from both clients is available in this folder:

**[Generated data (items_threads.csv, items_async.csv)](https://drive.google.com/drive/folders/1kEpJFehFZ6Hi4keL9sgdPFwEB_Jnd2id?usp=sharing)**

## How the clients work

- **Rate limiting:** Clients stay under the server’s 20 req/s limit by capping at 18 req/s (threaded: `ratelimit` decorator; async: `AsyncLimiter(18, 1)`).
- **Retries:**
  - **429** — Read `Retry-After` header, sleep, then retry (logged at WARNING).
  - **5xx** — Sleep 1 s, retry (WARNING).
  - **4xx (except 429)** — No retry; log at ERROR.
  - **Timeouts / transport errors** — Bounded retries (e.g. 5), sleep 1 s (WARNING).
- **Output:** CSV with columns  
  `order_id`, `account_id`, `company`, `status`, `currency`, `subtotal`, `tax`, `total`, `created_at`  
  (no nested objects). Target ~1,000 rows; fewer are acceptable if retries are exhausted.

## Requirements

- Python 3.13+ (to match the server package)
- Dependencies: see `pyproject.toml`

## License

See [LICENSE](LICENSE) in this repository.
