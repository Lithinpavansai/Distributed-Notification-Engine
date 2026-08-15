# Distributed Notification Engine

A high-throughput, idempotent notification dispatch system built with Python, FastAPI, Redis, PostgreSQL, and ARQ. Designed around at-least-once delivery guarantees, atomic rate-limiting, and duplicate-send protection under concurrent load.

---

## Architecture Overview

```
                      +-----------------------------+
                      |         HTTP Client         |
                      +-----------------------------+
                                     |
                                     | POST /v1/notifications
                                     v
                      +-----------------------------+
                      |      FastAPI Ingestion      |
                      |  (Combined Lock + Rate Lua) |
                      +-----------------------------+
                                     |
                         +-----------+-----------+
                         |                       |
                     (Enqueue)               (Response)
                         |                       |
                         v                       v
                  +-------------+         202 Accepted /
                  | Redis (ARQ) |         Cached Response
                  +-------------+
                         |
                         | Atomic Lua Claim
                         v
              +---------------------+
              |    ARQ Worker(s)    |
              +---------------------+
              | 1. State Guard      |  (SENDING / SENT)
              | 2. Reconcile Lock   |  (Fencing Token)
              | 3. Gateway Dispatch |  (Idempotent Mock/Provider)
              +---------------------+
                   |             |
               (Success)      (Max Contention / Failure)
                   |             |
                   v             v
             Delivery Ack    +------------------------+
                             | PostgreSQL DLQ Table   |
                             +------------------------+
```

### Key Components

- **FastAPI Ingestion (`POST /v1/notifications`)**:
  - Executes a single atomic Lua script (`COMBINED_LOCK_RATE_LIMIT_LUA`) that checks/acquires a per-user idempotency lock and enforces a sliding-window rate limit in 1 round-trip.
  - Dispatches valid jobs to ARQ and returns `202 Accepted`.
  - Never writes terminal state — terminal updates are delegated to workers.
- **Worker Three-State Guard (`SENDING` / `SENT`)**:
  - Guards against duplicate sends under concurrent worker claims.
  - If a collision occurs on a `SENDING` key, worker acquires a fencing-token reconcile lock before querying gateway status, eliminating duplicate-send race conditions.
- **Atomic Job Claiming (`LuaClaimWorker`)**:
  - Replaces ARQ's default claim mechanism (`WATCH`/`EXISTS`/`ZSCORE`/`MULTI-EXEC`, 4 sequential round-trips) with a custom atomic Lua script (`LUA_BATCH_CLAIM_SCRIPT`), completely eliminating cross-worker `WatchError` contention.
- **Contention Handling & Backoff**:
  - Jobs experiencing claim collisions requeue with jittered backoff.
  - Repeated failures exceeding `CONTENTION_MAX_ATTEMPTS` automatically escalate to a PostgreSQL Dead Letter Queue (DLQ).
- **Redis Pipelining**:
  - Worker Redis operations are pipelined into 2 batched round-trips (down from 4 sequential calls) where dependency ordering permits.

---

## Performance

Sustained throughput (steady-state, saturated-queue measurement on a Windows + Docker Desktop development environment):

| Configuration        | Throughput      |
|-----------------------|-----------------|
| 1 ARQ worker process   | ~737 jobs/sec  |
| 2 ARQ worker processes | ~916 jobs/sec  |
| 4 ARQ worker processes | ~1,058 jobs/sec |

### Key Optimizations:
- **Atomic Lua-based Job Claiming**: Eliminated `WatchError`-driven transaction aborts (from **8,153 down to 0** under 4-way concurrency).
- **Redis Pipelining**: Reduced round-trips per job from 4 to 2 (**+29.5% throughput**, verified via controlled A/B testing on identical harnesses).
- **Multi-process Scaling**: Verified safe scaling across multiple worker processes with **0 WatchErrors**.

> **Note on Environment Bottlenecks**: Throughput ceilings were root-caused via isolated benchmarks to Windows-to-WSL2-to-Docker socket translation overhead under async concurrency — not application logic or Redis capacity. A native `redis-benchmark` cross-check confirmed the Redis server itself sustains ~64,000 ops/sec with sub-millisecond latency. Native Linux deployment is expected to raise throughput ceilings substantially.

---

## Tech Stack

- **Application & Ingestion**: Python 3.11+, FastAPI, Uvicorn
- **Asynchronous Task Queue**: ARQ, Redis 7 (with custom Lua scripts)
- **Database & Persistence**: PostgreSQL 16, asyncpg, SQLAlchemy 2.0
- **Validation & Serialization**: Pydantic
- **Testing & Benchmarks**: pytest, pytest-asyncio, Locust, httpx

---

## Getting Started

### Prerequisites

- Python 3.11+
- Docker & Docker Compose
- `git`

### 1. Clone & Setup Environment

```bash
# Clone repository
git clone <repo-url>
cd "Distributed Notification Engine"

# Create and activate a virtual environment
python -m venv .venv
# On Windows:
.venv\Scripts\activate
# On Linux/macOS:
source .venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

### 2. Configure Environment Variables

Copy `.env.example` to `.env`:

```bash
cp .env.example .env
```

Default configuration in `.env`:

```env
# Database configuration
POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres
POSTGRES_DB=notification_db
POSTGRES_PORT=5432
DATABASE_URL=postgresql+asyncpg://postgres:postgres@localhost:5432/notification_db

# Redis configuration
REDIS_PORT=6379
REDIS_URL=redis://localhost:6379/0
```

### 3. Start Infrastructure with Docker Compose

Launch PostgreSQL and Redis:

```bash
docker compose up -d
```

### 4. Run Database Migrations

Apply table definitions and defaults for the Dead Letter Queue:

```bash
python -m app.migrate
```

---

## Running the Services

### Start the FastAPI API Server

```bash
uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
```
Interactive Swagger documentation is available at `http://localhost:8000/docs`.

### Start the ARQ Worker

In a separate terminal (with virtual environment active):

```bash
python -m app.worker
```

---

## API Endpoints

### 1. Ingestion & Health

- **`GET /health`**
  - Simple health check endpoint.
  - Returns `{"status": "ok"}`.

- **`POST /v1/notifications`**
  - Ingestion endpoint with atomic sliding-window rate limiting & idempotency protection.
  - **Headers**:
    - `X-User-Id`: User identifier (e.g. `test_user_id`).
  - **Request Body**:
    ```json
    {
      "idempotency_key": "unique-request-uuid-12345",
      "title": "Welcome",
      "message": "Welcome to our platform!"
    }
    ```
  - **Responses**:
    - `202 Accepted`: Job successfully accepted or existing job currently processing.
    - `429 Too Many Requests`: Sliding-window rate limit exceeded.

### 2. Admin & Operational Endpoints

All admin endpoints require the header `X-User-Id: admin_user_id`.

- **`GET /v1/admin/metrics`**
  - Returns real-time metrics including ingestion contention, reconciliation hits, fencing failures, and DLQ depth.
  - **Sample Response**:
    ```json
    {
      "ingestion_lock_contention": 0,
      "reconciliation_hits": 12,
      "reconciliation_lock_contention": 1,
      "fencing_token_release_failures": 0,
      "contention_requeue_exhaustion": 0,
      "dlq_depth": 0
    }
    ```

- **`GET /v1/admin/dlq?limit=50&offset=0`**
  - List dead-letter queue records and their failure stack traces.

- **`POST /v1/admin/dlq/replay`**
  - Re-enqueue a failed job from the DLQ back into the ARQ pipeline for processing.
  - **Request Body**:
    ```json
    {
      "dlq_job_id": "f81d4fae-7dec-11d0-a765-00a0c91e6bf6"
    }
    ```

---

## Running Tests & Benchmarks

### Running Unit & Integration Tests

```bash
pytest
```

### Running Load Tests (Locust)

```bash
locust -f benchmarks/locustfile.py --host http://localhost:8000
```

### Running Verification & Scaling Benchmarks

```bash
# Preseed queue with 50,000 jobs
python benchmarks/preseed_50k.py

# Benchmark unpipelined baseline
python benchmarks/test_unpipelined_50k_15s.py

# Benchmark optimized pipelined worker
python benchmarks/run_final_50k_15s_verification.py

# Test multi-process worker scaling
python benchmarks/test_multi_process_scaling.py
```

---

## Known Limitations

- **Admin Authentication**: Uses a stub header check (`X-User-Id` mapped to an in-memory role table) for local demo purposes. A production deployment should implement JWT/OAuth2 authentication.
- **Adaptive Reconcile Lock TTL**: Reconcile lock TTL uses a bootstrap default. The dynamic adaptation path (calculating TTL from rolling p99 queue depth and processing latency) is designed with sample ingestion metrics, but queue-depth sampling is currently stubbed.
- **Environment-Bound Throughput**: Reported throughput figures reflect a Windows/WSL2/Docker Desktop development machine. Native Linux execution without container virtualization socket overhead will yield higher throughput.
