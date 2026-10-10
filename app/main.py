import json
import os
import time
import asyncio
import uuid
import random
import csv
import logging
from contextlib import asynccontextmanager
from urllib.parse import urlparse
from pathlib import Path
from typing import Optional
from dotenv import load_dotenv

# Ensure environment variables are loaded immediately upon import
load_dotenv()

from fastapi import FastAPI, Depends, HTTPException, Security, Header
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from datetime import datetime
from redis.asyncio import Redis
from arq import create_pool
from arq.connections import RedisSettings

logger = logging.getLogger("notification_engine")

# Base directory for portable log and scratch paths
BASE_DIR = Path(__file__).resolve().parent.parent
LOG_DIR = Path(os.getenv("LOG_DIR", str(BASE_DIR / "logs")))
SCRATCH_DIR = Path(os.getenv("SCRATCH_DIR", str(BASE_DIR / "scratch")))
TIMING_LOG_PATH = LOG_DIR / "timing.log"
INGESTION_TIMING_LOG = SCRATCH_DIR / "ingestion_timing.log"
JOB_EXEC_TIMING_LOG = SCRATCH_DIR / "job_execution_timing.log"
REDIS_CALL_LATENCY_LOG = SCRATCH_DIR / "redis_call_latencies.log"

# Default configuration values
AVG_JOB_PROCESS_SECONDS = float(os.getenv("AVG_JOB_PROCESS_SECONDS", "5.0"))
RATE_LIMIT_WINDOW_SECONDS = int(os.getenv("RATE_LIMIT_WINDOW_SECONDS", "60"))
RATE_LIMIT_MAX_REQUESTS = int(os.getenv("RATE_LIMIT_MAX_REQUESTS", "5"))

def get_database_url(for_sqlalchemy: bool = False) -> str:
    """
    Returns normalized database connection string.
    asyncpg requires 'postgresql://', SQLAlchemy async engine requires 'postgresql+asyncpg://'.
    """
    url = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/notification_db")
    if for_sqlalchemy:
        if not url.startswith("postgresql+asyncpg://") and url.startswith("postgresql://"):
            return url.replace("postgresql://", "postgresql+asyncpg://", 1)
        return url
    else:
        if url.startswith("postgresql+asyncpg://"):
            return url.replace("postgresql+asyncpg://", "postgresql://", 1)
        return url

# Sorted Set (zset) Sliding Window Rate Limit Lua Script
RATE_LIMIT_LUA = """
local key = KEYS[1]
local now = tonumber(ARGV[1])
local window = tonumber(ARGV[2])
local limit = tonumber(ARGV[3])
local member = ARGV[4]

-- Remove members older than the sliding window
local clear_before = now - window
redis.call('ZREMRANGEBYSCORE', key, '-inf', clear_before)

-- Count requests in the window
local current_requests = redis.call('ZCARD', key)
if current_requests < limit then
    -- Add current request's unique member and extend key TTL
    redis.call('ZADD', key, now, member)
    redis.call('EXPIRE', key, window)
    return 1
else
    return 0
end
"""

# Combined Idempotency Lock and Rate Limit Lua Script
COMBINED_LOCK_RATE_LIMIT_LUA = """
local idempotency_key = KEYS[1]
local rate_limit_key = KEYS[2]

local idempotency_ttl = tonumber(ARGV[1])
local rate_limit_window = tonumber(ARGV[2])
local rate_limit_max = tonumber(ARGV[3])
local now = tonumber(ARGV[4])
local member = ARGV[5]

-- 1. Check if the idempotency key already exists.
local existing_val = redis.call('GET', idempotency_key)
if existing_val then
    return {1, existing_val}
end

-- 2. Run sliding-window rate-limiting.
local clear_before = now - rate_limit_window
redis.call('ZREMRANGEBYSCORE', rate_limit_key, '-inf', clear_before)

local current_requests = redis.call('ZCARD', rate_limit_key)
if current_requests >= rate_limit_max then
    return {2, false}
end

-- 3. Success: set idempotency key to PROCESSING and add entry to rate-limit set.
redis.call('SET', idempotency_key, 'PROCESSING', 'EX', idempotency_ttl)
redis.call('ZADD', rate_limit_key, now, member)
redis.call('EXPIRE', rate_limit_key, rate_limit_window)

return {3, false}
"""

# Worker metrics and constants
RECONCILE_LATENCY_KEY = "metrics:reconcile_latency_ms"
RECONCILE_LATENCY_SAMPLE_CAP = 1000
RECONCILE_TTL_MIN_SAMPLES = 500

class NotificationJob:
    """Job context matching ARQ job style."""
    def __init__(self, idempotency_key: str, payload: dict):
        self.idempotency_key = idempotency_key
        self.payload = payload
        self.requeued = False
        self.requeue_reason = None
        self.final_status = None
        self.contention_requeue_count = 0
        self.replay_count = 0
        self.ctx = None
        self.last_delay = 0.0

class TransientGatewayError(Exception):
    """Exception raised for transient, retryable delivery errors."""
    pass

class GatewayStatus:
    def __init__(self, was_received: bool):
        self.was_received = was_received

class BaseNotificationGateway:
    """Abstract base class for notification delivery providers (e.g. SMTP, Push, Webhook)."""
    async def send(self, idempotency_key: str, payload: dict) -> dict:
        raise NotImplementedError

    async def check_status(self, idempotency_key: str) -> GatewayStatus:
        raise NotImplementedError

# Global Mock variables for test configuration
GATEWAY_SEND_DELAY = float(os.getenv("GATEWAY_SEND_DELAY", "0.0"))
GATEWAY_SEND_EVENT = None
GATEWAY_START_EVENT = None
GATEWAY_FORCE_TRANSIENT_ERROR = False
GATEWAY_CALL_COUNT = 0

class MockGatewayAdapter(BaseNotificationGateway):
    """
    Deterministic development and test delivery adapter.
    Simulates external notification delivery with configurable delays, event triggers, and transient failures.
    """
    def __init__(self):
        self.delay = 0.0
        self.was_received_map = {}

    async def check_status(self, idempotency_key: str) -> GatewayStatus:
        if self.delay > 0:
            await asyncio.sleep(self.delay)
        was_received = self.was_received_map.get(idempotency_key, False)
        return GatewayStatus(was_received=was_received)

    async def send(self, idempotency_key: str, payload: dict) -> dict:
        global GATEWAY_CALL_COUNT
        GATEWAY_CALL_COUNT += 1
        
        if GATEWAY_SEND_DELAY > 0:
            await asyncio.sleep(GATEWAY_SEND_DELAY)
            
        if GATEWAY_START_EVENT is not None:
            GATEWAY_START_EVENT.set()
            
        if GATEWAY_SEND_EVENT is not None:
            await GATEWAY_SEND_EVENT.wait()
            
        if GATEWAY_FORCE_TRANSIENT_ERROR:
            raise TransientGatewayError("Transient gateway error (forced)")
            
        self.was_received_map[idempotency_key] = True
        return {"status": "success", "idempotency_key": idempotency_key}

# Global gateway instance (using Mock adapter for local/test environments)
gateway_mock = MockGatewayAdapter()

async def call_gateway_mock(idempotency_key: str, payload: dict) -> dict:
    return await gateway_mock.send(idempotency_key, payload)

async def finalize_job_status(job, status: str):
    worker_logger = logging.getLogger("worker")
    worker_logger.info("Finalizing job %s with status: %s", job.idempotency_key, status)
    job.final_status = status

CONTENTION_MAX_ATTEMPTS = 5

async def insert_dead_letter_queue_row(job, stack_trace: str, replay_count: int):
    import asyncpg
    db_url = get_database_url()
    conn = await asyncpg.connect(db_url)
    try:
        payload_json = json.dumps(job.payload)
        await conn.execute(
            "INSERT INTO dead_letter_queue (payload, replay_count, stack_trace) VALUES ($1, $2, $3);",
            payload_json,
            replay_count,
            stack_trace
        )
    finally:
        await conn.close()

async def requeue_with_backoff(job, reason: str):
    attempt = getattr(job, "contention_requeue_count", 0)

    if attempt >= CONTENTION_MAX_ATTEMPTS:
        await insert_dead_letter_queue_row(
            job=job,
            stack_trace=f"ContentionExhausted: {reason}, max_attempts={CONTENTION_MAX_ATTEMPTS} reached",
            replay_count=job.replay_count if hasattr(job, "replay_count") else 0,
        )
        return

    if arq_pool is None:
        raise RuntimeError("requeue_with_backoff called with no arq_pool available — worker was not properly initialized")

    current_reconcile_ttl = await get_reconcile_lock_ttl()
    backoff_floor = current_reconcile_ttl / 2
    delay = backoff_floor + random.uniform(0, backoff_floor)
    job.contention_requeue_count = attempt + 1
    
    # Actually requeue via ARQ with the computed delay
    replay_count = job.replay_count if hasattr(job, "replay_count") else 0
    await arq_pool.enqueue_job("send_notification", job.payload, replay_count=replay_count, _defer_by=delay)
        
    job.requeued = True
    job.requeue_reason = reason
    job.last_delay = delay

class NotificationIn(BaseModel):
    idempotency_key: str
    title: str = "Test"
    message: str = "Test message"

USER_ROLES = {
    "admin_user_id": "admin",
    "test_user_id": "user"
}

class User:
    def __init__(self, id: str, role: str):
        self.id = id
        self.role = role

async def get_current_user(x_user_id: str = Header("test_user_id")) -> User:
    role = USER_ROLES.get(x_user_id, "user")
    return User(id=x_user_id, role=role)

async def get_current_admin_user(current_user: User = Depends(get_current_user)) -> User:
    if current_user.role != "admin":
        raise HTTPException(status_code=403, detail="Admin privileges required.")
    return current_user

class JobQueueStub:
    async def get_depth(self) -> int:
        return 0

job_queue = JobQueueStub()

# Redis and ARQ Initialization
redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
redis = Redis.from_url(redis_url, max_connections=200)
arq_pool = None
db_pool = None

_timing_buffer = []

@asynccontextmanager
async def lifespan(app: FastAPI):
    global redis, arq_pool, db_pool
    # Startup: ensure redis, arq pool, and database connection pool are initialized
    redis = Redis.from_url(redis_url, max_connections=200)
    parsed = urlparse(redis_url)
    host = parsed.hostname or 'localhost'
    port = parsed.port or 6379
    db = int(parsed.path.lstrip('/') or 0)
    password = parsed.password
    arq_settings = RedisSettings(host=host, port=port, database=db, password=password)
    arq_pool = await create_pool(arq_settings)
    
    # Initialize Postgres db_pool
    import asyncpg
    db_url = get_database_url()
    try:
        db_pool = await asyncpg.create_pool(db_url)
    except Exception as exc:
        logger.warning("Could not initialize database connection pool in lifespan: %s", exc)
        db_pool = None
    
    yield
    
    # Shutdown
    await redis.aclose()
    if arq_pool:
        await arq_pool.aclose()
    if db_pool:
        await db_pool.close()

    if _timing_buffer:
        try:
            TIMING_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
            with open(TIMING_LOG_PATH, "a") as f:
                for msg in _timing_buffer:
                    f.write(msg + "\n")
        except Exception:
            pass

app = FastAPI(
    title="Distributed Notification Engine",
    description="A FastAPI-based notification backend",
    version="0.1.0",
    lifespan=lifespan
)

_ingestion_log_initialised = False

def _write_ingestion_timing(row: dict):
    """Append one timing row to ingestion_timing.log (CSV) defensively."""
    global _ingestion_log_initialised
    fieldnames = [
        "ts", "source", "status_code",
        "t_pre_lua_ms",    # time from request start to lua eval start (key build + ttl)
        "t_lua_ms",        # time inside redis.eval
        "t_post_lua_ms",   # time after lua until response dispatched (incr + redis.get OR arq.enqueue)
        "t_total_ms",      # end-to-end handler time
    ]
    try:
        write_header = not _ingestion_log_initialised
        INGESTION_TIMING_LOG.parent.mkdir(parents=True, exist_ok=True)
        with open(INGESTION_TIMING_LOG, "a", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            if write_header:
                writer.writeheader()
            writer.writerow(row)
        _ingestion_log_initialised = True
    except Exception as e:
        logger.debug("Failed writing ingestion timing: %s", e)

@app.get("/health", status_code=200)
async def health_check():
    """Health check endpoint to verify that the application boots and runs."""
    return {"status": "ok"}

def make_idempotency_key(user_id: str, idempotency_key: str) -> str:
    return f"idempotency:{user_id}:{idempotency_key}"

async def get_ingestion_lock_ttl(queue) -> int:
    depth = await queue.get_depth()
    estimated_drain_seconds = depth * AVG_JOB_PROCESS_SECONDS
    return max(60, min(estimated_drain_seconds + 30, 900))  # floor 60s, cap 15min

async def handle_existing_idempotency_key(key: str):
    """
    Three-way branch on the idempotency key's current value:
      1. PROCESSING       -> 202, poll-again hint
      2. terminal (JSON)  -> replay the original cached response verbatim
      3. missing/corrupt  -> explicit error, never a silent guess
    """
    value = await redis.get(key)

    if value is None:
        raise HTTPException(
            status_code=409,
            detail="Idempotency key state expired mid-request; retry.",
        )

    if value == b"PROCESSING" or value == "PROCESSING":
        return JSONResponse(
            status_code=202,
            content={"status": "processing", "detail": "Request accepted, not yet complete."},
            headers={"Retry-After": "5"},
        )

    try:
        cached = json.loads(value)
    except (json.JSONDecodeError, TypeError):
        raise HTTPException(status_code=500, detail="Corrupted idempotency state for key.")

    return JSONResponse(
        status_code=cached.get("status_code", 200),
        content=cached.get("body", cached),
    )

_job_exec_log_file = None

def _record_job_exec_timing(duration_ms: float, key_type: str = "unique"):
    global _job_exec_log_file
    try:
        if _job_exec_log_file is None:
            JOB_EXEC_TIMING_LOG.parent.mkdir(parents=True, exist_ok=True)
            _job_exec_log_file = open(
                JOB_EXEC_TIMING_LOG,
                "a",
                buffering=1
            )
        _job_exec_log_file.write(f"{key_type},{duration_ms:.3f}\n")
    except Exception as e:
        logger.debug("Failed recording job execution timing: %s", e)

USE_NOOP_JOB = os.getenv("USE_NOOP_JOB", "false").lower() == "true"

async def process_notification_job_noop(job):
    """Synthetic no-op job: zero Redis calls, immediate return."""
    return

async def send_notification_noop(ctx, payload: dict, replay_count: int = 0):
    idempotency_key = payload.get("idempotency_key")
    if not idempotency_key:
        raise ValueError("Payload missing idempotency_key")

    job = NotificationJob(idempotency_key=idempotency_key, payload=payload)
    job.replay_count = replay_count
    job.ctx = ctx

    key_type = "collision" if "collision" in idempotency_key else "unique"

    t0 = time.perf_counter()
    await process_notification_job_noop(job)
    t1 = time.perf_counter()
    duration_ms = (t1 - t0) * 1000
    _record_job_exec_timing(duration_ms, key_type=key_type)

async def send_notification(ctx, payload: dict, replay_count: int = 0):
    arq_log = logging.getLogger("arq")
    # Log sanitized metadata instead of unbounded raw payload
    arq_log.info(
        "Notification job received. IdempotencyKey: %s, Replay Count: %d",
        payload.get("idempotency_key"),
        replay_count
    )

    idempotency_key = payload.get("idempotency_key")
    if not idempotency_key:
        raise ValueError("Payload missing idempotency_key")

    job = NotificationJob(idempotency_key=idempotency_key, payload=payload)
    job.replay_count = replay_count
    job.ctx = ctx

    key_type = "collision" if "collision" in idempotency_key else "unique"

    t0 = time.perf_counter()
    if USE_NOOP_JOB:
        await process_notification_job_noop(job)
    else:
        await process_notification_job(job)
    t1 = time.perf_counter()
    duration_ms = (t1 - t0) * 1000
    _record_job_exec_timing(duration_ms, key_type=key_type)

@app.post("/v1/notifications")
async def create_notification(
    payload: NotificationIn,
    user=Depends(get_current_user),
    x_request_source: Optional[str] = Header(default="unknown"),
):
    t_handler_start = time.monotonic()

    key = make_idempotency_key(user.id, payload.idempotency_key)
    ttl = await get_ingestion_lock_ttl(job_queue)

    rate_limit_key = f"rate_limit:{user.id}"
    now = time.time()
    member = key

    # ── Segment: pre-Lua (key construction + TTL fetch) ──────────────────
    t_pre_lua_end = time.monotonic()
    t_pre_lua_ms = (t_pre_lua_end - t_handler_start) * 1000

    # ── Segment: Lua eval ─────────────────────────────────────────────────
    t0_lua = time.monotonic()
    result = await redis.eval(
        COMBINED_LOCK_RATE_LIMIT_LUA,
        2,
        key,
        rate_limit_key,
        ttl,
        RATE_LIMIT_WINDOW_SECONDS,
        RATE_LIMIT_MAX_REQUESTS,
        str(now),
        member,
    )
    t1_lua = time.monotonic()
    elapsed_lua = (t1_lua - t0_lua) * 1000
    _timing_buffer.append(f"TIMING - Combined script execution: {elapsed_lua:.3f} ms")

    status_code = result[0]

    # ── Segment: post-Lua dispatch ────────────────────────────────────────
    t0_post_lua = time.monotonic()

    if status_code == 1:
        await redis.incr("metrics:ingestion_lock_contention")
        response = await handle_existing_idempotency_key(key)
    elif status_code == 2:
        raise HTTPException(
            status_code=429,
            detail="Rate limit exceeded. Please try again later."
        )
    else:
        # status_code == 3: new key — enqueue via ARQ
        payload_dict = payload.model_dump()
        payload_dict["user_id"] = user.id
        payload_dict["full_idempotency_key"] = key
        
        t0_arq = time.monotonic()
        job_func_name = "send_notification_noop" if USE_NOOP_JOB else "send_notification"
        if arq_pool is None:
            raise HTTPException(status_code=500, detail="ARQ queue pool not initialized.")
        await arq_pool.enqueue_job(job_func_name, payload_dict, replay_count=0)
        t1_arq = time.monotonic()
        elapsed_arq = (t1_arq - t0_arq) * 1000
        _timing_buffer.append(f"TIMING - ARQ enqueue: {elapsed_arq:.3f} ms")
        response = JSONResponse(
            status_code=202,
            content={"status": "accepted", "detail": "Idempotency lock acquired; notification queued."},
        )

    t_post_lua_ms = (time.monotonic() - t0_post_lua) * 1000
    t_total_ms    = (time.monotonic() - t_handler_start) * 1000

    # ── Write timing row ─────────────────────────────────────────────────
    _write_ingestion_timing({
        "ts":            time.strftime("%H:%M:%S"),
        "source":        x_request_source,
        "status_code":   status_code,
        "t_pre_lua_ms":  f"{t_pre_lua_ms:.3f}",
        "t_lua_ms":      f"{elapsed_lua:.3f}",
        "t_post_lua_ms": f"{t_post_lua_ms:.3f}",
        "t_total_ms":    f"{t_total_ms:.3f}",
    })

    return response

@app.post("/v1/admin/flush_timing")
async def flush_timing():
    global _timing_buffer
    try:
        TIMING_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
        with open(TIMING_LOG_PATH, "a") as f:
            for msg in _timing_buffer:
                f.write(msg + "\n")
    except Exception as e:
        logger.debug("Failed flushing timing buffer: %s", e)
    timing_count = len(_timing_buffer)
    _timing_buffer.clear()
    return {"status": "success", "flushed_count": timing_count}

# --- Worker Job Processor Implementation ---

async def record_reconcile_latency(elapsed_ms: float):
    async with redis.pipeline() as pipe:
        pipe.lpush(RECONCILE_LATENCY_KEY, elapsed_ms)
        pipe.ltrim(RECONCILE_LATENCY_KEY, 0, RECONCILE_LATENCY_SAMPLE_CAP - 1)
        await pipe.execute()

async def get_reconcile_lock_ttl() -> int:
    samples = await redis.lrange(RECONCILE_LATENCY_KEY, 0, -1)
    if len(samples) < RECONCILE_TTL_MIN_SAMPLES:
        return await get_ingestion_lock_ttl(job_queue)
    values = sorted(float(s) for s in samples)
    max_observed_ms = values[-1]
    ttl_seconds = int(max_observed_ms / 1000) + 2
    return max(10, min(ttl_seconds, 300))

RECONCILE_RELEASE_SCRIPT = """
if redis.call("get", KEYS[1]) == ARGV[1] then
    return redis.call("del", KEYS[1])
end
return 0
"""

_redis_call_log_file = None

def _record_redis_call_latency(op_name: str, duration_ms: float):
    global _redis_call_log_file
    try:
        if _redis_call_log_file is None:
            REDIS_CALL_LATENCY_LOG.parent.mkdir(parents=True, exist_ok=True)
            _redis_call_log_file = open(
                REDIS_CALL_LATENCY_LOG,
                "a",
                buffering=1
            )
        _redis_call_log_file.write(f"{op_name},{duration_ms:.4f}\n")
    except Exception as e:
        logger.debug("Failed recording redis call latency: %s", e)

_current_concurrent_jobs = 0

async def _cache_terminal_idempotency_response(job, sent_ttl: int):
    """Stores terminal JSON response under idempotency key so idempotent queries can replay verbatim."""
    user_id = job.payload.get("user_id", "test_user_id")
    full_key = job.payload.get("full_idempotency_key") or make_idempotency_key(user_id, job.idempotency_key)
    cached_data = {
        "status_code": 200,
        "body": {
            "status": "success",
            "idempotency_key": job.idempotency_key,
            "detail": "Notification delivered successfully."
        }
    }
    await redis.set(full_key, json.dumps(cached_data), ex=sent_ttl)

async def process_notification_job(job):
    global _current_concurrent_jobs
    _current_concurrent_jobs += 1
    try:
        sent_key = f"sent:{job.idempotency_key}"
        worker_id = str(uuid.uuid4())
        sent_ttl = await get_ingestion_lock_ttl(job_queue)

        t_c0 = time.perf_counter()
        state_set = await redis.set(sent_key, "SENDING", nx=True, ex=sent_ttl)
        _record_redis_call_latency("set_sending", (time.perf_counter() - t_c0) * 1000)

        if not state_set:
            t_c0 = time.perf_counter()
            existing_state = await redis.get(sent_key)
            _record_redis_call_latency("get_sent_state", (time.perf_counter() - t_c0) * 1000)

            if existing_state == b"SENT" or existing_state == "SENT":
                await _cache_terminal_idempotency_response(job, sent_ttl)
                await finalize_job_status(job, status="DELIVERED")
                return

            if existing_state == b"SENDING" or existing_state == "SENDING":
                await redis.incr("metrics:reconciliation_hits")
                reconcile_lock = f"reconciling:{job.idempotency_key}"
                reconcile_ttl = await get_reconcile_lock_ttl()
                t_c0 = time.perf_counter()
                got_lock = await redis.set(reconcile_lock, worker_id, nx=True, ex=reconcile_ttl)
                _record_redis_call_latency("set_reconcile_lock", (time.perf_counter() - t_c0) * 1000)

                if not got_lock:
                    await redis.incr("metrics:reconciliation_lock_contention")
                    await requeue_with_backoff(job, reason="reconciliation_in_progress")
                    return

                section_start = time.monotonic()
                try:
                    gateway_status = await gateway_mock.check_status(job.idempotency_key)

                    if gateway_status.was_received:
                        t_c0 = time.perf_counter()
                        await redis.set(sent_key, "SENT", ex=sent_ttl)
                        _record_redis_call_latency("set_sent", (time.perf_counter() - t_c0) * 1000)
                        await _cache_terminal_idempotency_response(job, sent_ttl)
                        await finalize_job_status(job, status="DELIVERED")
                        return

                    try:
                        result = await call_gateway_mock(job.idempotency_key, job.payload)
                        t_c0 = time.perf_counter()
                        await redis.set(sent_key, "SENT", ex=sent_ttl)
                        _record_redis_call_latency("set_sent", (time.perf_counter() - t_c0) * 1000)
                        await _cache_terminal_idempotency_response(job, sent_ttl)
                        await finalize_job_status(job, status="DELIVERED")
                        return
                    except TransientGatewayError as exc:
                        await redis.delete(sent_key)
                        raise exc
                finally:
                    elapsed_ms = (time.monotonic() - section_start) * 1000
                    await record_reconcile_latency(elapsed_ms)
                    t_c0 = time.perf_counter()
                    released = await redis.eval(RECONCILE_RELEASE_SCRIPT, 1, reconcile_lock, worker_id)
                    _record_redis_call_latency("eval_release", (time.perf_counter() - t_c0) * 1000)
                    if not released:
                        await redis.incr("metrics:fencing_token_release_failures")

        reconcile_lock = f"reconciling:{job.idempotency_key}"
        reconcile_ttl = await get_reconcile_lock_ttl()
        t_c0 = time.perf_counter()
        got_lock = await redis.set(reconcile_lock, worker_id, nx=True, ex=reconcile_ttl)
        _record_redis_call_latency("set_reconcile_lock", (time.perf_counter() - t_c0) * 1000)

        if not got_lock:
            await redis.incr("metrics:reconciliation_lock_contention")
            await requeue_with_backoff(job, reason="reconciliation_in_progress")
            return

        try:
            result = await call_gateway_mock(job.idempotency_key, job.payload)
            t_c0 = time.perf_counter()
            await redis.set(sent_key, "SENT", ex=sent_ttl)
            _record_redis_call_latency("set_sent", (time.perf_counter() - t_c0) * 1000)
            await _cache_terminal_idempotency_response(job, sent_ttl)
            await finalize_job_status(job, status="DELIVERED")
        except TransientGatewayError as exc:
            await redis.delete(sent_key)
            raise exc
        finally:
            t_c0 = time.perf_counter()
            released = await redis.eval(RECONCILE_RELEASE_SCRIPT, 1, reconcile_lock, worker_id)
            _record_redis_call_latency("eval_release", (time.perf_counter() - t_c0) * 1000)
            if not released:
                await redis.incr("metrics:fencing_token_release_failures")
    finally:
        _current_concurrent_jobs -= 1

MAX_REPLAY_LIMIT = int(os.getenv("MAX_REPLAY_LIMIT", "5"))

async def replay_dlq_job(dlq_job_id: str, queue):
    try:
        dlq_uuid = uuid.UUID(str(dlq_job_id))
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid UUID format for DLQ job ID.")

    if db_pool is None:
        raise HTTPException(status_code=500, detail="Database pool not initialized.")

    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            UPDATE dead_letter_queue
            SET replay_count = replay_count + 1
            WHERE id = $1 AND replay_count < $2
            RETURNING id, payload, replay_count
            """,
            dlq_uuid, MAX_REPLAY_LIMIT
        )
        if row is None:
            existing = await conn.fetchrow(
                "SELECT replay_count FROM dead_letter_queue WHERE id = $1", dlq_uuid
            )
            if existing and existing["replay_count"] >= MAX_REPLAY_LIMIT:
                raise HTTPException(status_code=400, detail="Maximum replay limit reached.")
            raise HTTPException(status_code=404, detail="DLQ job not found.")

        payload = row["payload"]
        if isinstance(payload, str):
            payload = json.loads(payload)
        await queue.enqueue_job("send_notification", payload, replay_count=row["replay_count"])
        return {"replayed": True, "replay_count": row["replay_count"]}

class DLQJobResponse(BaseModel):
    id: uuid.UUID
    payload: dict
    replay_count: int
    stack_trace: str | None
    created_at: datetime

class ReplayIn(BaseModel):
    dlq_job_id: str

@app.get("/v1/admin/dlq", response_model=list[DLQJobResponse])
async def get_dlq_jobs(
    limit: int = 50,
    offset: int = 0,
    admin_user: User = Security(get_current_admin_user)
):
    if db_pool is None:
        raise HTTPException(status_code=500, detail="Database pool not initialized.")
    async with db_pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT id, payload, replay_count, stack_trace, created_at
            FROM dead_letter_queue
            ORDER BY created_at DESC
            LIMIT $1 OFFSET $2
            """,
            limit, offset
        )
    
    return [
        {
            "id": r["id"],
            "payload": json.loads(r["payload"]) if isinstance(r["payload"], str) else r["payload"],
            "replay_count": r["replay_count"],
            "stack_trace": r["stack_trace"],
            "created_at": r["created_at"]
        }
        for r in rows
    ]

@app.post("/v1/admin/dlq/replay")
async def replay_dlq_endpoint(
    body: ReplayIn,
    admin_user: User = Security(get_current_admin_user)
):
    if arq_pool is None:
        raise HTTPException(status_code=500, detail="ARQ pool not initialized.")
    return await replay_dlq_job(body.dlq_job_id, arq_pool)

class MetricsResponse(BaseModel):
    ingestion_lock_contention: int
    reconciliation_hits: int
    reconciliation_lock_contention: int
    fencing_token_release_failures: int
    contention_requeue_exhaustion: int
    dlq_depth: int

@app.get("/v1/admin/metrics", response_model=MetricsResponse)
async def get_metrics(admin_user: User = Security(get_current_admin_user)):
    if db_pool is None:
        raise HTTPException(status_code=500, detail="Database pool not initialized.")
    
    ingestion_lock_contention = int(await redis.get("metrics:ingestion_lock_contention") or 0)
    reconciliation_hits = int(await redis.get("metrics:reconciliation_hits") or 0)
    reconciliation_lock_contention = int(await redis.get("metrics:reconciliation_lock_contention") or 0)
    fencing_token_release_failures = int(await redis.get("metrics:fencing_token_release_failures") or 0)
    
    async with db_pool.acquire() as conn:
        contention_requeue_exhaustion = await conn.fetchval(
            "SELECT COUNT(*) FROM dead_letter_queue WHERE stack_trace LIKE 'ContentionExhausted:%';"
        )
        dlq_depth = await conn.fetchval(
            "SELECT COUNT(*) FROM dead_letter_queue;"
        )
        
    return {
        "ingestion_lock_contention": ingestion_lock_contention,
        "reconciliation_hits": reconciliation_hits,
        "reconciliation_lock_contention": reconciliation_lock_contention,
        "fencing_token_release_failures": fencing_token_release_failures,
        "contention_requeue_exhaustion": contention_requeue_exhaustion or 0,
        "dlq_depth": dlq_depth or 0
    }
