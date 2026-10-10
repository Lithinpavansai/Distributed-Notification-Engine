import json
import os
import uuid
import time
import asyncio
from unittest.mock import AsyncMock, patch

import pytest
import httpx
from fastapi import HTTPException
from fastapi.responses import JSONResponse
from redis.asyncio import Redis
from arq import create_pool
from arq.connections import RedisSettings
from urllib.parse import urlparse

import app.main as main_module
from app.worker import LuaClaimWorker, WorkerSettings

TEST_USER_ID = "test_user_id"
ADMIN_USER_ID = "admin_user_id"
TEST_KEY_PREFIX = f"idempotency:{TEST_USER_ID}:"

# Use dedicated test Redis database (DB 15) to isolate all test keys from DB 0
TEST_REDIS_URL = os.getenv("TEST_REDIS_URL", "redis://localhost:6379/15")
TEST_REDIS_DB = 15

# Track all created DLQ record IDs during test runs to ensure we only clean test rows
_created_test_dlq_ids: set[uuid.UUID] = set()

@pytest.fixture
def anyio_backend():
    return "asyncio"

async def _clean_isolated_test_redis(client: Redis):
    """Clean only keys within the isolated test Redis database (DB 15)."""
    # Scan and delete keys strictly in DB 15
    async for key in client.scan_iter(match="*", count=200):
        await client.delete(key)

async def _clean_tracked_test_postgres_rows():
    """Clean only specifically created test DLQ rows in PostgreSQL."""
    global _created_test_dlq_ids
    if _created_test_dlq_ids and main_module.db_pool:
        try:
            async with main_module.db_pool.acquire() as conn:
                await conn.execute(
                    "DELETE FROM dead_letter_queue WHERE id = ANY($1::uuid[]);",
                    list(_created_test_dlq_ids)
                )
            _created_test_dlq_ids.clear()
        except Exception:
            pass

@pytest.fixture(autouse=True)
async def setup_environment():
    # Configure the test Redis connection pointing to DB 15
    main_module.redis = Redis.from_url(TEST_REDIS_URL)
    
    parsed = urlparse(TEST_REDIS_URL)
    host = parsed.hostname or 'localhost'
    port = parsed.port or 6379
    password = parsed.password
    arq_settings = RedisSettings(host=host, port=port, database=TEST_REDIS_DB, password=password)
    main_module.arq_pool = await create_pool(arq_settings)
    
    # Configure WorkerSettings to point to DB 15
    WorkerSettings.redis_settings = arq_settings
    
    # Re-initialize the global db_pool for testing
    import asyncpg
    db_url = main_module.get_database_url()
    try:
        main_module.db_pool = await asyncpg.create_pool(db_url)
    except Exception:
        main_module.db_pool = None
    
    # Reset mock gateway variables
    main_module.GATEWAY_SEND_DELAY = 0.0
    main_module.GATEWAY_FORCE_TRANSIENT_ERROR = False
    main_module.GATEWAY_CALL_COUNT = 0
    main_module.GATEWAY_START_EVENT = None
    main_module.GATEWAY_SEND_EVENT = None
    main_module.gateway_mock.delay = 0.0
    main_module.gateway_mock.was_received_map.clear()
    main_module.USE_NOOP_JOB = False
    
    await _clean_isolated_test_redis(main_module.redis)
    await _clean_tracked_test_postgres_rows()

    yield

    await _clean_isolated_test_redis(main_module.redis)
    await _clean_tracked_test_postgres_rows()
    
    if main_module.db_pool:
        await main_module.db_pool.close()
    if main_module.arq_pool:
        await main_module.arq_pool.aclose()
    await main_module.redis.aclose()


# ==============================================================================
# 1. Application Startup and Health Check
# ==============================================================================

@pytest.mark.anyio
async def test_health_check_endpoint():
    transport = httpx.ASGITransport(app=main_module.app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as ac:
        response = await ac.get("/health")
        assert response.status_code == 200
        assert response.json() == {"status": "ok"}


# ==============================================================================
# 2. Valid Notification Request
# ==============================================================================

@pytest.mark.anyio
async def test_valid_notification_request():
    transport = httpx.ASGITransport(app=main_module.app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as ac:
        payload = {
            "idempotency_key": f"valid-key-{uuid.uuid4()}",
            "title": "Welcome",
            "message": "Notification message"
        }
        response = await ac.post("/v1/notifications", json=payload, headers={"x-user-id": TEST_USER_ID})
        assert response.status_code == 202
        data = response.json()
        assert data["status"] == "accepted"
        assert "Idempotency lock acquired" in data["detail"]


# ==============================================================================
# 3. Invalid Request Payload
# ==============================================================================

@pytest.mark.anyio
async def test_invalid_request_payload_missing_idempotency_key():
    transport = httpx.ASGITransport(app=main_module.app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as ac:
        response = await ac.post("/v1/notifications", json={"title": "Missing Key", "message": "No key"})
        assert response.status_code == 422  # Pydantic validation error


# ==============================================================================
# 4. Authentication / Role Behavior
# ==============================================================================

@pytest.mark.anyio
async def test_authentication_and_admin_authorization():
    transport = httpx.ASGITransport(app=main_module.app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as ac:
        # Standard user forbidden on admin endpoints
        resp_user = await ac.get("/v1/admin/metrics", headers={"x-user-id": TEST_USER_ID})
        assert resp_user.status_code == 403

        # Admin user allowed
        resp_admin = await ac.get("/v1/admin/metrics", headers={"x-user-id": ADMIN_USER_ID})
        assert resp_admin.status_code == 200


# ==============================================================================
# 5. First Idempotency Key Acceptance
# ==============================================================================

@pytest.mark.anyio
async def test_first_idempotency_key_acceptance():
    key_id = f"first-key-{uuid.uuid4()}"
    full_key = main_module.make_idempotency_key(TEST_USER_ID, key_id)
    
    transport = httpx.ASGITransport(app=main_module.app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as ac:
        response = await ac.post("/v1/notifications", json={
            "idempotency_key": key_id,
            "title": "A",
            "message": "B"
        }, headers={"x-user-id": TEST_USER_ID})
        assert response.status_code == 202
        
        # In Redis, state must be PROCESSING with valid TTL
        val = await main_module.redis.get(full_key)
        assert val == b"PROCESSING"
        ttl = await main_module.redis.ttl(full_key)
        assert ttl > 0


# ==============================================================================
# 6. Duplicate Idempotency Key Handling & Replay
# ==============================================================================

@pytest.mark.anyio
async def test_handle_existing_idempotency_key_processing():
    key = f"{TEST_KEY_PREFIX}processing"
    await main_module.redis.set(key, b"PROCESSING")
    response = await main_module.handle_existing_idempotency_key(key)
    assert isinstance(response, JSONResponse)
    assert response.status_code == 202
    assert response.headers["Retry-After"] == "5"
    content = json.loads(response.body)
    assert content["status"] == "processing"

@pytest.mark.anyio
async def test_handle_existing_idempotency_key_terminal_json():
    key = f"{TEST_KEY_PREFIX}terminal"
    cached_response = {
        "status_code": 200,
        "body": {"status": "success", "data": "original_payload"}
    }
    await main_module.redis.set(key, json.dumps(cached_response))
    response = await main_module.handle_existing_idempotency_key(key)
    assert isinstance(response, JSONResponse)
    assert response.status_code == 200
    content = json.loads(response.body)
    assert content["status"] == "success"
    assert content["data"] == "original_payload"

@pytest.mark.anyio
async def test_handle_existing_idempotency_key_expired_or_missing():
    key = f"{TEST_KEY_PREFIX}nonexistent"
    await main_module.redis.delete(key)
    with pytest.raises(HTTPException) as exc_info:
        await main_module.handle_existing_idempotency_key(key)
    assert exc_info.value.status_code == 409
    assert "expired" in exc_info.value.detail

@pytest.mark.anyio
async def test_handle_existing_idempotency_key_corrupted():
    key = f"{TEST_KEY_PREFIX}corrupted"
    await main_module.redis.set(key, b"invalid-non-json-value")
    with pytest.raises(HTTPException) as exc_info:
        await main_module.handle_existing_idempotency_key(key)
    assert exc_info.value.status_code == 500
    assert "Corrupted" in exc_info.value.detail

@pytest.mark.anyio
async def test_duplicate_submission_after_delivery_returns_cached_200():
    key_id = f"duplicate-delivered-{uuid.uuid4()}"
    full_key = main_module.make_idempotency_key(TEST_USER_ID, key_id)
    
    # Simulate completed delivery
    job = main_module.NotificationJob(
        idempotency_key=key_id,
        payload={"idempotency_key": key_id, "user_id": TEST_USER_ID, "full_idempotency_key": full_key}
    )
    await main_module.process_notification_job(job)
    
    # Ensure idempotency key is now terminal JSON
    cached = await main_module.redis.get(full_key)
    assert cached is not None
    data = json.loads(cached)
    assert data["status_code"] == 200
    assert data["body"]["status"] == "success"
    
    # POST the same notification again -> must return 200 OK verbatim without re-running
    transport = httpx.ASGITransport(app=main_module.app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as ac:
        response = await ac.post("/v1/notifications", json={
            "idempotency_key": key_id,
            "title": "A",
            "message": "B"
        }, headers={"x-user-id": TEST_USER_ID})
        assert response.status_code == 200
        assert response.json()["status"] == "success"


# ==============================================================================
# 7. Rate-Limit Enforcement
# ==============================================================================

@pytest.mark.anyio
async def test_rate_limit_exceeded():
    transport = httpx.ASGITransport(app=main_module.app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as ac:
        for i in range(main_module.RATE_LIMIT_MAX_REQUESTS):
            key = f"quota-key-{i}-{uuid.uuid4()}"
            response = await ac.post("/v1/notifications", json={
                "idempotency_key": key,
                "title": "A",
                "message": "B"
            }, headers={"x-user-id": TEST_USER_ID})
            assert response.status_code == 202
        
        # Next request must be rate limited
        blocked_key = f"blocked-key-{uuid.uuid4()}"
        response_blocked = await ac.post("/v1/notifications", json={
            "idempotency_key": blocked_key,
            "title": "A",
            "message": "B"
        }, headers={"x-user-id": TEST_USER_ID})
        assert response_blocked.status_code == 429
        assert "Rate limit exceeded" in response_blocked.json()["detail"]


# ==============================================================================
# 8. Redis Enqueueing and Serialization
# ==============================================================================

@pytest.mark.anyio
async def test_redis_enqueueing():
    key_id = f"enqueue-test-{uuid.uuid4()}"
    transport = httpx.ASGITransport(app=main_module.app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as ac:
        response = await ac.post("/v1/notifications", json={
            "idempotency_key": key_id,
            "title": "Enqueue Test",
            "message": "Payload"
        }, headers={"x-user-id": TEST_USER_ID})
        assert response.status_code == 202
        
        # Verify job is present in arq:queue
        queue_count = await main_module.redis.zcard("arq:queue")
        assert queue_count >= 1


# ==============================================================================
# 9. Worker Consumes and Processes a Job (Burst Mode)
# ==============================================================================

@pytest.mark.anyio
async def test_worker_consumes_and_delivers_job():
    from arq.worker import create_worker
    
    key_id = f"worker-test-{uuid.uuid4()}"
    full_key = main_module.make_idempotency_key(TEST_USER_ID, key_id)
    payload = {
        "idempotency_key": key_id,
        "user_id": TEST_USER_ID,
        "full_idempotency_key": full_key,
        "title": "Worker Test",
        "message": "Worker Hello"
    }
    
    # 1. Enqueue job
    await main_module.arq_pool.enqueue_job("send_notification", payload, replay_count=0)
    
    # 2. Run burst worker
    worker = create_worker(
        WorkerSettings,
        redis_pool=main_module.arq_pool,
        burst=True,
        handle_signals=False
    )
    await worker.async_run()
    
    # 3. Assert Redis states
    sent_key = f"sent:{key_id}"
    assert await main_module.redis.get(sent_key) == b"SENT"
    
    cached = await main_module.redis.get(full_key)
    assert cached is not None
    data = json.loads(cached)
    assert data["status_code"] == 200
    assert data["body"]["status"] == "success"
    
    # 4. Gateway received map contains the key
    assert main_module.gateway_mock.was_received_map.get(key_id) is True


# ==============================================================================
# 10. Transient Errors and Retries
# ==============================================================================

@pytest.mark.anyio
async def test_transient_gateway_error_cleans_sent_key():
    idempotency_key = f"transient-{uuid.uuid4()}"
    job = main_module.NotificationJob(idempotency_key=idempotency_key, payload={"item": "data"})
    main_module.GATEWAY_FORCE_TRANSIENT_ERROR = True
    
    with pytest.raises(main_module.TransientGatewayError):
        await main_module.process_notification_job(job)
        
    sent_key = f"sent:{idempotency_key}"
    assert await main_module.redis.get(sent_key) is None


# ==============================================================================
# 11. Dead-Letter Queue & Contention Exhaustion
# ==============================================================================

@pytest.mark.anyio
async def test_requeue_at_limit_inserts_into_dlq():
    job = main_module.NotificationJob(
        idempotency_key=f"dlq-key-{uuid.uuid4()}",
        payload={"test": "dlq_payload"}
    )
    job.contention_requeue_count = 5  # == CONTENTION_MAX_ATTEMPTS
    
    await main_module.requeue_with_backoff(job, reason="lock_conflict_exhausted")
    
    async with main_module.db_pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT id, payload, replay_count, stack_trace FROM dead_letter_queue WHERE payload::text LIKE '%dlq_payload%';"
        )
        assert len(rows) >= 1
        test_row = rows[0]
        _created_test_dlq_ids.add(test_row["id"])
        assert json.loads(test_row["payload"]) == {"test": "dlq_payload"}
        assert "ContentionExhausted" in test_row["stack_trace"]


# ==============================================================================
# 12. DLQ Listing and Replay Endpoints
# ==============================================================================

@pytest.mark.anyio
async def test_admin_dlq_replay_endpoint():
    job_id = uuid.uuid4()
    _created_test_dlq_ids.add(job_id)
    payload = {"idempotency_key": f"dlq-replay-test-{job_id}", "val": 100}
    
    async with main_module.db_pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO dead_letter_queue (id, payload, replay_count, stack_trace)
            VALUES ($1, $2, $3, $4)
            """,
            job_id,
            json.dumps(payload),
            0,
            "mock trace"
        )
        
    transport = httpx.ASGITransport(app=main_module.app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as ac:
        # Replay
        response = await ac.post(
            "/v1/admin/dlq/replay",
            json={"dlq_job_id": str(job_id)},
            headers={"x-user-id": ADMIN_USER_ID}
        )
        assert response.status_code == 200
        res = response.json()
        assert res["replayed"] is True
        assert res["replay_count"] == 1
        
        # Verify db incremented
        async with main_module.db_pool.acquire() as conn:
            cnt = await conn.fetchval("SELECT replay_count FROM dead_letter_queue WHERE id = $1", job_id)
            assert cnt == 1


# ==============================================================================
# 13. No-Op Mode Behavior
# ==============================================================================

@pytest.mark.anyio
async def test_noop_mode_execution():
    main_module.USE_NOOP_JOB = True
    
    key_id = f"noop-key-{uuid.uuid4()}"
    job = main_module.NotificationJob(idempotency_key=key_id, payload={"idempotency_key": key_id})
    
    # In no-op mode, send_notification_noop returns immediately without gateway dispatch
    await main_module.send_notification_noop(None, {"idempotency_key": key_id})
    assert main_module.gateway_mock.was_received_map.get(key_id) is None


# ==============================================================================
# 14. Logging & Instrumentation Failures Do Not Crash Ingestion
# ==============================================================================

@pytest.mark.anyio
async def test_telemetry_write_failure_does_not_break_ingestion():
    with patch("builtins.open", side_effect=OSError("Disk full / permission denied")):
        # Telemetry helpers catch exceptions safely
        main_module._write_ingestion_timing({"ts": "12:00:00"})
        main_module._record_job_exec_timing(10.0)
        main_module._record_redis_call_latency("ping", 1.0)


# ==============================================================================
# 15. Concurrent Duplicate Requests (Fencing and Reconciliation Race)
# ==============================================================================

@pytest.mark.anyio
async def test_concurrent_processing_race_condition():
    idempotency_key = f"concurrency-race-{uuid.uuid4()}"
    
    main_module.GATEWAY_CALL_COUNT = 0
    main_module.gateway_mock.was_received_map.clear()
    
    main_module.GATEWAY_START_EVENT = asyncio.Event()
    main_module.GATEWAY_SEND_EVENT = asyncio.Event()
    
    job1 = main_module.NotificationJob(idempotency_key=idempotency_key, payload={"item": "data"})
    job2 = main_module.NotificationJob(idempotency_key=idempotency_key, payload={"item": "data"})
    
    task1 = asyncio.create_task(main_module.process_notification_job(job1))
    await main_module.GATEWAY_START_EVENT.wait()
    
    # Second job arrives while first is in-flight -> fails reconcile lock and requeues
    await main_module.process_notification_job(job2)
    assert job2.requeued is True
    assert job2.requeue_reason == "reconciliation_in_progress"
    
    # Let first job finish
    main_module.GATEWAY_SEND_EVENT.set()
    await task1
    
    assert job1.final_status == "DELIVERED"
    assert main_module.GATEWAY_CALL_COUNT == 1


# ==============================================================================
# 16. Redis Unavailable Handling
# ==============================================================================

@pytest.mark.anyio
async def test_redis_unavailable_handling():
    with patch.object(main_module.redis, "eval", side_effect=ConnectionError("Redis connection refused")):
        transport = httpx.ASGITransport(app=main_module.app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as ac:
            with pytest.raises(ConnectionError):
                await ac.post(
                    "/v1/notifications",
                    json={"idempotency_key": "redis-down-key", "title": "A", "message": "B"},
                    headers={"x-user-id": TEST_USER_ID}
                )


# ==============================================================================
# 17. PostgreSQL Unavailable Handling
# ==============================================================================

@pytest.mark.anyio
async def test_postgresql_unavailable_handling():
    original_db_pool = main_module.db_pool
    main_module.db_pool = None
    try:
        transport = httpx.ASGITransport(app=main_module.app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as ac:
            # DLQ listing should return 500 when database pool is not available
            resp = await ac.get("/v1/admin/dlq", headers={"x-user-id": ADMIN_USER_ID})
            assert resp.status_code == 500
            assert "Database pool not initialized" in resp.json()["detail"]
            
            # Metrics endpoint should also return 500
            resp_m = await ac.get("/v1/admin/metrics", headers={"x-user-id": ADMIN_USER_ID})
            assert resp_m.status_code == 500
    finally:
        main_module.db_pool = original_db_pool


# ==============================================================================
# 18. End-to-End Integration Flow: Ingestion -> ARQ -> Worker -> Replay
# ==============================================================================

@pytest.mark.anyio
async def test_full_end_to_end_notification_lifecycle():
    from arq.worker import create_worker

    unique_key = f"e2e-live-{uuid.uuid4()}"
    full_key = main_module.make_idempotency_key(TEST_USER_ID, unique_key)
    
    # Step 1: Client submits new notification via API
    transport = httpx.ASGITransport(app=main_module.app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as ac:
        response_init = await ac.post("/v1/notifications", json={
            "idempotency_key": unique_key,
            "title": "Welcome User",
            "message": "Your account is activated."
        }, headers={"x-user-id": TEST_USER_ID})
        assert response_init.status_code == 202
        assert response_init.json()["status"] == "accepted"
        
        # Step 2: Intermediate polling while job is in queue returns 202 PROCESSING
        response_poll = await ac.post("/v1/notifications", json={
            "idempotency_key": unique_key,
            "title": "Welcome User",
            "message": "Your account is activated."
        }, headers={"x-user-id": TEST_USER_ID})
        assert response_poll.status_code == 202
        assert response_poll.headers["Retry-After"] == "5"
        assert response_poll.json()["status"] == "processing"
        
        # Step 3: Worker processes the queue in burst mode
        worker = create_worker(
            WorkerSettings,
            redis_pool=main_module.arq_pool,
            burst=True,
            handle_signals=False
        )
        await worker.async_run()
        
        # Step 4: Verify worker delivered the notification through configured mock gateway
        assert main_module.gateway_mock.was_received_map.get(unique_key) is True
        
        # Step 5: Duplicate request after completion returns 200 OK verbatim
        response_done = await ac.post("/v1/notifications", json={
            "idempotency_key": unique_key,
            "title": "Welcome User",
            "message": "Your account is activated."
        }, headers={"x-user-id": TEST_USER_ID})
        assert response_done.status_code == 200
        data_done = response_done.json()
        assert data_done["status"] == "success"
        assert data_done["idempotency_key"] == unique_key
        assert "delivered" in data_done["detail"].lower()
