import json
import pytest
import httpx
import asyncio
from fastapi import HTTPException
from fastapi.responses import JSONResponse
from redis.asyncio import Redis
from arq import create_pool
from arq.connections import RedisSettings
from urllib.parse import urlparse
import app.main as main_module

# Test key prefix to ensure we clean up after our tests
TEST_USER_ID = "test_user_id"
TEST_KEY_PREFIX = f"idempotency:{TEST_USER_ID}:"

@pytest.fixture
def anyio_backend():
    return "asyncio"

@pytest.fixture(autouse=True)
async def setup_redis_client():
    # Re-initialize the global redis client for the active event loop of this test
    main_module.redis = Redis.from_url(main_module.redis_url)
    
    # Re-initialize the global arq_pool connection
    parsed = urlparse(main_module.redis_url)
    host = parsed.hostname or 'localhost'
    port = parsed.port or 6379
    db = int(parsed.path.lstrip('/') or 0)
    password = parsed.password
    arq_settings = RedisSettings(host=host, port=port, database=db, password=password)
    main_module.arq_pool = await create_pool(arq_settings)
    
    # Re-initialize the global db_pool for testing
    import asyncpg
    import os
    db_url = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/notification_db")
    if "postgresql+asyncpg://" in db_url:
        db_url = db_url.replace("postgresql+asyncpg://", "postgresql://")
    main_module.db_pool = await asyncpg.create_pool(db_url)
    
    yield
    
    await main_module.redis.aclose()
    if main_module.arq_pool:
        await main_module.arq_pool.aclose()
    if main_module.db_pool:
        await main_module.db_pool.close()

@pytest.fixture(autouse=True)
async def cleanup_redis(setup_redis_client):
    # Setup - reset mock gateway variables
    main_module.GATEWAY_SEND_DELAY = 0.0
    main_module.GATEWAY_FORCE_TRANSIENT_ERROR = False
    main_module.GATEWAY_CALL_COUNT = 0
    main_module.gateway_mock.delay = 0.0
    main_module.gateway_mock.was_received_map.clear()
    # Clean up Redis keys before the test runs
    await main_module.redis.flushdb()
    
    yield
    
    # Clean up Redis keys after the test runs
    await main_module.redis.flushdb()

    # Also clean up dead_letter_queue table in Postgres
    import asyncpg
    import os
    db_url = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/notification_db")
    if "postgresql+asyncpg://" in db_url:
        db_url = db_url.replace("postgresql+asyncpg://", "postgresql://")
    conn = await asyncpg.connect(db_url)
    try:
        await conn.execute("DELETE FROM dead_letter_queue;")
    finally:
        await conn.close()

# --- Unit tests for handle_existing_idempotency_key function branches ---

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


# --- End-to-end integration tests using httpx.AsyncClient ---

@pytest.mark.anyio
async def test_api_idempotency_flow():
    transport = httpx.ASGITransport(app=main_module.app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as ac:
        idempotency_key = "unique-flow-key"
        payload = {"idempotency_key": idempotency_key, "title": "A", "message": "B"}
        
        response_1 = await ac.post("/v1/notifications", json=payload)
        assert response_1.status_code == 202
        data_1 = response_1.json()
        assert data_1["status"] == "accepted"
        
        response_2 = await ac.post("/v1/notifications", json=payload)
        assert response_2.status_code == 202
        assert response_2.headers["Retry-After"] == "5"
        assert response_2.json()["status"] == "processing"
        
        processing_key = "unique-processing-key"
        real_key = main_module.make_idempotency_key(TEST_USER_ID, processing_key)
        await main_module.redis.set(real_key, b"PROCESSING")
        
        payload_processing = {"idempotency_key": processing_key, "title": "A", "message": "B"}
        response_3 = await ac.post("/v1/notifications", json=payload_processing)
        assert response_3.status_code == 202
        assert response_3.headers["Retry-After"] == "5"
        assert response_3.json()["status"] == "processing"


# --- Rate Limit and ARQ enqueuing tests ---

@pytest.mark.anyio
async def test_rate_limit_under_quota():
    transport = httpx.ASGITransport(app=main_module.app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as ac:
        for i in range(main_module.RATE_LIMIT_MAX_REQUESTS):
            idempotency_key = f"under-quota-key-{i}"
            payload = {"idempotency_key": idempotency_key, "title": "A", "message": "B"}
            response = await ac.post("/v1/notifications", json=payload)
            assert response.status_code == 202
            assert response.json()["status"] == "accepted"

@pytest.mark.anyio
async def test_rate_limit_exceeded():
    transport = httpx.ASGITransport(app=main_module.app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as ac:
        for i in range(main_module.RATE_LIMIT_MAX_REQUESTS):
            idempotency_key = f"quota-fill-key-{i}"
            payload = {"idempotency_key": idempotency_key, "title": "A", "message": "B"}
            response = await ac.post("/v1/notifications", json=payload)
            assert response.status_code == 202
            assert response.json()["status"] == "accepted"
        
        blocked_key = "blocked-request-key"
        payload_blocked = {"idempotency_key": blocked_key, "title": "A", "message": "B"}
        response_blocked = await ac.post("/v1/notifications", json=payload_blocked)
        assert response_blocked.status_code == 429
        assert "Rate limit exceeded" in response_blocked.json()["detail"]
        
        real_key = main_module.make_idempotency_key(TEST_USER_ID, blocked_key)
        assert await main_module.redis.get(real_key) is None

@pytest.mark.anyio
async def test_idempotency_key_remains_processing():
    transport = httpx.ASGITransport(app=main_module.app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as ac:
        idempotency_key = "check-processing-key"
        payload = {"idempotency_key": idempotency_key, "title": "A", "message": "B"}
        
        response = await ac.post("/v1/notifications", json=payload)
        assert response.status_code == 202
        assert response.json()["status"] == "accepted"
        
        real_key = main_module.make_idempotency_key(TEST_USER_ID, idempotency_key)
        value = await main_module.redis.get(real_key)
        assert value == b"PROCESSING"


# --- Worker Job Processor and Concurrency Tests ---

@pytest.mark.anyio
async def test_concurrent_processing_race_condition():
    successes = 0
    runs = 50
    
    for i in range(runs):
        idempotency_key = f"concurrency-race-key-{i}"
        
        # Reset mock states for this run
        main_module.GATEWAY_CALL_COUNT = 0
        main_module.gateway_mock.was_received_map.clear()
        
        # Use asyncio.Events to coordinate deterministically
        main_module.GATEWAY_START_EVENT = asyncio.Event()
        main_module.GATEWAY_SEND_EVENT = asyncio.Event()
        
        main_module.GATEWAY_SEND_DELAY = 0.0
        main_module.gateway_mock.delay = 0.0
        
        job1 = main_module.NotificationJob(idempotency_key=idempotency_key, payload={"item": "data"})
        job2 = main_module.NotificationJob(idempotency_key=idempotency_key, payload={"item": "data"})
        
        # Spawn Job 1 (first worker).
        # It sets status to 'SENDING', gets reconcile lock, and starts calling call_gateway_mock.
        task1 = asyncio.create_task(main_module.process_notification_job(job1))
        
        # Deterministically wait until Job 1 has actually entered call_gateway_mock
        # and is about to block waiting for our signal
        await main_module.GATEWAY_START_EVENT.wait()
        
        # Spawn Job 2 (second worker).
        # Since Job 1 is still in flight (holding both sent_key and reconcile_lock),
        # Job 2 must fail to acquire got_lock and exit immediately to be requeued.
        await main_module.process_notification_job(job2)
        
        # Assertions for Job 2:
        # - Job 2 must fail lock acquisition and requeue
        assert job2.requeued is True
        assert job2.requeue_reason == "reconciliation_in_progress"
        assert job2.final_status is None
        
        # Now trigger Job 1 to finish
        main_module.GATEWAY_SEND_EVENT.set()
        await task1
        
        # Assertions for Job 1:
        # - Job 1 must finish and be DELIVERED
        assert job1.final_status == "DELIVERED"
        
        # - call_gateway_mock must be called exactly once
        assert main_module.GATEWAY_CALL_COUNT == 1
        
        successes += 1

    print(f"\n[Race Condition Test] Pass Count: {successes} out of {runs} runs succeeded.")
    assert successes == runs


@pytest.mark.anyio
async def test_reconcile_lock_acquisition_failure():
    idempotency_key = "lock-fail-key"
    sent_key = f"sent:{idempotency_key}"
    await main_module.redis.set(sent_key, "SENDING")
    
    # Manually hold reconcile lock
    reconcile_lock = f"reconciling:{idempotency_key}"
    await main_module.redis.set(reconcile_lock, "some-other-worker-id")
    
    job = main_module.NotificationJob(idempotency_key=idempotency_key, payload={"item": "data"})
    await main_module.process_notification_job(job)
    
    assert job.requeued is True
    assert job.requeue_reason == "reconciliation_in_progress"


@pytest.mark.anyio
async def test_transient_gateway_error_deletes_sent_key():
    idempotency_key = "transient-err-key"
    job = main_module.NotificationJob(idempotency_key=idempotency_key, payload={"item": "data"})
    
    # Force transient error
    main_module.GATEWAY_FORCE_TRANSIENT_ERROR = True
    
    with pytest.raises(main_module.TransientGatewayError):
        await main_module.process_notification_job(job)
        
    sent_key = f"sent:{idempotency_key}"
    assert await main_module.redis.get(sent_key) is None


# --- Requeue and DLQ tests ---

@pytest.mark.anyio
async def test_requeue_with_backoff_under_limit():
    from unittest.mock import AsyncMock
    import asyncpg
    import os
    
    original_pool = main_module.arq_pool
    main_module.arq_pool = AsyncMock()
    
    db_url = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/notification_db")
    if "postgresql+asyncpg://" in db_url:
        db_url = db_url.replace("postgresql+asyncpg://", "postgresql://")
        
    try:
        job = main_module.NotificationJob(idempotency_key="requeue-under", payload={"test": "requeue_under"})
        job.contention_requeue_count = 2  # < 5
        
        # Call requeue_with_backoff
        await main_module.requeue_with_backoff(job, reason="lock_conflict")
        
        # Assert ARQ pool enqueue was called with correct delay
        main_module.arq_pool.enqueue_job.assert_called_once()
        args, kwargs = main_module.arq_pool.enqueue_job.call_args
        assert args[0] == "send_notification"
        assert args[1] == {"test": "requeue_under"}
        assert kwargs["replay_count"] == 0
        assert "_defer_by" in kwargs
        assert kwargs["_defer_by"] == job.last_delay
        
        # Assert contention_requeue_count was incremented to 3
        assert job.contention_requeue_count == 3
        assert job.requeued is True
        
        # Assert no row was written to DLQ
        conn = await asyncpg.connect(db_url)
        try:
            count = await conn.fetchval("SELECT count(*) FROM dead_letter_queue;")
            assert count == 0
        finally:
            await conn.close()
            
    finally:
        main_module.arq_pool = original_pool


@pytest.mark.anyio
async def test_requeue_with_backoff_at_limit_inserts_to_dlq():
    from unittest.mock import AsyncMock
    import asyncpg
    import os
    
    original_pool = main_module.arq_pool
    main_module.arq_pool = AsyncMock()
    
    db_url = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/notification_db")
    if "postgresql+asyncpg://" in db_url:
        db_url = db_url.replace("postgresql+asyncpg://", "postgresql://")
        
    try:
        job = main_module.NotificationJob(idempotency_key="requeue-limit", payload={"test": "requeue_limit"})
        job.contention_requeue_count = 5  # == CONTENTION_MAX_ATTEMPTS
        job.replay_count = 1
        
        # Call requeue_with_backoff
        await main_module.requeue_with_backoff(job, reason="lock_conflict_exhausted")
        
        # Assert ARQ pool was NOT called
        main_module.arq_pool.enqueue_job.assert_not_called()
        
        # Query the row back out of Postgres
        conn = await asyncpg.connect(db_url)
        try:
            rows = await conn.fetch("SELECT id, payload, replay_count, stack_trace, created_at FROM dead_letter_queue;")
            assert len(rows) == 1
            row = rows[0]
            
            # Assert payload, replay_count, and stack_trace match what was passed
            payload_read = json.loads(row["payload"])
            assert payload_read == {"test": "requeue_limit"}
            assert row["replay_count"] == 1
            assert "ContentionExhausted: lock_conflict_exhausted" in row["stack_trace"]
            assert "max_attempts=5 reached" in row["stack_trace"]
            
            # Assert id and created_at were auto-populated by database (not None, not app-supplied)
            assert row["id"] is not None
            assert row["created_at"] is not None
            
            import uuid
            from datetime import datetime
            assert isinstance(row["id"], uuid.UUID)
            assert isinstance(row["created_at"], datetime)
        finally:
            await conn.close()
            
    finally:
        main_module.arq_pool = original_pool


@pytest.mark.anyio
async def test_requeue_with_backoff_raises_runtime_error_when_no_pool():
    original_pool = main_module.arq_pool
    main_module.arq_pool = None
    
    try:
        job = main_module.NotificationJob(idempotency_key="requeue-error", payload={"test": "requeue_error"})
        job.contention_requeue_count = 2  # < 5
        
        with pytest.raises(RuntimeError) as exc_info:
            await main_module.requeue_with_backoff(job, reason="lock_conflict")
            
        assert "requeue_with_backoff called with no arq_pool available" in str(exc_info.value)
        
    finally:
        main_module.arq_pool = original_pool


@pytest.mark.anyio
async def test_worker_dispatch_end_to_end():
    from arq.worker import create_worker
    from app.worker import WorkerSettings
    
    idempotency_key = "e2e-dispatch-key"
    payload = {"idempotency_key": idempotency_key, "title": "E2E Test", "message": "Hello World"}
    
    # 1. Enqueue the job exactly like create_notification / requeue does
    await main_module.arq_pool.enqueue_job("send_notification", payload, replay_count=0)
    
    # 2. Run the worker in burst mode to process the queued job
    worker = create_worker(
        WorkerSettings,
        redis_pool=main_module.arq_pool,
        burst=True,
        handle_signals=False
    )
    await worker.async_run()
    
    # 3. Assert Redis state is correct (sent_key == b"SENT")
    sent_key = f"sent:{idempotency_key}"
    val = await main_module.redis.get(sent_key)
    assert val == b"SENT"
    
    # 4. Assert Postgres/DLQ state is correct (should be empty since it succeeded)
    import asyncpg
    import os
    db_url = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/notification_db")
    if "postgresql+asyncpg://" in db_url:
        db_url = db_url.replace("postgresql+asyncpg://", "postgresql://")
    conn = await asyncpg.connect(db_url)
    try:
        count = await conn.fetchval("SELECT count(*) FROM dead_letter_queue;")
        assert count == 0
    finally:
        await conn.close()


@pytest.mark.anyio
async def test_dlq_endpoints_forbidden_for_non_admin():
    transport = httpx.ASGITransport(app=main_module.app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as ac:
        # 1. Non-admin accessing GET /v1/admin/dlq
        response = await ac.get("/v1/admin/dlq")
        assert response.status_code == 403
        assert "privileges required" in response.json()["detail"]

        response = await ac.get("/v1/admin/dlq", headers={"x-user-id": "test_user_id"})
        assert response.status_code == 403

        # 2. Non-admin accessing POST /v1/admin/dlq/replay
        response = await ac.post("/v1/admin/dlq/replay", json={"dlq_job_id": "00000000-0000-0000-0000-000000000000"})
        assert response.status_code == 403
        
        response = await ac.post(
            "/v1/admin/dlq/replay", 
            json={"dlq_job_id": "00000000-0000-0000-0000-000000000000"},
            headers={"x-user-id": "test_user_id"}
        )
        assert response.status_code == 403


@pytest.mark.anyio
async def test_admin_can_list_dlq_jobs():
    async with main_module.db_pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO dead_letter_queue (id, payload, replay_count, stack_trace)
            VALUES ($1, $2, $3, $4)
            """,
            "11111111-1111-1111-1111-111111111111",
            '{"item": "val"}',
            0,
            "dummy trace"
        )
    
    transport = httpx.ASGITransport(app=main_module.app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as ac:
        response = await ac.get("/v1/admin/dlq", headers={"x-user-id": "admin_user_id"})
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 1
        assert data[0]["id"] == "11111111-1111-1111-1111-111111111111"
        assert data[0]["payload"] == {"item": "val"}
        assert data[0]["replay_count"] == 0
        assert data[0]["stack_trace"] == "dummy trace"


@pytest.mark.anyio
async def test_admin_replay_increments_count_and_enqueues():
    import uuid
    job_id = "22222222-2222-2222-2222-222222222222"
    payload = {"idempotency_key": "replay-test-key", "val": 42}
    
    async with main_module.db_pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO dead_letter_queue (id, payload, replay_count, stack_trace)
            VALUES ($1, $2, $3, $4)
            """,
            job_id,
            json.dumps(payload),
            1,
            "dummy trace"
        )
    
    from unittest.mock import AsyncMock
    original_pool = main_module.arq_pool
    main_module.arq_pool = AsyncMock()
    
    try:
        transport = httpx.ASGITransport(app=main_module.app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as ac:
            response = await ac.post(
                "/v1/admin/dlq/replay",
                json={"dlq_job_id": job_id},
                headers={"x-user-id": "admin_user_id"}
            )
            assert response.status_code == 200
            res_data = response.json()
            assert res_data["replayed"] is True
            assert res_data["replay_count"] == 2
            
            main_module.arq_pool.enqueue_job.assert_called_once_with(
                "send_notification",
                payload,
                replay_count=2
            )
            
            async with main_module.db_pool.acquire() as conn:
                db_val = await conn.fetchval(
                    "SELECT replay_count FROM dead_letter_queue WHERE id = $1",
                    uuid.UUID(job_id)
                )
                assert db_val == 2
    finally:
        main_module.arq_pool = original_pool


@pytest.mark.anyio
async def test_concurrent_replay_calls_limit_boundary():
    import uuid
    runs = 20
    success_count = 0
    
    transport = httpx.ASGITransport(app=main_module.app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as ac:
        for i in range(runs):
            job_id = str(uuid.uuid4())
            payload = {"idempotency_key": f"concurrency-replay-{i}", "val": i}
            
            async with main_module.db_pool.acquire() as conn:
                await conn.execute("DELETE FROM dead_letter_queue;")
                await conn.execute(
                    """
                    INSERT INTO dead_letter_queue (id, payload, replay_count, stack_trace)
                    VALUES ($1, $2, $3, $4)
                    """,
                    uuid.UUID(job_id),
                    json.dumps(payload),
                    main_module.MAX_REPLAY_LIMIT - 1,
                    "concurrency trace"
                )
            
            t1 = ac.post(
                "/v1/admin/dlq/replay",
                json={"dlq_job_id": job_id},
                headers={"x-user-id": "admin_user_id"}
            )
            t2 = ac.post(
                "/v1/admin/dlq/replay",
                json={"dlq_job_id": job_id},
                headers={"x-user-id": "admin_user_id"}
            )
            
            r1, r2 = await asyncio.gather(t1, t2)
            
            status_codes = [r1.status_code, r2.status_code]
            if 200 in status_codes and 400 in status_codes:
                success_count += 1
            
            async with main_module.db_pool.acquire() as conn:
                db_val = await conn.fetchval(
                    "SELECT replay_count FROM dead_letter_queue WHERE id = $1",
                    uuid.UUID(job_id)
                )
                assert db_val == main_module.MAX_REPLAY_LIMIT

    print(f"\n[Concurrent Replay Test] Pass Rate: {success_count} / {runs} runs succeeded.")
    assert success_count == runs


@pytest.mark.anyio
async def test_metrics_ingestion_lock_contention():
    transport = httpx.ASGITransport(app=main_module.app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as ac:
        idempotency_key = "ingestion-contention-metric-key"
        payload = {"idempotency_key": idempotency_key, "title": "A", "message": "B"}
        
        # First request succeeds
        resp1 = await ac.post("/v1/notifications", json=payload)
        assert resp1.status_code == 202
        
        # Second request triggers ingestion lock contention
        resp2 = await ac.post("/v1/notifications", json=payload)
        assert resp2.status_code == 202
        
        # Verify the counter
        cnt = int(await main_module.redis.get("metrics:ingestion_lock_contention") or 0)
        assert cnt == 1


@pytest.mark.anyio
async def test_metrics_reconciliation_hits_and_contention():
    idempotency_key = "reconcile-metrics-key"
    
    main_module.GATEWAY_CALL_COUNT = 0
    main_module.gateway_mock.was_received_map.clear()
    main_module.GATEWAY_START_EVENT = asyncio.Event()
    main_module.GATEWAY_SEND_EVENT = asyncio.Event()
    
    job1 = main_module.NotificationJob(idempotency_key=idempotency_key, payload={"item": "data"})
    job2 = main_module.NotificationJob(idempotency_key=idempotency_key, payload={"item": "data"})
    
    # Task 1 starts and blocks inside gateway call
    task1 = asyncio.create_task(main_module.process_notification_job(job1))
    await main_module.GATEWAY_START_EVENT.wait()
    
    # Task 2 runs. Since Task 1 is still in flight, Task 2 hits reconciler branch
    # and fails to acquire the reconcile_lock.
    await main_module.process_notification_job(job2)
    
    # Trigger Task 1 to complete
    main_module.GATEWAY_SEND_EVENT.set()
    await task1
    
    # Verify both metrics:
    rec_hits = int(await main_module.redis.get("metrics:reconciliation_hits") or 0)
    assert rec_hits == 1
    
    rec_lock_cont = int(await main_module.redis.get("metrics:reconciliation_lock_contention") or 0)
    assert rec_lock_cont == 1


@pytest.mark.anyio
async def test_metrics_fencing_token_release_failures():
    idempotency_key = "fencing-metric-key"
    main_module.GATEWAY_CALL_COUNT = 0
    main_module.gateway_mock.was_received_map.clear()
    main_module.GATEWAY_START_EVENT = asyncio.Event()
    main_module.GATEWAY_SEND_EVENT = asyncio.Event()
    
    job = main_module.NotificationJob(idempotency_key=idempotency_key, payload={"item": "data"})
    
    # Task starts as original sender
    task = asyncio.create_task(main_module.process_notification_job(job))
    await main_module.GATEWAY_START_EVENT.wait()
    
    # Manually overwrite the lock in Redis to simulate TTL expiry/steal
    reconcile_lock = f"reconciling:{idempotency_key}"
    await main_module.redis.set(reconcile_lock, "stolen-worker-id")
    
    # Complete gateway call
    main_module.GATEWAY_SEND_EVENT.set()
    await task
    
    # Verify release failure
    fencing_fails = int(await main_module.redis.get("metrics:fencing_token_release_failures") or 0)
    assert fencing_fails == 1


@pytest.mark.anyio
async def test_metrics_distinctness_sender_lock_contention():
    idempotency_key = "distinct-contention-key"
    
    # Pre-acquire the lock
    reconcile_lock = f"reconciling:{idempotency_key}"
    await main_module.redis.set(reconcile_lock, "stolen-worker-id")
    
    # Run process_notification_job without setting "sent" key (so it runs original sender path)
    job = main_module.NotificationJob(idempotency_key=idempotency_key, payload={"item": "data"})
    await main_module.process_notification_job(job)
    
    # Verify metrics:
    rec_lock_cont = int(await main_module.redis.get("metrics:reconciliation_lock_contention") or 0)
    assert rec_lock_cont == 1
    
    rec_hits = int(await main_module.redis.get("metrics:reconciliation_hits") or 0)
    assert rec_hits == 0


@pytest.mark.anyio
async def test_admin_metrics_endpoint():
    async with main_module.db_pool.acquire() as conn:
        await conn.execute("DELETE FROM dead_letter_queue;")
        await conn.execute(
            """
            INSERT INTO dead_letter_queue (payload, replay_count, stack_trace)
            VALUES ($1, $2, $3);
            """,
            '{"val": 1}', 0, "ContentionExhausted: lock contention"
        )
        await conn.execute(
            """
            INSERT INTO dead_letter_queue (payload, replay_count, stack_trace)
            VALUES ($1, $2, $3);
            """,
            '{"val": 2}', 0, "OtherError: standard gateway failure"
        )
        
    await main_module.redis.set("metrics:ingestion_lock_contention", 10)
    await main_module.redis.set("metrics:reconciliation_hits", 20)
    await main_module.redis.set("metrics:reconciliation_lock_contention", 30)
    await main_module.redis.set("metrics:fencing_token_release_failures", 40)
    
    transport = httpx.ASGITransport(app=main_module.app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as ac:
        resp_user = await ac.get("/v1/admin/metrics", headers={"x-user-id": "test_user_id"})
        assert resp_user.status_code == 403
        
        resp_admin = await ac.get("/v1/admin/metrics", headers={"x-user-id": "admin_user_id"})
        assert resp_admin.status_code == 200
        
        metrics = resp_admin.json()
        assert metrics["ingestion_lock_contention"] == 10
        assert metrics["reconciliation_hits"] == 20
        assert metrics["reconciliation_lock_contention"] == 30
        assert metrics["fencing_token_release_failures"] == 40
        assert metrics["contention_requeue_exhaustion"] == 1
        assert metrics["dlq_depth"] == 2


@pytest.mark.anyio
async def test_combined_script_outcome_existing_key():
    import time
    key = f"{TEST_KEY_PREFIX}existing_key"
    await main_module.redis.set(key, b"PROCESSING")
    
    rate_limit_key = f"rate_limit:{TEST_USER_ID}"
    initial_count = await main_module.redis.zcard(rate_limit_key)
    
    result = await main_module.redis.eval(
        main_module.COMBINED_LOCK_RATE_LIMIT_LUA,
        2,
        key,
        rate_limit_key,
        60,
        60,
        5,
        str(time.time()),
        key
    )
    assert result[0] == 1
    assert result[1] == b"PROCESSING"
    
    final_count = await main_module.redis.zcard(rate_limit_key)
    assert final_count == initial_count


@pytest.mark.anyio
async def test_combined_script_outcome_rate_limit_exceeded():
    import time
    rate_limit_key = f"rate_limit:{TEST_USER_ID}"
    now = time.time()
    for i in range(5):
        await main_module.redis.zadd(rate_limit_key, {f"dummy_member_{i}": now})
        
    key = f"{TEST_KEY_PREFIX}new_key_rate_limited"
    
    result = await main_module.redis.eval(
        main_module.COMBINED_LOCK_RATE_LIMIT_LUA,
        2,
        key,
        rate_limit_key,
        60,
        60,
        5,
        str(now),
        key
    )
    assert result[0] == 2
    
    val = await main_module.redis.get(key)
    assert val is None
    
    await main_module.redis.flushdb()
    for i in range(5):
        await main_module.redis.zadd(rate_limit_key, {f"dummy_member_{i}": now})
    
    transport = httpx.ASGITransport(app=main_module.app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as ac:
        resp = await ac.post("/v1/notifications", json={
            "idempotency_key": "some_new_key",
            "title": "Test",
            "message": "Msg"
        }, headers={"x-user-id": TEST_USER_ID})
        assert resp.status_code == 429
        full_key = main_module.make_idempotency_key(TEST_USER_ID, "some_new_key")
        assert await main_module.redis.get(full_key) is None


@pytest.mark.anyio
async def test_combined_script_outcome_success():
    import time
    rate_limit_key = f"rate_limit:{TEST_USER_ID}"
    key = f"{TEST_KEY_PREFIX}success_key"
    now = time.time()
    
    result = await main_module.redis.eval(
        main_module.COMBINED_LOCK_RATE_LIMIT_LUA,
        2,
        key,
        rate_limit_key,
        60,
        60,
        5,
        str(now),
        key
    )
    assert result[0] == 3
    
    val = await main_module.redis.get(key)
    assert val == b"PROCESSING"
    ttl = await main_module.redis.ttl(key)
    assert 55 <= ttl <= 60
    
    members = await main_module.redis.zrange(rate_limit_key, 0, -1)
    assert key.encode('utf-8') in members or key in members

    await main_module.redis.flushdb()
    
    original_enqueue = main_module.arq_pool.enqueue_job
    call_count = 0
    async def mock_enqueue(*args, **kwargs):
        nonlocal call_count
        call_count += 1
        return None
    main_module.arq_pool.enqueue_job = mock_enqueue
    
    try:
        transport = httpx.ASGITransport(app=main_module.app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as ac:
            resp = await ac.post("/v1/notifications", json={
                "idempotency_key": "some_success_key",
                "title": "Test",
                "message": "Msg"
            }, headers={"x-user-id": TEST_USER_ID})
            assert resp.status_code == 202
            assert call_count == 1
    finally:
        main_module.arq_pool.enqueue_job = original_enqueue
