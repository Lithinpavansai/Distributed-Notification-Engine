import sys
from pathlib import Path
sys.path.insert(0, r"c:\Users\lithe\Downloads\Distributed-Notification-Engine")

import asyncio
import uuid
import json
import os
from urllib.parse import urlparse
from redis.asyncio import Redis
from arq import create_pool
from arq.connections import RedisSettings
import asyncpg
import httpx

import app.main as main_module
from app.worker import WorkerSettings

# Use isolated test Redis DB 15
TEST_REDIS_URL = os.getenv("TEST_REDIS_URL", "redis://localhost:6379/15")
TEST_REDIS_DB = 15

async def run_e2e_verification():
    print("[E2E] Starting live system verification...")
    
    # 1. Connect to Redis (DB 15) and Postgres
    redis = Redis.from_url(TEST_REDIS_URL)
    assert await redis.ping() is True
    print(f"[E2E] 1. Redis connected and responsive on isolated DB {TEST_REDIS_DB}.")

    db_url = main_module.get_database_url()
    pg_conn = await asyncpg.connect(db_url)
    table_exists = await pg_conn.fetchval(
        "SELECT EXISTS (SELECT FROM pg_tables WHERE tablename = 'dead_letter_queue');"
    )
    assert table_exists is True
    await pg_conn.close()
    print("[E2E] 2. PostgreSQL connected and dead_letter_queue table verified.")

    # Override main module and worker settings with test database
    main_module.redis_url = TEST_REDIS_URL
    main_module.redis = redis
    
    parsed = urlparse(TEST_REDIS_URL)
    arq_settings = RedisSettings(
        host=parsed.hostname or 'localhost',
        port=parsed.port or 6379,
        database=TEST_REDIS_DB,
        password=parsed.password,
    )
    main_module.arq_pool = await create_pool(arq_settings)
    WorkerSettings.redis_settings = arq_settings

    try:
        unique_key = f"live-e2e-{uuid.uuid4()}"
        transport = httpx.ASGITransport(app=main_module.app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            # 2. Submit notification via API
            resp1 = await client.post("/v1/notifications", json={
                "idempotency_key": unique_key,
                "title": "E2E Verification",
                "message": "Testing distributed notification pipeline"
            }, headers={"x-user-id": "test_user_id"})
            assert resp1.status_code == 202, f"Expected 202, got {resp1.status_code}: {resp1.text}"
            print(f"[E2E] 3. Ingestion endpoint returned 202 Accepted: {resp1.json()}")

            # Check Redis job queue
            q_depth = await redis.zcard("arq:queue")
            assert q_depth >= 1
            print(f"[E2E] 4. Job successfully queued in Redis arq:queue (depth: {q_depth}).")

            # 3. Process via Worker
            from arq.worker import create_worker
            worker = create_worker(
                WorkerSettings,
                redis_pool=main_module.arq_pool,
                burst=True,
                handle_signals=False
            )
            await worker.async_run()
            print("[E2E] 5. Worker processed queued job.")

            # 4. Verify Delivery State
            sent_state = await redis.get(f"sent:{unique_key}")
            assert sent_state == b"SENT"
            assert main_module.gateway_mock.was_received_map.get(unique_key) is True
            print(f"[E2E] 6. Delivery confirmed: sent state is SENT, mock gateway received notification: True.")

            # 5. Verify Idempotent Query Replay (verbatim 200 OK)
            resp_duplicate = await client.post("/v1/notifications", json={
                "idempotency_key": unique_key,
                "title": "E2E Verification",
                "message": "Testing distributed notification pipeline"
            }, headers={"x-user-id": "test_user_id"})
            assert resp_duplicate.status_code == 200
            data = resp_duplicate.json()
            assert data["status"] == "success"
            assert data["idempotency_key"] == unique_key
            assert "delivered" in data["detail"].lower()
            print(f"[E2E] 7. Idempotent duplicate replay returned verbatim cached response: {data}")

    finally:
        # Clean up only the keys created in DB 15
        async for key in redis.scan_iter(match="*", count=200):
            await redis.delete(key)
        if main_module.arq_pool:
            await main_module.arq_pool.aclose()
        await redis.aclose()

    print("[E2E] ALL ACCEPTANCE CRITERIA VERIFIED SUCCESSFULLY!")

if __name__ == "__main__":
    asyncio.run(run_e2e_verification())
