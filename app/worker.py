import os
import sys
import time
import logging
from urllib.parse import urlparse

from redis.exceptions import ResponseError, WatchError
from arq.connections import RedisSettings, create_pool
from arq.constants import in_progress_key_prefix
from arq.utils import timestamp_ms
from arq.worker import Worker, logger as arq_logger
import app.main as main_module

LUA_BATCH_CLAIM_SCRIPT = """
local queue = KEYS[1]
local in_prog_prefix = ARGV[1]
local timeout_ms = tonumber(ARGV[2])
local now_ms = tonumber(ARGV[3])
local claimed = {}

for i = 4, #ARGV do
    local job_id = ARGV[i]
    local in_prog_key = in_prog_prefix .. job_id
    local exists = redis.call('EXISTS', in_prog_key)
    if exists == 0 then
        local score = redis.call('ZSCORE', queue, job_id)
        if score and tonumber(score) <= now_ms then
            redis.call('PSETEX', in_prog_key, timeout_ms, '1')
            table.insert(claimed, {job_id, score})
        end
    end
end
return claimed
"""

import asyncio

_concurrency_sampler_task = None
_stop_concurrency_sampler = None

async def _sample_concurrency_loop():
    log_path = "c:/Users/lithe/Downloads/Distributed Notification Engine/scratch/concurrent_jobs.log"
    os.makedirs(os.path.dirname(log_path), exist_ok=True)
    with open(log_path, "w") as f:
        f.write("elapsed_s,concurrent_jobs\n")
    t0 = time.monotonic()
    while _stop_concurrency_sampler and not _stop_concurrency_sampler.is_set():
        elapsed = time.monotonic() - t0
        cnt = getattr(main_module, "_current_concurrent_jobs", 0)
        with open(log_path, "a") as f:
            f.write(f"{elapsed:.2f},{cnt}\n")
        try:
            await asyncio.sleep(0.5)
        except asyncio.CancelledError:
            break

async def startup(ctx):
    global _concurrency_sampler_task, _stop_concurrency_sampler
    main_module.arq_pool = ctx['redis']
    main_module.redis = ctx['redis']
    _stop_concurrency_sampler = asyncio.Event()
    _concurrency_sampler_task = asyncio.create_task(_sample_concurrency_loop())
    print("[Worker] Startup completed. Global arq_pool, redis, and concurrency sampler initialized.")

async def shutdown(ctx):
    global _concurrency_sampler_task, _stop_concurrency_sampler
    if _stop_concurrency_sampler:
        _stop_concurrency_sampler.set()
    if _concurrency_sampler_task:
        _concurrency_sampler_task.cancel()
    if getattr(main_module, "_job_exec_log_file", None) is not None:
        try:
            main_module._job_exec_log_file.flush()
            main_module._job_exec_log_file.close()
            main_module._job_exec_log_file = None
        except Exception:
            pass
    print("[Worker] Shutdown completed.")


class LuaClaimWorker(Worker):
    """
    ARQ Worker subclass implementing:
    1. Atomic Lua-based batch job claiming (replaces WATCH/MULTI/EXEC transaction loop)
    2. Sub-millisecond / configurable polling delay
    3. Direct metric tracking for claims and aborts
    """
    def __init__(self, *args, **kwargs):
        poll_delay = float(os.getenv("ARQ_POLL_DELAY", "0.005"))
        queue_read_limit = int(os.getenv("ARQ_QUEUE_READ_LIMIT", "100"))
        
        kwargs.setdefault("poll_delay", poll_delay)
        kwargs.setdefault("queue_read_limit", queue_read_limit)
        super().__init__(*args, **kwargs)
        self.poll_delay_s = poll_delay

    async def start_jobs(self, job_ids: list[bytes]) -> None:
        if not job_ids:
            return

        available_slots = self.max_jobs - self.job_counter
        if available_slots <= 0:
            return

        candidates = [j.decode() for j in job_ids[:available_slots]]
        if not candidates:
            return

        now = timestamp_ms()
        timeout_ms = int(self.in_progress_timeout_s * 1000)

        try:
            claimed = await self.pool.eval(
                LUA_BATCH_CLAIM_SCRIPT,
                1,
                self.queue_name,
                in_progress_key_prefix,
                timeout_ms,
                now,
                *candidates
            )
        except Exception as e:
            arq_logger.error("Error executing Lua batch claim: %s", e)
            return

        if not claimed:
            return

        for item in claimed:
            job_id_raw, score_raw = item[0], item[1]
            job_id = job_id_raw.decode() if isinstance(job_id_raw, bytes) else str(job_id_raw)
            score = int(score_raw)

            await self.sem.acquire()
            self.job_counter += 1

            t = self.loop.create_task(self.run_job(job_id, score))
            t.add_done_callback(lambda _: self._release_sem_dec_counter_on_complete())
            self.tasks[job_id] = t

        if claimed:
            await self.pool.incrby("metrics:claims_success", len(claimed))


class WorkerSettings:
    functions = [main_module.send_notification, main_module.send_notification_noop]
    max_jobs = int(os.getenv("ARQ_MAX_JOBS", "100"))
    poll_delay = float(os.getenv("ARQ_POLL_DELAY", "0.005"))
    queue_read_limit = int(os.getenv("ARQ_QUEUE_READ_LIMIT", "100"))
    
    # Configure Redis connection parameters from REDIS_URL
    redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    parsed = urlparse(redis_url)
    host = parsed.hostname or 'localhost'
    port = parsed.port or 6379
    db = int(parsed.path.lstrip('/') or 0)
    password = parsed.password
    
    redis_settings = RedisSettings(host=host, port=port, database=db, password=password)
    on_startup = startup
    on_shutdown = shutdown


def run_worker():
    redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    parsed = urlparse(redis_url)
    settings = RedisSettings(
        host=parsed.hostname or 'localhost',
        port=parsed.port or 6379,
        database=int(parsed.path.lstrip('/') or 0),
        password=parsed.password,
    )
    worker = LuaClaimWorker(
        functions=[main_module.send_notification, main_module.send_notification_noop],
        redis_settings=settings,
        max_jobs=int(os.getenv("ARQ_MAX_JOBS", "100")),
        poll_delay=float(os.getenv("ARQ_POLL_DELAY", "0.005")),
        queue_read_limit=int(os.getenv("ARQ_QUEUE_READ_LIMIT", "100")),
        on_startup=startup,
        on_shutdown=shutdown,
    )
    worker.run()


if __name__ == "__main__":
    run_worker()
