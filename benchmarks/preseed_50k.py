import asyncio
import time
import pickle
import uuid
import redis.asyncio as redis_lib

def serialize_job_raw(function_name, args, kwargs, job_try, enqueue_time_ms):
    data = {'t': job_try, 'f': function_name, 'a': args, 'k': kwargs, 'et': enqueue_time_ms}
    return pickle.dumps(data)

async def seed_50k(count=50000):
    client = redis_lib.Redis.from_url("redis://localhost:6379/0", max_connections=200)
    await client.flushall()

    t0 = time.perf_counter()
    batch_size = 5000
    now = int(time.time() * 1000)

    for b in range(0, count, batch_size):
        async with client.pipeline(transaction=False) as pipe:
            zadd_mapping = {}
            for i in range(batch_size):
                idx = b + i
                job_id = f"job-50k-{idx}-{uuid.uuid4()}"
                key = f"unique-50k-{idx}"
                payload = {
                    "idempotency_key": key,
                    "title": "50k Load Test",
                    "message": f"Message {idx}"
                }
                raw_bytes = serialize_job_raw(
                    function_name="send_notification",
                    args=(payload,),
                    kwargs={"replay_count": 0},
                    job_try=0,
                    enqueue_time_ms=now
                )
                pipe.set(f"arq:job:{job_id}", raw_bytes, px=86400000)
                zadd_mapping[job_id] = now
            pipe.zadd("arq:queue", zadd_mapping)
            await pipe.execute()
        print(f"  Enqueued {b + batch_size}/{count} jobs...")

    t1 = time.perf_counter()
    depth = await client.zcard("arq:queue")
    print(f"Pre-seeded {depth} jobs in {t1 - t0:.2f}s.")
    await client.aclose()

if __name__ == "__main__":
    asyncio.run(seed_50k(50000))
