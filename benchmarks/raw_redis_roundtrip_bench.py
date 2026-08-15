import asyncio
import time
import redis.asyncio as redis_lib

def calc_percentile(sorted_list, pct):
    if not sorted_list:
        return 0.0
    k = (len(sorted_list) - 1) * (pct / 100.0)
    f = int(k)
    c = min(f + 1, len(sorted_list) - 1)
    d = k - f
    return sorted_list[f] + d * (sorted_list[c] - sorted_list[f])

async def bench_sequential(client, count=1000):
    latencies = []
    t_start = time.perf_counter()
    for i in range(count):
        t0 = time.perf_counter()
        await client.set("bench:key", "1")
        t1 = time.perf_counter()
        latencies.append((t1 - t0) * 1000)
    total_elapsed = time.perf_counter() - t_start
    return latencies, total_elapsed

async def bench_concurrent(client, count=1000, concurrency=100):
    latencies = []
    sem = asyncio.Semaphore(concurrency)

    async def single_call(idx):
        async with sem:
            t0 = time.perf_counter()
            await client.set(f"bench:key:{idx}", "1")
            t1 = time.perf_counter()
            latencies.append((t1 - t0) * 1000)

    t_start = time.perf_counter()
    tasks = [single_call(i) for i in range(count)]
    await asyncio.gather(*tasks)
    total_elapsed = time.perf_counter() - t_start
    return latencies, total_elapsed

async def main():
    client = redis_lib.Redis.from_url("redis://localhost:6379/0", max_connections=200)
    await client.ping()

    print("\n" + "="*75)
    print("        RAW REDIS ROUND-TRIP ISOLATION TEST (NO ARQ, NO APP LOGIC)")
    print("="*75)

    # 1. Sequential 1,000 calls
    seq_latencies, seq_total = await bench_sequential(client, 1000)
    s_seq = sorted(seq_latencies)
    n_seq = len(s_seq)
    print(f"\n[MODE 1: 1,000 SEQUENTIAL AWAIT CALLS (1 in-flight)]:")
    print(f"  • Total Time Elapsed : {seq_total:.3f} s ({1000/seq_total:.1f} ops/sec)")
    print(f"  • Min Latency        : {s_seq[0]:.3f} ms")
    print(f"  • p50 (Median)       : {calc_percentile(s_seq, 50):.3f} ms")
    print(f"  • Mean (Avg)         : {sum(s_seq)/n_seq:.3f} ms")
    print(f"  • p90 Latency        : {calc_percentile(s_seq, 90):.3f} ms")
    print(f"  • p99 Latency        : {calc_percentile(s_seq, 99):.3f} ms")
    print(f"  • Max Latency        : {s_seq[-1]:.3f} ms")

    # 2. Concurrent 1,000 calls (100-way asyncio.gather)
    conc_latencies, conc_total = await bench_concurrent(client, 1000, 100)
    s_conc = sorted(conc_latencies)
    n_conc = len(s_conc)
    print(f"\n[MODE 2: 1,000 CONCURRENT CALLS (100-way asyncio.gather)]:")
    print(f"  • Total Time Elapsed : {conc_total:.3f} s ({1000/conc_total:.1f} ops/sec)")
    print(f"  • Min Latency        : {s_conc[0]:.3f} ms")
    print(f"  • p50 (Median)       : {calc_percentile(s_conc, 50):.3f} ms")
    print(f"  • Mean (Avg)         : {sum(s_conc)/n_conc:.3f} ms")
    print(f"  • p90 Latency        : {calc_percentile(s_conc, 90):.3f} ms")
    print(f"  • p99 Latency        : {calc_percentile(s_conc, 99):.3f} ms")
    print(f"  • Max Latency        : {s_conc[-1]:.3f} ms")

    print("="*75 + "\n")
    await client.aclose()

if __name__ == "__main__":
    asyncio.run(main())
