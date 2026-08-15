import subprocess
import time
import os
import sys
import statistics
import redis as redis_lib
import psutil

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PYTHON = sys.executable
sys.path.insert(0, BASE)

def kill_stale_workers():
    for p in psutil.process_iter(['pid', 'name', 'cmdline']):
        try:
            cmd = " ".join(p.info['cmdline'] or [])
            if "app.worker" in cmd:
                p.kill()
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue
    time.sleep(1)

def run_unpipelined_15s(run_idx):
    kill_stale_workers()

    from benchmarks.preseed_50k import seed_50k
    import asyncio
    asyncio.run(seed_50k(50000))

    r = redis_lib.Redis()
    initial_depth = r.zcard("arq:queue")

    env = os.environ.copy()
    env["ARQ_MAX_JOBS"] = "25"
    env["ARQ_QUEUE_READ_LIMIT"] = "25"
    env["USE_NOOP_JOB"] = "false"

    t0 = time.time()
    worker_proc = subprocess.Popen(
        [PYTHON, "-m", "app.worker"],
        cwd=BASE,
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )

    time.sleep(15.0)
    t1 = time.time()

    worker_proc.terminate()
    try:
        worker_proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        worker_proc.kill()

    time.sleep(1)

    final_depth = r.zcard("arq:queue")
    claims_ok = int(r.get("metrics:claims_success") or 0)
    watch_errs = int(r.get("metrics:watch_errors") or 0)
    elapsed = t1 - t0
    rate = claims_ok / elapsed
    drained = initial_depth - final_depth

    assert final_depth > 0, "Queue hit zero early!"

    print(f"  Run {run_idx} (UNPIPELINED 1-Worker @ max_jobs=25, 15s): {rate:>6.2f} jobs/s (claimed={claims_ok}, remaining={final_depth:,}, drained={drained} in {elapsed:.2f}s, WatchErrors={watch_errs})")
    return rate, final_depth, watch_errs

def main():
    print("\n" + "="*80)
    print(" UNPIPELINED CODE TEST (50,000 BACKLOG, 15-SECOND WINDOW, 1 WORKER @ max_jobs=25)")
    print("="*80)

    rates = []
    for i in range(1, 4):
        rate, rem, we = run_unpipelined_15s(i)
        rates.append(rate)
        time.sleep(1.0)

    mean_r = statistics.mean(rates)
    stdev_r = statistics.stdev(rates)

    print("\n" + "="*80)
    print("                  UNPIPELINED TEST SUMMARY")
    print("="*80)
    print(f"UNPIPELINED 1 Worker (3 runs) : Mean = {mean_r:.2f} jobs/s (±{stdev_r:.2f}) | Runs = {[round(x, 2) for x in rates]}")
    print(f"PIPELINED   1 Worker (Ref)    : Mean = 737.03 jobs/s (±6.39)")
    print("="*80 + "\n")

if __name__ == "__main__":
    main()
