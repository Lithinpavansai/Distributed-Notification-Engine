import subprocess
import time
import os
import sys
import statistics
import redis as redis_lib
import psutil

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PYTHON = sys.executable
PRESEED = os.path.join(os.path.dirname(os.path.abspath(__file__)), "preseed_50k.py")
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

def run_multi_process_test(num_workers, cap_per_worker, run_idx):
    kill_stale_workers()

    # Preseed 50,000 jobs
    subprocess.run([PYTHON, PRESEED], cwd=BASE, capture_output=True, text=True)
    r = redis_lib.Redis()
    initial_depth = r.zcard("arq:queue")

    env = os.environ.copy()
    env["ARQ_MAX_JOBS"] = str(cap_per_worker)
    env["ARQ_QUEUE_READ_LIMIT"] = str(cap_per_worker)
    env["USE_NOOP_JOB"] = "false"

    worker_procs = []
    t0 = time.time()
    for w in range(num_workers):
        p = subprocess.Popen(
            [PYTHON, "-m", "app.worker"],
            cwd=BASE,
            env=env,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        worker_procs.append(p)

    time.sleep(10.0)
    t1 = time.time()

    for p in worker_procs:
        p.terminate()
    for p in worker_procs:
        try:
            p.wait(timeout=5)
        except subprocess.TimeoutExpired:
            p.kill()

    time.sleep(1)

    final_depth = r.zcard("arq:queue")
    claims_ok = int(r.get("metrics:claims_success") or 0)
    watch_errs = int(r.get("metrics:watch_errors") or 0)
    elapsed = t1 - t0
    rate = claims_ok / elapsed
    drained = initial_depth - final_depth

    print(f"    Run {run_idx} ({num_workers} workers x cap={cap_per_worker}): {rate:>6.2f} jobs/s (claimed={claims_ok}, drained={drained} in {elapsed:.2f}s, WatchErrors={watch_errs})")
    return rate, watch_errs

def main():
    print("\n" + "="*75)
    print(" MULTI-PROCESS SCALING TEST (1 vs 2 vs 4 PROCESSES @ max_jobs=25 each)")
    print("="*75)

    worker_configs = [1, 2, 4]
    results = {}

    for num_w in worker_configs:
        print(f"\nEvaluating {num_w} Worker Process(es) @ max_jobs=25 each (3 iterations)...")
        rates = []
        total_watch_errors = 0
        for i in range(1, 4):
            rate, w_errs = run_multi_process_test(num_w, 25, i)
            rates.append(rate)
            total_watch_errors += w_errs
            time.sleep(1.0)
        mean_rate = statistics.mean(rates)
        stdev_rate = statistics.stdev(rates) if len(rates) > 1 else 0.0
        results[num_w] = {
            "rates": rates,
            "mean": mean_rate,
            "stdev": stdev_rate,
            "watch_errors": total_watch_errors
        }

    print("\n" + "="*80)
    print("             MULTI-PROCESS ARQ WORKER SCALING SUMMARY")
    print("="*80)
    print(f"{'Processes':<12} | {'Run 1 (j/s)':<12} | {'Run 2 (j/s)':<12} | {'Run 3 (j/s)':<12} | {'Mean Rate':<15} | {'Std Dev':<10} | {'WatchErrors':<12}")
    print("-" * 80)
    for num_w in worker_configs:
        r = results[num_w]
        r1, r2, r3 = r["rates"]
        print(f"{num_w:<12} | {r1:<12.2f} | {r2:<12.2f} | {r3:<12.2f} | {r['mean']:<15.2f} | {r['stdev']:<10.2f} | {r['watch_errors']:<12}")
    print("="*80 + "\n")

if __name__ == "__main__":
    main()
