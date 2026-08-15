import subprocess
import time
import os
import sys
import statistics
import redis as redis_lib
import psutil

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PYTHON = sys.executable
PRESEED_50K = os.path.join(os.path.dirname(os.path.abspath(__file__)), "preseed_50k.py")
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

def run_single_15s_test(num_workers, cap_per_worker, run_idx):
    kill_stale_workers()

    # Preseed 50,000 jobs
    subprocess.run([PYTHON, PRESEED_50K], cwd=BASE, capture_output=True, text=True)
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

    time.sleep(15.0)
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

    # CRITICAL CHECK: Queue depth must be non-zero
    assert final_depth > 0, f"ERROR: Queue hit 0 early! (final_depth={final_depth})"

    print(f"    Run {run_idx} ({num_workers} workers x cap={cap_per_worker}): {rate:>6.2f} jobs/s (claimed={claims_ok}, remaining={final_depth}, drained={drained} in {elapsed:.2f}s, WatchErrors={watch_errs})")
    return {
        "rate": rate,
        "claims": claims_ok,
        "remaining": final_depth,
        "drained": drained,
        "elapsed": elapsed,
        "watch_errors": watch_errs
    }

def main():
    print("\n" + "="*80)
    print(" FINAL VERIFICATION TEST: 50,000 PRE-SEEDED BACKLOG (15-SECOND WINDOW)")
    print("="*80)

    worker_configs = [1, 2, 4]
    summary_results = {}

    for num_w in worker_configs:
        print(f"\nEvaluating {num_w} Worker Process(es) @ max_jobs=25 each (3 runs x 15s)...")
        runs_data = []
        for i in range(1, 4):
            data = run_single_15s_test(num_w, 25, i)
            runs_data.append(data)
            time.sleep(1.0)

        rates = [d["rate"] for d in runs_data]
        mean_rate = statistics.mean(rates)
        stdev_rate = statistics.stdev(rates) if len(rates) > 1 else 0.0
        total_watch = sum(d["watch_errors"] for d in runs_data)
        summary_results[num_w] = {
            "runs": runs_data,
            "mean": mean_rate,
            "stdev": stdev_rate,
            "total_watch_errors": total_watch
        }

    print("\n" + "="*95)
    print("               FINAL SUSTAINED STEADY-STATE THROUGHPUT SUMMARY (50k JOBS, 15s)")
    print("="*95)
    print(f"{'Processes':<12} | {'Run 1 (j/s)':<12} | {'Run 2 (j/s)':<12} | {'Run 3 (j/s)':<12} | {'Mean Rate':<15} | {'Std Dev':<10} | {'WatchErrors':<12}")
    print("-" * 95)
    for num_w in worker_configs:
        r = summary_results[num_w]
        r1, r2, r3 = [d["rate"] for d in r["runs"]]
        print(f"{num_w:<12} | {r1:<12.2f} | {r2:<12.2f} | {r3:<12.2f} | {r['mean']:<15.2f} | {r['stdev']:<10.2f} | {r['total_watch_errors']:<12}")
    print("="*95)

    print("\n[QUEUE DEPTH AND DRAIN DETAILS]:")
    for num_w in worker_configs:
        print(f"\n  --- {num_w} Worker Process(es) ---")
        for idx, d in enumerate(summary_results[num_w]["runs"], 1):
            print(f"    Run {idx}: Drained {d['drained']} jobs | Remaining Backlog: {d['remaining']:,} jobs (NON-ZERO confirmed) | WatchErrors: {d['watch_errors']}")
    print("\n" + "="*95 + "\n")

if __name__ == "__main__":
    main()
