import argparse
from pathlib import Path
import subprocess
import sys
import time
from datetime import datetime, timedelta
import signal

# --- Configuration ---
# Defaults (can be overridden by CLI args)
START_DATE = "2015-01-01"
END_DATE = "2015-01-10"
NUM_WORKERS = 3
BASE_PORT = 9222
DATA_ROOT = Path(__file__).parent / "data"
MAX_CONCURRENT_CASES = 5
MAX_CONCURRENT_DOWNLOADS = 10


def get_date_range(start_str, end_str):
    start = datetime.strptime(start_str, "%Y-%m-%d")
    end = datetime.strptime(end_str, "%Y-%m-%d")
    dates = []
    curr = start
    while curr <= end:
        dates.append(curr.strftime("%Y-%m-%d"))
        curr += timedelta(days=1)
    return dates


def split_dates(dates, n):
    k, m = divmod(len(dates), n)
    return [dates[i * k + min(i, m) : (i + 1) * k + min(i + 1, m)] for i in range(n)]


def main():
    global START_DATE, END_DATE, NUM_WORKERS, MAX_CONCURRENT_CASES, MAX_CONCURRENT_DOWNLOADS
    global BASE_PORT, DATA_ROOT

    parser = argparse.ArgumentParser(description="SF Scraper Launcher")
    parser.add_argument(
        "--start-date", type=str, default="2015-01-01", help="Start date (YYYY-MM-DD)"
    )
    parser.add_argument(
        "--end-date", type=str, default="2015-01-10", help="End date (YYYY-MM-DD)"
    )
    parser.add_argument(
        "--num-workers",
        type=int,
        default=3,
        help="Number of parallel workers (Chrome instances)",
    )
    parser.add_argument(
        "--base-port",
        type=int,
        default=BASE_PORT,
        help="Base Chrome debug port; worker ports increment from here",
    )
    parser.add_argument(
        "--data-root",
        type=Path,
        default=DATA_ROOT,
        help="Root directory for scraped case data and day summaries",
    )
    parser.add_argument(
        "--max-concurrent-cases",
        type=int,
        default=5,
        help="Max concurrent case tabs per worker",
    )
    parser.add_argument(
        "--max-concurrent-downloads",
        type=int,
        default=10,
        help="Max concurrent document downloads per worker",
    )
    parser.add_argument(
        "--clear",
        action="store_true",
        help="Clear existing data before scraping",
    )
    parser.add_argument(
        "--browser",
        choices=("chrome", "camoufox"),
        default="chrome",
        help="Browser backend passed to each worker. Camoufox auto-clears the "
             "Cloudflare/Turnstile gate (no manual click); chrome relies on the "
             "passive CDP-disconnect pass and otherwise needs a human.",
    )
    parser.add_argument(
        "--rotate-on-gate",
        action="store_true",
        help="Forward to each worker so it exits with code 2 + an IP_RESTRICTED "
             "marker on sustained per-day Cloudflare gating. When any worker trips, "
             "the launcher tears down its siblings, prints IP_RESTRICTED, and exits "
             "2 so a VPN-rotating wrapper (rotate.py) can switch IP and resume.",
    )
    args = parser.parse_args()

    START_DATE = args.start_date
    END_DATE = args.end_date
    NUM_WORKERS = args.num_workers
    BASE_PORT = args.base_port
    DATA_ROOT = args.data_root
    MAX_CONCURRENT_CASES = args.max_concurrent_cases
    MAX_CONCURRENT_DOWNLOADS = args.max_concurrent_downloads

    all_dates = get_date_range(START_DATE, END_DATE)
    print(f"Total dates to scrape: {len(all_dates)}")
    print(
        "Launching "
        f"{NUM_WORKERS} workers with "
        f"{MAX_CONCURRENT_CASES} concurrent case tabs and "
        f"{MAX_CONCURRENT_DOWNLOADS} concurrent downloads each."
    )

    chunks = split_dates(all_dates, NUM_WORKERS)
    processes = []
    fast_worker = Path(__file__).parent / "fast_scraper" / "scraper.py"

    for i, chunk in enumerate(chunks):
        if not chunk:
            continue

        worker_start = chunk[0]
        worker_end = chunk[-1]
        port = BASE_PORT + i

        print(
            f"Launching Worker {i+1} on port {port}: {worker_start} to {worker_end} ({len(chunk)} days)"
        )

        cmd = [
            sys.executable,
            str(fast_worker),
            "--port",
            str(port),
            "--start-date",
            worker_start,
            "--end-date",
            worker_end,
            "--data-root",
            str(DATA_ROOT),
            "--max-concurrent-cases",
            str(MAX_CONCURRENT_CASES),
            "--max-concurrent-downloads",
            str(MAX_CONCURRENT_DOWNLOADS),
            "--worker-id",
            str(i),
            "--browser",
            args.browser,
        ]

        if args.clear:
            cmd.append("--clear")

        if args.rotate_on_gate:
            cmd.append("--rotate-on-gate")

        # Launch process
        p = subprocess.Popen(cmd)
        processes.append(p)

        # Small stagger prevents all workers from competing for startup at once.
        time.sleep(1)

    print(f"\nAll {len(processes)} workers launched.")
    if args.browser == "camoufox":
        print("Camoufox auto-clears the Cloudflare gate; only step in if a window "
              "asks for manual input.")
    else:
        print("Please solve the Cloudflare challenge in EACH Chrome window that opens.")
    print("Waiting for workers to complete...")

    def _describe_exit(p, rc):
        if rc == 0:
            return f"Worker PID {p.pid} finished successfully."
        if rc == 2:
            return f"Worker PID {p.pid} exited with code 2 (IP gate)."
        if rc < 0:
            try:
                sig_name = signal.Signals(-rc).name
            except Exception:
                sig_name = f"SIG{-rc}"
            return f"Worker PID {p.pid} exited due to signal {sig_name} ({rc})."
        return f"Worker PID {p.pid} exited with code {rc}."

    def _teardown(survivors):
        for p in survivors:
            if p.poll() is None:
                p.terminate()
        # Give them a moment, then hard-kill any stragglers.
        deadline = time.time() + 15
        for p in survivors:
            remaining = max(0, deadline - time.time())
            try:
                p.wait(timeout=remaining)
            except subprocess.TimeoutExpired:
                p.kill()

    gate_tripped = False
    try:
        finished = set()
        while len(finished) < len(processes):
            for p in processes:
                if p.pid in finished:
                    continue
                rc = p.poll()
                if rc is None:
                    continue
                finished.add(p.pid)
                print(_describe_exit(p, rc))
                if args.rotate_on_gate and rc == 2:
                    gate_tripped = True
                    break
            if gate_tripped:
                break
            time.sleep(2)

        if gate_tripped:
            survivors = [p for p in processes if p.pid not in finished]
            print(
                f"IP_RESTRICTED: a worker reported sustained Cloudflare gating; "
                f"tearing down {len(survivors)} sibling worker(s) and requesting IP rotation."
            )
            _teardown(survivors)
    except KeyboardInterrupt:
        print("\nStopping all workers...")
        _teardown(processes)
        raise

    if gate_tripped:
        print("Launcher exiting 2 for IP rotation.")
        sys.exit(2)

    print("All workers finished.")


if __name__ == "__main__":
    main()
