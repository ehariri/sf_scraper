import argparse
import signal
import subprocess
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path


START_DATE = "2015-01-01"
END_DATE = "2015-01-10"
NUM_WORKERS = 3
BASE_PORT = 9322
DATA_ROOT = Path(__file__).parent / "data"
MAX_CONCURRENT_CASES = 2
MAX_CONCURRENT_DOWNLOADS = 6


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
    parser = argparse.ArgumentParser(description="SF Scraper Camoufox Launcher")
    parser.add_argument("--start-date", type=str, default=START_DATE, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end-date", type=str, default=END_DATE, help="End date (YYYY-MM-DD)")
    parser.add_argument(
        "--num-workers",
        type=int,
        default=NUM_WORKERS,
        help="Number of parallel Camoufox workers",
    )
    parser.add_argument(
        "--base-port",
        type=int,
        default=BASE_PORT,
        help="Compatibility worker id base; Camoufox does not use remote debugging ports.",
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
        default=MAX_CONCURRENT_CASES,
        help="Max concurrent case tabs per worker",
    )
    parser.add_argument(
        "--max-concurrent-downloads",
        type=int,
        default=MAX_CONCURRENT_DOWNLOADS,
        help="Max concurrent document downloads per worker",
    )
    parser.add_argument("--clear", action="store_true", help="Clear existing data before scraping")
    parser.add_argument("--retry-passes", type=int, default=0)
    parser.add_argument("--retry-concurrency", type=int, default=0)
    parser.add_argument("--case-launch-stagger-ms", type=int, default=0)
    parser.add_argument(
        "--pdf-filter-profile",
        choices=("all", "high_value"),
        default="all",
        help="Download all linked PDFs or only a metadata-selected high-value subset.",
    )
    parser.add_argument(
        "--disable-request-roa",
        action="store_true",
        help="Force the legacy browser-tab case scrape path instead of using direct GetROA requests.",
    )
    args = parser.parse_args()

    all_dates = get_date_range(args.start_date, args.end_date)
    print(f"Total dates to scrape: {len(all_dates)}")
    print(
        "Launching "
        f"{args.num_workers} Camoufox workers with "
        f"{args.max_concurrent_cases} concurrent case tabs and "
        f"{args.max_concurrent_downloads} concurrent downloads each."
    )

    chunks = split_dates(all_dates, args.num_workers)
    processes = []
    fast_worker = Path(__file__).parent / "fast_scraper" / "scraper.py"

    for i, chunk in enumerate(chunks):
        if not chunk:
            continue

        worker_start = chunk[0]
        worker_end = chunk[-1]
        port = args.base_port + i
        print(
            f"Launching Camoufox worker {i+1} on slot {port}: "
            f"{worker_start} to {worker_end} ({len(chunk)} days)"
        )

        cmd = [
            sys.executable,
            str(fast_worker),
            "--browser",
            "camoufox",
            "--port",
            str(port),
            "--start-date",
            worker_start,
            "--end-date",
            worker_end,
            "--data-root",
            str(args.data_root),
            "--max-concurrent-cases",
            str(args.max_concurrent_cases),
            "--max-concurrent-downloads",
            str(args.max_concurrent_downloads),
            "--retry-passes",
            str(args.retry_passes),
            "--retry-concurrency",
            str(args.retry_concurrency),
            "--case-launch-stagger-ms",
            str(args.case_launch_stagger_ms),
            "--pdf-filter-profile",
            args.pdf_filter_profile,
            "--worker-id",
            str(i),
        ]

        if args.clear:
            cmd.append("--clear")
        if args.disable_request_roa:
            cmd.append("--disable-request-roa")

        proc = subprocess.Popen(cmd)
        processes.append(proc)
        time.sleep(1)

    print(f"\nAll {len(processes)} Camoufox workers launched.")
    print("Please solve the Cloudflare challenge in EACH Camoufox window that opens.")
    print("Waiting for workers to complete...")

    try:
        for proc in processes:
            rc = proc.wait()
            if rc == 0:
                print(f"Worker PID {proc.pid} finished successfully.")
                continue
            if rc < 0:
                try:
                    sig_name = signal.Signals(-rc).name
                except Exception:
                    sig_name = f"SIG{-rc}"
                print(f"Worker PID {proc.pid} exited due to signal {sig_name} ({rc}).")
            else:
                print(f"Worker PID {proc.pid} exited with code {rc}.")
    except KeyboardInterrupt:
        print("\nStopping all workers...")
        for proc in processes:
            proc.terminate()

    print("All workers finished.")


if __name__ == "__main__":
    main()
