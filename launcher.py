import argparse
from pathlib import Path
import subprocess
import sys
import time
from datetime import datetime, timedelta

# --- Configuration ---
# Defaults (can be overridden by CLI args)
START_DATE = "2015-01-01"
END_DATE = "2015-01-10"
NUM_WORKERS = 3
BASE_PORT = 9222
MAX_CONCURRENT_CASES = 5
MAX_CONCURRENT_DOWNLOADS = 10
HF_REPO_ID = "Arifov/sf_superior_court"
MAX_CONCURRENT_HF_UPLOADS = 1


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
    global HF_REPO_ID, MAX_CONCURRENT_HF_UPLOADS

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
        "--hf-repo-id",
        type=str,
        default=HF_REPO_ID,
        help="HF dataset repo to upload case outputs into",
    )
    parser.add_argument(
        "--disable-hf-upload",
        action="store_true",
        help="Do not upload outputs to Hugging Face",
    )
    parser.add_argument(
        "--keep-local-pdfs",
        action="store_true",
        help="Keep downloaded PDFs on local disk after upload",
    )
    parser.add_argument(
        "--max-concurrent-hf-uploads",
        type=int,
        default=MAX_CONCURRENT_HF_UPLOADS,
        help="Max concurrent HF case/day-summary commits per worker",
    )
    args = parser.parse_args()

    START_DATE = args.start_date
    END_DATE = args.end_date
    NUM_WORKERS = args.num_workers
    MAX_CONCURRENT_CASES = args.max_concurrent_cases
    MAX_CONCURRENT_DOWNLOADS = args.max_concurrent_downloads
    HF_REPO_ID = args.hf_repo_id
    MAX_CONCURRENT_HF_UPLOADS = args.max_concurrent_hf_uploads

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
            "--max-concurrent-cases",
            str(MAX_CONCURRENT_CASES),
            "--max-concurrent-downloads",
            str(MAX_CONCURRENT_DOWNLOADS),
            "--max-concurrent-hf-uploads",
            str(MAX_CONCURRENT_HF_UPLOADS),
        ]

        if args.clear:
            cmd.append("--clear")
        if args.disable_hf_upload:
            cmd.append("--disable-hf-upload")
        else:
            cmd.extend(["--hf-repo-id", HF_REPO_ID])
        if args.keep_local_pdfs:
            cmd.append("--keep-local-pdfs")

        # Launch process
        p = subprocess.Popen(cmd)
        processes.append(p)

        # Small stagger prevents all workers from competing for startup at once.
        time.sleep(1)

    print(f"\nAll {len(processes)} workers launched.")
    print("Please solve the Cloudflare challenge in EACH Chrome window that opens.")
    print("Waiting for workers to complete...")

    try:
        for p in processes:
            p.wait()
    except KeyboardInterrupt:
        print("\nStopping all workers...")
        for p in processes:
            p.terminate()

    print("All workers finished.")


if __name__ == "__main__":
    main()
