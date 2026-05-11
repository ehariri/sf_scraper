# SF Superior Court Scraper

Local-only scraper for the San Francisco Superior Court civil case portal.
Handles Cloudflare, navigates the site, extracts case detail and register-of-actions
data, and downloads linked docket PDFs to disk.

## Key Features

*   **Cloudflare Handling**: Uses a persistent Chrome profile to bypass Cloudflare checks.
*   **Robust Scraping**: Handles page load timeouts, browser freezes, and restricted cases (e.g., CCP 1161.2).
*   **Parallel Downloads**: Downloads documents concurrently to speed up the process.
*   **Multi-Process Support**: Can run multiple Chrome instances in parallel to scrape different date ranges simultaneously.
*   **Resumable**: Tracks progress and can resume from where it left off (including specific cases).
*   **Local storage**: All scraped data stays on disk; no remote uploads.

## Scripts

*   **`launcher.py`**: Main entry point for multi-process scraping. Launches the concurrent `fast_scraper` worker.
*   **`fast_scraper/scraper.py`**: The worker path. Opens case detail tabs concurrently, extracts tables in one browser-side pass, and downloads documents concurrently.
*   **`run_failed_cleanup_shard.py`**: Sharded failed-only cleanup runner for retry passes.
*   **`timed_scrape_runner.py`**: Bounded wrapper around the worker with a hard timeout.
*   **`repair_local_metadata.py`**: Repairs broken `day_summary.json` / `register_of_actions.json` artifacts in the local corpus.
*   **`filter_high_value_pdfs.py`**: Offline document-type classifier and text-richness scorer.
*   **Monitoring**: now lives in the shared `monitor/` package at the repo root (see "Monitoring" below).

## Setup

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python -m playwright install chromium
```

Google Chrome must also be installed on the system — the scraper drives a
real Chrome instance via a remote debug port.

## Usage

### Multi-Process Scraping (Recommended)

1.  **Configure**: Pass arguments to `launcher.py`:
    *   `--start-date`: Start date (YYYY-MM-DD)
    *   `--end-date`: End date (YYYY-MM-DD)
    *   `--num-workers`: Number of parallel Chrome instances (default: 3)
    *   `--base-port`: Base Chrome debug port for worker windows (default: 9222)
    *   `--data-root`: Local output root for this machine or run
    *   `--max-concurrent-cases`: Max concurrent case tabs per worker (default: 5)
    *   `--max-concurrent-downloads`: Max concurrent document downloads per worker (default: 10)

2.  **Run**:
    ```bash
    python launcher.py --start-date 2024-01-02 --end-date 2024-01-31 --num-workers 3
    ```

3.  **Solve Cloudflare**:
    *   Multiple Chrome windows will open (one per worker).
    *   **You must manually solve the Cloudflare challenge in EACH window.**
    *   Once solved, the scraper proceeds automatically.

### Failed-Only Cleanup

After a first pass, rerun only the cases left in `failed_cases.json`:

```bash
python fast_scraper/scraper.py \
  --start-date 2024-01-02 \
  --end-date 2024-01-31 \
  --failed-only \
  --port 9222 \
  --max-concurrent-cases 2 \
  --max-concurrent-downloads 6 \
  --retry-passes 0
```

### Multi-Machine Operation

If running across more than one machine, assign each machine a non-overlapping
date range and a separate `--data-root`. Because nothing syncs remotely, the
only coordination needed is avoiding duplicate scrape of the same filing day.

## Output

Data is saved under `--data-root` (default `data/`), organized by filing date
and case number:

```
data/
├── 2024-01-02/
│   ├── day_summary.json
│   ├── failed_cases.json        (only if any cases failed)
│   ├── CGC-24-123456/
│   │   ├── register_of_actions.json
│   │   ├── 2024-01-02_DocID.pdf
│   │   └── ...
│   └── ...
└── ...
```

*   **`register_of_actions.json`**: Case metadata, parties, and the full register of actions.
*   **`day_summary.json`**: Filing-day progress (total cases vs. scraped cases).
*   **`failed_cases.json`**: Cases that failed during scraping — input for `--failed-only` reruns.

Benchmark scripts and summary markdown live under `benchmarks/`. Raw generated
benchmark logs and JSON outputs are intentionally not tracked.

## Monitoring

The cross-scraper dashboard at `<repo>/monitor/` aggregates SF, OK, and SC
into one page. Launch it from the repo root:

```bash
detection_pilot/.venv/bin/python monitor/server.py
# default: http://127.0.0.1:8791
```

SF runs write a heartbeat under each worker's data root
(`_heartbeat_worker_<N>.json`) so the live-runs panel in the monitor shows
which days each worker is currently on. See `monitor/README.md` for the
status legend and config format.

## Notes

*   **Restricted Cases**: Cases marked "Per CCP 1161.2" or "Case Is Not Available For Viewing" are skipped, and their status is recorded.
*   **Rate Limiting**: The worker uses separate semaphores for concurrent case tabs and document downloads.
*   **Browser Stuck**: If a browser hangs, the script attempts to kill and restart the process automatically.
*   **Per-worker profiles**: Each launcher worker uses its own Chrome profile, which avoids profile lock contention when multiple browsers run in parallel.
