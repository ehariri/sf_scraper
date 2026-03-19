# SF Superior Court Scraper

This project automates the scraping of civil case data from the San Francisco Superior Court website. It handles Cloudflare challenges, navigates the site, extracts case details, and downloads associated documents.

## Key Features

*   **Cloudflare Handling**: Uses a persistent Chrome profile to bypass Cloudflare checks.
*   **Robust Scraping**: Handles page load timeouts, browser freezes, and restricted cases (e.g., CCP 1161.2).
*   **Parallel Downloads**: Downloads documents concurrently to speed up the process.
*   **Multi-Process Support**: Can run multiple Chrome instances in parallel to scrape different date ranges simultaneously.
*   **Resumable**: Tracks progress and can resume from where it left off (including specific cases).
*   **Data Management**: Saves data in a structured JSON format and skips already processed cases/days.

## Scripts

*   **`launcher.py`**: The main entry point for multi-process scraping. It now launches the concurrent `fast_scraper` worker by default.
*   **`fast_scraper/scraper.py`**: The fastest worker path. It opens case detail tabs concurrently, extracts tables in one browser-side pass, and downloads documents concurrently.
*   **`run_failed_cleanup_shard.py`**: Sharded failed-only cleanup runner for retry passes.
*   **`sync_existing_to_hf_and_prune.py`**: Uploads local data to HF, verifies it remotely, and prunes local copies.
*   **`upload_data_in_batches.py`**: Bulk uploader for draining large local backlogs to HF in sized batches.
*   **`run_bulk_upload_with_restart.py`**: Wrapper that relaunches the bulk uploader after restartable HF `504` commit failures.
*   **`monitor_app.py`**: A local monitoring web app for scrape coverage, sync progress, live process health, and recent log errors/stalls.

## Setup

1.  **Install Dependencies**:
    ```bash
    pip install playwright
    playwright install chromium
    ```

2.  **Chrome Installation**: Ensure Google Chrome is installed on your system.

## Shared Operation

For multi-machine operation, read [MULTI_MACHINE.md](MULTI_MACHINE.md).

The short version:

*   Use the shared HF dataset repo `please-the-bot/sf_superior_court`.
*   Split machines by non-overlapping date ranges.
*   Do not let two machines scrape the same filing day at the same time.
*   Do not point two sync/prune jobs at the same local `data/` tree.
*   Use `--hf-only` on low-disk machines, or local-first mode plus `sync_existing_to_hf_and_prune.py` on higher-disk machines.

## Usage

### Multi-Process Scraping (Recommended)

This is the fastest way to scrape a range of dates.

1.  **Configure**: You can configure the scraper by editing `launcher.py` or passing arguments directly.
    *   `--start-date`: Start date (YYYY-MM-DD)
    *   `--end-date`: End date (YYYY-MM-DD)
    *   `--num-workers`: Number of parallel Chrome instances (default: 3)
    *   `--base-port`: Base Chrome debug port for worker windows
    *   `--data-root`: Local output root for this machine or run
    *   `--max-concurrent-cases`: Max concurrent case tabs per worker (default: 2)
    *   `--max-concurrent-downloads`: Max concurrent document downloads per worker (default: 6)

2.  **Run**:
    ```bash
    # Default (3 workers, 2 concurrent case tabs, 6 concurrent downloads)
    python launcher.py

    # Custom configuration
    python launcher.py --start-date 2015-01-01 --end-date 2015-02-01 --num-workers 5 --base-port 9400 --data-root data_machine_a --max-concurrent-cases 2 --max-concurrent-downloads 6
    ```

3.  **Solve Cloudflare**:
    *   Multiple Chrome windows will open (one for each worker).
    *   **You must manually solve the Cloudflare challenge in EACH window.**
    *   Once solved, the scraper will automatically proceed.

### Failed-Only Cleanup

After a first pass, rerun only the cases left in `failed_cases.json`:

```bash
python fast_scraper/scraper.py \
  --start-date 2023-01-02 \
  --end-date 2023-12-29 \
  --failed-only \
  --data-root data_2023 \
  --port 9222 \
  --max-concurrent-cases 2 \
  --max-concurrent-downloads 6 \
  --retry-passes 0
```

## Output

Data is saved in the `data/` directory, organized by filing date and case number:

```
data/
├── 2015-01-01/
│   ├── day_summary.json
│   ├── CGC-15-123456/
│   │   ├── register_of_actions.json
│   │   ├── 2015-01-01_DocID.pdf
│   │   └── ...
│   └── ...
└── ...
```

*   **`register_of_actions.json`**: Contains case metadata, parties, and the full register of actions.
*   **`day_summary.json`**: Tracks the scraping progress for that date (total cases vs. scraped cases).

Benchmark scripts and summary markdown live under `benchmarks/`. Raw generated benchmark logs and JSON outputs are intentionally not tracked.

## Monitoring

Run the local dashboard to monitor scrape coverage, uploader health, stale processes, and recent log output:

```bash
cd /Users/jovik/Desktop/docket_gen/sf_scraper_fork
source .venv/bin/activate
python monitor_app.py --host 127.0.0.1 --port 8787
```

On the scraper machine itself, open:

```text
http://127.0.0.1:8787
```

For sharing the monitor with other people or devices, use the hosted Vercel deployment instead of any local-network address:

```text
https://vercelmonitor.vercel.app
```

The dashboard reads:

*   `data/*/day_summary.json` for day-level coverage and timing
*   `data/*/sync_metadata.json` for upload/prune progress
*   `logs/*.log` for recent warnings/errors
*   live `ps` output for scrape/sync process detection

Source of truth for the monitor UI:

*   Edit `monitor/index.html`, `monitor/app.js`, and `monitor/styles.css`
*   Do not edit the copied Vercel frontend files in `vercel_monitor/` directly
*   After monitor UI changes, run:

```bash
python sync_vercel_monitor.py
```

That refreshes the Vercel frontend copies and the committed `vercel_monitor/status-snapshot.json` fallback.

Status badges:

*   **healthy**: active process with recent file activity
*   **waiting**: likely blocked on Cloudflare or another manual gate
*   **degraded**: active but showing elevated retries/timeouts
*   **stalled**: active process with no recent file activity
*   **error**: recent traceback or sync verification/rate-limit issue

## Notes

*   **Restricted Cases**: Cases marked "Per CCP 1161.2" or "Case Is Not Available For Viewing" are skipped, and their status is recorded.
*   **Rate Limiting**: The fast worker uses separate semaphores for concurrent case tabs and document downloads.
*   **Browser Stuck**: If a browser hangs, the script attempts to kill and restart the process automatically.
*   **Per-worker profiles**: Each launcher worker uses its own Chrome profile, which avoids profile lock contention when multiple browsers run in parallel.
