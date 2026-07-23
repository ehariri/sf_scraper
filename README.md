# SF Superior Court Scraper

Local-only scraper for the San Francisco Superior Court civil case portal.
Handles Cloudflare, navigates the site, extracts case detail and register-of-actions
data, and downloads linked docket PDFs to disk.

## Key Features

*   **Autonomous Cloudflare Handling (Camoufox)**: The Camoufox backend clears the Cloudflare/Turnstile gate on its own — no manual clicking. It self-recovers from escalated challenges and per-request gates on the current IP (no VPN/IP rotation needed). See "How the automated gate pass works" below.
*   **Robust Scraping**: Handles page load timeouts, browser freezes, and restricted cases (e.g., CCP 1161.2).
*   **Parallel Downloads**: Downloads documents concurrently to speed up the process.
*   **Multi-Process Support**: Runs multiple browser instances in parallel to scrape different date ranges simultaneously.
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

The Camoufox backend is the current, recommended path. Set up the venv and
fetch the Camoufox browser bundle:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
pip install -r requirements-camoufox.txt
python -m camoufox fetch
```

Camoufox is self-contained — no system Chrome install and no Playwright
Chromium download are needed for the recommended workflow.

> **Legacy Chrome/CDP backend (outdated).** The original path drove a real
> Google Chrome via a remote debug port (`python -m playwright install chromium`,
> plus a system Chrome install). It requires a **manual** Cloudflare solve in
> every window and is subject to per-request gating that the Camoufox backend now
> handles automatically. It is kept only for fallback; prefer Camoufox.

## Usage

### Multi-Process Scraping with Camoufox (Recommended)

1.  **Configure**: Pass arguments to `launcher.py`:
    *   `--browser camoufox`: Use the autonomous Camoufox backend. This is now the default, so you can omit it; pass `--browser chrome` to fall back to the legacy backend.
    *   `--start-date`: Start date (YYYY-MM-DD)
    *   `--end-date`: End date (YYYY-MM-DD)
    *   `--num-workers`: Number of parallel browser instances (default: 3)
    *   `--data-root`: Local output root for this machine or run
    *   `--max-concurrent-cases`: Max concurrent case tabs per worker (default: 5)
    *   `--max-concurrent-downloads`: Max concurrent document downloads per worker (default: 10)

2.  **Run** (Camoufox is the default backend):
    ```bash
    python launcher.py --start-date 2024-01-02 --end-date 2024-01-31 --num-workers 3
    ```

3.  **No manual step**: Camoufox windows open and clear the Cloudflare gate on
    their own. Only step in if a window explicitly asks for manual input — the
    normal Turnstile challenge is auto-cleared.

`launcher_camoufox.py` is a thin alias for the same backend; the main
`launcher.py --browser camoufox` is the preferred entry point.

### How the automated gate pass works

The Camoufox backend clears the SF portal's Cloudflare gate autonomously on the
current IP — no manual solve and no VPN/IP rotation. Three mechanisms cooperate
(all in `fast_scraper/scraper.py`):

*   **Fresh-fingerprint relaunch.** An escalated challenge that an in-place
    reload can't clear is defeated by relaunching a fresh Camoufox window, which
    draws a new browser fingerprint. This is bounded (`asyncio.wait_for` + a
    capped number of attempts) so a stuck gate never hangs the run.
*   **Rocket Loader bypass for case pages.** Per-case register pages ship every
    script deferred (Cloudflare Rocket Loader), so a *navigated* tab never
    renders and would wedge. Instead the scraper reads case metadata and the
    register of actions via an in-context `fetch()` against the site's datasnap
    REST layer + skeleton HTML (`fetch_case_metadata_via_request`), with no
    navigation — decoupling capture from the render trap.
*   **Deadlock-proof session recovery.** All in-session `page.evaluate` calls and
    the session-refresh solve are wrapped in hard timeouts, so a Cloudflare-
    destroyed context turns into a retryable error instead of hanging a worker
    forever. A challenged metadata fetch routes into an autonomous
    re-clear (a fresh light search-page solve) and the case is retried.

Optional: `--rotate-on-gate` makes a worker exit with an `IP_RESTRICTED` marker
on sustained per-day gating so an external VPN-rotating wrapper (`rotate.py`) can
switch IP and resume. This is a fallback lever and is **not** required — the
autonomous gate pass above is the primary mechanism and clears the gate on a
single, non-rotated IP.

### Legacy Chrome/CDP backend (Outdated)

> **Outdated — prefer the Camoufox workflow above.** The original backend drives
> a real Google Chrome over the CDP remote-debugging protocol (opt in with
> `--browser chrome`). It opens one Chrome window per worker and **you must
> manually solve the Cloudflare challenge in EACH window**; it is also
> vulnerable to the per-request gating that Camoufox now handles automatically.
> Kept only for fallback.

```bash
# Legacy manual-solve path — not recommended
python launcher.py --browser chrome \
  --start-date 2024-01-02 --end-date 2024-01-31 --num-workers 3
```

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
*   **Per-worker isolation**: Each launcher worker runs its own browser instance (a fresh Camoufox context, or a separate Chrome profile on the legacy backend), which avoids lock contention when multiple browsers run in parallel.
