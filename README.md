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
*   **`worker.py`**: The older worker implementation retained for reference.
*   **`scraper.py`**: A standalone single-process scraper with parallel document downloads.
*   **`legacy.py`**: The original single-process scraper (legacy).

## Setup

1.  **Install Dependencies**:
    ```bash
    pip install playwright
    playwright install chromium
    ```

2.  **Chrome Installation**: Ensure Google Chrome is installed on your system.

## Usage

### Multi-Process Scraping (Recommended)

This is the fastest way to scrape a range of dates.

1.  **Configure**: You can configure the scraper by editing `launcher.py` or passing arguments directly.
    *   `--start-date`: Start date (YYYY-MM-DD)
    *   `--end-date`: End date (YYYY-MM-DD)
    *   `--num-workers`: Number of parallel Chrome instances (default: 3)
    *   `--max-concurrent-cases`: Max concurrent case tabs per worker (default: 2)
    *   `--max-concurrent-downloads`: Max concurrent document downloads per worker (default: 6)

2.  **Run**:
    ```bash
    # Default (3 workers, 2 concurrent case tabs, 6 concurrent downloads)
    python launcher.py

    # Custom configuration
    python launcher.py --start-date 2015-01-01 --end-date 2015-02-01 --num-workers 5 --max-concurrent-cases 2 --max-concurrent-downloads 6
    ```

3.  **Solve Cloudflare**:
    *   Multiple Chrome windows will open (one for each worker).
    *   **You must manually solve the Cloudflare challenge in EACH window.**
    *   Once solved, the scraper will automatically proceed.

### Single-Process Scraping

To scrape a specific date range using a single browser instance:

1.  **Configure**: Edit `scraper.py` to set `START_DATE` and `END_DATE`.

2.  **Run**:
    ```bash
    python scraper.py
    ```

3.  **Solve Cloudflare**: Solve the challenge in the opened Chrome window.

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

## Notes

*   **Restricted Cases**: Cases marked "Per CCP 1161.2" or "Case Is Not Available For Viewing" are skipped, and their status is recorded.
*   **Rate Limiting**: The fast worker uses separate semaphores for concurrent case tabs and document downloads.
*   **Browser Stuck**: If a browser hangs, the script attempts to kill and restart the process automatically.
*   **Per-worker profiles**: Each launcher worker uses its own Chrome profile, which avoids profile lock contention when multiple browsers run in parallel.
