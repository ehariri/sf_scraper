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

*   **`sf_launcher.py`**: The main entry point for multi-process scraping. It splits a date range into chunks and launches multiple worker processes.
*   **`sf_multi.py`**: The worker script used by `sf_launcher.py`. It handles the actual scraping for a assigned date range and port.
*   **`sf_parallel.py`**: A standalone script for single-process scraping with parallel document downloads.
*   **`sf_man.py`**: The original single-process scraper (legacy).

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

1.  **Configure**: Edit `sf_launcher.py` to set your desired `START_DATE`, `END_DATE`, and `NUM_WORKERS`.
    ```python
    START_DATE = "2015-01-01"
    END_DATE = "2015-01-10"
    NUM_WORKERS = 3
    ```

2.  **Run**:
    ```bash
    python sf_launcher.py
    ```

3.  **Solve Cloudflare**:
    *   Multiple Chrome windows will open (one for each worker).
    *   **You must manually solve the Cloudflare challenge in EACH window.**
    *   Once solved, the scraper will automatically proceed.

### Single-Process Scraping

To scrape a specific date range using a single browser instance:

1.  **Configure**: Edit `sf_parallel.py` to set `START_DATE` and `END_DATE`.

2.  **Run**:
    ```bash
    python sf_parallel.py
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
*   **Rate Limiting**: The scraper uses a semaphore to limit concurrent downloads to avoid server bans.
*   **Browser Stuck**: If a browser hangs, the script attempts to kill and restart the process automatically.
