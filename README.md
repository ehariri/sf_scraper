# SF Superior Court scraper

This is a starter Playwright scraper for the San Francisco Superior Court civil case search site. The site is fronted by Cloudflare and will present a Turnstile-style verification step, so the flow requires a real browser session and a one-time manual solve to establish a usable session.

## What it does
- Opens the public “Civil Case Query” entry page and follows the “Access Now” link to the court search.
- Lets you manually clear the Cloudflare verification once per browser profile.
- Searches the “Search by New Filings” tab for a given filing date.
- Extracts the results table into JSON.

## Setup
1) Install dependencies:
```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python -m playwright install chromium
```

2a) Preferred: use your own Chrome with remote debugging (looks most “human”):
```bash
# macOS example
/Applications/Google\\ Chrome.app/Contents/MacOS/Google\\ Chrome \\
  --remote-debugging-port=9222 \\
  --user-data-dir=\"$HOME/.sf_scraper/chrome_profile\" \\
  --no-first-run --no-default-browser-check
```
Then, in another terminal:
```bash
python scraper.py interactive --headless false --cdp-endpoint http://localhost:9222
```

2b) Alternative: let Playwright launch Chrome/Chromium for you:
```bash
python scraper.py interactive
```
- A Chromium/Chrome window opens. Click the “Access Now” under **Civil Case Query**, solve the Cloudflare verification, and wait until the “Civil Case Information Search” page loads.
- The browser is launched with automation flags removed and a realistic user agent to reduce Cloudflare errors. Use the opened tab only; do not spawn new tabs or devtools while solving.
- Return to the terminal and press Enter to persist the session. Cookies are stored under `~/.sf_scraper/chromium` so subsequent runs can stay headless.

If you see repeated Cloudflare errors during the solve, delete the saved profile and retry:
```bash
rm -rf ~/.sf_scraper/chromium
python scraper.py interactive --headless false
```

## Running the scraper
Fetch new filings for a given date (YYYY-MM-DD):
```bash
python scraper.py new-filings --date 2025-01-23 --out data/new_filings_2025-01-23.json
```
You can pair this with a live Chrome session too:
```bash
python scraper.py new-filings --date 2025-01-23 --cdp-endpoint http://localhost:9222 --headless false
```

Flags:
- `--headless false` to watch the run in a visible browser.
- `--timeout-ms` to tweak waits if the site feels slow.
- `--profile-dir` to point at a specific user data dir (copy of an existing Chrome profile is fine).
- `--cdp-endpoint` to attach to a Chrome you started manually with `--remote-debugging-port`.

Outputs:
- JSON array of rows. Headers are derived from the result table; if there’s no header row, fallback keys like `col_0`, `col_1` are used.

## Notes and limitations
- This starter does **not** attempt to bypass Cloudflare; it relies on the saved browser profile after you complete the verification. If the session expires, re-run `interactive`.
- The site’s markup is simple but can change. Selectors favor visible text and should be easy to adjust in `scraper.py` if needed.
- Pagination on the result grid is not yet automated; if the court adds paging controls, you can extend `collect_results` to click “Next” until disabled.
