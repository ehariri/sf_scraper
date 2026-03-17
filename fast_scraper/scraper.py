"""Fast SF Superior Court scraper — concurrent browser tabs.

Uses Playwright (browser) for everything, but parallelizes the bottleneck:
  - Opens N case detail tabs concurrently (vs 1 at a time in original)
  - Downloads PDFs concurrently via the browser's HTTP API

The original scraper processes cases sequentially with 2s sleeps.
This version processes N cases at once, which should be ~5x faster.
"""

import argparse
import asyncio
import json
import os
import re
import shutil
import signal
import subprocess
import time
from datetime import datetime, timedelta
from pathlib import Path
from urllib.parse import parse_qs, urlparse

from huggingface_hub import CommitOperationAdd, HfApi
from huggingface_hub.errors import HfHubHTTPError
from playwright.async_api import Error as PlaywrightError
from playwright.async_api import TimeoutError as PlaywrightTimeoutError
from playwright.async_api import async_playwright
from tqdm.asyncio import tqdm

# --- Configuration ---
BASE_URL = "https://webapps.sftc.org/ci"
TARGET_URL = f"{BASE_URL}/CaseInfo.dll"
CHROME_PROFILE_PREFIX = ".sf_manual_profile"
SEARCH_RESULTS_TIMEOUT_MS = 30000
TABLE_IDLE_TIMEOUT_MS = 30000
CASE_READY_POLL_ATTEMPTS = 20
CASE_LAUNCH_STAGGER_MS = 0
SESSION_TIMEOUT_MARKERS = (
    "Your session has timed out",
    "Please refresh the page and start again",
)
RETRYABLE_ERROR_MARKERS = (
    "Execution context was destroyed",
    "ERR_ADDRESS_UNREACHABLE",
    "net::ERR_",
    "Target page, context or browser has been closed",
)
DEFAULT_HF_REPO_ID = "please-the-bot/sf_superior_court"
LOCAL_DATA_ROOT = Path("data")


def chrome_profile_for_port(port):
    return Path.home() / f"{CHROME_PROFILE_PREFIX}_{port}"


def utc_now_iso():
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def absolute_case_url(url):
    if not url:
        return None
    if url.startswith("http"):
        return url
    return f"{BASE_URL}/{url.lstrip('/')}"


def preferred_chrome_window_bounds():
    """
    Prefer the first non-primary display so the challenge window stays off the
    user's main screen when a second monitor is attached.
    """
    try:
        from Quartz import CGDisplayBounds, CGGetActiveDisplayList, CGMainDisplayID

        err, displays, count = CGGetActiveDisplayList(16, None, None)
        if err != 0:
            raise RuntimeError(f"CGGetActiveDisplayList failed: {err}")

        main_display = CGMainDisplayID()
        target_display = main_display
        for display_id in displays[:count]:
            if display_id != main_display:
                target_display = display_id
                break

        bounds = CGDisplayBounds(target_display)
        x = int(bounds.origin.x) + 48
        y = int(bounds.origin.y) + 40
        width = max(900, int(bounds.size.width) - 96)
        height = max(700, int(bounds.size.height) - 80)
        return x, y, width, height
    except Exception:
        return 0, 0, 800, 600


def move_chrome_windows(bounds):
    left, top, width, height = bounds
    right = left + width
    bottom = top + height
    script = f"""
    tell application "Google Chrome"
      if not running then return "not-running"
      if (count of windows) = 0 then make new window
      repeat with w in windows
        set bounds of w to {{{left}, {top}, {right}, {bottom}}}
      end repeat
      return count of windows
    end tell
    """
    subprocess.run(["osascript"], input=script, text=True, capture_output=True)


# --- Chrome Management ---


def launch_chrome(port, manage_windows=False):
    """Launch a real Chrome instance with remote debugging enabled."""
    profile = chrome_profile_for_port(port)
    profile.mkdir(exist_ok=True)
    window_x, window_y, window_width, window_height = preferred_chrome_window_bounds()

    try:
        subprocess.check_output(f"lsof -i :{port}", shell=True)
        print(f"Chrome is already running on port {port}.")
        if manage_windows:
            move_chrome_windows((window_x, window_y, window_width, window_height))
        return
    except subprocess.CalledProcessError:
        pass

    cmd = [
        "open",
        "-g",
        "-na",
        "Google Chrome",
        "--args",
        f"--user-data-dir={profile}",
        f"--remote-debugging-port={port}",
        "--no-first-run",
        "--no-default-browser-check",
        f"--window-size={window_width},{window_height}",
        f"--window-position={window_x},{window_y}",
    ]
    subprocess.Popen(cmd)
    print(f"Launched Chrome on port {port}. Waiting 2s for startup...")
    time.sleep(2)
    if manage_windows:
        move_chrome_windows((window_x, window_y, window_width, window_height))


def kill_chrome(port):
    """Kills the Chrome process running on the debugging port."""
    try:
        pid = (
            subprocess.check_output(f"lsof -i :{port} -t", shell=True)
            .decode()
            .strip()
        )
        if pid:
            os.kill(int(pid), signal.SIGTERM)
            print(f"Killed Chrome PID: {pid}")
            time.sleep(2)
    except Exception as e:
        print(f"Error killing Chrome: {e}")


def minimize_chrome():
    """Minimize Chrome windows via AppleScript so they don't pop to foreground."""
    try:
        subprocess.run(
            [
                "osascript",
                "-e",
                'tell application "Google Chrome" to set miniaturized of every window to true',
            ],
            capture_output=True,
        )
    except Exception:
        pass


# --- Browser Session Setup ---


class BrowserStuckError(Exception):
    def __init__(self, message, failed_case_num=None):
        super().__init__(message)
        self.failed_case_num = failed_case_num


class SessionExpiredError(Exception):
    """Raised when the court site returns a timed-out session page."""


class RetryableCaseError(Exception):
    def __init__(self, message, failed_case_num=None):
        super().__init__(message)
        self.failed_case_num = failed_case_num


class RequestPathUnavailableError(Exception):
    """Raised when the direct GetROA request path cannot be used for a case."""


async def open_sf_page(port):
    """Navigate to the court site, then disconnect to let Cloudflare verify."""
    cdp = f"http://localhost:{port}"
    try:
        async with async_playwright() as p:
            browser = await p.chromium.connect_over_cdp(cdp)
            if browser.contexts:
                context = browser.contexts[0]
            else:
                context = await browser.new_context()
            page = await context.new_page()
            await page.goto(TARGET_URL)
            print("Navigated to court site. Disconnecting to let Cloudflare verify...")
    except Exception as e:
        print(f"Navigation error: {e}")


async def wait_for_session(port):
    """
    Poll Chrome via brief CDP connections until Cloudflare is solved.
    Returns (session_id, cookies).
    """
    cdp = f"http://localhost:{port}"

    await open_sf_page(port)
    print("Waiting for Cloudflare to be solved...")
    print(">>> Please solve the Cloudflare challenge in the Chrome window. <<<")
    await asyncio.sleep(1)

    while True:
        try:
            async with async_playwright() as p:
                browser = await p.chromium.connect_over_cdp(cdp)
                for ctx in browser.contexts:
                    for pg in ctx.pages:
                        if "SessionID=" in pg.url:
                            url_parts = urlparse(pg.url)
                            query = parse_qs(url_parts.query)
                            session_id = query.get("SessionID", [None])[0]
                            if session_id:
                                context = browser.contexts[0]
                                raw_cookies = await context.cookies()
                                cookies = {
                                    c["name"]: c["value"] for c in raw_cookies
                                }
                                print(
                                    f"\nCloudflare passed! SessionID: {session_id}"
                                )
                                return session_id, cookies
        except Exception as e:
            print(f"Waiting for browser... ({e})")

        await asyncio.sleep(3)


async def get_browser_page(port):
    """
    Connect to Chrome and return the page with SessionID.
    Caller keeps playwright context alive.
    """
    cdp = f"http://localhost:{port}"
    p = await async_playwright().start()
    browser = await p.chromium.connect_over_cdp(cdp)

    for ctx in browser.contexts:
        for pg in ctx.pages:
            if "SessionID=" in pg.url:
                return p, browser, pg

    raise RuntimeError("No page with SessionID found")


# --- Case List (browser-based, same as original) ---


async def wait_for_datatable_idle(page, timeout_ms=TABLE_IDLE_TIMEOUT_MS):
    """Wait until DataTables finishes redrawing the current table."""
    await page.wait_for_function(
        """
        () => {
            const processing = document.querySelector('#example_processing');
            if (!processing) {
                return true;
            }
            return window.getComputedStyle(processing).display === 'none';
        }
        """,
        timeout=timeout_ms,
    )


async def page_has_session_timeout(page):
    """Return True when the court site shows its timed-out session page."""
    try:
        body_text = await page.evaluate(
            "() => document.body ? document.body.innerText : ''"
        )
    except Exception:
        return False

    return all(marker in body_text for marker in SESSION_TIMEOUT_MARKERS)


async def wait_for_case_page_state(page):
    """Poll the current page with cheap browser-side checks."""
    for _ in range(CASE_READY_POLL_ATTEMPTS):
        state = await page.evaluate(
            """
            () => {
                const title = document.title || '';
                const bodyText = document.body ? document.body.innerText : '';
                return {
                    hasChallenge:
                        title.includes('Just a moment') ||
                        title.includes('Cloudflare') ||
                        bodyText.includes('challenge-platform'),
                    restricted:
                        bodyText.includes('Per CCP 1161.2') ||
                        bodyText.includes('Case Is Not Available For Viewing'),
                    sessionExpired:
                        bodyText.includes('Your session has timed out') &&
                        bodyText.includes('Please refresh the page and start again'),
                    ready: Boolean(document.querySelector('select[name="example_length"]')),
                };
            }
            """
        )

        if state["hasChallenge"]:
            await asyncio.sleep(2)
            continue
        if state["restricted"]:
            return "restricted"
        if state["sessionExpired"]:
            raise SessionExpiredError("Session expired while loading case page")
        if state["ready"]:
            return "ready"

        await asyncio.sleep(1)

    raise BrowserStuckError("Timeout waiting for case page load")


async def scrape_cases_from_page(page):
    """Extract the full case list in one browser-side pass."""
    rows = page.locator("#example tbody tr")
    raw_cases = await rows.evaluate_all(
        """
        (rows) => rows.map((row, index) => {
            const cells = row.querySelectorAll('td');
            const caseCell = cells[0];
            const link = caseCell ? caseCell.querySelector('a') : null;
            return {
                result_index: index,
                case_num_raw: caseCell ? caseCell.innerText : '',
                title: cells[1] ? cells[1].innerText.trim() : '',
                link: link ? link.getAttribute('href') : null,
            };
        })
        """
    )

    cases = []
    for case in raw_cases:
        case_num = re.sub(r"[^a-zA-Z0-9]", "", case["case_num_raw"])
        cases.append(
            {
                "case_num": case_num,
                "title": case["title"],
                "link": case["link"],
                "result_index": case["result_index"],
            }
        )
    return cases


async def click_new_filings_tab(page):
    """Click the 'Search by New Filings' tab once."""
    await page.click("#ui-id-3")
    print("Clicked 'Search by New Filings' tab.")
    await page.wait_for_selector("#FilingDate", state="visible", timeout=SEARCH_RESULTS_TIMEOUT_MS)


async def get_session_page(context, session_id=None):
    """Return the best current page for a live SessionID, or open one."""
    for pg in context.pages:
        if session_id and f"SessionID={session_id}" in pg.url:
            return pg
    for pg in context.pages:
        if "SessionID=" in pg.url:
            return pg
    return await context.new_page()


async def refresh_session(page, port):
    """Re-enter the court site and wait for a fresh SessionID."""
    print("Session expired. Refreshing session...")
    context = page.context
    try:
        await page.goto(TARGET_URL, wait_until="domcontentloaded")
    except Exception:
        pass

    session_id, _ = await wait_for_session(port)
    session_url = f"{TARGET_URL}?&SessionID={session_id}"
    active_page = await get_session_page(context, session_id)

    last_error = None
    for attempt in range(3):
        try:
            await active_page.goto(session_url, wait_until="domcontentloaded")
            if await page_has_session_timeout(active_page):
                raise SessionExpiredError("Session expired after refresh")
            await active_page.wait_for_selector(
                "#ui-id-3", state="visible", timeout=SEARCH_RESULTS_TIMEOUT_MS
            )
            await click_new_filings_tab(active_page)
            return session_id, active_page
        except (PlaywrightTimeoutError, PlaywrightError, SessionExpiredError) as e:
            last_error = e
            if attempt == 2:
                raise
            await asyncio.sleep(2 * (attempt + 1))
            active_page = await get_session_page(context, session_id)

    raise last_error


async def prepare_search_page(page, session_id, port):
    """Open the new-filings search page, refreshing the session if needed."""
    for attempt in range(3):
        try:
            session_url = f"{TARGET_URL}?&SessionID={session_id}"
            await page.goto(session_url, wait_until="domcontentloaded")
            if await page_has_session_timeout(page):
                raise SessionExpiredError("Session expired before opening search page")

            await page.wait_for_selector(
                "#ui-id-3", state="visible", timeout=SEARCH_RESULTS_TIMEOUT_MS
            )
            await click_new_filings_tab(page)
            if await page_has_session_timeout(page):
                raise SessionExpiredError("Session expired after opening search page")
            return session_id, page
        except SessionExpiredError:
            if attempt == 2:
                raise
            session_id, page = await refresh_session(page, port)
        except (PlaywrightTimeoutError, PlaywrightError) as e:
            if await page_has_session_timeout(page):
                if attempt == 2:
                    raise
                session_id, page = await refresh_session(page, port)
                continue
            if attempt == 2:
                raise
            print(f"Search page not ready ({e}). Refreshing session...")
            session_id, page = await refresh_session(page, port)

    raise SessionExpiredError("Could not prepare search page")


async def fetch_case_list_via_browser(page, date_str):
    """Use browser to search by filing date and return case list."""
    await page.fill("#FilingDate", date_str)
    await page.get_by_role("button", name="Search").click()

    if await page_has_session_timeout(page):
        raise SessionExpiredError("Session expired after submitting date search")

    try:
        await page.wait_for_selector(
            "#example_info, #resultsCount", timeout=SEARCH_RESULTS_TIMEOUT_MS
        )
        await wait_for_datatable_idle(page)
    except Exception:
        if await page_has_session_timeout(page):
            raise SessionExpiredError("Session expired while waiting for search results")
        print(f"  Timed out waiting for search results for {date_str}.")
        return []

    try:
        results_count = page.locator("#resultsCount")
        if await results_count.is_visible():
            text = await results_count.inner_text()
            if "No cases found" in text:
                print(f"  No cases found for {date_str}.")
                return []
    except Exception:
        pass

    try:
        await page.wait_for_selector(
            'select[name="example_length"]', state="visible", timeout=5000
        )
        await page.select_option(
            'select[name="example_length"]', "-1", timeout=5000
        )
        await wait_for_datatable_idle(page)
    except Exception as e:
        print(f"  Could not select 'All' entries: {e}")
        return []

    # Get entry count
    try:
        info_text = await page.locator("#example_info").inner_text()
        match = re.search(r"of\s+([\d,]+)\s+entries", info_text)
        if match:
            total = int(match.group(1).replace(",", ""))
            print(f"  Entry count: {total}")
    except Exception:
        pass

    cases = await scrape_cases_from_page(page)
    print(f"  Scraped {len(cases)} cases from browser.")
    return cases


# --- Case Detail (concurrent browser tabs) ---


DOWNLOAD_SEMAPHORE = None
HF_UPLOAD_SEMAPHORE = None
HF_COMMIT_MAX_ATTEMPTS = 8
DOC_ID_RE = re.compile(r"DocID%3D(\d+)", re.IGNORECASE)


def repo_case_dir(filing_date, case_num):
    return f"data/{filing_date}/{case_num}"


def is_hf_commit_conflict(exc: Exception):
    if not isinstance(exc, HfHubHTTPError):
        return False
    response = getattr(exc, "response", None)
    status_code = getattr(response, "status_code", None)
    if status_code == 412:
        return True
    text = str(exc).lower()
    return "precondition failed" in text or "a commit has happened since" in text


async def create_hf_commit(api, repo_id, operations, message):
    """Create a dataset commit without blocking the event loop."""
    if not operations:
        return

    loop = asyncio.get_running_loop()
    for attempt in range(1, HF_COMMIT_MAX_ATTEMPTS + 1):
        try:
            await loop.run_in_executor(
                None,
                lambda: api.create_commit(
                    repo_id=repo_id,
                    repo_type="dataset",
                    operations=operations,
                    commit_message=message,
                ),
            )
            return
        except Exception as exc:
            if not is_hf_commit_conflict(exc) or attempt == HF_COMMIT_MAX_ATTEMPTS:
                raise
            delay = min(30, 1.5 * (2 ** (attempt - 1)))
            print(
                f"HF commit conflict during '{message}' "
                f"(attempt {attempt}/{HF_COMMIT_MAX_ATTEMPTS}). Retrying in {delay:.1f}s..."
            )
            await asyncio.sleep(delay)


async def upload_case_bundle_to_hf(api, repo_id, filing_date, case_num, output_data, pdf_blobs):
    """Upload case JSON and PDFs to Hugging Face as a single case-level commit."""
    operations = [
        CommitOperationAdd(
            path_in_repo=f"{repo_case_dir(filing_date, case_num)}/register_of_actions.json",
            path_or_fileobj=json.dumps(output_data, indent=2).encode("utf-8"),
        )
    ]

    for filename, body in pdf_blobs.items():
        operations.append(
            CommitOperationAdd(
                path_in_repo=f"{repo_case_dir(filing_date, case_num)}/{filename}",
                path_or_fileobj=body,
            )
        )

    async with HF_UPLOAD_SEMAPHORE:
        await create_hf_commit(
            api,
            repo_id,
            operations,
            f"Add SF Superior Court case {case_num} ({filing_date})",
        )


async def upload_day_summary_to_hf(api, repo_id, date_str, summary):
    """Mirror day summary JSON to the dataset repo."""
    async with HF_UPLOAD_SEMAPHORE:
        await create_hf_commit(
            api,
            repo_id,
            [
                CommitOperationAdd(
                    path_in_repo=f"data/{date_str}/day_summary.json",
                    path_or_fileobj=json.dumps(summary, indent=2).encode("utf-8"),
                )
            ],
            f"Update SF Superior Court day summary {date_str}",
        )


async def upload_failed_cases_to_hf(api, repo_id, date_str, payload):
    """Mirror failed_cases.json to the dataset repo."""
    async with HF_UPLOAD_SEMAPHORE:
        await create_hf_commit(
            api,
            repo_id,
            [
                CommitOperationAdd(
                    path_in_repo=f"data/{date_str}/failed_cases.json",
                    path_or_fileobj=json.dumps(payload, indent=2).encode("utf-8"),
                )
            ],
            f"Update SF Superior Court failed cases {date_str}",
        )


def persist_pdf_blobs_locally(case_dir, pdf_blobs):
    """Persist in-memory PDFs to the case directory as a fallback."""
    case_dir.mkdir(parents=True, exist_ok=True)
    for filename, body in pdf_blobs.items():
        with open(case_dir / filename, "wb") as f:
            f.write(body)


def delete_local_case_pdfs(case_dir):
    """Delete locally cached PDFs for a case while keeping JSON metadata."""
    if not case_dir.exists():
        return
    for path in case_dir.iterdir():
        if path.is_file() and path.suffix.lower() == ".pdf":
            path.unlink(missing_ok=True)


async def save_doc(context, url, folder, filename, keep_local_pdfs):
    """Download a document via browser HTTP API (handles session cookies)."""
    async with DOWNLOAD_SEMAPHORE:
        folder.mkdir(parents=True, exist_ok=True)
        file_path = folder / filename

        if keep_local_pdfs and file_path.exists():
            body = file_path.read_bytes()
            return {
                "body": body,
                "elapsed_seconds": 0.0,
                "bytes": len(body),
                "source": "local_cache",
                "attempts": 0,
            }

        for attempt in range(3):
            started = time.perf_counter()
            try:
                response = await context.request.get(url)
                if response.status == 200:
                    body = await response.body()
                    if keep_local_pdfs:
                        with open(file_path, "wb") as f:
                            f.write(body)
                    return {
                        "body": body,
                        "elapsed_seconds": round(time.perf_counter() - started, 3),
                        "bytes": len(body),
                        "source": "network",
                        "attempts": attempt + 1,
                    }
                else:
                    print(
                        f"    Download failed {filename}: HTTP {response.status} "
                        f"(attempt {attempt+1}/3)"
                    )
            except Exception as e:
                print(
                    f"    Download error {filename}: {e} (attempt {attempt+1}/3)"
                )

            await asyncio.sleep(2 * (attempt + 1))

        return None


async def extract_case_header_metadata(page):
    """Best-effort capture of case-level header fields outside the ROA table."""
    try:
        return await page.evaluate(
            """
            () => {
                const clean = (value) => (value || '').replace(/\\s+/g, ' ').trim();
                const pageTitle = clean(document.title);
                const headerText = [];
                const seenText = new Set();
                const headerSelectors = [
                    'h1', 'h2', 'h3', '.caseTitle', '.case-title',
                    '.pageTitle', '.page-title', 'caption'
                ];
                for (const selector of headerSelectors) {
                    for (const node of document.querySelectorAll(selector)) {
                        if (node.closest('#example_wrapper, #example')) continue;
                        const text = clean(node.textContent);
                        if (text && !seenText.has(text)) {
                            headerText.push(text);
                            seenText.add(text);
                        }
                        if (headerText.length >= 6) break;
                    }
                    if (headerText.length >= 6) break;
                }

                const headerFields = {};
                const seenLabels = new Set();
                for (const row of document.querySelectorAll('table tr')) {
                    if (row.closest('#example_wrapper, #example')) continue;
                    const cells = Array.from(row.querySelectorAll('th, td'))
                        .map((cell) => clean(cell.textContent))
                        .filter(Boolean);
                    if (cells.length < 2) continue;
                    const label = cells[0].replace(/:$/, '');
                    if (!label || label.length > 60 || seenLabels.has(label)) continue;
                    const value = cells.slice(1).join(' | ');
                    if (!value || value.length > 500) continue;
                    headerFields[label] = value;
                    seenLabels.add(label);
                    if (Object.keys(headerFields).length >= 20) break;
                }

                return {
                    page_title: pageTitle,
                    header_text: headerText,
                    header_fields: headerFields,
                };
            }
            """
        )
    except Exception:
        return {
            "page_title": "",
            "header_text": [],
            "header_fields": {},
        }


def empty_case_header_metadata():
    return {
        "page_title": "",
        "header_text": [],
        "header_fields": {},
    }


def parse_case_identifiers(link):
    parsed_url = urlparse(link)
    qs = parse_qs(parsed_url.query)
    case_num = qs.get("CaseNum", ["Unknown"])[0]
    session_id = qs.get("SessionID", [None])[0]
    return case_num, session_id


def action_from_roa_row(row):
    doc_url = row.get("URL") or None
    doc_id_match = DOC_ID_RE.search(doc_url or "")
    doc_id = doc_id_match.group(1) if doc_id_match else None
    action_date = (row.get("FILEDATE") or "").strip()
    return {
        "date": action_date,
        "proceedings": (row.get("RTEXT") or "").strip(),
        "fee": (row.get("FEE") or "").strip(),
        "doc_url": absolute_case_url(doc_url),
        "doc_id": doc_id,
        "doc_filename": (
            f"{action_date}_{doc_id or 'Unknown'}.pdf" if doc_url else None
        ),
    }


async def fetch_case_actions_via_request(context, case_num, session_id):
    if not session_id:
        raise RequestPathUnavailableError(
            f"Missing SessionID for request-based ROA fetch on {case_num}"
        )

    roa_url = (
        f"{BASE_URL}/CaseInfo.dll/datasnap/rest/TServerMethods1/"
        f"GetROA/{case_num}/{session_id}/"
    )
    last_error = None
    for attempt in range(1, 4):
        try:
            response = await context.request.get(
                roa_url, timeout=SEARCH_RESULTS_TIMEOUT_MS
            )
            text = await response.text()

            if all(marker in text for marker in SESSION_TIMEOUT_MARKERS):
                raise SessionExpiredError(
                    f"Session expired while fetching request ROA for {case_num}"
                )

            if response.status != 200:
                raise RequestPathUnavailableError(
                    f"GetROA returned HTTP {response.status} for {case_num}"
                )

            try:
                payload = json.loads(text)
            except json.JSONDecodeError as exc:
                raise RequestPathUnavailableError(
                    f"GetROA returned non-JSON for {case_num}: {exc}"
                ) from exc

            result = payload.get("result")
            if not isinstance(result, list) or len(result) < 2:
                raise RequestPathUnavailableError(
                    f"GetROA payload missing result rows for {case_num}"
                )

            serialized_rows = result[1]
            try:
                if isinstance(serialized_rows, str):
                    stripped_rows = serialized_rows.strip()
                    raw_rows = json.loads(stripped_rows) if stripped_rows else []
                elif isinstance(serialized_rows, list):
                    raw_rows = serialized_rows
                elif serialized_rows in (None, ""):
                    raw_rows = []
                else:
                    raise TypeError(
                        f"Unexpected row payload type: {type(serialized_rows)}"
                    )
            except Exception as exc:
                raise RequestPathUnavailableError(
                    f"GetROA row payload parse failed for {case_num}: {exc}"
                ) from exc

            if not isinstance(raw_rows, list):
                raise RequestPathUnavailableError(
                    f"GetROA row payload is not a list for {case_num}"
                )

            return (
                [action_from_roa_row(row) for row in raw_rows],
                empty_case_header_metadata(),
            )
        except SessionExpiredError:
            raise
        except (RequestPathUnavailableError, PlaywrightError) as exc:
            last_error = exc
            if attempt == 3:
                break
            await asyncio.sleep(0.5 * attempt)

    raise last_error


async def scrape_case_actions_via_browser(context, link, case_num):
    page = await context.new_page()
    try:
        await page.goto(link, wait_until="domcontentloaded")

        if await page_has_session_timeout(page):
            raise SessionExpiredError(f"Session expired while opening case {case_num}")

        state = await wait_for_case_page_state(page)
        header_metadata = await extract_case_header_metadata(page)
        if state == "restricted":
            return [], header_metadata, True

        try:
            await page.select_option(
                'select[name="example_length"]', "-1", timeout=3000
            )
            await wait_for_datatable_idle(page)
        except Exception:
            pass

        rows = page.locator("#example tbody tr")
        raw_actions = await rows.evaluate_all(
            """
            (rows) => rows.map((row) => {
                const cells = row.querySelectorAll('td');
                const anchor = cells[2] ? cells[2].querySelector('a') : null;
                const docUrl = anchor ? anchor.getAttribute('href') : null;
                const docIdMatch = docUrl ? docUrl.match(/DocID%3D(\\d+)/) : null;
                const actionDate = cells[0] ? cells[0].innerText.trim() : '';
                const docId = docIdMatch ? docIdMatch[1] : null;
                return {
                    date: actionDate,
                    proceedings: cells[1] ? cells[1].innerText.trim() : '',
                    fee: cells[3] ? cells[3].innerText.trim() : '',
                    doc_url: docUrl,
                    doc_id: docId,
                    doc_filename: docUrl ? `${actionDate}_${docId || 'Unknown'}.pdf` : null,
                };
            })
            """
        )

        actions = []
        for action in raw_actions:
            actions.append(
                {
                    "date": action["date"],
                    "proceedings": action["proceedings"],
                    "fee": action["fee"],
                    "doc_url": absolute_case_url(action["doc_url"]),
                    "doc_id": action["doc_id"],
                    "doc_filename": action["doc_filename"],
                }
            )

        return actions, header_metadata, False
    finally:
        await page.close()


async def scrape_case(
    context, case, filing_date, api, hf_repo_id, keep_local_pdfs, hf_only
):
    """
    Scrape a single case in its own browser tab.
    Same logic as original but runs concurrently with other cases.
    """
    link = case["link"]
    link = absolute_case_url(link)
    case_num, session_id = parse_case_identifiers(link)

    case_dir = LOCAL_DATA_ROOT / filing_date / case_num
    case_dir.mkdir(parents=True, exist_ok=True)
    json_path = case_dir / "register_of_actions.json"
    persist_local_pdfs = keep_local_pdfs or not hf_repo_id
    scrape_started_at = utc_now_iso()
    scrape_started_perf = time.perf_counter()

    # Check if already scraped
    if json_path.exists():
        try:
            with open(json_path, "r") as f:
                data = json.load(f)
                if isinstance(data, dict) and "metadata" in data:
                    meta = data["metadata"]
                    if meta.get("status") == "restricted":
                        return
                    storage = meta.get("storage")
                    if (
                        meta.get("scraped_links", 0) == meta.get("total_links", 0)
                        and meta.get("total_links", 0) > 0
                        and storage not in {"local_fallback", "hf_only_pending"}
                    ):
                        return
        except Exception:
            pass

    try:
        restricted = False
        roa_source = "request"
        try:
            actions, header_metadata = await fetch_case_actions_via_request(
                context, case_num, session_id
            )
        except RequestPathUnavailableError as request_error:
            tqdm.write(
                f"  Request ROA unavailable for {case_num}; falling back to browser: {request_error}"
            )
            actions, header_metadata, restricted = await scrape_case_actions_via_browser(
                context, link, case_num
            )
            roa_source = "browser_fallback"

        if restricted:
            output_data = {
                "metadata": {
                    "case_number": case_num,
                    "case_title": case.get("title", ""),
                    "filing_date": filing_date,
                    "case_url": link,
                    "result_index": case.get("result_index"),
                    "source": {
                        "search_result_title": case.get("title", ""),
                        "search_result_link": link,
                        "source_filing_date": filing_date,
                    },
                    "case_header": header_metadata,
                    "roa_source": roa_source,
                    "status": "restricted",
                    "reason": "CCP 1161.2",
                    "timing": {
                        "scrape_started_at": scrape_started_at,
                        "scrape_finished_at": utc_now_iso(),
                        "scrape_elapsed_seconds": round(
                            time.perf_counter() - scrape_started_perf, 3
                        ),
                        "download_elapsed_seconds": 0.0,
                        "downloaded_bytes": 0,
                        "downloaded_docs": 0,
                        "cached_docs": 0,
                        "download_attempts": 0,
                    },
                }
            }
            with open(json_path, "w") as f:
                json.dump(output_data, f, indent=2)
            return

        download_tasks = []
        total_links = 0
        pdf_filenames = []
        for action in actions:
            if action["doc_url"]:
                total_links += 1
                pdf_filenames.append(action["doc_filename"])
                download_tasks.append(
                    save_doc(
                        context,
                        action["doc_url"],
                        case_dir,
                        action["doc_filename"],
                        persist_local_pdfs,
                    )
                )
        # Download documents in parallel
        pdf_blobs = {}
        download_elapsed_seconds = 0.0
        downloaded_bytes = 0
        download_attempts = 0
        cached_docs = 0
        if download_tasks:
            download_results = await asyncio.gather(*download_tasks)
            for filename, result in zip(pdf_filenames, download_results):
                if result is None:
                    continue
                pdf_blobs[filename] = result["body"]
                download_elapsed_seconds += result.get("elapsed_seconds", 0.0)
                downloaded_bytes += result.get("bytes", 0)
                download_attempts += result.get("attempts", 0)
                if result.get("source") == "local_cache":
                    cached_docs += 1

        # Count successful downloads
        if persist_local_pdfs:
            scraped_links = sum(
                1
                for a in actions
                if a["doc_filename"] and (case_dir / a["doc_filename"]).exists()
            )
        else:
            scraped_links = len(pdf_blobs)

        storage_mode = "local" if persist_local_pdfs else "huggingface"
        output_data = {
            "metadata": {
                "case_number": case_num,
                "case_title": case.get("title", ""),
                "filing_date": filing_date,
                "case_url": link,
                "result_index": case.get("result_index"),
                "source": {
                    "search_result_title": case.get("title", ""),
                    "search_result_link": link,
                    "source_filing_date": filing_date,
                },
                "case_header": header_metadata,
                "roa_source": roa_source,
                "total_entries": len(actions),
                "total_links": total_links,
                "scraped_links": scraped_links,
                "storage": storage_mode,
                "timing": {
                    "scrape_started_at": scrape_started_at,
                    "scrape_finished_at": utc_now_iso(),
                    "scrape_elapsed_seconds": round(
                        time.perf_counter() - scrape_started_perf, 3
                    ),
                    "download_elapsed_seconds": round(download_elapsed_seconds, 3),
                    "downloaded_bytes": downloaded_bytes,
                    "downloaded_docs": len(pdf_blobs),
                    "cached_docs": cached_docs,
                    "download_attempts": download_attempts,
                },
            },
            "actions": actions,
        }

        if hf_repo_id:
            hf_upload_started_perf = time.perf_counter()
            hf_upload_started_at = utc_now_iso()
            try:
                await upload_case_bundle_to_hf(
                    api, hf_repo_id, filing_date, case_num, output_data, pdf_blobs
                )
                if hf_only:
                    delete_local_case_pdfs(case_dir)
                output_data["metadata"]["timing"]["hf_upload_started_at"] = (
                    hf_upload_started_at
                )
                output_data["metadata"]["timing"]["hf_upload_finished_at"] = (
                    utc_now_iso()
                )
                output_data["metadata"]["timing"]["hf_upload_elapsed_seconds"] = round(
                    time.perf_counter() - hf_upload_started_perf, 3
                )
            except Exception as e:
                if not hf_only:
                    persist_pdf_blobs_locally(case_dir, pdf_blobs)
                    output_data["metadata"]["storage"] = "local_fallback"
                else:
                    output_data["metadata"]["storage"] = "hf_only_pending"
                output_data["metadata"]["hf_upload_error"] = str(e)
                output_data["metadata"]["timing"]["hf_upload_started_at"] = (
                    hf_upload_started_at
                )
                output_data["metadata"]["timing"]["hf_upload_finished_at"] = (
                    utc_now_iso()
                )
                output_data["metadata"]["timing"]["hf_upload_elapsed_seconds"] = round(
                    time.perf_counter() - hf_upload_started_perf, 3
                )
                if hf_only:
                    tqdm.write(
                        f"  HF upload failed for {case_num}; kept only JSON metadata locally"
                    )
                else:
                    tqdm.write(
                        f"  HF upload failed for {case_num}; kept PDFs locally for retry"
                    )

        with open(json_path, "w") as f:
            json.dump(output_data, f, indent=2)

        tqdm.write(f"  Case {case_num}: {scraped_links}/{total_links} docs")

    except SessionExpiredError:
        raise
    except BrowserStuckError:
        raise
    except Exception as e:
        error_text = str(e)
        if "Execution context was destroyed" in error_text:
            raise BrowserStuckError(
                "Execution context destroyed during case scrape",
                failed_case_num=case_num,
            )
        if any(marker in error_text for marker in RETRYABLE_ERROR_MARKERS):
            raise RetryableCaseError(error_text, failed_case_num=case_num)
        tqdm.write(f"  Error scraping case {case_num}: {e}")


# --- Progress Tracking (same format as original) ---


def update_day_summary(date_str, total_cases=None, run_metadata=None):
    """Updates the day_summary.json for a given date."""
    date_dir = LOCAL_DATA_ROOT / date_str
    if not date_dir.exists():
        return {"fully_completed": False}

    summary_path = date_dir / "day_summary.json"

    current_summary = {}
    if summary_path.exists():
        try:
            with open(summary_path, "r") as f:
                current_summary = json.load(f)
        except Exception:
            pass

    if total_cases is None:
        total_cases = current_summary.get("total_cases", 0)

    scraped_cases = 0
    cases_with_timing = 0
    total_scrape_elapsed_seconds = 0.0
    total_download_elapsed_seconds = 0.0
    total_downloaded_bytes = 0
    total_downloaded_docs = 0
    for cd in date_dir.iterdir():
        if cd.is_dir():
            jp = cd / "register_of_actions.json"
            if jp.exists():
                try:
                    with open(jp, "r") as f:
                        data = json.load(f)
                        if isinstance(data, dict) and "metadata" in data:
                            meta = data["metadata"]
                            timing = meta.get("timing", {})
                            if meta.get("status") == "restricted":
                                scraped_cases += 1
                            elif meta.get("storage") == "local":
                                pdf_count = sum(
                                    1
                                    for path in cd.iterdir()
                                    if path.is_file()
                                    and path.suffix.lower() == ".pdf"
                                )
                                if pdf_count == meta.get("total_links", 0):
                                    scraped_cases += 1
                            elif meta.get("storage") not in {
                                "local_fallback",
                                "hf_only_pending",
                            } and meta.get("scraped_links", 0) == meta.get(
                                "total_links", 0
                            ):
                                scraped_cases += 1
                            if timing:
                                cases_with_timing += 1
                                total_scrape_elapsed_seconds += timing.get(
                                    "scrape_elapsed_seconds", 0.0
                                )
                                total_download_elapsed_seconds += timing.get(
                                    "download_elapsed_seconds", 0.0
                                )
                                total_downloaded_bytes += timing.get(
                                    "downloaded_bytes", 0
                                )
                                total_downloaded_docs += timing.get(
                                    "downloaded_docs", 0
                                )
                except Exception:
                    pass

    scraped_cases = max(scraped_cases, current_summary.get("scraped_cases", 0))
    fully_completed = (total_cases > 0) and (scraped_cases >= total_cases)
    if current_summary.get("fully_completed") and current_summary.get(
        "total_cases"
    ) == total_cases:
        fully_completed = True

    summary = {
        "date": date_str,
        "total_cases": total_cases,
        "scraped_cases": scraped_cases,
        "fully_completed": fully_completed,
        "updated_at": utc_now_iso(),
        "timing": {
            "cases_with_timing": cases_with_timing,
            "total_scrape_elapsed_seconds": round(total_scrape_elapsed_seconds, 3),
            "total_download_elapsed_seconds": round(
                total_download_elapsed_seconds, 3
            ),
            "total_downloaded_bytes": total_downloaded_bytes,
            "total_downloaded_docs": total_downloaded_docs,
        },
    }
    if run_metadata is not None:
        summary["last_run"] = run_metadata
    elif "last_run" in current_summary:
        summary["last_run"] = current_summary["last_run"]

    with open(summary_path, "w") as f:
        json.dump(summary, f, indent=2)

    return summary


def case_json_path(date_str, case_num):
    return LOCAL_DATA_ROOT / date_str / case_num / "register_of_actions.json"


def case_is_complete(date_str, case_num):
    json_path = case_json_path(date_str, case_num)
    if not json_path.exists():
        return False

    try:
        with open(json_path, "r") as f:
            data = json.load(f)
    except Exception:
        return False

    if not isinstance(data, dict) or "metadata" not in data:
        return False

    meta = data["metadata"]
    if meta.get("status") == "restricted":
        return True

    if meta.get("storage") in {"local_fallback", "hf_only_pending"}:
        return False

    if meta.get("storage") == "local" and meta.get("total_links", 0) > 0:
        case_dir = json_path.parent
        pdf_count = sum(1 for path in case_dir.iterdir() if path.suffix.lower() == ".pdf")
        return pdf_count == meta.get("total_links", 0)

    return meta.get("scraped_links", 0) == meta.get("total_links", 0)


def write_failed_cases(date_str, failed_cases):
    day_dir = LOCAL_DATA_ROOT / date_str
    day_dir.mkdir(parents=True, exist_ok=True)
    failed_path = day_dir / "failed_cases.json"
    payload = {
        "date": date_str,
        "updated_at": utc_now_iso(),
        "failed_cases": [
            {
                "case_num": case["case_num"],
                "link": case["link"],
                "title": case["title"],
                "result_index": case.get("result_index"),
                "source_filing_date": case.get("source_filing_date", date_str),
            }
            for case in failed_cases
        ],
    }
    with open(failed_path, "w") as f:
        json.dump(payload, f, indent=2)
    return payload


def case_link_for_session(case_num, session_id):
    return f"{TARGET_URL}?CaseNum={case_num}&SessionID={session_id}"


def load_failed_cases(date_str, session_id):
    failed_path = LOCAL_DATA_ROOT / date_str / "failed_cases.json"
    if not failed_path.exists():
        return []

    try:
        with open(failed_path, "r") as f:
            payload = json.load(f)
    except Exception:
        return []

    failed_cases = []
    for case in payload.get("failed_cases", []):
        case_num = case.get("case_num")
        if not case_num:
            continue
        failed_cases.append(
            {
                "case_num": case_num,
                "title": case.get("title", ""),
                "link": case_link_for_session(case_num, session_id),
                "result_index": case.get("result_index"),
                "source_filing_date": case.get("source_filing_date", date_str),
            }
        )
    return failed_cases


# --- Main ---


def get_dates(start_str, end_str):
    start = datetime.strptime(start_str, "%Y-%m-%d")
    end = datetime.strptime(end_str, "%Y-%m-%d")
    dates = []
    curr = start
    while curr <= end:
        if curr.weekday() < 5:
            dates.append(curr.strftime("%Y-%m-%d"))
        curr += timedelta(days=1)
    return dates


async def main():
    global DOWNLOAD_SEMAPHORE, HF_UPLOAD_SEMAPHORE
    global SEARCH_RESULTS_TIMEOUT_MS, TABLE_IDLE_TIMEOUT_MS, CASE_READY_POLL_ATTEMPTS
    global CASE_LAUNCH_STAGGER_MS
    global LOCAL_DATA_ROOT

    parser = argparse.ArgumentParser(
        description="Fast SF Court Scraper (concurrent tabs)"
    )
    parser.add_argument(
        "--start-date", type=str, default="2015-01-01",
        help="Start date (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--end-date", type=str, default="2015-01-10",
        help="End date (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--port", type=int, default=9222,
        help="Chrome remote debugging port",
    )
    parser.add_argument(
        "--max-concurrent-cases", type=int, default=2,
        help="Max case tabs open at once",
    )
    parser.add_argument(
        "--max-concurrent-downloads", type=int, default=6,
        help="Max concurrent document downloads",
    )
    parser.add_argument(
        "--clear", action="store_true",
        help="Clear existing data before scraping",
    )
    parser.add_argument(
        "--hf-repo-id", type=str, default=DEFAULT_HF_REPO_ID,
        help="HF dataset repo to upload case outputs into",
    )
    parser.add_argument(
        "--disable-hf-upload", action="store_true",
        help="Do not upload outputs to Hugging Face",
    )
    parser.add_argument(
        "--keep-local-pdfs", action="store_true",
        help="Keep downloaded PDFs on local disk after upload",
    )
    parser.add_argument(
        "--hf-only",
        action="store_true",
        help="HF-first mode: discard PDF fallback on upload failures and keep only lightweight local metadata",
    )
    parser.add_argument(
        "--max-concurrent-hf-uploads", type=int, default=1,
        help="Max concurrent HF case/day-summary commits per worker",
    )
    parser.add_argument(
        "--retry-passes", type=int, default=0,
        help="Number of lower-concurrency retry passes after the first sweep",
    )
    parser.add_argument(
        "--retry-concurrency", type=int, default=0,
        help="Case-tab concurrency for retry passes; 0 uses half of the main concurrency",
    )
    parser.add_argument(
        "--search-timeout-ms", type=int, default=SEARCH_RESULTS_TIMEOUT_MS,
        help="Timeout for search-page selectors and search result loads",
    )
    parser.add_argument(
        "--table-idle-timeout-ms", type=int, default=TABLE_IDLE_TIMEOUT_MS,
        help="Timeout for DataTables redraw waits",
    )
    parser.add_argument(
        "--case-ready-poll-attempts", type=int, default=CASE_READY_POLL_ATTEMPTS,
        help="Case-page readiness polls before timing out",
    )
    parser.add_argument(
        "--case-launch-stagger-ms", type=int, default=CASE_LAUNCH_STAGGER_MS,
        help="Delay before starting each additional case in a batch to reduce simultaneous navigations",
    )
    parser.add_argument(
        "--failed-only",
        action="store_true",
        help="Retry only cases listed in each day's failed_cases.json instead of querying the full filing day",
    )
    parser.add_argument(
        "--data-root", type=Path, default=LOCAL_DATA_ROOT,
        help="Root directory for scraped case data and day summaries",
    )
    parser.add_argument(
        "--manage-chrome-windows",
        action="store_true",
        help="Opt in to AppleScript window positioning for Chrome. Disabled by default to avoid affecting unrelated apps.",
    )
    parser.add_argument(
        "--minimize-chrome-after-session",
        action="store_true",
        help="Opt in to minimizing Chrome after session setup. Disabled by default to avoid affecting unrelated Chrome windows.",
    )
    args = parser.parse_args()

    SEARCH_RESULTS_TIMEOUT_MS = args.search_timeout_ms
    TABLE_IDLE_TIMEOUT_MS = args.table_idle_timeout_ms
    CASE_READY_POLL_ATTEMPTS = args.case_ready_poll_attempts
    CASE_LAUNCH_STAGGER_MS = args.case_launch_stagger_ms
    LOCAL_DATA_ROOT = args.data_root

    DOWNLOAD_SEMAPHORE = asyncio.Semaphore(args.max_concurrent_downloads)
    HF_UPLOAD_SEMAPHORE = asyncio.Semaphore(args.max_concurrent_hf_uploads)
    session_refresh_lock = asyncio.Lock()
    hf_repo_id = None if args.disable_hf_upload else args.hf_repo_id
    hf_api = HfApi() if hf_repo_id else None

    if args.hf_only:
        if not hf_repo_id:
            raise SystemExit("--hf-only requires HF uploads to be enabled")
        if args.keep_local_pdfs:
            raise SystemExit("--hf-only cannot be combined with --keep-local-pdfs")

    dates = get_dates(args.start_date, args.end_date)
    print(f"Dates to scrape: {len(dates)} (weekdays only)")
    if hf_repo_id:
        print(f"Uploading case outputs to HF dataset: {hf_repo_id}")
    print(f"Keeping local PDFs: {args.keep_local_pdfs}")
    print(f"HF-only mode: {args.hf_only}")

    if args.clear:
        for date_str in dates:
            date_dir = LOCAL_DATA_ROOT / date_str
            if date_dir.exists():
                shutil.rmtree(date_dir)
                print(f"Cleared data for {date_str}")

    # Step 1: Launch Chrome and wait for Cloudflare
    launch_chrome(args.port, manage_windows=args.manage_chrome_windows)
    session_id, cookies = await wait_for_session(args.port)

    # Step 2: Connect Playwright (persistent connection for the session)
    p, browser, page = await get_browser_page(args.port)
    context = page.context

    # Step 3: Leave Chrome alone by default so unrelated apps/windows are not affected.
    if args.minimize_chrome_after_session:
        minimize_chrome()
        print("Chrome minimized. Tabs will run in background.")
    else:
        print("Chrome window management disabled. Leaving Chrome windows unchanged.")

    # Step 4: Process each date
    for date_str in dates:
        print(f"\nProcessing date: {date_str}")
        date_started_at = utc_now_iso()
        date_started_perf = time.perf_counter()

        summary = update_day_summary(date_str)
        if summary.get("fully_completed"):
            print(
                f"  Day {date_str} fully scraped "
                f"({summary['scraped_cases']}/{summary['total_cases']}). Skipping."
            )
            continue

        async def recover_shared_session():
            nonlocal session_id, page
            async with session_refresh_lock:
                session_id, page = await refresh_session(page, args.port)

        # Reset page to clean state before each search
        try:
            session_id, page = await prepare_search_page(page, session_id, args.port)
        except Exception as e:
            print(f"  Skipping {date_str} after search-page failure: {e}")
            continue

        # Browser: search and get case list unless retrying failed-only manifests.
        if args.failed_only:
            cases = load_failed_cases(date_str, session_id)
            if not cases:
                print(f"  No failed_cases.json entries for {date_str}. Skipping.")
                continue
            print(f"  Loaded {len(cases)} failed cases from manifest.")
            (LOCAL_DATA_ROOT / date_str).mkdir(parents=True, exist_ok=True)
            update_day_summary(date_str)
        else:
            try:
                cases = await fetch_case_list_via_browser(page, date_str)
            except SessionExpiredError:
                await recover_shared_session()
                cases = await fetch_case_list_via_browser(page, date_str)
            except (PlaywrightTimeoutError, PlaywrightError) as e:
                print(f"  Skipping {date_str} after search failure: {e}")
                continue
            if not cases:
                continue

            (LOCAL_DATA_ROOT / date_str).mkdir(parents=True, exist_ok=True)
            for case in cases:
                case["source_filing_date"] = date_str
            update_day_summary(date_str, total_cases=len(cases))

        pending_cases = [
            case for case in cases if not case_is_complete(date_str, case["case_num"])
        ]
        retry_rounds_run = 0
        if not pending_cases:
            summary = update_day_summary(
                date_str,
                run_metadata={
                    "mode": "failed_only" if args.failed_only else "full_day",
                    "started_at": date_started_at,
                    "finished_at": utc_now_iso(),
                    "elapsed_seconds": round(
                        time.perf_counter() - date_started_perf, 3
                    ),
                    "case_count": len(cases),
                    "pending_case_count": 0,
                    "failed_case_count": 0,
                    "retry_rounds_run": retry_rounds_run,
                    "max_concurrent_cases": args.max_concurrent_cases,
                    "max_concurrent_downloads": args.max_concurrent_downloads,
                    "case_launch_stagger_ms": args.case_launch_stagger_ms,
                },
            )
            if hf_repo_id:
                await upload_day_summary_to_hf(hf_api, hf_repo_id, date_str, summary)
            print(
                f"  Day {date_str} already complete on disk "
                f"({summary['scraped_cases']}/{summary['total_cases']})."
            )
            continue

        retry_concurrency = args.retry_concurrency or max(
            1, args.max_concurrent_cases // 2
        )

        async def run_case_batch(batch_cases, concurrency, label):
            failures = []
            batch_sem = asyncio.Semaphore(concurrency)
            pbar = tqdm(total=len(batch_cases), desc=label, unit="case")

            async def scrape_once(case, case_index):
                async with batch_sem:
                    if not case["link"] or case_is_complete(date_str, case["case_num"]):
                        pbar.update(1)
                        return

                    if CASE_LAUNCH_STAGGER_MS and case_index:
                        slot_offset = case_index % max(1, concurrency)
                        if slot_offset:
                            await asyncio.sleep(
                                (CASE_LAUNCH_STAGGER_MS * slot_offset) / 1000.0
                            )

                    try:
                        await scrape_case(
                            context,
                            case,
                            date_str,
                            hf_api,
                            hf_repo_id,
                            args.keep_local_pdfs,
                            args.hf_only,
                        )
                    except SessionExpiredError:
                        tqdm.write(
                            f"  Session expired while scraping {case['case_num']}; queued for retry"
                        )
                        await recover_shared_session()
                        failures.append(case)
                    except (BrowserStuckError, RetryableCaseError) as e:
                        tqdm.write(
                            f"  Retrying later {case['case_num']}: {e}"
                        )
                        failures.append(case)
                    except Exception as e:
                        tqdm.write(f"  Error on {case['case_num']}: {e}")
                    finally:
                        pbar.update(1)

            await asyncio.gather(
                *(scrape_once(case, idx) for idx, case in enumerate(batch_cases))
            )
            pbar.close()
            return [
                case for case in failures if not case_is_complete(date_str, case["case_num"])
            ]

        failed_cases = await run_case_batch(
            pending_cases, args.max_concurrent_cases, f"  {date_str}"
        )

        for retry_round in range(1, args.retry_passes + 1):
            if not failed_cases:
                break
            retry_rounds_run = retry_round

            tqdm.write(
                f"  Retry pass {retry_round} for {date_str}: "
                f"{len(failed_cases)} cases at concurrency {retry_concurrency}"
            )
            await recover_shared_session()
            failed_cases = await run_case_batch(
                failed_cases,
                retry_concurrency,
                f"  {date_str} retry {retry_round}",
            )

        failed_payload = write_failed_cases(date_str, failed_cases)

        summary = update_day_summary(
            date_str,
            run_metadata={
                "mode": "failed_only" if args.failed_only else "full_day",
                "started_at": date_started_at,
                "finished_at": utc_now_iso(),
                "elapsed_seconds": round(time.perf_counter() - date_started_perf, 3),
                "case_count": len(cases),
                "pending_case_count": len(pending_cases),
                "failed_case_count": len(failed_cases),
                "retry_rounds_run": retry_rounds_run,
                "max_concurrent_cases": args.max_concurrent_cases,
                "max_concurrent_downloads": args.max_concurrent_downloads,
                "case_launch_stagger_ms": args.case_launch_stagger_ms,
            },
        )
        if hf_repo_id:
            await upload_failed_cases_to_hf(
                hf_api, hf_repo_id, date_str, failed_payload
            )
            await upload_day_summary_to_hf(hf_api, hf_repo_id, date_str, summary)
        print(
            f"  Date {date_str} done: "
            f"{summary['scraped_cases']}/{summary['total_cases']} cases"
        )

    # Cleanup
    await browser.close()
    await p.stop()
    kill_chrome(args.port)
    print("\nAll dates processed!")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nExiting...")
