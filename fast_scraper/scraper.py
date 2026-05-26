from __future__ import annotations

"""Fast SF Superior Court scraper — concurrent browser tabs.

Uses Playwright (browser) for everything, but parallelizes the bottleneck:
  - Opens N case detail tabs concurrently (vs 1 at a time in original)
  - Downloads PDFs concurrently via the browser's HTTP API

The original scraper processes cases sequentially with 2s sleeps.
This version processes N cases at once, which should be ~5x faster.
"""

import argparse
import asyncio
import html
import json
import os
import re
import shutil
import signal
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path as _Path

# Cross-scraper heartbeat helper (lives in <repo>/monitor/).
_repo_root_str = str(_Path(__file__).resolve().parent.parent.parent)
if _repo_root_str not in sys.path:
    sys.path.insert(0, _repo_root_str)
from monitor.heartbeat import (  # noqa: E402
    Heartbeat, probe_public_ip, rotation_managed, utc_now_iso,
)

HEARTBEAT: Heartbeat | None = None

from pathlib import Path
from typing import Optional
from urllib.parse import parse_qs, urlencode, urlparse

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
USE_REQUEST_ROA = True
PDF_FILTER_PROFILE = "all"
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
LOCAL_DATA_ROOT = Path("data")
HIGH_VALUE_BRIEF_RE = re.compile(
    r"\b("
    r"MOTION|OPPOSITION|REPLY|DEMURRER|MEMORANDUM OF POINTS|"
    r"POINTS AND AUTHORITIES|TRIAL BRIEF|BRIEF|EX PARTE|"
    r"REQUEST FOR ORDER|RFO|STIPULATION"
    r")\b",
    re.IGNORECASE,
)
HIGH_VALUE_DECLARATION_RE = re.compile(
    r"\b(DECLARATION|AFFIDAVIT|RESPONSIVE DECLARATION)\b",
    re.IGNORECASE,
)
HIGH_VALUE_PLEADING_RE = re.compile(
    r"\b("
    r"ANSWER TO COMPLAINT|COMPLAINT|PETITION|"
    r"CROSS-COMPLAINT|AMENDED COMPLAINT|AMENDED PETITION"
    r")\b",
    re.IGNORECASE,
)
LOW_VALUE_DOC_RE = re.compile(
    r"\b("
    r"PROOF OF SERVICE|SUMMONS|CASE MANAGEMENT|CMC|STATUS CONFERENCE|"
    r"NOTICE OF CASE MANAGEMENT|NOTICE SENT BY COURT|OFF CALENDAR|"
    r"REQUEST FOR DISMISSAL|DISMISSAL|REQUEST FOR ENTRY OF DEFAULT|"
    r"DEFAULT ENTERED|CLERK'?S JUDGMENT|CIVIL CASE COVER SHEET|"
    r"CASE COVER SHEET|COVERSHEET|JUDICIAL COUNCIL|"
    r"STATEMENT OF LOCATION|NON-MILITARY STATUS|FEE WAIVER|"
    r"COST BILL|NOTICE OF HEARING|MINUTE ORDER|ORDER AFTER HEARING|"
    r"NOTICE OF RULING"
    r")\b",
    re.IGNORECASE,
)


def chrome_profile_for_port(port):
    return Path.home() / f"{CHROME_PROFILE_PREFIX}_{port}"


def absolute_case_url(url):
    if not url:
        return None
    if url.startswith("http"):
        return url
    return f"{BASE_URL}/{url.lstrip('/')}"


def required_links_from_metadata(meta):
    selected_links = meta.get("selected_links")
    if selected_links is not None:
        return selected_links
    return meta.get("total_links", 0)


def pdf_count_in_case_dir(case_dir):
    return sum(
        1
        for path in case_dir.iterdir()
        if path.is_file() and path.suffix.lower() == ".pdf"
    )


def case_metadata_is_complete(meta, case_dir=None):
    if meta.get("status") == "restricted":
        return True

    if not meta.get("roa_source"):
        return False

    if meta.get("storage") == "local_fallback":
        return False

    if meta.get("roa_source") == "request" and meta.get("total_entries", 0) == 0:
        return False

    required_links = required_links_from_metadata(meta)
    if meta.get("storage") == "local":
        if case_dir is None or not case_dir.exists():
            return False
        return pdf_count_in_case_dir(case_dir) == required_links

    return meta.get("scraped_links", 0) == required_links


def is_restricted_case_number(case_num):
    """San Francisco unlawful detainer cases are not publicly viewable."""
    return (case_num or "").upper().startswith("CUD")


def build_restricted_case_record(
    case,
    case_num,
    filing_date,
    link,
    scrape_started_at,
    scrape_started_perf,
    header_metadata=None,
    participant_metadata=None,
    roa_source="case_number_prefix",
    reason="CCP 1161.2",
):
    header_metadata = header_metadata or empty_case_header_metadata()
    participant_metadata = participant_metadata or empty_participant_metadata()
    return {
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
            "parties": participant_metadata["parties"],
            "attorneys": participant_metadata["attorneys"],
            "attorney_party_link": participant_metadata["attorney_party_link"],
            "plaintiff_has_counsel": participant_metadata["plaintiff_has_counsel"],
            "defendant_has_counsel": participant_metadata["defendant_has_counsel"],
            "roa_source": roa_source,
            "status": "restricted",
            "reason": reason,
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


def classify_playwright_error(exc):
    error_text = str(exc)
    if "Execution context was destroyed" in error_text:
        return "browser_stuck", error_text
    if any(marker in error_text for marker in RETRYABLE_ERROR_MARKERS):
        return "retryable", error_text
    return None, error_text


def validate_pdf_response(headers, body):
    lowered_headers = {k.lower(): v for k, v in (headers or {}).items()}
    content_type = lowered_headers.get("content-type", "").lower()
    if body.startswith(b"%PDF-"):
        return True, None
    if "pdf" in content_type:
        return True, None
    sample = body[:256].decode("utf-8", errors="ignore").lower()
    if "<html" in sample or "cloudflare" in sample or "your session has timed out" in sample:
        return False, "received HTML/session challenge instead of PDF"
    return False, f"unexpected PDF response content-type '{content_type or 'unknown'}'"


async def get_response_headers(response):
    if hasattr(response, "all_headers"):
        return await response.all_headers()
    return dict(response.headers or {})


def preferred_chrome_window_bounds():
    """Place the challenge browser in the bottom-right half of the screen."""
    try:
        result = subprocess.run(
            ["osascript", "-e", 'tell application "Finder" to get bounds of window of desktop'],
            capture_output=True,
            text=True,
            timeout=3,
            check=False,
        )
        parts = [int(p.strip()) for p in result.stdout.replace("{", "").replace("}", "").split(",")]
        if len(parts) == 4:
            left, top, right, bottom = parts
            width = max(720, (right - left) // 2)
            height = max(450, (bottom - top) // 2)
            return left + width, top + height, width, height
    except Exception:
        pass
    return 720, 450, 720, 450


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


def google_chrome_app_available():
    """Return True when macOS can resolve Google Chrome.app."""
    result = subprocess.run(
        ["open", "-Ra", "Google Chrome"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        check=False,
    )
    return result.returncode == 0


def playwright_chromium_executable():
    """Find Playwright's bundled Chromium binary as a local Chrome fallback."""
    patterns = (
        "Library/Caches/ms-playwright/chromium-*/chrome-mac-arm64/Google Chrome for Testing.app/Contents/MacOS/Google Chrome for Testing",
        "Library/Caches/ms-playwright/chromium-*/chrome-mac/Chromium.app/Contents/MacOS/Chromium",
    )
    for pattern in patterns:
        for path in sorted(Path.home().glob(pattern), reverse=True):
            if path.exists() and os.access(path, os.X_OK):
                return path
    return None


def debug_port_pids(port):
    """Return PIDs listening on the scraper's Chrome debug port."""
    try:
        result = subprocess.run(
            ["lsof", "-nP", "-i", f":{port}", "-t"],
            capture_output=True,
            text=True,
            check=False,
        )
        if result.returncode != 0:
            return []
        return [int(pid) for pid in result.stdout.split() if pid.strip().isdigit()]
    except Exception:
        return []


def launch_chrome(port, manage_windows=False, reuse_existing=False):
    """Launch a real Chrome instance with remote debugging enabled."""
    profile = chrome_profile_for_port(port)
    profile.mkdir(exist_ok=True)
    window_x, window_y, window_width, window_height = preferred_chrome_window_bounds()

    existing_pids = debug_port_pids(port)
    if existing_pids:
        if reuse_existing:
            print(f"Chrome is already running on port {port}. Reusing existing debug browser.")
            if manage_windows:
                move_chrome_windows((window_x, window_y, window_width, window_height))
            return
        print(
            f"Chrome is already running on port {port}. "
            "Killing existing debug browser before relaunch."
        )
        kill_chrome(port)

    browser_args = [
        f"--user-data-dir={profile}",
        f"--remote-debugging-port={port}",
        "--no-first-run",
        "--no-default-browser-check",
        f"--window-size={window_width},{window_height}",
        f"--window-position={window_x},{window_y}",
    ]
    if google_chrome_app_available():
        cmd = ["open", "-g", "-na", "Google Chrome", "--args", *browser_args]
        subprocess.Popen(cmd)
        print(f"Launched Google Chrome on port {port}. Waiting 2s for startup...")
    else:
        chromium = playwright_chromium_executable()
        if chromium is None:
            raise RuntimeError(
                "Could not find Google Chrome.app or Playwright's bundled Chromium. "
                "Install Chrome or run `python -m playwright install chromium`."
            )
        cmd = [str(chromium), *browser_args, "about:blank"]
        subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        print(f"Launched Playwright Chromium on port {port}. Waiting 2s for startup...")
    time.sleep(2)
    if manage_windows:
        move_chrome_windows((window_x, window_y, window_width, window_height))


def kill_chrome(port):
    """Kill all Chrome processes bound to the debugging port.

    `lsof -i :<port> -t` can return multiple PIDs separated by newlines
    when Chrome has multiple listeners; handle that by iterating.
    """
    try:
        pids = debug_port_pids(port)
        if not pids:
            return
        killed_any = False
        for pid in pids:
            try:
                os.kill(pid, signal.SIGTERM)
                print(f"Killed Chrome PID: {pid}")
                killed_any = True
            except ProcessLookupError:
                continue
            except Exception as e:
                print(f"Failed to kill Chrome PID {pid}: {e}")
        if killed_any:
            time.sleep(2)
    except subprocess.CalledProcessError:
        # lsof returns non-zero when nothing matches; treat as no-op.
        return
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


class CloudflareSolveTimeoutError(Exception):
    """Raised when a manual Cloudflare solve does not happen in time."""


@dataclass
class SessionRefreshState:
    session_id: Optional[str] = None
    completed_at: float = 0.0


async def open_sf_page(port):
    """Navigate to the court site, then disconnect to let Cloudflare verify.

    Reuses an existing about:blank or court-site tab if one is present, so
    repeated CF re-challenges during a run don't stack up fresh tabs.
    """
    cdp = f"http://localhost:{port}"
    try:
        async with async_playwright() as p:
            browser = await p.chromium.connect_over_cdp(cdp)
            try:
                if browser.contexts:
                    context = browser.contexts[0]
                else:
                    context = await browser.new_context()

                reusable = None
                for pg in context.pages:
                    pg_url = pg.url or ""
                    if pg_url == "about:blank" or "webapps.sftc.org" in pg_url:
                        reusable = pg
                        break
                page = reusable if reusable is not None else await context.new_page()

                await page.goto(TARGET_URL)
                print("Navigated to court site. Disconnecting to let Cloudflare verify...")
            finally:
                await browser.close()
    except Exception as e:
        print(f"Navigation error: {e}")


async def wait_for_session(port, max_wait_seconds=None):
    """
    Poll Chrome via brief CDP connections until Cloudflare is solved.
    Returns the live SessionID.
    """
    cdp = f"http://localhost:{port}"

    await open_sf_page(port)
    print("Waiting for Cloudflare to be solved...")
    print(">>> Please solve the Cloudflare challenge in the Chrome window. <<<")
    await asyncio.sleep(1)
    started = time.monotonic()

    while True:
        if (
            max_wait_seconds is not None
            and (time.monotonic() - started) >= max_wait_seconds
        ):
            raise CloudflareSolveTimeoutError(
                f"Timed out waiting {max_wait_seconds}s for Cloudflare solve"
            )
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
                                print(
                                    f"\nCloudflare passed! SessionID: {session_id}"
                                )
                                return session_id
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
    browser = None
    try:
        browser = await p.chromium.connect_over_cdp(cdp)

        for ctx in browser.contexts:
            for pg in ctx.pages:
                if "SessionID=" in pg.url:
                    return p, browser, pg

        raise RuntimeError("No page with SessionID found")
    except Exception:
        if browser is not None:
            await browser.close()
        await p.stop()
        raise


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


async def try_reuse_existing_session(page, session_id_hint=None):
    """Try to recover the search page without forcing a fresh Cloudflare solve."""
    context = page.context
    session_id = current_session_id_from_context(context) or session_id_hint
    if not session_id:
        raise SessionExpiredError("No live SessionID available for reuse")

    session_url = f"{TARGET_URL}?&SessionID={session_id}"
    active_page = await get_session_page(context, session_id)
    await active_page.goto(session_url, wait_until="domcontentloaded")
    if await page_has_session_timeout(active_page):
        raise SessionExpiredError("Session expired while reusing existing session")
    await active_page.wait_for_selector(
        "#ui-id-3", state="visible", timeout=SEARCH_RESULTS_TIMEOUT_MS
    )
    await click_new_filings_tab(active_page)
    if await page_has_session_timeout(active_page):
        raise SessionExpiredError("Session expired after reopening search page")
    return session_id, active_page


async def refresh_session(page, port, session_id_hint=None, max_wait_seconds=90):
    """Recover the shared session, preferring cheap reuse before a full solve."""
    global LAST_SHARED_REFRESH
    print("Session expired. Refreshing session...")
    context = page.context

    now = time.monotonic()
    if (
        LAST_SHARED_REFRESH.session_id
        and (now - LAST_SHARED_REFRESH.completed_at) <= SHARED_REFRESH_COOLDOWN_SECONDS
    ):
        try:
            reused_session_id, reused_page = await try_reuse_existing_session(
                page, LAST_SHARED_REFRESH.session_id
            )
            print("Reused recently refreshed shared session.")
            return reused_session_id, reused_page
        except Exception:
            pass

    try:
        reused_session_id, reused_page = await try_reuse_existing_session(
            page, session_id_hint
        )
        LAST_SHARED_REFRESH.session_id = reused_session_id
        LAST_SHARED_REFRESH.completed_at = time.monotonic()
        print("Recovered shared session without full Cloudflare solve.")
        return reused_session_id, reused_page
    except Exception:
        pass

    try:
        await page.goto(TARGET_URL, wait_until="domcontentloaded")
    except Exception:
        pass

    session_id = await wait_for_session(port, max_wait_seconds=max_wait_seconds)
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
            LAST_SHARED_REFRESH.session_id = session_id
            LAST_SHARED_REFRESH.completed_at = time.monotonic()
            return session_id, active_page
        except (PlaywrightTimeoutError, PlaywrightError, SessionExpiredError) as e:
            last_error = e
            if attempt == 2:
                raise CloudflareSolveTimeoutError(
                    f"Session refresh failed after 3 attempts: {e}"
                ) from e
            # Close the failed page before asking for a new one; otherwise
            # get_session_page() will spawn a fresh about:blank tab on
            # every retry and we leak tabs across CF events.
            try:
                await active_page.close()
            except Exception:
                pass
            await asyncio.sleep(2 * (attempt + 1))
            active_page = await get_session_page(context, session_id)

    raise CloudflareSolveTimeoutError(
        f"Session refresh failed after 3 attempts: {last_error}"
    ) from last_error


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
            session_id, page = await refresh_session(
                page, port, session_id_hint=session_id
            )
        except (PlaywrightTimeoutError, PlaywrightError) as e:
            if await page_has_session_timeout(page):
                if attempt == 2:
                    raise
                session_id, page = await refresh_session(
                    page, port, session_id_hint=session_id
                )
                continue
            if attempt == 2:
                raise
            print(f"Search page not ready ({e}). Refreshing session...")
            session_id, page = await refresh_session(
                page, port, session_id_hint=session_id
            )

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
REQUEST_BOOTSTRAP_LOCK = None
SESSION_REFRESH_LOCK = None
LAST_SHARED_REFRESH = SessionRefreshState()
SHARED_REFRESH_COOLDOWN_SECONDS = 20
DOC_ID_RE = re.compile(r"DocID%3D(\d+)", re.IGNORECASE)
CASE_VAR_RE = {
    "casenum": re.compile(r"casenum\s*=\s*['\"]([^'\"]+)['\"]", re.IGNORECASE),
    "seshID": re.compile(r"seshID\s*=\s*['\"]([^'\"]+)['\"]", re.IGNORECASE),
    "accessCode": re.compile(r"accessCode\s*=\s*['\"]([^'\"]*)['\"]", re.IGNORECASE),
}
BAR_NUMBER_RE = re.compile(
    r"(?:bar(?:\s*(?:no\.?|number|#))?|sbn)\s*[:#]?\s*(\d+)",
    re.IGNORECASE,
)
TRANSACTION_ID_RE = re.compile(r"\(TRANSACTION ID #\s*([0-9]+)\)", re.IGNORECASE)
HTML_BREAK_RE = re.compile(r"<br\s*/?>", re.IGNORECASE)
HTML_TAG_RE = re.compile(r"<[^>]+>")
PARTY_ROLE_PATTERNS = (
    ("Cross-Complainant", re.compile(r"\bCROSS[- ]COMPLAINANT\b", re.IGNORECASE)),
    ("Cross-Defendant", re.compile(r"\bCROSS[- ]DEFENDANT\b", re.IGNORECASE)),
    ("Petitioner", re.compile(r"\bPETITIONER\b", re.IGNORECASE)),
    ("Respondent", re.compile(r"\bRESPONDENT\b", re.IGNORECASE)),
    ("Appellant", re.compile(r"\bAPPELLANT\b", re.IGNORECASE)),
    ("Appellee", re.compile(r"\bAPPELLEE\b", re.IGNORECASE)),
    ("Plaintiff", re.compile(r"\bPLAINTIFFS?\b", re.IGNORECASE)),
    ("Defendant", re.compile(r"\bDEFENDANTS?\b", re.IGNORECASE)),
)


def normalize_metadata_text(value):
    return re.sub(r"\s+", " ", (value or "")).strip()


def canonical_party_role(value):
    normalized = normalize_metadata_text(value)
    if not normalized:
        return ""
    for role, pattern in PARTY_ROLE_PATTERNS:
        if pattern.search(normalized):
            return role
    return normalized


def detected_party_role(value):
    normalized = normalize_metadata_text(value)
    if not normalized:
        return ""
    for role, pattern in PARTY_ROLE_PATTERNS:
        if pattern.search(normalized):
            return role
    return ""


def parse_bar_number(value):
    match = BAR_NUMBER_RE.search(value or "")
    return match.group(1) if match else None


def clean_attorney_name(value):
    name = normalize_metadata_text(value)
    if not name:
        return ""
    name = re.sub(
        r"\s*\(?\b(?:bar(?:\s*(?:no\.?|number|#))?|sbn)\s*[:#]?\s*\d+\)?",
        "",
        name,
        flags=re.IGNORECASE,
    )
    return normalize_metadata_text(name.rstrip(" ,;:-()"))


def action_transaction_id(action):
    proceedings = action.get("proceedings") or ""
    match = TRANSACTION_ID_RE.search(proceedings)
    return match.group(1) if match else None


def htmlish_lines(value):
    if not value:
        return []
    text = HTML_BREAK_RE.sub("\n", value)
    text = HTML_TAG_RE.sub(" ", text)
    text = html.unescape(text)
    return [line for line in (normalize_metadata_text(part) for part in text.splitlines()) if line]


def empty_participant_metadata():
    return {
        "parties": [],
        "attorneys": [],
        "attorney_party_link": [],
        "plaintiff_has_counsel": False,
        "defendant_has_counsel": False,
    }


def parse_datasnap_result_rows(response_text, endpoint_name):
    try:
        payload = json.loads(response_text)
    except json.JSONDecodeError as exc:
        raise RequestPathUnavailableError(
            f"{endpoint_name} returned non-JSON payload: {exc}"
        ) from exc

    result = payload.get("result")
    if not isinstance(result, list) or len(result) < 2:
        raise RequestPathUnavailableError(
            f"{endpoint_name} payload missing result rows"
        )
    if result[0] == -1:
        raise RequestPathUnavailableError(
            f"{endpoint_name} returned session-invalid sentinel"
        )

    serialized_rows = result[1]
    try:
        if isinstance(serialized_rows, str):
            stripped_rows = serialized_rows.strip()
            rows = json.loads(stripped_rows) if stripped_rows else []
        elif isinstance(serialized_rows, list):
            rows = serialized_rows
        elif serialized_rows in (None, ""):
            rows = []
        else:
            raise TypeError(f"Unexpected row payload type: {type(serialized_rows)}")
    except Exception as exc:
        raise RequestPathUnavailableError(
            f"{endpoint_name} row payload parse failed: {exc}"
        ) from exc

    if not isinstance(rows, list):
        raise RequestPathUnavailableError(
            f"{endpoint_name} row payload is not a list"
        )
    return rows


def parse_parties_from_endpoint_rows(rows):
    parties = []
    seen = set()
    for row in rows:
        name = normalize_metadata_text(" ".join(htmlish_lines(row.get("PARTY"))))
        role = canonical_party_role(row.get("PARTYTYPE"))
        attorney_lines = htmlish_lines(row.get("ATTORNEY"))
        filing_lines = htmlish_lines(row.get("FILING"))
        if not name:
            continue
        dedupe_key = (name.upper(), role.upper())
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)
        raw_parts = [name, role] + attorney_lines + filing_lines
        parties.append(
            {
                "name": name,
                "role": role,
                "raw_text": " | ".join(part for part in raw_parts if part),
            }
        )
    return parties


def parse_attorneys_from_endpoint_rows(rows, parties):
    attorneys = []
    links = []
    seen_attorneys = set()
    seen_links = set()
    party_lookup = {party["name"].upper(): party for party in parties}

    for row in rows:
        name = clean_attorney_name(" ".join(htmlish_lines(row.get("NAME"))))
        if not name:
            continue

        bar_number = normalize_metadata_text(" ".join(htmlish_lines(row.get("BARNUM")))) or None
        address_lines = htmlish_lines(row.get("ADDRESS"))
        represented_lines = htmlish_lines(row.get("PARTY"))

        represented_parties = []
        represented_roles = []
        for represented_line in represented_lines:
            match = re.match(r"^(.*?)\s*\(([^()]+)\)\s*$", represented_line)
            if match:
                represented_name = normalize_metadata_text(match.group(1))
                represented_role = canonical_party_role(match.group(2))
            else:
                represented_name = normalize_metadata_text(represented_line)
                represented_role = detected_party_role(represented_line)

            if represented_name:
                matched_party = party_lookup.get(represented_name.upper())
                if matched_party:
                    represented_parties.append(matched_party["name"])
                    link_key = (
                        name.upper(),
                        matched_party["name"].upper(),
                        canonical_party_role(matched_party.get("role")).upper(),
                    )
                    if link_key not in seen_links:
                        seen_links.add(link_key)
                        links.append(
                            {
                                "attorney_name": name,
                                "party_name": matched_party["name"],
                                "party_role": matched_party.get("role", ""),
                                "raw_text": represented_line,
                            }
                        )
                    continue

            if represented_role:
                represented_roles.append(represented_role)
                matching_parties = [
                    party["name"]
                    for party in parties
                    if canonical_party_role(party.get("role")) == represented_role
                ]
                if len(matching_parties) == 1:
                    party_name = matching_parties[0]
                    represented_parties.append(party_name)
                    link_key = (name.upper(), party_name.upper(), represented_role.upper())
                else:
                    party_name = ""
                    link_key = (name.upper(), "", represented_role.upper())
                if link_key not in seen_links:
                    seen_links.add(link_key)
                    links.append(
                        {
                            "attorney_name": name,
                            "party_name": party_name,
                            "party_role": represented_role,
                            "raw_text": represented_line,
                        }
                    )

        represented_parties = list(dict.fromkeys(represented_parties))
        represented_role = represented_roles[0] if represented_roles else ""
        raw_parts = [name]
        if bar_number:
            raw_parts.append(bar_number)
        raw_parts.extend(address_lines)
        raw_parts.extend(represented_lines)
        attorney_entry = {
            "name": name,
            "bar_number": bar_number or parse_bar_number(" | ".join(raw_parts)),
            "represented_parties": represented_parties,
            "represented_role": represented_role,
            "raw_text": " | ".join(part for part in raw_parts if part),
        }
        dedupe_key = (
            attorney_entry["name"].upper(),
            attorney_entry["bar_number"] or "",
            tuple(attorney_entry["represented_parties"]),
            attorney_entry["represented_role"],
        )
        if dedupe_key in seen_attorneys:
            continue
        seen_attorneys.add(dedupe_key)
        attorneys.append(attorney_entry)

    return attorneys, links


def parse_parties_from_tab_payload(tab_payload):
    parties = []
    seen = set()
    for tab in tab_payload.get("tabs", []):
        label = (tab.get("label") or "").lower()
        if "part" not in label:
            continue
        for table in tab.get("tables", []):
            headers = [normalize_metadata_text(h).lower() for h in table.get("headers", [])]
            rows = table.get("rows", [])
            name_idx = next(
                (idx for idx, header in enumerate(headers) if "party" in header or header == "name"),
                None,
            )
            role_idx = next(
                (
                    idx
                    for idx, header in enumerate(headers)
                    if "role" in header or "type" in header or "capacity" in header
                ),
                None,
            )
            for row in rows:
                cells = [normalize_metadata_text(cell) for cell in row]
                cells = [cell for cell in cells if cell]
                if not cells:
                    continue
                joined = " | ".join(cells)
                name = ""
                role = ""
                if name_idx is not None and name_idx < len(cells):
                    name = cells[name_idx]
                if role_idx is not None and role_idx < len(cells):
                    role = cells[role_idx]
                if not name and len(cells) >= 2:
                    first_role = canonical_party_role(cells[0])
                    second_role = canonical_party_role(cells[1])
                    if first_role and first_role != cells[0] and not second_role:
                        role = cells[0]
                        name = cells[1]
                    else:
                        name = cells[0]
                        role = cells[1]
                if not name and len(cells) == 1:
                    solo = cells[0]
                    colon_match = re.match(r"([^:]{1,50}):\s*(.+)$", solo)
                    if colon_match:
                        left_role = canonical_party_role(colon_match.group(1))
                        if left_role and left_role != colon_match.group(1):
                            role = colon_match.group(1)
                            name = colon_match.group(2)
                    if not name:
                        parts = [part.strip(" -") for part in re.split(r"\s+-\s+", solo, maxsplit=1)]
                        if len(parts) == 2:
                            right_role = canonical_party_role(parts[1])
                            if right_role and right_role != parts[1]:
                                name, role = parts[0], parts[1]
                    if not name:
                        name = solo
                name = normalize_metadata_text(name)
                role = canonical_party_role(role)
                if not name:
                    continue
                dedupe_key = (name.upper(), role.upper())
                if dedupe_key in seen:
                    continue
                seen.add(dedupe_key)
                parties.append(
                    {
                        "name": name,
                        "role": role,
                        "raw_text": joined,
                    }
                )
    return parties


def parse_attorneys_from_tab_payload(tab_payload, parties):
    attorneys = []
    links = []
    seen_attorneys = set()
    seen_links = set()
    party_lookup = {party["name"].upper(): party for party in parties}
    for tab in tab_payload.get("tabs", []):
        label = (tab.get("label") or "").lower()
        if "attor" not in label and "counsel" not in label:
            continue
        for table in tab.get("tables", []):
            headers = [normalize_metadata_text(h).lower() for h in table.get("headers", [])]
            rows = table.get("rows", [])
            name_idx = next(
                (
                    idx
                    for idx, header in enumerate(headers)
                    if (
                        ("attorney" in header or "counsel" in header or header == "name")
                        and "represented" not in header
                        and "party" not in header
                    )
                ),
                None,
            )
            represented_idx = next(
                (
                    idx
                    for idx, header in enumerate(headers)
                    if (
                        "represented" in header
                        or "client" in header
                        or "party" in header
                        or header == "for"
                        or header.startswith("for ")
                    )
                ),
                None,
            )
            for row in rows:
                cells = [normalize_metadata_text(cell) for cell in row]
                cells = [cell for cell in cells if cell]
                if not cells:
                    continue
                joined = " | ".join(cells)
                name = ""
                represented_text = ""
                if name_idx is not None and name_idx < len(cells):
                    name = cells[name_idx]
                if represented_idx is not None and represented_idx < len(cells):
                    represented_text = cells[represented_idx]
                if not name:
                    name = cells[0]
                if len(cells) > 1 and not represented_text:
                    represented_text = cells[-1]
                name = clean_attorney_name(name)
                if not name:
                    continue
                represented_role = detected_party_role(represented_text) or detected_party_role(joined)
                represented_parties = []
                haystacks = [represented_text, joined]
                for party in parties:
                    upper_name = party["name"].upper()
                    if any(upper_name and upper_name in haystack.upper() for haystack in haystacks if haystack):
                        represented_parties.append(party["name"])
                represented_parties = list(dict.fromkeys(represented_parties))
                if not represented_parties and represented_role:
                    matching_parties = [
                        party["name"]
                        for party in parties
                        if canonical_party_role(party.get("role")) == represented_role
                    ]
                    if len(matching_parties) == 1:
                        represented_parties = matching_parties
                attorney_entry = {
                    "name": name,
                    "bar_number": parse_bar_number(joined),
                    "represented_parties": represented_parties,
                    "represented_role": represented_role,
                    "raw_text": joined,
                }
                dedupe_key = (
                    attorney_entry["name"].upper(),
                    attorney_entry["bar_number"] or "",
                    tuple(attorney_entry["represented_parties"]),
                    attorney_entry["represented_role"],
                )
                if dedupe_key not in seen_attorneys:
                    seen_attorneys.add(dedupe_key)
                    attorneys.append(attorney_entry)

                if represented_parties:
                    for party_name in represented_parties:
                        link_key = (name.upper(), party_name.upper(), "")
                        if link_key in seen_links:
                            continue
                        seen_links.add(link_key)
                        links.append(
                            {
                                "attorney_name": name,
                                "party_name": party_name,
                                "party_role": party_lookup.get(party_name.upper(), {}).get("role", ""),
                                "raw_text": represented_text or joined,
                            }
                        )
                elif attorney_entry["represented_role"]:
                    link_key = (name.upper(), "", attorney_entry["represented_role"].upper())
                    if link_key in seen_links:
                        continue
                    seen_links.add(link_key)
                    links.append(
                        {
                            "attorney_name": name,
                            "party_name": "",
                            "party_role": attorney_entry["represented_role"],
                            "raw_text": represented_text or joined,
                        }
                    )
    return attorneys, links


def derive_counsel_flags(parties, attorney_party_link):
    represented_names = {
        link["party_name"].upper()
        for link in attorney_party_link
        if normalize_metadata_text(link.get("party_name"))
    }
    represented_roles = {
        canonical_party_role(link.get("party_role"))
        for link in attorney_party_link
        if normalize_metadata_text(link.get("party_role"))
    }
    plaintiff_has_counsel = any(
        canonical_party_role(party.get("role")) == "Plaintiff"
        and (
            party["name"].upper() in represented_names
            or "Plaintiff" in represented_roles
        )
        for party in parties
    )
    defendant_has_counsel = any(
        canonical_party_role(party.get("role")) == "Defendant"
        and (
            party["name"].upper() in represented_names
            or "Defendant" in represented_roles
        )
        for party in parties
    )
    return plaintiff_has_counsel, defendant_has_counsel


def parse_case_participant_metadata(tab_payload):
    parties = parse_parties_from_tab_payload(tab_payload)
    attorneys, attorney_party_link = parse_attorneys_from_tab_payload(tab_payload, parties)
    plaintiff_has_counsel, defendant_has_counsel = derive_counsel_flags(
        parties, attorney_party_link
    )
    return {
        "parties": parties,
        "attorneys": attorneys,
        "attorney_party_link": attorney_party_link,
        "plaintiff_has_counsel": plaintiff_has_counsel,
        "defendant_has_counsel": defendant_has_counsel,
    }


def parse_case_participant_metadata_from_rows(party_rows, attorney_rows):
    parties = parse_parties_from_endpoint_rows(party_rows)
    attorneys, attorney_party_link = parse_attorneys_from_endpoint_rows(
        attorney_rows, parties
    )
    plaintiff_has_counsel, defendant_has_counsel = derive_counsel_flags(
        parties, attorney_party_link
    )
    return {
        "parties": parties,
        "attorneys": attorneys,
        "attorney_party_link": attorney_party_link,
        "plaintiff_has_counsel": plaintiff_has_counsel,
        "defendant_has_counsel": defendant_has_counsel,
    }


async def save_doc(context, url, folder, filename, write_pdf_to_disk):
    """Download a document via browser HTTP API (handles session cookies).

    The URL has a SessionID query param baked in at ROA-scrape time. If the
    shared session is refreshed before this download runs, the old SessionID
    is dead on the server and the endpoint returns an HTML challenge page.
    Before every attempt we (a) wait on SESSION_REFRESH_LOCK so we never
    dispatch during a refresh, (b) resolve the current live SessionID from
    the browser context (falling back to LAST_SHARED_REFRESH.session_id
    when the context is mid-cleanup), and (c) re-stamp the URL with it.
    """
    async with DOWNLOAD_SEMAPHORE:
        folder.mkdir(parents=True, exist_ok=True)
        file_path = folder / filename

        if write_pdf_to_disk and file_path.exists():
            body = file_path.read_bytes()
            return {
                "body": body,
                "elapsed_seconds": 0.0,
                "bytes": len(body),
                "source": "local_cache",
                "attempts": 0,
            }

        for attempt in range(3):
            # Block while any session refresh is in progress so we never
            # dispatch against a dying session. Acquire-then-release is a
            # no-op when idle.
            if SESSION_REFRESH_LOCK is not None:
                async with SESSION_REFRESH_LOCK:
                    pass

            started = time.perf_counter()
            # Prefer the live SID from open pages. During the tab-cleanup
            # window of a refresh, the context may transiently have no
            # session page open; fall back to the last refresh's SID,
            # which is kept up to date by refresh_session().
            live_sid = (
                current_session_id_from_context(context)
                or LAST_SHARED_REFRESH.session_id
            )
            request_url = replace_case_session_id(url, live_sid) if live_sid else url

            try:
                response = await context.request.get(
                    request_url, timeout=SEARCH_RESULTS_TIMEOUT_MS
                )
                if response.status == 200:
                    headers = await get_response_headers(response)
                    body = await response.body()
                    is_valid_pdf, validation_error = validate_pdf_response(headers, body)
                    if is_valid_pdf:
                        if write_pdf_to_disk:
                            with open(file_path, "wb") as f:
                                f.write(body)
                        return {
                            "body": body,
                            "elapsed_seconds": round(time.perf_counter() - started, 3),
                            "bytes": len(body),
                            "source": "network",
                            "attempts": attempt + 1,
                        }

                    sid_tag = (live_sid[:8] + "…") if live_sid else "none"
                    print(
                        f"    Download rejected {filename}: {validation_error} "
                        f"(attempt {attempt+1}/3, sid={sid_tag})"
                    )
                else:
                    print(
                        f"    Download failed {filename}: HTTP {response.status} "
                        f"(attempt {attempt+1}/3)"
                    )
            except Exception as e:
                err_text = str(e)
                print(
                    f"    Download error {filename}: {err_text} (attempt {attempt+1}/3)"
                )
                # If the browser context has been torn down (e.g. during a
                # Cloudflare re-solve), the request cannot be retried from
                # here. Bail out and let the outer retry pass pick it up.
                if "Target page, context or browser has been closed" in err_text:
                    return None

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

                const rawBodyText = document.body ? (document.body.innerText || '') : '';
                const bodyLines = rawBodyText
                    .split(/\\n+/)
                    .map((line) => clean(line))
                    .filter(Boolean);
                for (const line of bodyLines) {
                    const match = line.match(/^(Case Number|Title|Cause of Action|Generated):\\s*(.+)$/i);
                    if (!match) continue;
                    const label = match[1].replace(/:$/, '');
                    if (!headerFields[label]) {
                        headerFields[label] = match[2];
                    }
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


async def extract_case_participant_tab_payload(page):
    """Capture the raw tables rendered under the parties/attorneys tabs."""
    try:
        return await page.evaluate(
            """
            async () => {
                const clean = (value) => (value || '').replace(/\\s+/g, ' ').trim();
                const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
                const candidateAnchors = Array.from(document.querySelectorAll('a')).filter((anchor) => {
                    const text = clean(anchor.textContent);
                    return /\\b(part(?:y|ies)|attorneys?|counsel)\\b/i.test(text);
                });

                const seen = new Set();
                const tabs = [];

                const tablePayload = (table) => {
                    const headerRow = table.querySelector('thead tr') || table.querySelector('tr');
                    const headers = headerRow
                        ? Array.from(headerRow.querySelectorAll('th, td')).map((cell) => clean(cell.innerText || cell.textContent))
                        : [];
                    const rows = [];
                    for (const row of table.querySelectorAll('tr')) {
                        const cells = Array.from(row.querySelectorAll('td, th'))
                            .map((cell) => clean(cell.innerText || cell.textContent));
                        if (!cells.some(Boolean)) continue;
                        const sameAsHeaders =
                            headers.length === cells.length &&
                            headers.every((header, idx) => header === cells[idx]);
                        if (sameAsHeaders) continue;
                        rows.push(cells);
                    }
                    return { headers, rows };
                };

                for (const anchor of candidateAnchors) {
                    const label = clean(anchor.textContent);
                    const href = anchor.getAttribute('href') || '';
                    const panelId = anchor.getAttribute('aria-controls') || (href.startsWith('#') ? href.slice(1) : '');
                    const tabKey = `${label}|${panelId}`;
                    if (seen.has(tabKey)) continue;
                    seen.add(tabKey);

                    try {
                        anchor.click();
                        await sleep(150);
                    } catch (error) {
                        // Fall through and try to inspect the panel anyway.
                    }

                    let panel = null;
                    if (panelId) {
                        panel = document.getElementById(panelId);
                    }
                    if (!panel) {
                        const controls = anchor.getAttribute('aria-controls');
                        if (controls) {
                            panel = document.getElementById(controls);
                        }
                    }
                    if (!panel) {
                        panel = anchor.closest('[role="tablist"]')?.parentElement || null;
                    }

                    const tables = Array.from((panel || document).querySelectorAll('table'))
                        .map((table) => tablePayload(table))
                        .filter((table) => table.rows.length > 0 || table.headers.length > 0);

                    tabs.push({
                        label,
                        panel_id: panelId,
                        text: clean(panel ? panel.innerText : ''),
                        tables,
                    });
                }

                return { tabs };
            }
            """
        )
    except Exception:
        return {"tabs": []}


async def extract_case_participant_metadata(page):
    tab_payload = await extract_case_participant_tab_payload(page)
    return parse_case_participant_metadata(tab_payload)


async def fetch_case_participant_metadata_via_request(context, case_num):
    async def fetch_participant_rows(endpoint_name):
        async def run_fetch():
            live_session_id = current_session_id_from_context(context)
            if not live_session_id:
                raise RequestPathUnavailableError(
                    f"No live session available for {endpoint_name} {case_num}"
                )
            session_page = await get_session_page(context, live_session_id)
            response_payload = await session_page.evaluate(
                """
                async ({ url, timeoutMs }) => {
                    const controller = new AbortController();
                    const timeoutId = setTimeout(() => controller.abort(), timeoutMs);
                    try {
                        const response = await fetch(url, {
                            credentials: 'include',
                            signal: controller.signal,
                        });
                        return {
                            status: response.status,
                            text: await response.text(),
                        };
                    } catch (error) {
                        if (error && error.name === 'AbortError') {
                            return {
                                timeout: true,
                                error: `${url} timed out after ${timeoutMs}ms`,
                            };
                        }
                        return {
                            error: error ? String(error) : 'Unknown fetch error',
                        };
                    } finally {
                        clearTimeout(timeoutId);
                    }
                }
                """,
                {
                    "url": f"/ci/CaseInfo.dll/datasnap/rest/TServerMethods1/{endpoint_name}/{case_num}/{live_session_id}/",
                    "timeoutMs": SEARCH_RESULTS_TIMEOUT_MS,
                },
            )
            return response_payload
        if REQUEST_BOOTSTRAP_LOCK is not None:
            async with REQUEST_BOOTSTRAP_LOCK:
                response_payload = await run_fetch()
        else:
            response_payload = await run_fetch()

        if response_payload.get("timeout"):
            raise RetryableCaseError(
                f"{endpoint_name} fetch timed out for {case_num}",
                failed_case_num=case_num,
            )
        if response_payload.get("error"):
            raise RequestPathUnavailableError(
                f"{endpoint_name} fetch failed for {case_num}: {response_payload['error']}"
            )
        if response_payload.get("status") != 200:
            raise RequestPathUnavailableError(
                f"{endpoint_name} returned HTTP {response_payload.get('status')} for {case_num}"
            )
        return parse_datasnap_result_rows(
            response_payload.get("text", ""), endpoint_name
        )

    party_rows = await fetch_participant_rows("GetParties")
    attorney_rows = await fetch_participant_rows("GetAttorneys")
    return parse_case_participant_metadata_from_rows(party_rows, attorney_rows)


def parse_case_identifiers(link):
    parsed_url = urlparse(link)
    qs = parse_qs(parsed_url.query)
    case_num = qs.get("CaseNum", ["Unknown"])[0]
    session_id = qs.get("SessionID", [None])[0]
    return case_num, session_id


def replace_case_session_id(link, session_id):
    if not link or not session_id:
        return link
    parsed_url = urlparse(link)
    qs = parse_qs(parsed_url.query)
    qs["SessionID"] = [session_id]
    return parsed_url._replace(query=urlencode(qs, doseq=True)).geturl()


def html_has_cloudflare_challenge(html):
    if not html:
        return False
    lowered = html.lower()
    return (
        "g-recaptcha" in lowered
        or "challenge-platform" in lowered
        or "follow the prompt so that the court can verify" in lowered
        or "compat=recaptcha" in lowered
    )


def current_session_id_from_context(context):
    for pg in context.pages:
        if "SessionID=" not in pg.url:
            continue
        _, session_id = parse_case_identifiers(pg.url)
        if session_id:
            return session_id
    return None


async def close_stale_scraper_tabs(context, keep_pages=None):
    """Close leftover helper, case, captcha, and duplicate session tabs."""
    keep_ids = {id(page) for page in (keep_pages or []) if page is not None}
    closed = 0
    for page in list(context.pages):
        if id(page) in keep_ids:
            continue
        url = page.url or ""
        should_close = (
            url == "about:blank"
            or "CaseNum=" in url
            or url.startswith("data:text/html")
            or "/captcha/" in url
            or "CaseInfo.dll?&SessionID=" in url
        )
        if not should_close:
            continue
        try:
            await page.close()
            closed += 1
        except Exception:
            pass
    if closed:
        print(f"Closed {closed} stale Chrome tabs.")


def action_from_roa_row(row):
    doc_url = row.get("URL") or None
    doc_id_match = DOC_ID_RE.search(doc_url or "")
    doc_id = doc_id_match.group(1) if doc_id_match else None
    action_date = (row.get("FILEDATE") or "").strip()
    proceedings = (row.get("RTEXT") or "").strip()
    return {
        "date": action_date,
        "proceedings": proceedings,
        "fee": (row.get("FEE") or "").strip(),
        "doc_url": absolute_case_url(doc_url),
        "doc_id": doc_id,
        "transaction_id": action_transaction_id({"proceedings": proceedings}),
        "doc_filename": (
            f"{action_date}_{doc_id or 'Unknown'}.pdf" if doc_url else None
        ),
    }


def classify_pdf_selection(action):
    proceedings = (action.get("proceedings") or "").strip()
    if not action.get("doc_url"):
        return False, "no_document"

    if PDF_FILTER_PROFILE == "all":
        return True, "all_documents"

    if HIGH_VALUE_DECLARATION_RE.search(proceedings):
        return True, "declaration"
    if HIGH_VALUE_BRIEF_RE.search(proceedings):
        return True, "brief_motion"
    if HIGH_VALUE_PLEADING_RE.search(proceedings):
        return True, "pleading"
    if LOW_VALUE_DOC_RE.search(proceedings):
        return False, "low_value_admin"
    return False, "metadata_not_selected"


def annotate_actions_for_download(actions):
    total_links = 0
    selected_links = 0
    for action in actions:
        selected, category = classify_pdf_selection(action)
        if action.get("doc_url"):
            total_links += 1
            if selected:
                selected_links += 1
        action["download_selected"] = bool(selected)
        action["download_filter_category"] = category
    return total_links, selected_links


def parse_case_request_vars(html, fallback_case_num):
    values = {}
    for key, pattern in CASE_VAR_RE.items():
        match = pattern.search(html)
        values[key] = match.group(1) if match else ""

    case_num = values["casenum"] or fallback_case_num
    sesh_id = values["seshID"]
    access_code = values["accessCode"]
    if not sesh_id:
        raise RequestPathUnavailableError(
            f"Case page did not expose seshID for {fallback_case_num}"
        )
    return case_num, sesh_id, access_code


async def fetch_case_actions_via_request(context, link, case_num):
    async def fetch_case_html_via_page(page, url):
        return await page.evaluate(
            """
            async ({ url, timeoutMs }) => {
                const controller = new AbortController();
                const timeoutId = setTimeout(() => controller.abort(), timeoutMs);
                try {
                    const response = await fetch(url, {
                        credentials: 'include',
                        signal: controller.signal,
                    });
                    return {
                        status: response.status,
                        text: await response.text(),
                    };
                } catch (error) {
                    if (error && error.name === 'AbortError') {
                        return {
                            timeout: true,
                            error: `Case bootstrap fetch timed out after ${timeoutMs}ms`,
                        };
                    }
                    return {
                        error: error ? String(error) : 'Unknown fetch error',
                    };
                } finally {
                    clearTimeout(timeoutId);
                }
            }
            """,
            {"url": url, "timeoutMs": SEARCH_RESULTS_TIMEOUT_MS},
        )

    async def fetch_roa_via_page(page, request_case_num, request_sesh_id, access_code):
        return await page.evaluate(
            """
            async ({ caseNum, seshID, accessCode, timeoutMs }) => {
                const roaUrl =
                    `/ci/CaseInfo.dll/datasnap/rest/TServerMethods1/GetROA/${caseNum}/${seshID}/${accessCode || ''}`;
                const controller = new AbortController();
                const timeoutId = setTimeout(() => controller.abort(), timeoutMs);
                let response;
                try {
                    response = await fetch(roaUrl, {
                        credentials: 'include',
                        signal: controller.signal,
                    });
                } catch (error) {
                    if (error && error.name === 'AbortError') {
                        return {
                            timeout: true,
                            error: `GetROA fetch timed out after ${timeoutMs}ms`,
                        };
                    }
                    return {
                        error: error ? String(error) : 'Unknown fetch error',
                    };
                } finally {
                    clearTimeout(timeoutId);
                }
                return {
                    status: response.status,
                    text: await response.text(),
                };
            }
            """,
            {
                "caseNum": request_case_num,
                "seshID": request_sesh_id,
                "accessCode": access_code,
                "timeoutMs": SEARCH_RESULTS_TIMEOUT_MS,
            },
        )

    last_error = None
    for attempt in range(1, 4):
        try:
            async def run_request_fetch():
                live_session_id = current_session_id_from_context(context)
                normalized_link = replace_case_session_id(link, live_session_id)
                session_page = await get_session_page(context, live_session_id)
                case_page_response = await fetch_case_html_via_page(
                    session_page, normalized_link
                )

                if case_page_response.get("timeout"):
                    raise RetryableCaseError(
                        f"Case bootstrap fetch timed out for {case_num}",
                        failed_case_num=case_num,
                    )
                if case_page_response.get("error"):
                    raise RequestPathUnavailableError(
                        f"Case bootstrap fetch failed for {case_num}: {case_page_response['error']}"
                    )
                if case_page_response.get("status") != 200:
                    raise RequestPathUnavailableError(
                        f"Case bootstrap returned HTTP {case_page_response.get('status')} for {case_num}"
                    )
                case_page_html = case_page_response.get("text", "")

                if all(marker in case_page_html for marker in SESSION_TIMEOUT_MARKERS):
                    raise SessionExpiredError(
                        f"Session expired while bootstrapping request ROA for {case_num}"
                    )
                if html_has_cloudflare_challenge(case_page_html):
                    raise RetryableCaseError(
                        f"Cloudflare challenge page returned for request bootstrap {case_num}",
                        failed_case_num=case_num,
                    )

                request_case_num, request_sesh_id, access_code = parse_case_request_vars(
                    case_page_html, case_num
                )
                return await fetch_roa_via_page(
                    session_page, request_case_num, request_sesh_id, access_code
                )
            # Bootstrap is serialized because it reuses a shared authenticated page
            # to derive request vars; concurrent reuse caused session churn.
            if REQUEST_BOOTSTRAP_LOCK is not None:
                async with REQUEST_BOOTSTRAP_LOCK:
                    response_payload = await run_request_fetch()
            else:
                response_payload = await run_request_fetch()
            if response_payload.get("timeout"):
                raise RetryableCaseError(
                    f"GetROA fetch timed out for {case_num}",
                    failed_case_num=case_num,
                )
            if response_payload.get("error"):
                raise RequestPathUnavailableError(
                    f"GetROA fetch failed for {case_num}: {response_payload['error']}"
                )
            response_status = response_payload.get("status")
            text = response_payload.get("text", "")

            if all(marker in text for marker in SESSION_TIMEOUT_MARKERS):
                raise SessionExpiredError(
                    f"Session expired while fetching request ROA for {case_num}"
                )

            if response_status != 200:
                raise RequestPathUnavailableError(
                    f"GetROA returned HTTP {response_status} for {case_num}"
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
            if result[0] == -1:
                raise RequestPathUnavailableError(
                    f"GetROA returned session-invalid sentinel for {case_num}"
                )
            if result[0] == 0:
                raise RequestPathUnavailableError(
                    f"GetROA returned zero rows for {case_num}; verifying via browser"
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
        except (SessionExpiredError, RetryableCaseError):
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
        try:
            participant_metadata = await fetch_case_participant_metadata_via_request(
                context, case_num
            )
        except RequestPathUnavailableError:
            participant_metadata = await extract_case_participant_metadata(page)
        if state == "restricted":
            return [], header_metadata, participant_metadata, True

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
                    transaction_id: (() => {
                        const proceedings = cells[1] ? cells[1].innerText.trim() : '';
                        const match = proceedings.match(/\\(TRANSACTION ID #\\s*([0-9]+)\\)/i);
                        return match ? match[1] : null;
                    })(),
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
                    "transaction_id": action["transaction_id"],
                    "doc_filename": action["doc_filename"],
                }
            )

        return actions, header_metadata, participant_metadata, False
    finally:
        await page.close()


async def fetch_case_metadata_via_browser(context, link, case_num):
    page = await context.new_page()
    try:
        await page.goto(link, wait_until="domcontentloaded")

        if await page_has_session_timeout(page):
            raise SessionExpiredError(f"Session expired while opening case {case_num}")

        state = await wait_for_case_page_state(page)
        header_metadata = await extract_case_header_metadata(page)
        try:
            participant_metadata = await fetch_case_participant_metadata_via_request(
                context, case_num
            )
        except RequestPathUnavailableError:
            participant_metadata = await extract_case_participant_metadata(page)
        return header_metadata, participant_metadata, state == "restricted"
    finally:
        await page.close()


async def scrape_case(context, case, filing_date):
    """
    Scrape a single case in its own browser tab.
    Same logic as original but runs concurrently with other cases.
    """
    link = case["link"]
    link = absolute_case_url(link)
    case_num, session_id = parse_case_identifiers(link)
    live_session_id = current_session_id_from_context(context)
    link = replace_case_session_id(link, live_session_id or session_id)

    case_dir = LOCAL_DATA_ROOT / filing_date / case_num
    case_dir.mkdir(parents=True, exist_ok=True)
    json_path = case_dir / "register_of_actions.json"
    write_pdfs_to_disk_for_run = True
    scrape_started_at = utc_now_iso()
    scrape_started_perf = time.perf_counter()

    # Check if already scraped
    if json_path.exists():
        try:
            with open(json_path, "r") as f:
                data = json.load(f)
                if isinstance(data, dict) and "metadata" in data:
                    meta = data["metadata"]
                    if case_metadata_is_complete(meta, case_dir):
                        return
        except Exception:
            pass

    if is_restricted_case_number(case_num):
        with open(json_path, "w") as f:
            json.dump(
                build_restricted_case_record(
                    case,
                    case_num,
                    filing_date,
                    link,
                    scrape_started_at,
                    scrape_started_perf,
                    roa_source="case_number_prefix",
                ),
                f,
                indent=2,
            )
        return

    try:
        restricted = False
        roa_source = "browser_only"
        header_metadata = empty_case_header_metadata()
        participant_metadata = empty_participant_metadata()
        if USE_REQUEST_ROA:
            header_metadata, participant_metadata, restricted = await fetch_case_metadata_via_browser(
                context, link, case_num
            )
            if restricted:
                actions = []
            else:
                roa_source = "request"
                try:
                    actions, _ = await fetch_case_actions_via_request(
                        context, link, case_num
                    )
                except RequestPathUnavailableError as request_error:
                    tqdm.write(
                        f"  Request ROA unavailable for {case_num}; falling back to browser: {request_error}"
                    )
                    actions, header_metadata, participant_metadata, restricted = await scrape_case_actions_via_browser(
                        context, link, case_num
                    )
                    roa_source = "browser_fallback"
        else:
            actions, header_metadata, participant_metadata, restricted = await scrape_case_actions_via_browser(
                context, link, case_num
            )

        if restricted:
            with open(json_path, "w") as f:
                json.dump(
                    build_restricted_case_record(
                        case,
                        case_num,
                        filing_date,
                        link,
                        scrape_started_at,
                        scrape_started_perf,
                        header_metadata=header_metadata,
                        participant_metadata=participant_metadata,
                        roa_source=roa_source,
                    ),
                    f,
                    indent=2,
                )
            return

        total_links, selected_links = annotate_actions_for_download(actions)
        skipped_links = total_links - selected_links
        download_tasks = []
        pdf_filenames = []
        for action in actions:
            if action.get("doc_url") and action.get("download_selected"):
                pdf_filenames.append(action["doc_filename"])
                download_tasks.append(
                    save_doc(
                        context,
                        action["doc_url"],
                        case_dir,
                        action["doc_filename"],
                        write_pdfs_to_disk_for_run,
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
        if write_pdfs_to_disk_for_run:
            scraped_links = sum(
                1
                for a in actions
                if a.get("download_selected")
                and a["doc_filename"]
                and (case_dir / a["doc_filename"]).exists()
            )
        else:
            scraped_links = len(pdf_blobs)

        storage_mode = "local"
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
                "parties": participant_metadata["parties"],
                "attorneys": participant_metadata["attorneys"],
                "attorney_party_link": participant_metadata["attorney_party_link"],
                "plaintiff_has_counsel": participant_metadata["plaintiff_has_counsel"],
                "defendant_has_counsel": participant_metadata["defendant_has_counsel"],
                "roa_source": roa_source,
                "download_profile": PDF_FILTER_PROFILE,
                "total_entries": len(actions),
                "total_links": total_links,
                "selected_links": selected_links,
                "skipped_links": skipped_links,
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

        with open(json_path, "w") as f:
            json.dump(output_data, f, indent=2)

        if HEARTBEAT is not None:
            HEARTBEAT.increment("session_cases_scraped")
            if scraped_links:
                HEARTBEAT.increment("session_docs_collected", amount=scraped_links)

        tqdm.write(
            f"  Case {case_num}: {scraped_links}/{selected_links} selected docs "
            f"({total_links} total links)"
        )

    except SessionExpiredError:
        raise
    except BrowserStuckError:
        raise
    except RetryableCaseError:
        raise
    except Exception as e:
        error_kind, error_text = classify_playwright_error(e)
        if error_kind == "browser_stuck":
            raise BrowserStuckError(
                "Execution context destroyed during case scrape",
                failed_case_num=case_num,
            )
        if error_kind == "retryable":
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
    no_cases_found = bool(
        (run_metadata and run_metadata.get("no_cases_found"))
        or current_summary.get("no_cases_found")
    )

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
                            if case_metadata_is_complete(meta, cd):
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
    fully_completed = no_cases_found or (
        (total_cases > 0) and (scraped_cases >= total_cases)
    )
    if current_summary.get("fully_completed") and current_summary.get(
        "total_cases"
    ) == total_cases:
        fully_completed = True

    summary = {
        "date": date_str,
        "total_cases": total_cases,
        "scraped_cases": scraped_cases,
        "fully_completed": fully_completed,
        "no_cases_found": no_cases_found,
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
    return case_metadata_is_complete(meta, json_path.parent)


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
    global DOWNLOAD_SEMAPHORE, REQUEST_BOOTSTRAP_LOCK, SESSION_REFRESH_LOCK
    global SEARCH_RESULTS_TIMEOUT_MS, TABLE_IDLE_TIMEOUT_MS, CASE_READY_POLL_ATTEMPTS
    global CASE_LAUNCH_STAGGER_MS
    global LOCAL_DATA_ROOT
    global USE_REQUEST_ROA
    global PDF_FILTER_PROFILE

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
        "--reuse-existing-browser",
        action="store_true",
        help=(
            "Reuse a Chrome instance already listening on --port instead of "
            "killing it and launching a fresh debug browser."
        ),
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
    parser.add_argument(
        "--disable-request-roa",
        action="store_true",
        help="Force the legacy browser-tab case scrape path instead of using direct GetROA requests.",
    )
    parser.add_argument(
        "--pdf-filter-profile",
        choices=("all", "high_value"),
        default=PDF_FILTER_PROFILE,
        help="Download all linked PDFs or only a metadata-selected high-value subset.",
    )
    parser.add_argument(
        "--worker-id",
        type=int,
        default=None,
        help="Internal: identifies this worker for the monitor's per-worker heartbeat file.",
    )
    args = parser.parse_args()

    SEARCH_RESULTS_TIMEOUT_MS = args.search_timeout_ms
    TABLE_IDLE_TIMEOUT_MS = args.table_idle_timeout_ms
    CASE_READY_POLL_ATTEMPTS = args.case_ready_poll_attempts
    CASE_LAUNCH_STAGGER_MS = args.case_launch_stagger_ms
    LOCAL_DATA_ROOT = args.data_root
    USE_REQUEST_ROA = not args.disable_request_roa
    PDF_FILTER_PROFILE = args.pdf_filter_profile

    DOWNLOAD_SEMAPHORE = asyncio.Semaphore(args.max_concurrent_downloads)
    REQUEST_BOOTSTRAP_LOCK = asyncio.Lock()
    SESSION_REFRESH_LOCK = asyncio.Lock()

    dates = get_dates(args.start_date, args.end_date)
    print(f"Dates to scrape: {len(dates)} (weekdays only)")
    print("Writing PDFs to disk during scrape: True")
    print(f"PDF download profile: {PDF_FILTER_PROFILE}")
    print(f"Request-based ROA: {USE_REQUEST_ROA}")

    # Start cross-scraper heartbeat so the multi-county monitor can show
    # this worker as ACTIVE while running.
    global HEARTBEAT
    HEARTBEAT = Heartbeat(
        LOCAL_DATA_ROOT, scraper="sf",
        args=sys.argv[1:],
        worker_id=args.worker_id,
    )
    HEARTBEAT.update(
        start_date=args.start_date, end_date=args.end_date,
        dates_to_scrape=len(dates),
        port=args.port,
        pdf_filter_profile=PDF_FILTER_PROFILE,
        max_concurrent_cases=args.max_concurrent_cases,
        max_concurrent_downloads=args.max_concurrent_downloads,
        rotation_managed=rotation_managed(),
        current_ip=probe_public_ip(),
        session_cases_scraped=0,
        session_docs_collected=0,
    )
    HEARTBEAT.start()

    if args.clear:
        for date_str in dates:
            date_dir = LOCAL_DATA_ROOT / date_str
            if date_dir.exists():
                shutil.rmtree(date_dir)
                print(f"Cleared data for {date_str}")

    p = None
    browser = None
    page = None
    chrome_started = False
    try:
        # Step 1: Launch Chrome and wait for Cloudflare
        launch_chrome(
            args.port,
            manage_windows=args.manage_chrome_windows,
            reuse_existing=args.reuse_existing_browser,
        )
        chrome_started = True
        session_id = await wait_for_session(args.port)

        # Step 2: Connect Playwright (persistent connection for the session)
        p, browser, page = await get_browser_page(args.port)
        context = page.context
        await close_stale_scraper_tabs(context, keep_pages=[page])

        # Step 3: Leave Chrome alone by default so unrelated apps/windows are not affected.
        if args.minimize_chrome_after_session:
            minimize_chrome()
            print("Chrome minimized. Tabs will run in background.")
        else:
            print("Chrome window management disabled. Leaving Chrome windows unchanged.")

        # Step 4: Process each date
        for date_str in dates:
            print(f"\nProcessing date: {date_str}")
            if HEARTBEAT is not None:
                HEARTBEAT.update(current_day=date_str, current_case=None,
                                 current_action="day-start",
                                 current_ip=probe_public_ip())
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
                async with SESSION_REFRESH_LOCK:
                    session_id, page = await refresh_session(
                        page, args.port, session_id_hint=session_id
                    )
                    await close_stale_scraper_tabs(context, keep_pages=[page])

            # Reset page to clean state before each search
            try:
                session_id, page = await prepare_search_page(page, session_id, args.port)
                await close_stale_scraper_tabs(context, keep_pages=[page])
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
                    try:
                        await recover_shared_session()
                    except CloudflareSolveTimeoutError as e:
                        print(f"  Skipping {date_str} after session refresh timeout: {e}")
                        continue
                    cases = await fetch_case_list_via_browser(page, date_str)
                except (PlaywrightTimeoutError, PlaywrightError) as e:
                    print(f"  Skipping {date_str} after search failure: {e}")
                    continue
                if not cases:
                    write_failed_cases(date_str, [])
                    update_day_summary(
                        date_str,
                        total_cases=0,
                        run_metadata={
                            "mode": "failed_only" if args.failed_only else "full_day",
                            "started_at": date_started_at,
                            "finished_at": utc_now_iso(),
                            "elapsed_seconds": round(
                                time.perf_counter() - date_started_perf, 3
                            ),
                            "case_count": 0,
                            "pending_case_count": 0,
                            "failed_case_count": 0,
                            "retry_rounds_run": 0,
                            "max_concurrent_cases": args.max_concurrent_cases,
                            "max_concurrent_downloads": args.max_concurrent_downloads,
                            "case_launch_stagger_ms": args.case_launch_stagger_ms,
                            "no_cases_found": True,
                        },
                    )
                    print(f"  No cases found for {date_str}; recorded zero-case day.")
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

                        # Don't dispatch a new case tab while a session
                        # refresh is in progress. Acquire-then-release is a
                        # no-op when idle; when a refresh is running, this
                        # blocks new cases from being started against a
                        # dying session.
                        if SESSION_REFRESH_LOCK is not None:
                            async with SESSION_REFRESH_LOCK:
                                pass

                        if CASE_LAUNCH_STAGGER_MS and case_index:
                            slot_offset = case_index % max(1, concurrency)
                            if slot_offset:
                                await asyncio.sleep(
                                    (CASE_LAUNCH_STAGGER_MS * slot_offset) / 1000.0
                                )

                        try:
                            needs_shared_retry = True
                            while True:
                                try:
                                    await scrape_case(
                                        context,
                                        case,
                                        date_str,
                                    )
                                    break
                                except SessionExpiredError:
                                    if not needs_shared_retry:
                                        tqdm.write(
                                            f"  Session expired while scraping {case['case_num']}; queued for retry"
                                        )
                                        failures.append(case)
                                        break
                                    tqdm.write(
                                        f"  Session expired for {case['case_num']}; refreshing shared session and retrying once"
                                    )
                                    try:
                                        await recover_shared_session()
                                        needs_shared_retry = False
                                        continue
                                    except CloudflareSolveTimeoutError as refresh_error:
                                        tqdm.write(
                                            f"  Cloudflare solve timed out while refreshing session for {case['case_num']}: {refresh_error}"
                                        )
                                        failures.append(case)
                                        break
                                except (BrowserStuckError, RetryableCaseError) as e:
                                    is_challenge = (
                                        "Cloudflare challenge page returned for request bootstrap"
                                        in str(e)
                                    )
                                    if is_challenge and needs_shared_retry:
                                        tqdm.write(
                                            f"  Cloudflare challenge hit during request bootstrap for {case['case_num']}; refreshing shared session and retrying once"
                                        )
                                        try:
                                            await recover_shared_session()
                                            needs_shared_retry = False
                                            continue
                                        except CloudflareSolveTimeoutError as refresh_error:
                                            tqdm.write(
                                                f"  Cloudflare solve timed out while refreshing session for {case['case_num']}: {refresh_error}"
                                            )
                                    tqdm.write(
                                        f"  Retrying later {case['case_num']}: {e}"
                                    )
                                    failures.append(case)
                                    break
                                except Exception as e:
                                    tqdm.write(f"  Error on {case['case_num']}: {e}")
                                    break
                        finally:
                            pbar.update(1)

                await asyncio.gather(
                    *(scrape_once(case, idx) for idx, case in enumerate(batch_cases))
                )
                pbar.close()
                return [
                    case
                    for case in failures
                    if not case_is_complete(date_str, case["case_num"])
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
                try:
                    await recover_shared_session()
                except CloudflareSolveTimeoutError as e:
                    tqdm.write(
                        f"  Stopping retry pass {retry_round} for {date_str} after session refresh timeout: {e}"
                    )
                    break
                failed_cases = await run_case_batch(
                    failed_cases,
                    retry_concurrency,
                    f"  {date_str} retry {retry_round}",
                )

            write_failed_cases(date_str, failed_cases)

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
            print(
                f"  Date {date_str} done: "
                f"{summary['scraped_cases']}/{summary['total_cases']} cases"
            )

        print("\nAll dates processed!")
        if HEARTBEAT is not None:
            HEARTBEAT.close(status="exited", finished_reason="completed")
    finally:
        if HEARTBEAT is not None:
            # If we didn't reach the clean "completed" close above, write a
            # terminal record so the monitor doesn't keep showing ACTIVE.
            try:
                state = HEARTBEAT._state.get("status") if hasattr(HEARTBEAT, "_state") else None
            except Exception:
                state = None
            if state not in {"exited", "crashed"}:
                HEARTBEAT.close(status="crashed", finished_reason="aborted")
        if browser is not None:
            await browser.close()
        if p is not None:
            await p.stop()
        if chrome_started:
            kill_chrome(args.port)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nExiting...")
