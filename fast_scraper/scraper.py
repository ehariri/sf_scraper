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

from playwright.async_api import async_playwright
from tqdm.asyncio import tqdm

# --- Configuration ---
BASE_URL = "https://webapps.sftc.org/ci"
TARGET_URL = f"{BASE_URL}/CaseInfo.dll"
CHROME_PROFILE = Path.home() / ".sf_manual_profile"


# --- Chrome Management ---


def launch_chrome(port):
    """Launches a real Chrome instance with remote debugging enabled."""
    profile = CHROME_PROFILE
    profile.mkdir(exist_ok=True)

    try:
        subprocess.check_output(f"lsof -i :{port}", shell=True)
        print(f"Chrome is already running on port {port}.")
        return
    except subprocess.CalledProcessError:
        pass

    cmd = [
        "open",
        "-na",
        "Google Chrome",
        "--args",
        f"--user-data-dir={profile}",
        f"--remote-debugging-port={port}",
        "--no-first-run",
        "--no-default-browser-check",
        "--window-size=800,600",
        "--window-position=0,0",
    ]
    subprocess.Popen(cmd)
    print(f"Launched Chrome on port {port}. Waiting 2s for startup...")
    time.sleep(2)


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


async def scrape_cases_from_page(page):
    """Extract case list from the browser DOM."""
    cases = []
    rows = page.locator("#example tbody tr")
    count = await rows.count()

    for i in range(count):
        row = rows.nth(i)
        case_num_el = row.locator("td").nth(0)
        case_num_raw = await case_num_el.inner_text()
        case_num = re.sub(r"[^a-zA-Z0-9]", "", case_num_raw)

        link = None
        try:
            link_el = case_num_el.locator("a")
            if await link_el.count() > 0:
                link = await link_el.get_attribute("href")
        except Exception:
            pass

        title = await row.locator("td").nth(1).inner_text()
        cases.append({"case_num": case_num, "title": title, "link": link})

    return cases


async def click_new_filings_tab(page):
    """Click the 'Search by New Filings' tab once."""
    await page.click("#ui-id-3")
    print("Clicked 'Search by New Filings' tab.")
    await page.wait_for_timeout(1000)


async def fetch_case_list_via_browser(page, date_str):
    """Use browser to search by filing date and return case list."""
    # Fill date and search
    await page.fill("#FilingDate", date_str)
    await page.get_by_role("button", name="Search").click()

    # Wait for results table or "No cases found" to appear
    try:
        await page.wait_for_selector(
            '#example_info, #resultsCount', timeout=10000
        )
    except Exception:
        print(f"  Timed out waiting for search results for {date_str}.")
        return []

    await page.wait_for_timeout(500)

    # Check for "No cases found"
    try:
        results_count = page.locator("#resultsCount")
        if await results_count.is_visible():
            text = await results_count.inner_text()
            if "No cases found" in text:
                print(f"  No cases found for {date_str}.")
                return []
    except Exception:
        pass

    # Select "All" entries — wait for it to be visible first
    try:
        await page.wait_for_selector(
            'select[name="example_length"]', state='visible', timeout=5000
        )
        await page.select_option(
            'select[name="example_length"]', "-1", timeout=5000
        )
        await page.wait_for_timeout(1000)
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


async def save_doc(context, url, folder, filename):
    """Download a document via browser HTTP API (handles session cookies)."""
    async with DOWNLOAD_SEMAPHORE:
        folder.mkdir(parents=True, exist_ok=True)
        file_path = folder / filename

        if file_path.exists():
            return

        for attempt in range(3):
            try:
                response = await context.request.get(url)
                if response.status == 200:
                    body = await response.body()
                    with open(file_path, "wb") as f:
                        f.write(body)
                    return
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


async def scrape_case(context, link, filing_date):
    """
    Scrape a single case in its own browser tab.
    Same logic as original but runs concurrently with other cases.
    """
    if not link.startswith("http"):
        link = f"{BASE_URL}/{link}"

    parsed_url = urlparse(link)
    qs = parse_qs(parsed_url.query)
    case_num = qs.get("CaseNum", ["Unknown"])[0]

    case_dir = Path(f"data/{filing_date}/{case_num}")
    case_dir.mkdir(parents=True, exist_ok=True)
    json_path = case_dir / "register_of_actions.json"

    # Check if already scraped
    if json_path.exists():
        try:
            with open(json_path, "r") as f:
                data = json.load(f)
                if isinstance(data, dict) and "metadata" in data:
                    meta = data["metadata"]
                    if meta.get("status") == "restricted":
                        return
                    if (
                        meta.get("scraped_links", 0) == meta.get("total_links", 0)
                        and meta.get("total_links", 0) > 0
                    ):
                        return
        except Exception:
            pass

    page = await context.new_page()
    try:
        await page.goto(link)

        # Wait for page to load
        for _ in range(10):
            try:
                title = await page.title()
                content = await page.content()
                if (
                    "Just a moment" in title
                    or "Cloudflare" in title
                    or "challenge-platform" in content
                ):
                    await asyncio.sleep(2)
                    continue

                if (
                    "Per CCP 1161.2" in content
                    or "Case Is Not Available For Viewing" in content
                ):
                    output_data = {
                        "metadata": {
                            "status": "restricted",
                            "reason": "CCP 1161.2",
                        }
                    }
                    with open(json_path, "w") as f:
                        json.dump(output_data, f, indent=2)
                    return
            except Exception:
                pass

            if await page.locator(
                'select[name="example_length"]'
            ).is_visible():
                break

            await asyncio.sleep(1)
        else:
            tqdm.write(f"  Timed out waiting for case {case_num}")
            raise BrowserStuckError(
                "Timeout waiting for case page load",
                failed_case_num=case_num,
            )

        # Select "All" entries
        try:
            await page.select_option(
                'select[name="example_length"]', "-1", timeout=3000
            )
            await page.wait_for_timeout(1000)
        except Exception:
            pass

        # Scrape register of actions
        actions = []
        download_tasks = []
        rows = page.locator("#example tbody tr")
        count = await rows.count()
        total_links = 0

        for i in range(count):
            row = rows.nth(i)
            cols = row.locator("td")

            action_date = await cols.nth(0).inner_text()
            proceedings = await cols.nth(1).inner_text()
            fee = await cols.nth(3).inner_text()

            doc_link_el = cols.nth(2).locator("a")
            doc_url = None
            doc_id = None
            doc_filename = None

            if await doc_link_el.count() > 0:
                total_links += 1
                doc_url = await doc_link_el.get_attribute("href")
                if doc_url:
                    match = re.search(r"DocID%3D(\d+)", doc_url)
                    doc_id = match.group(1) if match else "Unknown"
                    doc_filename = f"{action_date}_{doc_id}.pdf"
                    download_tasks.append(
                        save_doc(context, doc_url, case_dir, doc_filename)
                    )

            actions.append(
                {
                    "date": action_date,
                    "proceedings": proceedings,
                    "fee": fee,
                    "doc_id": doc_id,
                    "doc_filename": doc_filename,
                }
            )

        # Download documents in parallel
        if download_tasks:
            await asyncio.gather(*download_tasks)

        # Count successful downloads
        scraped_links = sum(
            1
            for a in actions
            if a["doc_filename"] and (case_dir / a["doc_filename"]).exists()
        )

        output_data = {
            "metadata": {
                "total_entries": count,
                "total_links": total_links,
                "scraped_links": scraped_links,
            },
            "actions": actions,
        }

        with open(json_path, "w") as f:
            json.dump(output_data, f, indent=2)

        tqdm.write(f"  Case {case_num}: {scraped_links}/{total_links} docs")

    except BrowserStuckError:
        raise
    except Exception as e:
        tqdm.write(f"  Error scraping case {case_num}: {e}")
    finally:
        await page.close()


# --- Progress Tracking (same format as original) ---


def update_day_summary(date_str, total_cases=None):
    """Updates the day_summary.json for a given date."""
    date_dir = Path(f"data/{date_str}")
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
    for cd in date_dir.iterdir():
        if cd.is_dir():
            jp = cd / "register_of_actions.json"
            if jp.exists():
                try:
                    with open(jp, "r") as f:
                        data = json.load(f)
                        if isinstance(data, dict) and "metadata" in data:
                            meta = data["metadata"]
                            if meta.get("status") == "restricted":
                                scraped_cases += 1
                            elif meta.get("scraped_links", 0) == meta.get(
                                "total_links", 0
                            ):
                                scraped_cases += 1
                except Exception:
                    pass

    fully_completed = (total_cases > 0) and (scraped_cases >= total_cases)

    summary = {
        "date": date_str,
        "total_cases": total_cases,
        "scraped_cases": scraped_cases,
        "fully_completed": fully_completed,
    }

    with open(summary_path, "w") as f:
        json.dump(summary, f, indent=2)

    return summary


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
    global DOWNLOAD_SEMAPHORE

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
        "--max-concurrent-cases", type=int, default=5,
        help="Max case tabs open at once",
    )
    parser.add_argument(
        "--max-concurrent-downloads", type=int, default=10,
        help="Max concurrent document downloads",
    )
    parser.add_argument(
        "--clear", action="store_true",
        help="Clear existing data before scraping",
    )
    args = parser.parse_args()

    DOWNLOAD_SEMAPHORE = asyncio.Semaphore(args.max_concurrent_downloads)
    case_sem = asyncio.Semaphore(args.max_concurrent_cases)

    dates = get_dates(args.start_date, args.end_date)
    print(f"Dates to scrape: {len(dates)} (weekdays only)")

    if args.clear:
        for date_str in dates:
            date_dir = Path(f"data/{date_str}")
            if date_dir.exists():
                shutil.rmtree(date_dir)
                print(f"Cleared data for {date_str}")

    # Step 1: Launch Chrome and wait for Cloudflare
    launch_chrome(args.port)
    session_id, cookies = await wait_for_session(args.port)

    # Step 2: Connect Playwright (persistent connection for the session)
    p, browser, page = await get_browser_page(args.port)
    context = page.context

    # Step 3: Minimize Chrome so tabs don't pop to foreground
    minimize_chrome()
    print("Chrome minimized. Tabs will run in background.")

    # Step 4: Process each date
    for date_str in dates:
        print(f"\nProcessing date: {date_str}")

        summary = update_day_summary(date_str)
        if summary.get("fully_completed"):
            print(
                f"  Day {date_str} fully scraped "
                f"({summary['scraped_cases']}/{summary['total_cases']}). Skipping."
            )
            continue

        # Reset page to clean state before each search
        session_url = f"{TARGET_URL}?&SessionID={session_id}"
        await page.goto(session_url)
        await page.wait_for_timeout(1000)
        await click_new_filings_tab(page)

        # Browser: search and get case list
        cases = await fetch_case_list_via_browser(page, date_str)
        if not cases:
            continue

        Path(f"data/{date_str}").mkdir(parents=True, exist_ok=True)
        update_day_summary(date_str, total_cases=len(cases))

        # Concurrent browser tabs for case details with retry + progress
        completed = 0
        pbar = tqdm(total=len(cases), desc=f"  {date_str}", unit="case")

        async def scrape_with_sem(case):
            nonlocal completed
            async with case_sem:
                if not case["link"]:
                    pbar.update(1)
                    return

                max_retries = 3
                for attempt in range(max_retries):
                    try:
                        await scrape_case(context, case["link"], date_str)
                        break
                    except BrowserStuckError:
                        if attempt < max_retries - 1:
                            await asyncio.sleep(2 * (attempt + 1))
                        else:
                            tqdm.write(
                                f"  Failed {case['case_num']} after "
                                f"{max_retries} attempts"
                            )
                    except Exception as e:
                        tqdm.write(f"  Error on {case['case_num']}: {e}")
                        break

                pbar.update(1)

        tasks = [scrape_with_sem(case) for case in cases]
        await asyncio.gather(*tasks)
        pbar.close()

        summary = update_day_summary(date_str)
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
