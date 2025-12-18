import argparse
import asyncio
import json
import os
import re
import signal
import subprocess
import time
from datetime import datetime, timedelta
from pathlib import Path
from urllib.parse import parse_qs, urlparse

from playwright.async_api import async_playwright

# --- Configuration (defaults, can be overridden by CLI args) ---
CHROME_PORT = 9222
TARGET_URL = "https://webapps.sftc.org/ci/CaseInfo.dll"
START_DATE = "2020-01-01"
END_DATE = "2020-01-10"
CHROME_PROFILE = Path.home() / ".sf_manual_profile"

# Federal holidays that courts are definitely closed
# Format: (month, day) for fixed holidays, or computed for floating holidays
def get_federal_holidays(year):
    """Returns a set of (month, day) tuples for federal holidays in a given year."""
    holidays = set()
    
    # Fixed holidays
    holidays.add((1, 1))    # New Year's Day
    holidays.add((7, 4))    # Independence Day  
    holidays.add((12, 25))  # Christmas Day
    
    # Thanksgiving (4th Thursday of November)
    nov_first = datetime(year, 11, 1)
    # Find first Thursday
    days_until_thursday = (3 - nov_first.weekday()) % 7
    first_thursday = nov_first + timedelta(days=days_until_thursday)
    # 4th Thursday
    thanksgiving = first_thursday + timedelta(weeks=3)
    holidays.add((11, thanksgiving.day))
    
    return holidays


# --- Custom Exception ---
class BrowserStuckError(Exception):
    """Raised when the browser is stuck or unresponsive."""

    def __init__(self, message, failed_case_num=None):
        super().__init__(message)
        self.failed_case_num = failed_case_num


def get_dates():
    """Generate list of dates to scrape, excluding weekends and federal holidays."""
    start = datetime.strptime(START_DATE, "%Y-%m-%d")
    end = datetime.strptime(END_DATE, "%Y-%m-%d")
    dates = []
    curr = start
    
    # Build set of all holidays in the date range
    years_in_range = set(range(start.year, end.year + 1))
    all_holidays = set()
    for year in years_in_range:
        for month, day in get_federal_holidays(year):
            all_holidays.add((year, month, day))
    
    while curr <= end:
        # Skip weekends (5=Saturday, 6=Sunday)
        if curr.weekday() >= 5:
            curr += timedelta(days=1)
            continue
        
        # Skip federal holidays
        if (curr.year, curr.month, curr.day) in all_holidays:
            print(f"[INFO] Skipping federal holiday: {curr.strftime('%Y-%m-%d')}")
            curr += timedelta(days=1)
            continue
            
        dates.append(curr.strftime("%Y-%m-%d"))
        curr += timedelta(days=1)
    
    return dates


def launch_chrome():
    """Launches a real Chrome instance with remote debugging enabled.
    
    Uses -g flag to prevent Chrome from coming to the foreground.
    """
    print("Launching real Chrome (background mode)...")
    CHROME_PROFILE.mkdir(exist_ok=True)

    # Check if Chrome is already running on port 9222
    try:
        subprocess.check_output(f"lsof -i :{CHROME_PORT}", shell=True)
        print("Chrome is already running on port 9222.")
        return
    except subprocess.CalledProcessError:
        pass

    # MacOS specific Chrome path
    CHROME_PATH = "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"

    # -g = don't bring to foreground, -n = open new instance, -a = specify app
    cmd = [
        "open",
        "-gna",  # -g prevents focus stealing
        "Google Chrome",
        "--args",
        f"--user-data-dir={CHROME_PROFILE}",
        f"--remote-debugging-port={CHROME_PORT}",
        "--no-first-run",
        "--no-default-browser-check",
    ]

    subprocess.Popen(cmd)
    print("Waiting 2 seconds for Chrome to start...")
    time.sleep(2)


def kill_chrome():
    """Kills the Chrome process running on the debugging port."""
    print("Killing Chrome process...")
    try:
        # Find PID using lsof
        pid = (
            subprocess.check_output(f"lsof -i :{CHROME_PORT} -t", shell=True)
            .decode()
            .strip()
        )
        if pid:
            os.kill(int(pid), signal.SIGTERM)
            print(f"Killed Chrome PID: {pid}")
            time.sleep(2)
    except Exception as e:
        print(f"Error killing Chrome: {e}")


async def open_sf_page():
    cdp = f"http://localhost:{CHROME_PORT}"

    async with async_playwright() as p:
        # Connect to real Chrome
        browser = await p.chromium.connect_over_cdp(cdp)

        # Create a new tab if none exist
        if browser.contexts:
            context = browser.contexts[0]
        else:
            context = await browser.new_context()

        page = await context.new_page()
        print(f"Navigating Chrome → {TARGET_URL}")
        await page.goto(TARGET_URL)
        print(
            "Navigation complete. You can now solve the Cloudflare challenge manually."
        )


import re
from datetime import date, timedelta


async def scrape_cases(page):
    cases = []
    try:
        # Locate the table rows
        rows = page.locator("#example tbody tr")
        count = await rows.count()
        print(f"Found {count} rows in table.")

        for i in range(count):
            row = rows.nth(i)
            # Extract Case Number (1st column)
            case_num_el = row.locator("td").nth(0)
            case_num_raw = await case_num_el.inner_text()
            case_num = re.sub(r"[^a-zA-Z0-9]", "", case_num_raw)

            # Extract Link
            try:
                link_el = case_num_el.locator("a")
                if await link_el.count() > 0:
                    link = await link_el.get_attribute("href")
                else:
                    link = None
                    print(f"    WARNING: No link found for case {case_num}")
            except Exception as e:
                link = None
                print(f"    Error extracting link for {case_num}: {e}")

            # Extract Case Title (2nd column)
            case_title = await row.locator("td").nth(1).inner_text()

            cases.append({"case_num": case_num, "title": case_title, "link": link})

    except Exception as e:
        print(f"Error scraping cases: {e}")

    return cases


import json
from pathlib import Path
from urllib.parse import parse_qs, urlparse

# Note: DOWNLOAD_SEMAPHORE is created per-session in scrape_case to avoid event loop issues
# Lock for thread-safe register updates
import threading
_register_lock = threading.Lock()


async def save_doc(semaphore, context, url, folder, filename, json_path=None, action_idx=None):
    """Download a document and update register on success for crash recovery."""
    async with semaphore:
        folder.mkdir(parents=True, exist_ok=True)
        file_path = folder / filename

        if file_path.exists():
            # Skip if file exists and is reasonably sized
            if file_path.stat().st_size > 5000:
                return True
            else:
                file_path.unlink()  # Remove small/broken files

        print(f"    [DL] {filename}...")

        for attempt in range(3):
            try:
                response = await context.request.get(url)

                if response.status == 200:
                    body = await response.body()
                    
                    # Validate it's actually a PDF (not Cloudflare challenge HTML)
                    if len(body) < 5000 or b'%PDF' not in body[:100]:
                        print(f"    [FAIL] {filename}: Got Cloudflare challenge, not PDF (Attempt {attempt+1}/3)")
                        # Add extra delay when we hit Cloudflare
                        await asyncio.sleep(5)
                        continue
                    
                    with open(file_path, "wb") as f:
                        f.write(body)
                    print(f"    [OK] {filename}")
                    
                    # Update register immediately for crash recovery
                    if json_path and action_idx is not None:
                        _update_register_progress(json_path, action_idx, filename, True)
                    
                    return True
                else:
                    print(f"    [FAIL] {filename}: Status {response.status} (Attempt {attempt+1}/3)")

            except Exception as e:
                print(f"    [ERR] {filename}: {e} (Attempt {attempt+1}/3)")

            await asyncio.sleep(1 * (attempt + 1))

        print(f"    [GAVE UP] {filename} after 3 attempts.")
        
        # Mark as failed in register
        if json_path and action_idx is not None:
            _update_register_progress(json_path, action_idx, filename, False)
        
        return False


def _update_register_progress(json_path, action_idx, filename, success):
    """Thread-safe update of register_of_actions.json after each download."""
    with _register_lock:
        try:
            with open(json_path, "r") as f:
                data = json.load(f)
            
            # Update the specific action's download status
            if "actions" in data and action_idx < len(data["actions"]):
                data["actions"][action_idx]["downloaded"] = success
                data["actions"][action_idx]["download_time"] = datetime.now().isoformat()
            
            # Update scraped_links count
            if "metadata" in data:
                downloaded_count = sum(1 for a in data.get("actions", []) if a.get("downloaded") == True)
                data["metadata"]["scraped_links"] = downloaded_count
                data["metadata"]["last_updated"] = datetime.now().isoformat()
            
            with open(json_path, "w") as f:
                json.dump(data, f, indent=2)
        except Exception as e:
            print(f"    [WARN] Could not update register: {e}")


async def scrape_case(context, link, filing_date):
    # Construct full URL if relative
    if not link.startswith("http"):
        link = "https://webapps.sftc.org/ci/" + link

    # Extract Case Number from URL or Page
    # URL format: ...CaseNum=CGC15276378...
    parsed_url = urlparse(link)
    qs = parse_qs(parsed_url.query)
    case_num = qs.get("CaseNum", ["Unknown"])[0]

    # Create directory: data/{filing_date}/{case_num}
    case_dir = Path(f"data/{filing_date}/{case_num}")
    case_dir.mkdir(parents=True, exist_ok=True)
    json_path = case_dir / "register_of_actions.json"

    # Check for existing metadata to skip already-completed cases
    if json_path.exists():
        try:
            with open(json_path, "r") as f:
                data = json.load(f)
                if isinstance(data, dict) and "metadata" in data:
                    meta = data["metadata"]
                    status = meta.get("status", "")
                    
                    # Skip if explicitly marked complete or restricted
                    if status == "complete":
                        print(
                            f"  [SKIP] {case_num} already complete ({meta.get('scraped_links', 0)}/{meta.get('total_links', 0)} docs)"
                        )
                        return
                    if status == "restricted":
                        print(f"  [SKIP] {case_num} is restricted")
                        return
                    
                    # Also skip if old format with matching counts
                    if (
                        meta.get("scraped_links", 0) == meta.get("total_links", 0)
                        and meta.get("total_links", 0) > 0
                    ):
                        print(
                            f"  [SKIP] {case_num} (Already scraped: {meta['scraped_links']}/{meta['total_links']} links)"
                        )
                        return
                        
                    # If status is pending or partial, we'll re-scrape
                    if status in ["pending", "partial"]:
                        print(f"  [DEBUG] {case_num} has status={status}, will re-scrape")
        except Exception as e:
            print(f"  [DEBUG] Error reading existing JSON for {case_num}: {e}")

    print(f"  Scraping case: {link}")
    page = await context.new_page()
    try:
        await page.goto(link)

        # Wait for page to load or Cloudflare to pass
        print("  Waiting for page load...", end="", flush=True)
        for _ in range(10):  # Wait up to 10 seconds
            # 1. Check for Cloudflare
            try:
                title = await page.title()
                content = await page.content()
                if (
                    "Just a moment" in title
                    or "Cloudflare" in title
                    or "challenge-platform" in content
                ):
                    print(
                        "\r  !!! CLOUDFLARE DETECTED !!! Please solve manually.   ",
                        end="",
                        flush=True,
                    )
                    await asyncio.sleep(2)
                    continue

                # 2. Check for Restricted Case (CCP 1161.2)
                if (
                    "Per CCP 1161.2" in content
                    or "Case Is Not Available For Viewing" in content
                ):
                    print(
                        f"\r  Case {case_num} is RESTRICTED (CCP 1161.2). Saving status.      "
                    )
                    output_data = {
                        "metadata": {"status": "restricted", "reason": "CCP 1161.2"}
                    }
                    with open(json_path, "w") as f:
                        json.dump(output_data, f, indent=2)
                    return

            except Exception:
                pass

            # 3. Check for Dropdown (Success)
            if await page.locator('select[name="example_length"]').is_visible():
                print("\r  Page loaded.                                         ")
                break

            await asyncio.sleep(1)
            print(".", end="", flush=True)
        else:
            print("\n  Timed out waiting for page/dropdown.")
            raise BrowserStuckError(
                "Timeout waiting for case page load", failed_case_num=case_num
            )

        # Select "All" entries
        try:
            await page.select_option(
                'select[name="example_length"]', "-1", timeout=3000
            )
            await page.wait_for_timeout(1000)  # Wait for table reload
        except Exception as e:
            print(f"  Could not select 'All' in case view: {e}")

        # Scrape Register of Actions
        print(f"  [DEBUG] Extracting register of actions...")
        actions = []
        download_tasks = []
        rows = page.locator("#example tbody tr")
        count = await rows.count()
        print(f"  [DEBUG] Found {count} action rows in table.")

        total_links = 0
        doc_id = None  # Initialize doc_id to avoid unbound variable
        
        # Create semaphore in the current event loop - use LOW concurrency to avoid Cloudflare rate limits
        download_semaphore = asyncio.Semaphore(2)  # Only 2 concurrent downloads to avoid captcha

        # Phase 1: Extraction & Task Collection
        for i in range(count):
            row = rows.nth(i)
            cols = row.locator("td")

            # Extract columns
            action_date = await cols.nth(0).inner_text()
            proceedings = await cols.nth(1).inner_text()
            fee = await cols.nth(3).inner_text()

            # Check for Document View Link
            doc_link_el = cols.nth(2).locator("a")
            doc_url = None
            doc_filename = None
            current_doc_id = None

            if await doc_link_el.count() > 0:
                total_links += 1
                doc_url = await doc_link_el.get_attribute("href")
                if doc_url:
                    # Parse DocID from URL
                    # Example: ...&DocID=08272316&...
                    # It might be in the main query or nested in the 'URL' param
                    match = re.search(r"DocID%3D(\d+)", doc_url)
                    if match:
                        current_doc_id = match.group(1)
                    else:
                        current_doc_id = "Unknown"

                    # Filename: {action_date}_{doc_id}.pdf
                    doc_filename = f"{action_date}_{current_doc_id}.pdf"

                    # Add to download tasks - pass json_path and action index for live tracking
                    download_tasks.append(
                        save_doc(download_semaphore, context, doc_url, case_dir, doc_filename,
                                json_path=json_path, action_idx=i)
                    )

            actions.append(
                {
                    "date": action_date,
                    "proceedings": proceedings,
                    "fee": fee,
                    "doc_id": current_doc_id,
                    "doc_filename": doc_filename,
                    "doc_url": doc_url,
                }
            )

        # *** SAVE REGISTER OF ACTIONS FIRST (before downloading) ***
        print(f"  [DEBUG] Saving register of actions FIRST (status=pending)...")
        started_at = datetime.now().isoformat()
        output_data = {
            "metadata": {
                "status": "pending",
                "total_entries": count,
                "total_links": total_links,
                "scraped_links": 0,
                "started_at": started_at,
            },
            "actions": actions,
        }
        with open(json_path, "w") as f:
            json.dump(output_data, f, indent=2)
        print(f"  [DEBUG] Register saved: {count} actions, {total_links} documents to download")

        # Phase 2: Parallel Execution
        if download_tasks:
            print(f"  [DEBUG] Starting download of {len(download_tasks)} documents in parallel...")
            results = await asyncio.gather(*download_tasks, return_exceptions=True)
            
            # Count successful downloads
            success_count = sum(1 for r in results if r is True)
            fail_count = sum(1 for r in results if r is False or isinstance(r, Exception))
            print(f"  [DEBUG] Downloads complete: {success_count} succeeded, {fail_count} failed.")
        else:
            print(f"  [DEBUG] No documents to download for this case.")

        # Phase 3: Verification & Counting
        print(f"  [DEBUG] Verifying downloaded files...")
        scraped_links = 0
        for action in actions:
            if action["doc_filename"]:
                if (case_dir / action["doc_filename"]).exists():
                    scraped_links += 1

        # Update Register of Actions JSON with final status
        output_data = {
            "metadata": {
                "status": "complete" if scraped_links == total_links else "partial",
                "total_entries": count,
                "total_links": total_links,
                "scraped_links": scraped_links,
                "started_at": started_at,
                "completed_at": datetime.now().isoformat(),
            },
            "actions": actions,
        }

        with open(json_path, "w") as f:
            json.dump(output_data, f, indent=2)
        print(
            f"  [DONE] Saved register of actions: {scraped_links}/{total_links} documents downloaded"
        )

    except BrowserStuckError:
        raise
    except Exception as e:
        print(f"  Error scraping case {link}: {e}")
    finally:
        await page.close()


def update_day_summary(date_str, total_cases=None, cases_list=None):
    """Updates the day_summary.json for a given date.
    
    Args:
        date_str: The date string (YYYY-MM-DD)
        total_cases: Total number of cases for this day
        cases_list: List of case dicts with case_num, title, link
    """
    date_dir = Path(f"data/{date_str}")
    date_dir.mkdir(parents=True, exist_ok=True)

    summary_path = date_dir / "day_summary.json"

    # Load existing summary to preserve data if not provided
    current_summary = {}
    if summary_path.exists():
        try:
            with open(summary_path, "r") as f:
                current_summary = json.load(f)
        except:
            pass

    if total_cases is None:
        total_cases = current_summary.get("total_cases", 0)
    
    if cases_list is None:
        cases_list = current_summary.get("cases", [])

    # Count scraped cases by checking which have register_of_actions.json
    scraped_cases = 0
    completed_cases = 0
    for case_dir in date_dir.iterdir():
        if case_dir.is_dir():
            json_path = case_dir / "register_of_actions.json"
            if json_path.exists():
                scraped_cases += 1  # Started scraping this case
                try:
                    with open(json_path, "r") as f:
                        data = json.load(f)
                        if isinstance(data, dict) and "metadata" in data:
                            meta = data["metadata"]
                            # Check if restricted OR fully scraped
                            if meta.get("status") == "restricted":
                                completed_cases += 1
                            elif meta.get("status") == "complete":
                                completed_cases += 1
                            elif (
                                meta.get("scraped_links", 0) == meta.get("total_links", 0)
                                and meta.get("total_links", 0) > 0
                            ):
                                completed_cases += 1
                except:
                    pass

    fully_completed = (total_cases > 0) and (completed_cases >= total_cases)

    summary = {
        "date": date_str,
        "total_cases": total_cases,
        "scraped_cases": scraped_cases,
        "completed_cases": completed_cases,
        "fully_completed": fully_completed,
        "cases": cases_list,
        "last_updated": datetime.now().isoformat(),
    }

    with open(summary_path, "w") as f:
        json.dump(summary, f, indent=2)
    
    print(f"  [DEBUG] Day summary updated: {scraped_cases}/{total_cases} scraped, {completed_cases}/{total_cases} completed")

    return summary


async def scrape_date(page, date_str, resume_case_num=None):
    print(f"Processing date: {date_str}")

    # Check if day is already fully scraped
    summary = update_day_summary(date_str)
    if summary.get("fully_completed") and not resume_case_num:
        print(
            f"  Day {date_str} fully scraped ({summary['scraped_cases']}/{summary['total_cases']}). Skipping."
        )
        return

    try:
        # Fill FilingDate
        await page.fill("#FilingDate", date_str)

        # Click Search
        await page.get_by_role("button", name="Search").click()

        # Wait for results to load
        await page.wait_for_timeout(1000)

        # Check for "No cases found"
        try:
            results_count = page.locator("#resultsCount")
            if await results_count.is_visible():
                text = await results_count.inner_text()
                if "No cases found" in text:
                    print(f"No cases found for {date_str}. Skipping.")
                    return
        except Exception as e:
            pass

        # Select "All" entries
        try:
            await page.select_option(
                'select[name="example_length"]', "-1", timeout=5000
            )
            print("Selected 'All' entries.")
            await page.wait_for_timeout(1000)

            # Print entry count
            total_entries = 0
            try:
                info_text = await page.locator("#example_info").inner_text()
                match = re.search(r"of\s+([\d,]+)\s+entries", info_text)
                if match:
                    total_entries = int(match.group(1).replace(",", ""))
                    print(f"  [DEBUG] Entry count from page: {total_entries}")
                else:
                    print(f"  [DEBUG] Entry count parsing failed: {info_text}")
            except Exception as e:
                print(f"  [DEBUG] Could not get entry count: {e}")

            # Scrape cases from table
            print(f"  [DEBUG] Extracting case list from table...")
            cases = await scrape_cases(page)
            print(f"  [DEBUG] Extracted {len(cases)} cases from table.")
            
            # *** SAVE CASE LIST FIRST (before processing individual cases) ***
            print(f"  [DEBUG] Saving day summary with case list FIRST...")
            update_day_summary(date_str, total_cases=len(cases), cases_list=cases)
            print(f"  [SAVED] Day {date_str}: {len(cases)} cases saved to day_summary.json")

            # Process each case
            for case in cases:
                # Fast Resume Logic
                if resume_case_num:
                    if case["case_num"] == resume_case_num:
                        print(
                            f"Found resume target: {resume_case_num}. Resuming scrape..."
                        )
                        resume_case_num = None  # Clear resume flag to proceed normally
                    else:
                        # print(f"Skipping {case['case_num']} (Fast Resume seeking {resume_case_num})")
                        continue

                if case["link"]:
                    await scrape_case(page.context, case["link"], date_str)
                    await asyncio.sleep(2)

            # Update summary at the end of the day
            update_day_summary(date_str)

        except BrowserStuckError:
            raise
        except Exception as e:
            print(f"Could not select 'All' entries (maybe no results?): {e}")

    except BrowserStuckError:
        raise
    except Exception as e:
        print(f"Error processing {date_str}: {e}")


async def run_search_loop(page, dates, resume_case_num=None):
    print("Starting search loop...")
    # Click "Search by New Filings" tab
    await page.click("#ui-id-3")
    print("Clicked 'Search by New Filings' tab.")

    while dates:
        date_str = dates[0]

        # Pass resume_case_num only to the first date in the list (the one we failed on)
        # For subsequent dates, resume_case_num should be None
        await scrape_date(page, date_str, resume_case_num)
        resume_case_num = None

        dates.pop(0)  # Remove date after successful processing


async def monitor_browser(dates, resume_case_num=None):
    cdp = f"http://localhost:{CHROME_PORT}"
    print("Starting browser monitor...")

    # Wait for Cloudflare to be solved (SessionID in URL)
    while True:
        try:
            async with async_playwright() as p:
                browser = await p.chromium.connect_over_cdp(cdp)
                if not browser.contexts:
                    context = await browser.new_context()
                else:
                    context = browser.contexts[0]

                # Find the tab with the target URL
                page = None
                for ctx in browser.contexts:
                    for pg in ctx.pages:
                        if "SessionID=" in pg.url:
                            page = pg
                            break
                    if page:
                        break

                if page:
                    print("\n" + "=" * 50)
                    print("SUCCESS! Cloudflare challenge passed.")

                    # Extract SessionID
                    url_parts = urlparse(page.url)
                    query = parse_qs(url_parts.query)
                    session_id = query.get("SessionID", ["Unknown"])[0]
                    print(f"SessionID: {session_id}")

                    # Print Cookies
                    cookies = await context.cookies()
                    print(f"Cookies: {cookies}")
                    print("=" * 50 + "\n")

                    print("Playwright is now attached and controlling the browser.")

                    # Run the search loop with the remaining dates
                    await run_search_loop(page, dates, resume_case_num)
                    return  # Done with all dates

                else:
                    print(
                        "Waiting for Cloudflare challenge... (checking again in 3s)",
                        end="\r",
                    )
                    await asyncio.sleep(3)

        except BrowserStuckError:
            raise  # Propagate up to main to trigger restart
        except Exception as e:
            print(f"Monitor error: {e}")
            await asyncio.sleep(3)


async def main():
    dates = get_dates()
    resume_case_num = None

    while dates:
        print(f"\n--- Starting Session. Remaining dates: {len(dates)} ---")
        if resume_case_num:
            print(f"Resuming from case: {resume_case_num}")

        launch_chrome()

        # Initial navigation
        await open_sf_page()

        # Wait a bit before attaching monitor
        print(
            "Waiting 1 second before attaching monitor to allow Cloudflare check to proceed..."
        )
        await asyncio.sleep(1)

        try:
            await monitor_browser(dates, resume_case_num)
            print("All dates processed successfully!")
            break
        except BrowserStuckError as e:
            print("\n!!! BROWSER STUCK DETECTED !!!")
            if e.failed_case_num:
                print(f"Failed at case: {e.failed_case_num}")
                resume_case_num = e.failed_case_num
            print("Restarting browser session...")
            kill_chrome()
            await asyncio.sleep(2)
        except Exception as e:
            print(f"\nUnexpected error in main loop: {e}")
            print("Restarting browser session...")
            kill_chrome()
            await asyncio.sleep(2)
    
    # Clean up Chrome on successful completion
    print("Cleaning up Chrome...")
    kill_chrome()




def parse_args():
    """Parse command line arguments."""
    global START_DATE, END_DATE
    
    parser = argparse.ArgumentParser(
        description="SF Superior Court Scraper - scrapes civil case data"
    )
    parser.add_argument(
        "--start-date",
        type=str,
        default=START_DATE,
        help=f"Start date (YYYY-MM-DD), default: {START_DATE}"
    )
    parser.add_argument(
        "--end-date",
        type=str,
        default=END_DATE,
        help=f"End date (YYYY-MM-DD), default: {END_DATE}"
    )
    
    args = parser.parse_args()
    
    # Update global config
    START_DATE = args.start_date
    END_DATE = args.end_date
    
    print(f"[CONFIG] Date range: {START_DATE} to {END_DATE}")
    
    return args


if __name__ == "__main__":
    try:
        args = parse_args()
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nExiting... Cleaning up Chrome.")
        kill_chrome()
