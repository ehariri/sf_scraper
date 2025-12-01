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

# --- Configuration ---
CHROME_PORT = 9222
TARGET_URL = "https://webapps.sftc.org/ci/CaseInfo.dll"
START_DATE = "2015-01-01"
END_DATE = "2015-01-10"
CHROME_PROFILE = Path.home() / ".sf_manual_profile"


# --- Custom Exception ---
class BrowserStuckError(Exception):
    """Raised when the browser is stuck or unresponsive."""

    def __init__(self, message, failed_case_num=None):
        super().__init__(message)
        self.failed_case_num = failed_case_num


def get_dates():
    start = datetime.strptime(START_DATE, "%Y-%m-%d")
    end = datetime.strptime(END_DATE, "%Y-%m-%d")
    dates = []
    curr = start
    while curr <= end:
        dates.append(curr.strftime("%Y-%m-%d"))
        curr += timedelta(days=1)
    return dates


def launch_chrome():
    """Launches a real Chrome instance with remote debugging enabled."""
    print("Launching real Chrome...")
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

    cmd = [
        "open",
        "-na",
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


async def save_doc(context, url, folder, filename):
    try:
        folder.mkdir(parents=True, exist_ok=True)
        file_path = folder / filename

        if file_path.exists():
            print(f"    Skipping existing file: {filename}")
            return

        print(f"    Downloading {filename}...")
        # Note: The 'View' link might be a redirect or direct download.
        # Using context.request.get should handle cookies/session.
        response = await context.request.get(url)

        if response.status == 200:
            body = await response.body()
            with open(file_path, "wb") as f:
                f.write(body)
            print(f"    Saved {filename}")
        else:
            print(f"    Failed to download {filename}: Status {response.status}")

    except Exception as e:
        print(f"    Error saving doc {filename}: {e}")


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

    # Check for existing metadata to skip
    if json_path.exists():
        try:
            with open(json_path, "r") as f:
                data = json.load(f)
                # Check if it has the new structure and is complete
                if isinstance(data, dict) and "metadata" in data:
                    meta = data["metadata"]
                    if (
                        meta.get("scraped_links", 0) == meta.get("total_links", 0)
                        and meta.get("total_links", 0) > 0
                    ):
                        print(
                            f"  Skipping {case_num} (Already scraped: {meta['scraped_links']}/{meta['total_links']} links)"
                        )
                        return
        except Exception as e:
            print(f"  Error reading existing JSON for {case_num}: {e}")

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
        actions = []
        rows = page.locator("#example tbody tr")
        count = await rows.count()
        print(f"  Found {count} actions.")

        total_links = 0
        scraped_links = 0

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

            if await doc_link_el.count() > 0:
                total_links += 1
                doc_url = await doc_link_el.get_attribute("href")
                if doc_url:
                    # Parse DocID from URL
                    # Example: ...&DocID=08272316&...
                    # It might be in the main query or nested in the 'URL' param
                    match = re.search(r"DocID%3D(\d+)", doc_url)
                    if match:
                        doc_id = match.group(1)
                    else:
                        doc_id = "Unknown"

                    # Filename: {action_date}_{doc_id}.pdf
                    doc_filename = f"{action_date}_{doc_id}.pdf"

                    # Download Document
                    await save_doc(context, doc_url, case_dir, doc_filename)

                    # Check if file exists to confirm scrape
                    if (case_dir / doc_filename).exists():
                        scraped_links += 1

            actions.append(
                {
                    "date": action_date,
                    "proceedings": proceedings,
                    "fee": fee,
                    "doc_id": doc_id if doc_url else None,
                    "doc_filename": doc_filename,
                }
            )

        # Save Register of Actions JSON with Metadata
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
        print(
            f"  Saved register of actions to {json_path} (Links: {scraped_links}/{total_links})"
        )

    except BrowserStuckError:
        raise
    except Exception as e:
        print(f"  Error scraping case {link}: {e}")
    finally:
        await page.close()


def update_day_summary(date_str, total_cases=None):
    """Updates the day_summary.json for a given date."""
    date_dir = Path(f"data/{date_str}")
    if not date_dir.exists():
        return {"fully_completed": False}

    summary_path = date_dir / "day_summary.json"

    # Load existing summary to preserve total_cases if not provided
    current_summary = {}
    if summary_path.exists():
        try:
            with open(summary_path, "r") as f:
                current_summary = json.load(f)
        except:
            pass

    if total_cases is None:
        total_cases = current_summary.get("total_cases", 0)

    # Count scraped cases
    scraped_cases = 0
    for case_dir in date_dir.iterdir():
        if case_dir.is_dir():
            json_path = case_dir / "register_of_actions.json"
            if json_path.exists():
                try:
                    with open(json_path, "r") as f:
                        data = json.load(f)
                        if isinstance(data, dict) and "metadata" in data:
                            meta = data["metadata"]
                            # Check if restricted OR fully scraped
                            if meta.get("status") == "restricted":
                                scraped_cases += 1
                            elif meta.get("scraped_links", 0) == meta.get(
                                "total_links", 0
                            ):
                                scraped_cases += 1
                except:
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
            try:
                info_text = await page.locator("#example_info").inner_text()
                match = re.search(r"of\s+([\d,]+)\s+entries", info_text)
                if match:
                    total_entries = int(match.group(1).replace(",", ""))
                    print(f"Entry count: {total_entries}")
                    # Update summary with total count
                    update_day_summary(date_str, total_cases=total_entries)
                else:
                    print(f"Entry count: {info_text} (Regex failed)")
            except Exception as e:
                print(f"Could not get entry count: {e}")

            # Scrape cases
            cases = await scrape_cases(page)
            print(f"Scraped {len(cases)} cases.")

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

        # Skip weekends
        from datetime import datetime

        dt = datetime.strptime(date_str, "%Y-%m-%d")
        if dt.weekday() >= 5:  # 5=Sat, 6=Sun
            print(f"Skipping weekend: {date_str}")
            dates.pop(0)
            continue

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


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nExiting...")
