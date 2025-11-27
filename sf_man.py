import subprocess
import asyncio
import time
import os
import signal
import re
import json
from pathlib import Path
from urllib.parse import urlparse, parse_qs
from datetime import datetime, timedelta
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
    pass

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
        "-na", "Google Chrome",
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
        pid = subprocess.check_output(f"lsof -i :{CHROME_PORT} -t", shell=True).decode().strip()
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
        print("Navigation complete. You can now solve the Cloudflare challenge manually.")


from datetime import date, timedelta
import re

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
            case_num = await case_num_el.inner_text()
            
            # Extract Link
            try:
                link_el = case_num_el.locator("a")
                link = await link_el.get_attribute("href")
            except:
                link = None

            # Extract Case Title (2nd column)
            case_title = await row.locator("td").nth(1).inner_text()
            
            cases.append({
                "case_num": case_num,
                "title": case_title,
                "link": link
            })
            
    except Exception as e:
        print(f"Error scraping cases: {e}")
        
    return cases

import json
from pathlib import Path
from urllib.parse import urlparse, parse_qs

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
                    if meta.get("scraped_links", 0) == meta.get("total_links", 0) and meta.get("total_links", 0) > 0:
                        print(f"  Skipping {case_num} (Already scraped: {meta['scraped_links']}/{meta['total_links']} links)")
                        return
        except Exception as e:
            print(f"  Error reading existing JSON for {case_num}: {e}")

    print(f"  Scraping case: {link}")
    page = await context.new_page()
    try:
        await page.goto(link)
        
        # Wait for page to load or Cloudflare to pass
        print("  Waiting for page load...", end="", flush=True)
        for _ in range(10): # Wait up to 10 seconds
            # 1. Check for Cloudflare
            try:
                title = await page.title()
                content = await page.content()
                if "Just a moment" in title or "Cloudflare" in title or "challenge-platform" in content:
                    print("\r  !!! CLOUDFLARE DETECTED !!! Please solve manually.   ", end="", flush=True)
                    await asyncio.sleep(2)
                    continue
            except Exception:
                pass

            # 2. Check for Dropdown (Success)
            if await page.locator('select[name="example_length"]').is_visible():
                print("\r  Page loaded.                                         ")
                break
            
            await asyncio.sleep(1)
            print(".", end="", flush=True)
        else:
            print("\n  Timed out waiting for page/dropdown.")
            raise BrowserStuckError("Timeout waiting for case page load")

        # Select "All" entries
        try:
            await page.select_option('select[name="example_length"]', "-1", timeout=3000)
            await page.wait_for_timeout(1000) # Wait for table reload
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
                    doc_qs = parse_qs(urlparse(doc_url).query)
                    doc_id = doc_qs.get("DocID", ["Unknown"])[0]
                    
                    # Filename: {action_date}_{doc_id}.pdf
                    doc_filename = f"{action_date}_{doc_id}.pdf"
                    
                    # Download Document
                    await save_doc(context, doc_url, case_dir, doc_filename)
                    
                    # Check if file exists to confirm scrape
                    if (case_dir / doc_filename).exists():
                        scraped_links += 1

            actions.append({
                "date": action_date,
                "proceedings": proceedings,
                "fee": fee,
                "doc_id": doc_id if doc_url else None,
                "doc_filename": doc_filename
            })
            
        # Save Register of Actions JSON with Metadata
        output_data = {
            "metadata": {
                "total_entries": count,
                "total_links": total_links,
                "scraped_links": scraped_links
            },
            "actions": actions
        }
        
        with open(json_path, "w") as f:
            json.dump(output_data, f, indent=2)
        print(f"  Saved register of actions to {json_path} (Links: {scraped_links}/{total_links})")
            
    except BrowserStuckError:
        raise
    except Exception as e:
        print(f"  Error scraping case {link}: {e}")
    finally:
        await page.close()

async def scrape_date(page, date_str):
    print(f"Processing date: {date_str}")
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
            pass # Continue to try selecting if check fails
            pass 

        # Select "All" entries
        try:
            await page.select_option('select[name="example_length"]', "-1", timeout=5000)
            print("Selected 'All' entries.")
            await page.wait_for_timeout(1000) 
            
            # Print entry count
            try:
                info_text = await page.locator("#example_info").inner_text()
                match = re.search(r"of\s+([\d,]+)\s+entries", info_text)
                if match:
                    total_entries = match.group(1)
                    print(f"Entry count: {total_entries}")
                else:
                    print(f"Entry count: {info_text} (Regex failed)")
            except Exception as e:
                print(f"Could not get entry count: {e}")
                
            # Scrape cases
            cases = await scrape_cases(page)
            print(f"Scraped {len(cases)} cases.")
            
            # Process each case
            for case in cases:
                if case['link']:
                    await scrape_case(page.context, case['link'], date_str)
                    await asyncio.sleep(2) 

        except BrowserStuckError:
            raise
        except Exception as e:
            print(f"Could not select 'All' entries (maybe no results?): {e}")

    except BrowserStuckError:
        raise
    except Exception as e:
        print(f"Error processing {date_str}: {e}")

async def run_search_loop(page, dates):
    print("Starting search loop...")
    # Click "Search by New Filings" tab
    await page.click("#ui-id-3")
    print("Clicked 'Search by New Filings' tab.")
    
    while dates:
        date_str = dates[0]
        
        # Skip weekends
        from datetime import datetime
        dt = datetime.strptime(date_str, "%Y-%m-%d")
        if dt.weekday() >= 5: # 5=Sat, 6=Sun
            print(f"Skipping weekend: {date_str}")
            dates.pop(0)
            continue
            
        await scrape_date(page, date_str)
        dates.pop(0) # Remove date after successful processing

async def monitor_browser(dates):
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
                    if page: break
                
                if page:
                    print("\n" + "="*50)
                    print("SUCCESS! Cloudflare challenge passed.")
                    
                    # Extract SessionID
                    url_parts = urlparse(page.url)
                    query = parse_qs(url_parts.query)
                    session_id = query.get("SessionID", ["Unknown"])[0]
                    print(f"SessionID: {session_id}")
                    
                    # Print Cookies
                    cookies = await context.cookies()
                    print(f"Cookies: {cookies}")
                    print("="*50 + "\n")
                    
                    print("Playwright is now attached and controlling the browser.")
                    
                    # Run the search loop with the remaining dates
                    await run_search_loop(page, dates)
                    return # Done with all dates
                
                else:
                    print("Waiting for Cloudflare challenge... (checking again in 3s)", end="\r")
                    await asyncio.sleep(3)
                    
        except BrowserStuckError:
            raise # Propagate up to main to trigger restart
        except Exception as e:
            print(f"Monitor error: {e}")
            await asyncio.sleep(3)

async def main():
    dates = get_dates()
    
    while dates:
        print(f"\n--- Starting Session. Remaining dates: {len(dates)} ---")
        launch_chrome()
        
        # Initial navigation
        await open_sf_page()
        
        # Wait a bit before attaching monitor
        print("Waiting 1 second before attaching monitor to allow Cloudflare check to proceed...")
        await asyncio.sleep(1)
        
        try:
            await monitor_browser(dates)
            print("All dates processed successfully!")
            break
        except BrowserStuckError:
            print("\n!!! BROWSER STUCK DETECTED !!!")
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
