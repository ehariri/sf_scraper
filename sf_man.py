import subprocess
import asyncio
from pathlib import Path
from playwright.async_api import async_playwright

CHROME_PROFILE = Path.home() / ".sf_manual_profile"
CHROME_PORT = 9222
TARGET_URL = "https://webapps.sftc.org/ci/CaseInfo.dll"


def launch_real_chrome():
    CHROME_PROFILE.mkdir(exist_ok=True)

    cmd = [
        "open",
        "-na", "Google Chrome",
        "--args",
        f"--user-data-dir={CHROME_PROFILE}",
        f"--remote-debugging-port={CHROME_PORT}",
        "--no-first-run",
        "--no-default-browser-check",
    ]

    print("Launching real Chrome...")
    subprocess.Popen(cmd)


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
    print(f"  Scraping case: {link}")
    page = await context.new_page()
    try:
        # Construct full URL if relative
        if not link.startswith("http"):
            link = "https://webapps.sftc.org/ci/" + link
            
        await page.goto(link)
        
        # Check for Cloudflare
        try:
            title = await page.title()
            content = await page.content()
            if "Just a moment" in title or "Cloudflare" in title or "challenge-platform" in content:
                print("\n!!! CLOUDFLARE DETECTED IN CASE TAB !!!")
                print("Please solve the challenge in the new tab manually.")
                print("Waiting for challenge to pass...")
                
                while True:
                    await asyncio.sleep(2)
                    title = await page.title()
                    if "Just a moment" not in title and "Cloudflare" not in title:
                        print("Cloudflare passed! Resuming...")
                        break
        except Exception as e:
            print(f"  Error checking for Cloudflare: {e}")

        await page.wait_for_timeout(1000)
        
        # Extract Case Number from URL or Page
        # URL format: ...CaseNum=CGC15276378...
        parsed_url = urlparse(link)
        qs = parse_qs(parsed_url.query)
        case_num = qs.get("CaseNum", ["Unknown"])[0]
        
        # Create directory: data/{filing_date}/{case_num}
        case_dir = Path(f"data/{filing_date}/{case_num}")
        case_dir.mkdir(parents=True, exist_ok=True)
        
        # Select "All" entries
        try:
            if await page.locator('select[name="example_length"]').is_visible():
                await page.select_option('select[name="example_length"]', "-1", timeout=3000)
                print("  Selected 'All' entries in case view.")
                await page.wait_for_timeout(1000) # Wait for table reload
            else:
                print("  No 'Select All' dropdown found in case view (or page failed to load).")
        except Exception as e:
            print(f"  Could not select 'All' in case view: {e}")

        # Scrape Register of Actions
        actions = []
        rows = page.locator("#example tbody tr")
        count = await rows.count()
        print(f"  Found {count} actions.")
        
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

            actions.append({
                "date": action_date,
                "proceedings": proceedings,
                "fee": fee,
                "doc_id": doc_id if doc_url else None,
                "doc_filename": doc_filename
            })
            
        # Save Register of Actions JSON
        json_path = case_dir / "register_of_actions.json"
        with open(json_path, "w") as f:
            json.dump(actions, f, indent=2)
        print(f"  Saved register of actions to {json_path}")
            
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

        # Select "All" entries
        try:
            await page.select_option('select[name="example_length"]', "-1", timeout=5000)
            print("Selected 'All' entries.")
            await page.wait_for_timeout(1000) # Wait for table update
            
            # Print entry count
            try:
                info_text = await page.locator("#example_info").inner_text()
                # Extract total entries using regex (e.g., "Showing 1 to 72 of 72 entries")
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
                    await asyncio.sleep(2) # Be polite and avoid rate limits

        except Exception as e:
            print(f"Could not select 'All' entries (maybe no results?): {e}")

    except Exception as e:
        print(f"Error processing {date_str}: {e}")


async def run_search_loop(page):
    print("Starting search loop...")
    
    # Click "Search by New Filings" tab
    try:
        await page.click("#ui-id-3")
        print("Clicked 'Search by New Filings' tab.")
        await page.wait_for_timeout(1000) # Wait for tab switch
    except Exception as e:
        print(f"Error clicking tab: {e}")
        return

    start_date = date(2015, 1, 1)
    end_date = date(2015, 1, 10)
    delta = timedelta(days=1)
    
    current_date = start_date
    while current_date <= end_date:
        # Skip weekends (Saturday=5, Sunday=6)
        if current_date.weekday() >= 5:
            print(f"Skipping weekend: {current_date.strftime('%Y-%m-%d')}")
            current_date += delta
            continue

        date_str = current_date.strftime("%Y-%m-%d")
        await scrape_date(page, date_str)
        current_date += delta
        
    print("Search loop complete.")


async def monitor_browser():
    cdp = f"http://localhost:{CHROME_PORT}"
    print("Starting browser monitor...")

    while True:
        try:
            async with async_playwright() as p:
                browser = await p.chromium.connect_over_cdp(cdp)
                if not browser.contexts:
                    print("No browser context found. Retrying...")
                    await asyncio.sleep(2)
                    continue
                
                context = browser.contexts[0]
                # Find the page with the target URL or just check active page
                found = False
                for page in context.pages:
                    if "SessionID=" in page.url:
                        found = True
                        print("\n" + "="*50)
                        print(f"SUCCESS! Cloudflare challenge passed.")
                        
                        # Extract SessionID
                        try:
                            from urllib.parse import urlparse, parse_qs
                            parsed_url = urlparse(page.url)
                            query_params = parse_qs(parsed_url.query)
                            session_id = query_params.get("SessionID", [None])[0]
                            print(f"SessionID: {session_id}")
                        except:
                            print("Could not parse SessionID")

                        cookies = await context.cookies()
                        print(f"Cookies: {cookies}")
                        print("="*50 + "\n")
                        print("Playwright is now attached and controlling the browser.")
                        
                        # Run the search loop
                        await run_search_loop(page)
                        
                        print("Press Ctrl+C to exit.")
                        await asyncio.Future()
                
                if not found:
                    print("Waiting for Cloudflare challenge... (checking again in 3s)")
                    await asyncio.sleep(3)

        except Exception as e:
            print(f"Monitor error: {e}")
            await asyncio.sleep(3)


async def main():
    launch_real_chrome()

    print("Waiting 2 seconds for Chrome to start...")
    await asyncio.sleep(2)

    await open_sf_page()
    
    print("Waiting 1 second before attaching monitor to allow Cloudflare check to proceed...")
    await asyncio.sleep(1)

    # Start monitoring for completion
    await monitor_browser()


if __name__ == "__main__":
    asyncio.run(main())
