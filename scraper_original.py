import argparse
import asyncio
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional

from playwright.async_api import (
    BrowserContext,
    Browser,
    Page,
    TimeoutError as PlaywrightTimeoutError,
    async_playwright,
)

ENTRY_URL = "https://sf.courts.ca.gov/online-services/case-information"
DEFAULT_USER_DATA_DIR = Path.home() / ".sf_scraper" / "chromium"

# A less-automated browser profile helps Cloudflare accept the session.
# If you have Chrome installed, we'll launch that channel when not headless.
DEFAULT_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)
LAUNCH_ARGS = ["--disable-blink-features=AutomationControlled"]


@dataclass
class ScrapeResult:
    filing_date: str
    rows: List[Dict[str, str]]


def ensure_profile_dir(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path


async def launch_context(headless: bool, profile_dir: Optional[Path], cdp_endpoint: Optional[str]):
    """
    ALWAYS launch a persistent real Chrome profile, never Playwright's automation Chromium.
    This is required for Cloudflare (cf_clearance) to work interactively.
    """

    playwright = await async_playwright().start()

    CHROME_PATH = "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"
    user_data_dir = ensure_profile_dir(profile_dir or DEFAULT_USER_DATA_DIR)

    context = await playwright.chromium.launch_persistent_context(
        executable_path=CHROME_PATH,
        user_data_dir=str(user_data_dir),
        headless=False,  # MUST BE FALSE for CAPTCHA solve
        viewport={"width": 1280, "height": 900},
        args=[
            "--disable-blink-features=AutomationControlled",
            "--disable-features=IsolateOrigins,site-per-process",
            "--disable-web-security",
            "--remote-debugging-port=9222",
        ],
    )

    return playwright, context

async def open_civil_query(page: Page, timeout_ms: Optional[int] = None) -> Page:
    await page.goto(ENTRY_URL, wait_until="domcontentloaded", timeout=timeout_ms)
    access_link = page.get_by_role("link", name="Access Now").first
    popup_task = page.wait_for_event("popup")
    await access_link.click()
    try:
        popup = await popup_task
        target_page = popup
    except PlaywrightTimeoutError:
        target_page = page
    await target_page.wait_for_load_state("domcontentloaded", timeout=timeout_ms)
    return target_page


async def interactive_setup(
    headless: bool,
    timeout_ms: int,
    profile_dir: Optional[Path],
    cdp_endpoint: Optional[str],
) -> None:

    playwright, context = await launch_context(
        headless=False,                     # MUST be non-headless
        profile_dir=profile_dir,
        cdp_endpoint=None,                  # ignore CDP for interactive
    )

    page = context.pages[0] if context.pages else await context.new_page()

    # IMPORTANT: go directly to the Cloudflare-protected page
    await page.goto("https://webapps.sftc.org/cc/CaseCalendar.dll")

    print("\nSolve the Cloudflare CAPTCHA in the browser window.")
    input("Press Enter here once you see the real case search page...")

    # Save cookies including cf_clearance
    storage_path = (profile_dir or DEFAULT_USER_DATA_DIR) / "storage_state.json"
    await context.storage_state(path=storage_path)

    await context.close()
    await playwright.stop()

    print("Done. Saved Cloudflare cookies for future headless runs.")


async def search_new_filings(
    filing_date: str,
    headless: bool,
    timeout_ms: int,
    profile_dir: Optional[Path],
    cdp_endpoint: Optional[str],
) -> ScrapeResult:
    playwright, context = await launch_context(
        headless=headless, profile_dir=profile_dir, cdp_endpoint=cdp_endpoint
    )
    page = context.pages[0] if context.pages else await context.new_page()
    target_page = await open_civil_query(page, timeout_ms=timeout_ms)
    await target_page.wait_for_timeout(300)  # let scripts settle

    # Switch to the "Search by New Filings" tab.
    tab_locator = target_page.get_by_role("link", name="Search by New Filings")
    if await tab_locator.count() == 0:
        tab_locator = target_page.get_by_text("Search by New Filings", exact=True)
    await tab_locator.first.click()

    # Fill the date and trigger search.
    date_input = target_page.get_by_label("Filing Date", exact=False)
    await date_input.fill(filing_date)
    await target_page.get_by_role("button", name="Search").click()
    await target_page.wait_for_timeout(1000)

    rows = await collect_results(target_page)
    if not cdp_endpoint:
        await context.close()
    await playwright.stop()
    return ScrapeResult(filing_date=filing_date, rows=rows)


async def collect_results(page: Page) -> List[Dict[str, str]]:
    tables = page.locator("table")
    table_count = await tables.count()
    for idx in range(table_count):
        table = tables.nth(idx)
        data = await extract_table(table)
        if data:
            return data
    return []


async def extract_table(table_locator) -> List[Dict[str, str]]:
    rows = table_locator.locator("tr")
    row_count = await rows.count()
    if row_count == 0:
        return []

    header_cells = rows.nth(0).locator("th,td")
    headers = [h.strip() for h in await header_cells.all_inner_texts()]
    if len(headers) <= 1:
        headers = []

    results: List[Dict[str, str]] = []
    start_idx = 1 if headers else 0
    for row_idx in range(start_idx, row_count):
        cells = rows.nth(row_idx).locator("th,td")
        values = [c.strip() for c in await cells.all_inner_texts()]
        if not any(values):
            continue
        if headers and len(headers) == len(values):
            entry = {headers[i] or f"col_{i}": values[i] for i in range(len(values))}
        else:
            entry = {f"col_{i}": values[i] for i in range(len(values))}
        results.append(entry)
    return results


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="SF Superior Court civil case scraper (new filings)."
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    interactive = subparsers.add_parser(
        "interactive", help="Open a real browser to clear Cloudflare verification."
    )
    interactive.add_argument(
        "--headless",
        default="false",
        choices=["true", "false"],
        help="Run headless (not recommended for first run).",
    )
    interactive.add_argument(
        "--timeout-ms",
        type=int,
        default=45_000,
        help="Wait time for page network idle.",
    )
    interactive.add_argument(
        "--profile-dir",
        type=Path,
        default=None,
        help="Use a specific user data dir (optional). Defaults to ~/.sf_scraper/chromium.",
    )
    interactive.add_argument(
        "--cdp-endpoint",
        default=None,
        help="Connect to an already-open Chrome with remote debugging, e.g. http://localhost:9222.",
    )

    new_filings = subparsers.add_parser(
        "new-filings", help="Fetch new filings for a filing date (YYYY-MM-DD)."
    )
    new_filings.add_argument("--date", required=True, help="Filing date YYYY-MM-DD.")
    new_filings.add_argument(
        "--out",
        default=None,
        help="Output JSON path. Defaults to data/new_filings_<date>.json",
    )
    new_filings.add_argument(
        "--headless",
        default="true",
        choices=["true", "false"],
        help="Run headless (uses saved profile).",
    )
    new_filings.add_argument(
        "--timeout-ms",
        type=int,
        default=45_000,
        help="Wait time for page network idle.",
    )
    new_filings.add_argument(
        "--profile-dir",
        type=Path,
        default=None,
        help="Use a specific user data dir (optional). Defaults to ~/.sf_scraper/chromium.",
    )
    new_filings.add_argument(
        "--cdp-endpoint",
        default=None,
        help="Connect to an already-open Chrome with remote debugging, e.g. http://localhost:9222.",
    )
    return parser.parse_args()


def str_to_bool(val: str) -> bool:
    return val.lower() == "true"


def default_output_path(filing_date: str) -> Path:
    return Path("data") / f"new_filings_{filing_date}.json"


async def main_async() -> None:
    args = parse_args()
    if args.command == "interactive":
        await interactive_setup(
            headless=str_to_bool(args.headless),
            timeout_ms=args.timeout_ms,
            profile_dir=args.profile_dir,
            cdp_endpoint=args.cdp_endpoint,
        )
        return

    if args.command == "new-filings":
        out_path = Path(args.out) if args.out else default_output_path(args.date)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        result = await search_new_filings(
            filing_date=args.date,
            headless=str_to_bool(args.headless),
            timeout_ms=args.timeout_ms,
            profile_dir=args.profile_dir,
            cdp_endpoint=args.cdp_endpoint,
        )
        out_path.write_text(json.dumps(result.rows, indent=2))
        print(f"Wrote {len(result.rows)} rows to {out_path}")


def main() -> None:
    asyncio.run(main_async())


if __name__ == "__main__":
    main()
