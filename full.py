import subprocess
import asyncio
from pathlib import Path
from playwright.async_api import async_playwright

CHROME_PROFILE = Path.home() / ".sf_manual_profile"
CHROME_PORT = 9222
TARGET_URL = "https://webapps.sftc.org/cc/CaseCalendar.dll"


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
        print("Navigation complete.")
        print("Solve Cloudflare (if any), wait for the Case Calendar page to load.")
        print("Then press Enter here to extract cookies...")

        input("Press Enter when ready...")

        # Extract cookies
        cookies = await context.cookies()

        # Filter only SF court + Cloudflare cookies
        sftc_cookies = [c for c in cookies if "sftc" in c["domain"]]
        cf_cookies = [c for c in cookies if "cf" in c["name"].lower()]

        print("\n=== SF Court Cookies ===")
        for c in sftc_cookies:
            print(f"{c['name']} = {c['value']}")

        print("\n=== Cloudflare Tokens ===")
        for c in cf_cookies:
            print(f"{c['name']} = {c['value']}")

        # Save FULL storage state for future headless use
        storage_path = CHROME_PROFILE / "sf_storage_state.json"
        await context.storage_state(path=str(storage_path))
        print(f"\nSaved storage state → {storage_path}")

        return storage_path


async def main():
    launch_real_chrome()

    print("Waiting 2 seconds for Chrome to start...")
    await asyncio.sleep(2)

    await open_sf_page()


if __name__ == "__main__":
    asyncio.run(main())
