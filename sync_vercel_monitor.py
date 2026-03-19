#!/usr/bin/env python3
import json
from pathlib import Path

from monitor_app import MonitorHandler, build_status


ROOT = Path(__file__).resolve().parent
MONITOR_ROOT = ROOT / "monitor"
VERCEL_ROOT = ROOT / "vercel_monitor"
FILES = ("index.html", "app.js", "styles.css")
STATUS_SNAPSHOT = VERCEL_ROOT / "status-snapshot.json"
GENERATED_BANNER = {
    "index.html": "<!-- GENERATED FILE: edit monitor/index.html, then run python sync_vercel_monitor.py -->\n",
    "app.js": "// GENERATED FILE: edit monitor/app.js, then run python sync_vercel_monitor.py\n",
    "styles.css": "/* GENERATED FILE: edit monitor/styles.css, then run python sync_vercel_monitor.py */\n",
}


def main():
    for name in FILES:
        src = MONITOR_ROOT / name
        dst = VERCEL_ROOT / name
        if not src.exists():
            raise FileNotFoundError(src)
        content = src.read_text()
        banner = GENERATED_BANNER.get(name, "")
        dst.write_text(f"{banner}{content}")
        print(f"Synced {src} -> {dst}")

    payload = build_status()
    STATUS_SNAPSHOT.write_text(
        json.dumps(payload, indent=2, default=MonitorHandler._json_default) + "\n"
    )
    print(f"Wrote {STATUS_SNAPSHOT}")


if __name__ == "__main__":
    main()
