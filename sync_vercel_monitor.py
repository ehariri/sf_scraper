#!/usr/bin/env python3
from pathlib import Path
import shutil


ROOT = Path(__file__).resolve().parent
MONITOR_ROOT = ROOT / "monitor"
VERCEL_ROOT = ROOT / "vercel_monitor"
FILES = ("index.html", "app.js", "styles.css")


def main():
    for name in FILES:
        src = MONITOR_ROOT / name
        dst = VERCEL_ROOT / name
        if not src.exists():
            raise FileNotFoundError(src)
        shutil.copy2(src, dst)
        print(f"Synced {src} -> {dst}")


if __name__ == "__main__":
    main()
