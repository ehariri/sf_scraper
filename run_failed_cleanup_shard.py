import argparse
import json
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parent
SCRAPER = ROOT / ".venv" / "bin" / "python"
SCRAPER_SCRIPT = ROOT / "fast_scraper" / "scraper.py"


def parse_args():
    parser = argparse.ArgumentParser(
        description="Run failed-only cleanup for a shard of incomplete day folders."
    )
    parser.add_argument("--shard-index", type=int, required=True, help="0-based shard index")
    parser.add_argument("--shard-count", type=int, required=True, help="Total number of shards")
    parser.add_argument("--port", type=int, required=True, help="Chrome debug port for this shard")
    parser.add_argument("--data-root", default="data", help="Scraper data root")
    parser.add_argument("--max-concurrent-cases", type=int, default=2)
    parser.add_argument("--max-concurrent-downloads", type=int, default=6)
    parser.add_argument("--search-timeout-ms", type=int, default=30000)
    parser.add_argument("--table-idle-timeout-ms", type=int, default=30000)
    parser.add_argument("--case-ready-poll-attempts", type=int, default=20)
    parser.add_argument("--limit-days", type=int, default=0, help="Optional max days to process")
    return parser.parse_args()


def load_incomplete_days(data_root: Path):
    days = []
    for summary_path in sorted(data_root.glob("*/day_summary.json")):
        summary = json.loads(summary_path.read_text())
        total = summary.get("total_cases", 0)
        fully_completed = summary.get("fully_completed", False)
        if not total or fully_completed:
            continue
        days.append(summary_path.parent.name)
    return days


def run_day(args, day):
    cmd = [
        str(SCRAPER),
        "-u",
        str(SCRAPER_SCRIPT),
        "--port",
        str(args.port),
        "--start-date",
        day,
        "--end-date",
        day,
        "--data-root",
        args.data_root,
        "--failed-only",
        "--disable-hf-upload",
        "--keep-local-pdfs",
        "--max-concurrent-cases",
        str(args.max_concurrent_cases),
        "--max-concurrent-downloads",
        str(args.max_concurrent_downloads),
        "--retry-passes",
        "0",
        "--search-timeout-ms",
        str(args.search_timeout_ms),
        "--table-idle-timeout-ms",
        str(args.table_idle_timeout_ms),
        "--case-ready-poll-attempts",
        str(args.case_ready_poll_attempts),
    ]
    print(f"[cleanup] running {day}")
    return subprocess.run(cmd, cwd=ROOT).returncode


def main():
    args = parse_args()
    if args.shard_index < 0 or args.shard_index >= args.shard_count:
        raise SystemExit("shard-index must be in [0, shard-count)")

    data_root = (ROOT / args.data_root).resolve()
    days = load_incomplete_days(data_root)
    shard_days = days[args.shard_index :: args.shard_count]
    if args.limit_days > 0:
        shard_days = shard_days[: args.limit_days]

    print(
        f"[cleanup] shard {args.shard_index + 1}/{args.shard_count} "
        f"port={args.port} days={len(shard_days)}"
    )
    if shard_days:
        print(f"[cleanup] first={shard_days[0]} last={shard_days[-1]}")
    else:
        print("[cleanup] no days assigned")
        return 0

    failures = 0
    for day in shard_days:
        rc = run_day(args, day)
        if rc != 0:
            failures += 1
            print(f"[cleanup] day {day} exited with code {rc}", file=sys.stderr)
    print(f"[cleanup] done failures={failures}")
    return 0 if failures == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
