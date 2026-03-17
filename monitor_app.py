#!/usr/bin/env python3
import argparse
import json
import mimetypes
import os
import socket
import subprocess
import threading
import time
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta, timezone
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse


ROOT = Path(__file__).resolve().parent
DATA_ROOT = ROOT / "data"
LOG_ROOT = ROOT / "logs"
STATIC_ROOT = ROOT / "monitor"
SCOPE_START = "2020-01-01"
SCOPE_END = "2025-12-31"
CACHE_LOCK = threading.Lock()
CACHE = {}

PROCESS_PATTERNS = {
    "sync": ["sync_existing_to_hf_and_prune.py"],
    "scrape": [
        "fast_scraper/scraper.py",
        "timed_scrape_runner.py",
        "run_failed_cleanup_shard.py",
        "launcher.py",
    ],
}

SYNC_LOGS = [
    "hf_sync_and_prune.log",
    "hf_sync_wifi.log",
    "hf_completed_sync.log",
]


def utc_now():
    return datetime.now(timezone.utc)


def parse_iso8601(value):
    if not value or not isinstance(value, str):
        return None
    try:
        if value.endswith("Z"):
            value = value[:-1] + "+00:00"
        return datetime.fromisoformat(value).astimezone(timezone.utc)
    except ValueError:
        return None


def format_dt(value):
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z") if value else None


def relative_seconds(value):
    if not value:
        return None
    return max(0.0, (utc_now() - value).total_seconds())


def run_command(cmd):
    proc = subprocess.run(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        check=False,
    )
    return proc.stdout


def cache_get_or_compute(key, ttl_seconds, fn):
    now = time.time()
    with CACHE_LOCK:
        cached = CACHE.get(key)
        if cached and now - cached["ts"] < ttl_seconds:
            return cached["value"]
    value = fn()
    with CACHE_LOCK:
        CACHE[key] = {"ts": now, "value": value}
    return value


def parse_ps_rows():
    output = run_command(["ps", "-axo", "pid=,etime=,command="])
    rows = []
    for line in output.splitlines():
        line = line.strip()
        if not line:
            continue
        parts = line.split(None, 2)
        if len(parts) < 3:
            continue
        pid, etime, command = parts
        rows.append({"pid": int(pid), "elapsed": etime, "command": command})
    return rows


def matching_processes(kind):
    patterns = PROCESS_PATTERNS[kind]
    rows = parse_ps_rows()
    return [
        row
        for row in rows
        if any(pattern in row["command"] for pattern in patterns)
        and "monitor_app.py" not in row["command"]
    ]


def tail_log(path, line_count=40):
    if not path.exists():
        return {"path": str(path), "exists": False, "lines": [], "updated_at": None}
    text = path.read_text(errors="replace")
    lines = text.splitlines()[-line_count:]
    updated_at = datetime.fromtimestamp(path.stat().st_mtime, timezone.utc)
    return {
        "path": str(path),
        "exists": True,
        "lines": lines,
        "updated_at": format_dt(updated_at),
    }


def list_scrape_logs():
    preferred = []
    others = []
    for path in sorted(LOG_ROOT.glob("*.log")):
        if path.name in SYNC_LOGS:
            continue
        if "overnight" in path.name or "scrape" in path.name or "cleanup" in path.name:
            preferred.append(path.name)
        else:
            others.append(path.name)
    return preferred + others


def collect_logs(log_names, line_count=40):
    logs = []
    for name in log_names:
        path = LOG_ROOT / name
        entry = tail_log(path, line_count=line_count)
        entry["name"] = name
        logs.append(entry)
    return logs


def human_bytes(num_bytes):
    units = ["B", "KB", "MB", "GB", "TB"]
    value = float(num_bytes)
    for unit in units:
        if value < 1024 or unit == units[-1]:
            return f"{value:.1f} {unit}"
        value /= 1024
    return f"{num_bytes} B"


def directory_size_bytes(path):
    def compute():
        total = 0
        for dirpath, _, filenames in os.walk(path):
            for name in filenames:
                try:
                    total += (Path(dirpath) / name).stat().st_size
                except FileNotFoundError:
                    continue
        return total

    return cache_get_or_compute(("dir_size", str(path)), 60, compute)


def load_json(path):
    try:
        return json.loads(path.read_text())
    except (OSError, json.JSONDecodeError):
        return None


def iter_day_dirs():
    for day_dir in sorted(DATA_ROOT.iterdir()):
        if not day_dir.is_dir():
            continue
        if not (SCOPE_START <= day_dir.name <= SCOPE_END):
            continue
        yield day_dir


def iter_scope_dates():
    current = date.fromisoformat(SCOPE_START)
    end = date.fromisoformat(SCOPE_END)
    while current <= end:
        yield current
        current += timedelta(days=1)


def collect_day_rows():
    def compute():
        rows = []
        for day_dir in iter_day_dirs():
            summary_path = day_dir / "day_summary.json"
            summary = load_json(summary_path)
            if not summary:
                continue
            sync = load_json(day_dir / "sync_metadata.json") or {}
            failed = load_json(day_dir / "failed_cases.json") or {}
            updated_at = parse_iso8601(summary.get("updated_at"))
            last_run = summary.get("last_run") or {}
            sync_updated_at = parse_iso8601(sync.get("updated_at"))
            pruned_cases = 0
            tracked_cases = 0
            total_synced_bytes = 0
            for payload in (sync.get("cases") or {}).values():
                tracked_cases += 1
                if payload.get("pruned_local"):
                    pruned_cases += 1
                total_synced_bytes += payload.get("total_bytes", 0)
            row = {
                "date": day_dir.name,
                "year": day_dir.name[:4],
                "month": day_dir.name[:7],
                "total_cases": int(summary.get("total_cases", 0) or 0),
                "scraped_cases": int(summary.get("scraped_cases", 0) or 0),
                "fully_completed": bool(summary.get("fully_completed", False)),
                "updated_at": updated_at,
                "updated_at_iso": format_dt(updated_at),
                "failed_case_count": len(failed.get("failed_cases", [])),
                "last_run": last_run,
                "sync_updated_at": sync_updated_at,
                "sync_updated_at_iso": format_dt(sync_updated_at),
                "sync_case_records": tracked_cases,
                "sync_pruned_cases": pruned_cases,
                "sync_total_bytes": total_synced_bytes,
                "timing": summary.get("timing") or {},
            }
            rows.append(row)
        return rows

    return cache_get_or_compute("day_rows", 15, compute)


def summarize_days(rows):
    year_buckets = defaultdict(
        lambda: {
            "days": 0,
            "full_days": 0,
            "scraped_cases": 0,
            "total_cases": 0,
            "failed_cases": 0,
        }
    )
    total_days = len(rows)
    total_cases = sum(row["total_cases"] for row in rows)
    scraped_cases = sum(row["scraped_cases"] for row in rows)
    full_days = sum(1 for row in rows if row["fully_completed"])
    total_failed_cases = sum(row["failed_case_count"] for row in rows)
    total_pruned_cases = sum(row["sync_pruned_cases"] for row in rows)
    total_synced_bytes = sum(row["sync_total_bytes"] for row in rows)
    rows_with_runs = 0
    total_run_elapsed_seconds = 0.0
    total_run_case_count = 0
    recent_rows = []

    for row in rows:
        bucket = year_buckets[row["year"]]
        bucket["days"] += 1
        bucket["scraped_cases"] += row["scraped_cases"]
        bucket["total_cases"] += row["total_cases"]
        bucket["failed_cases"] += row["failed_case_count"]
        if row["fully_completed"]:
            bucket["full_days"] += 1
        last_run = row.get("last_run") or {}
        elapsed_seconds = last_run.get("elapsed_seconds") or 0
        case_count = last_run.get("case_count") or 0
        if elapsed_seconds > 0:
            rows_with_runs += 1
            total_run_elapsed_seconds += elapsed_seconds
            total_run_case_count += case_count
            recent_rows.append(row)

    years = []
    for year in sorted(year_buckets):
        bucket = year_buckets[year]
        coverage = (
            (bucket["scraped_cases"] / bucket["total_cases"]) * 100
            if bucket["total_cases"]
            else 0.0
        )
        completion = (
            (bucket["full_days"] / bucket["days"]) * 100 if bucket["days"] else 0.0
        )
        years.append(
            {
                "year": year,
                **bucket,
                "coverage_pct": round(coverage, 1),
                "full_day_pct": round(completion, 1),
            }
        )

    latest_rows = sorted(
        rows,
        key=lambda row: row["updated_at"] or datetime.fromtimestamp(0, timezone.utc),
        reverse=True,
    )[:12]
    worst_rows = sorted(
        [row for row in rows if row["total_cases"] > 0 and not row["fully_completed"]],
        key=lambda row: (
            (row["scraped_cases"] / row["total_cases"]) if row["total_cases"] else 1.0,
            -row["total_cases"],
        ),
    )[:12]
    recent_completed = sorted(
        [row for row in rows if row["fully_completed"]],
        key=lambda row: row["updated_at"] or datetime.fromtimestamp(0, timezone.utc),
        reverse=True,
    )[:10]
    recent_speed_rows = sorted(
        recent_rows,
        key=lambda row: row["updated_at"] or datetime.fromtimestamp(0, timezone.utc),
        reverse=True,
    )[:25]
    recent_speed_cases = sum((row.get("last_run") or {}).get("case_count", 0) for row in recent_speed_rows)
    recent_speed_scraped = sum(row.get("scraped_cases", 0) for row in recent_speed_rows)
    recent_speed_elapsed = sum((row.get("last_run") or {}).get("elapsed_seconds", 0) for row in recent_speed_rows)
    avg_cases_per_minute = (
        scraped_cases / (total_run_elapsed_seconds / 60.0)
        if total_run_elapsed_seconds > 0
        else 0.0
    )
    recent_cases_per_minute = (
        recent_speed_scraped / (recent_speed_elapsed / 60.0)
        if recent_speed_elapsed > 0
        else 0.0
    )
    recent_success_rate = (
        (recent_speed_scraped / recent_speed_cases) * 100
        if recent_speed_cases > 0
        else 0.0
    )

    return {
        "scope": {"start": SCOPE_START, "end": SCOPE_END},
        "total_days": total_days,
        "full_days": full_days,
        "incomplete_days": total_days - full_days,
        "scraped_cases": scraped_cases,
        "total_cases": total_cases,
        "coverage_pct": round((scraped_cases / total_cases) * 100, 1) if total_cases else 0.0,
        "full_day_pct": round((full_days / total_days) * 100, 1) if total_days else 0.0,
        "failed_case_count": total_failed_cases,
        "synced_pruned_cases": total_pruned_cases,
        "synced_bytes": total_synced_bytes,
        "avg_cases_per_minute": round(avg_cases_per_minute, 2),
        "recent_cases_per_minute": round(recent_cases_per_minute, 2),
        "recent_success_rate": round(recent_success_rate, 1),
        "run_rows": rows_with_runs,
        "years": years,
        "latest_rows": latest_rows,
        "worst_rows": worst_rows,
        "recent_completed": recent_completed,
    }


def build_calendar(rows):
    row_map = {row["date"]: row for row in rows}
    years = defaultdict(list)

    for current in iter_scope_dates():
        iso = current.isoformat()
        weekday = current.weekday()
        if weekday >= 5:
            continue
        row = row_map.get(iso)

        if row:
            total_cases = row["total_cases"]
            scraped_cases = row["scraped_cases"]
            remaining_cases = max(0, total_cases - scraped_cases)
            if total_cases <= 0:
                status = "no_cases"
                shade = 0
            else:
                completion_ratio = scraped_cases / total_cases if total_cases else 0.0
                if scraped_cases == 0 or completion_ratio < 0.25:
                    shade = 1
                elif completion_ratio < 0.5:
                    shade = 2
                elif completion_ratio < 0.85:
                    shade = 3
                else:
                    shade = 4
                status = "complete" if row["fully_completed"] else "touched"
        else:
            total_cases = 0
            scraped_cases = 0
            remaining_cases = 0
            status = "untouched"
            shade = 0

        years[current.year].append(
            {
                "date": iso,
                "weekday": weekday,
                "month": current.month,
                "day": current.day,
                "week_index": 0,
                "status": status,
                "shade": shade,
                "total_cases": total_cases,
                "scraped_cases": scraped_cases,
                "remaining_cases": remaining_cases,
                "updated_at": row["updated_at_iso"] if row else None,
            }
        )

    month_labels = [
        "Jan", "Feb", "Mar", "Apr", "May", "Jun",
        "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
    ]
    year_entries = []
    for year, entries in sorted(years.items()):
        jan1 = date(year, 1, 1)
        start = jan1 - timedelta(days=(jan1.weekday() + 1) % 7)
        month_positions = {}
        for entry in entries:
            current = date.fromisoformat(entry["date"])
            week_index = ((current - start).days) // 7
            entry["week_index"] = week_index
            if current.day <= 7 and current.month not in month_positions:
                month_positions[current.month] = week_index

        year_entries.append(
            {
                "year": year,
                "weeks": max((entry["week_index"] for entry in entries), default=0) + 1,
                "days": entries,
                "months": [
                    {"label": month_labels[month - 1], "week_index": week_index}
                    for month, week_index in sorted(month_positions.items())
                ],
            }
        )

    return {
        "years": year_entries,
        "legend": [
            {"status": "untouched", "label": "Untouched weekday"},
            {"status": "no_cases", "label": "Known zero-case weekday"},
            {"status": "shade1", "label": "Many cases left"},
            {"status": "shade2", "label": "Some progress"},
            {"status": "shade3", "label": "Mostly scraped"},
            {"status": "shade4", "label": "Nearly done or complete"},
        ],
    }


def categorize_log_health(logs, max_age_seconds=3600):
    recent_logs = []
    for log in logs:
        if not log["exists"]:
            continue
        updated_at = parse_iso8601(log.get("updated_at"))
        age_seconds = relative_seconds(updated_at)
        if age_seconds is not None and age_seconds <= max_age_seconds:
            recent_logs.append(log)

    joined = "\n".join("\n".join(log["lines"]) for log in recent_logs)
    lowered = joined.lower()
    issues = []
    level = "healthy"
    if not recent_logs:
        return level, issues
    if "traceback" in lowered or "runtimeerror" in lowered:
        level = "error"
        issues.append("traceback detected in logs")
    if "verification failed" in lowered or "too many requests" in lowered:
        level = "error"
        issues.append("sync verification or rate-limit error detected")
    if "please solve the cloudflare challenge" in lowered:
        if level != "error":
            level = "waiting"
        issues.append("waiting for Cloudflare challenge")
    if "retrying later" in lowered or "timeout waiting for case page load" in lowered:
        if level == "healthy":
            level = "degraded"
        issues.append("high retry rate or page-load timeouts present")
    return level, issues


def derive_service_status(kind, processes, latest_activity_at, logs):
    log_level, issues = categorize_log_health(logs)
    age_seconds = relative_seconds(latest_activity_at)

    if processes:
        status = "healthy"
        if log_level == "error":
            status = "error"
        elif log_level == "waiting":
            status = "waiting"
        elif age_seconds is not None and age_seconds > 1800:
            status = "stalled"
            issues.append("no recent file activity for over 30 minutes")
        elif log_level == "degraded":
            status = "degraded"
        return {
            "kind": kind,
            "status": status,
            "active": True,
            "process_count": len(processes),
            "processes": processes,
            "latest_activity_at": format_dt(latest_activity_at),
            "latest_activity_age_seconds": age_seconds,
            "issues": issues,
        }

    if log_level == "error":
        status = "error"
    elif log_level == "waiting":
        status = "waiting"
    else:
        status = "stopped"

    return {
        "kind": kind,
        "status": status,
        "active": False,
        "process_count": 0,
        "processes": [],
        "latest_activity_at": format_dt(latest_activity_at),
        "latest_activity_age_seconds": age_seconds,
        "issues": issues,
    }


def summarize_case_prefixes():
    def compute():
        prefix_counter = Counter()
        scraped_counter = Counter()

        for day_dir in iter_day_dirs():
            failed = load_json(day_dir / "failed_cases.json") or {}
            for case in failed.get("failed_cases", []):
                case_number = case.get("case_num") or ""
                if case_number:
                    prefix_counter[case_number[:3]] += 1

            for case_dir in day_dir.iterdir():
                if not case_dir.is_dir():
                    continue
                roa = load_json(case_dir / "register_of_actions.json")
                if not roa:
                    continue
                case_number = (
                    ((roa.get("metadata") or {}).get("case_number"))
                    or case_dir.name
                )
                prefix = case_number[:3]
                prefix_counter[prefix] += 1
                scraped_counter[prefix] += 1

        entries = []
        for prefix, total in prefix_counter.most_common(12):
            scraped = scraped_counter[prefix]
            rate = (scraped / total) * 100 if total else 0.0
            entries.append(
                {
                    "prefix": prefix,
                    "scraped_cases": scraped,
                    "discovered_cases": total,
                    "coverage_pct": round(rate, 1),
                }
            )
        return entries

    return cache_get_or_compute("case_prefixes", 120, compute)


def build_status():
    rows = collect_day_rows()
    corpus = summarize_days(rows)
    scrape_logs = collect_logs(list_scrape_logs(), line_count=35)
    sync_logs = collect_logs(SYNC_LOGS, line_count=35)
    scrape_processes = matching_processes("scrape")
    sync_processes = matching_processes("sync")

    latest_scrape_at = max(
        (row["updated_at"] for row in rows if row["updated_at"]),
        default=None,
    )
    latest_sync_at = max(
        (row["sync_updated_at"] for row in rows if row["sync_updated_at"]),
        default=None,
    )

    return {
        "generated_at": format_dt(utc_now()),
        "scope": corpus["scope"],
        "corpus": corpus,
        "storage": {
            "data_bytes": directory_size_bytes(DATA_ROOT),
            "data_human": human_bytes(directory_size_bytes(DATA_ROOT)),
        },
        "services": {
            "scrape": derive_service_status(
                "scrape", scrape_processes, latest_scrape_at, scrape_logs
            ),
            "sync": derive_service_status("sync", sync_processes, latest_sync_at, sync_logs),
        },
        "logs": {"scrape": scrape_logs, "sync": sync_logs},
        "prefixes": summarize_case_prefixes(),
        "calendar": build_calendar(rows),
    }


class MonitorHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        parsed = urlparse(self.path)
        if parsed.path == "/api/status":
            payload = build_status()
            self._send_json(payload)
            return

        if parsed.path == "/":
            self._send_file(STATIC_ROOT / "index.html")
            return

        candidate = STATIC_ROOT / parsed.path.lstrip("/")
        if candidate.exists() and candidate.is_file():
            self._send_file(candidate)
            return

        self.send_error(HTTPStatus.NOT_FOUND, "Not found")

    def do_HEAD(self):
        parsed = urlparse(self.path)
        if parsed.path == "/api/status":
            body = json.dumps(build_status(), default=self._json_default).encode("utf-8")
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            return

        target = STATIC_ROOT / "index.html" if parsed.path == "/" else STATIC_ROOT / parsed.path.lstrip("/")
        if target.exists() and target.is_file():
            body = target.read_bytes()
            content_type, _ = mimetypes.guess_type(str(target))
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", content_type or "application/octet-stream")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            return

        self.send_error(HTTPStatus.NOT_FOUND, "Not found")

    def log_message(self, format_, *args):
        return

    def _send_json(self, payload):
        body = json.dumps(payload, default=self._json_default).encode("utf-8")
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _send_file(self, path):
        body = path.read_bytes()
        content_type, _ = mimetypes.guess_type(str(path))
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", content_type or "application/octet-stream")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    @staticmethod
    def _json_default(value):
        if isinstance(value, datetime):
            return format_dt(value)
        raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


def main():
    parser = argparse.ArgumentParser(description="Monitor scraper and HF sync state")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8787)
    parser.add_argument(
        "--public",
        action="store_true",
        help="Bind to 0.0.0.0 so the monitor is reachable from other devices on the network",
    )
    args = parser.parse_args()
    if args.public:
        args.host = "0.0.0.0"

    server = ThreadingHTTPServer((args.host, args.port), MonitorHandler)
    print(f"Monitor available at http://{args.host}:{args.port}")
    if args.host == "0.0.0.0":
        try:
            hostname = socket.gethostname()
            for candidate in sorted(set(socket.gethostbyname_ex(hostname)[2])):
                if candidate and not candidate.startswith("127."):
                    print(f"Monitor may also be reachable at http://{candidate}:{args.port}")
        except OSError:
            pass
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
