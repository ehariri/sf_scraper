#!/usr/bin/env python3
import argparse
import json
import mimetypes
import os
import re
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

from hf_remote_state import load_remote_state


ROOT = Path(__file__).resolve().parent
DATA_ROOT = ROOT / "data"
LOG_ROOT = ROOT / "logs"
STATIC_ROOT = ROOT / "monitor"
SCOPE_START = "2020-01-01"
SCOPE_END = "2025-12-31"
CACHE_LOCK = threading.Lock()
CACHE = {}

PROCESS_PATTERNS = {
    "sync": ["sync_existing_to_hf_and_prune.py", "upload_data_in_batches.py"],
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
ANSI_RE = re.compile(r"\x1b\[[0-9;?]*[A-Za-z]")
LFS_PROGRESS_RE = re.compile(
    r"Upload\s+(?P<total>\d+)\s+LFS files:\s+(?P<pct>\d+)%.*?(?P<done>\d+)/(?P=total)"
)
PREPARED_RE = re.compile(
    r"Prepared\s+(?P<batch_count>\d+)\s+batches\s+from\s+(?P<day_count>\d+)\s+day folders"
)
BATCH_PLAN_RE = re.compile(
    r"Batch\s+(?P<index>\d+)/(?P<total>\d+):\s+(?P<days>\d+)\s+days,\s+(?P<files>\d+)\s+files,\s+(?P<gb>[0-9.]+)\s+GB"
)
BATCH_FINISHED_RE = re.compile(
    r"Batch\s+(?P<index>\d+)/(?P<total>\d+)\s+finished\s+in\s+(?P<seconds>[0-9.]+)s"
)
UPLOADING_DAY_RE = re.compile(
    r"Uploading\s+(?P<day>\d{4}-\d{2}-\d{2})(?::\s+(?P<cases>\d+)\s+cases,\s+(?P<files>\d+)\s+files,\s+(?P<mb>[0-9.]+)\s+MB)?"
)
SEARCH_TIMEOUT_RE = re.compile(
    r"Timed out waiting for search results for (?P<day>\d{4}-\d{2}-\d{2})\."
)
NO_CASES_RE = re.compile(
    r"No cases found with filings on (?P<day>\d{4}-\d{2}-\d{2})\."
)


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


def strip_ansi(text):
    return ANSI_RE.sub("", text).replace("\r", "\n")


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


def list_bulk_upload_logs():
    return sorted(path.name for path in LOG_ROOT.glob("hf_bulk_upload*.log"))


def collect_logs(log_names, line_count=40):
    logs = []
    for name in log_names:
        path = LOG_ROOT / name
        entry = tail_log(path, line_count=line_count)
        entry["name"] = name
        logs.append(entry)
    return logs


def known_attempt_statuses():
    def compute():
        statuses = {}
        for name in list_scrape_logs():
            path = LOG_ROOT / name
            if not path.exists():
                continue
            try:
                for raw_line in path.read_text(errors="replace").splitlines():
                    line = strip_ansi(raw_line)
                    timeout_match = SEARCH_TIMEOUT_RE.search(line)
                    if timeout_match:
                        statuses[timeout_match.group("day")] = "attempted_error"
                        continue
                    no_cases_match = NO_CASES_RE.search(line)
                    if no_cases_match:
                        statuses.setdefault(no_cases_match.group("day"), "no_cases")
            except OSError:
                continue
        return statuses

    return cache_get_or_compute("known_attempt_statuses", 30, compute)


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


def iter_data_roots():
    roots = []
    if DATA_ROOT.exists():
        roots.append(DATA_ROOT)
    for path in sorted(ROOT.glob("data*")):
        if not path.is_dir() or path == DATA_ROOT:
            continue
        roots.append(path)
    return roots


def total_data_size_bytes():
    return sum(directory_size_bytes(path) for path in iter_data_roots())


def latest_log_activity(log_names):
    latest = None
    for name in log_names:
        path = LOG_ROOT / name
        if not path.exists():
            continue
        try:
            updated = datetime.fromtimestamp(path.stat().st_mtime, timezone.utc)
        except FileNotFoundError:
            continue
        if latest is None or updated > latest:
            latest = updated
    return latest


def cached_remote_state():
    def compute():
        return load_remote_state() or {"days": {}}

    return cache_get_or_compute("cached_remote_state", 15, compute)


def cached_remote_hf_day_summary_map():
    state = cached_remote_state()
    summaries = {}
    for day, payload in (state.get("days") or {}).items():
        if not (SCOPE_START <= day <= SCOPE_END):
            continue
        summaries[day] = {
            "date": day,
            "source": "hf",
            "total_cases": int(payload.get("total_cases", 0) or 0),
            "scraped_cases": int(payload.get("scraped_cases", 0) or 0),
            "fully_completed": bool(payload.get("fully_completed", False)),
            "updated_at_iso": payload.get("updated_at"),
        }
    return summaries


def cached_remote_hf_day_folders():
    return sorted(cached_remote_hf_day_summary_map().keys())


def load_json(path):
    try:
        return json.loads(path.read_text())
    except (OSError, json.JSONDecodeError):
        return None


def iter_day_dirs():
    for root in iter_data_roots():
        for day_dir in sorted(root.iterdir()):
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
        rows_by_date = {}
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
                "root": day_dir.parent.name,
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
            existing = rows_by_date.get(row["date"])
            if not existing:
                rows_by_date[row["date"]] = row
                continue

            existing_updated = existing["updated_at"] or datetime.fromtimestamp(0, timezone.utc)
            row_updated = row["updated_at"] or datetime.fromtimestamp(0, timezone.utc)
            if (
                row_updated > existing_updated
                or (
                    row_updated == existing_updated
                    and (
                        row["scraped_cases"] > existing["scraped_cases"]
                        or row["total_cases"] > existing["total_cases"]
                    )
                )
            ):
                rows_by_date[row["date"]] = row
        return [rows_by_date[key] for key in sorted(rows_by_date)]

    return cache_get_or_compute("day_rows", 15, compute)


def combine_day_rows(local_rows, hf_summaries):
    combined = {}

    for row in local_rows:
        payload = dict(row)
        payload["source"] = "local"
        combined[row["date"]] = payload

    for day, remote_row in (hf_summaries or {}).items():
        updated_at = parse_iso8601(remote_row.get("updated_at_iso"))
        if day not in combined:
            combined[day] = {
                "date": day,
                "root": "hf",
                "year": day[:4],
                "month": day[:7],
                "total_cases": int(remote_row.get("total_cases", 0) or 0),
                "scraped_cases": int(remote_row.get("scraped_cases", 0) or 0),
                "fully_completed": bool(remote_row.get("fully_completed", False)),
                "updated_at": updated_at,
                "updated_at_iso": remote_row.get("updated_at_iso"),
                "failed_case_count": 0,
                "last_run": {},
                "sync_updated_at": None,
                "sync_updated_at_iso": None,
                "sync_case_records": 0,
                "sync_pruned_cases": 0,
                "sync_total_bytes": 0,
                "timing": {},
                "source": "hf",
            }
            continue

        local_row = combined[day]
        merged = dict(local_row)
        merged["total_cases"] = max(
            int(local_row.get("total_cases", 0) or 0),
            int(remote_row.get("total_cases", 0) or 0),
        )
        merged["scraped_cases"] = max(
            int(local_row.get("scraped_cases", 0) or 0),
            int(remote_row.get("scraped_cases", 0) or 0),
        )
        merged["fully_completed"] = bool(local_row.get("fully_completed")) or bool(
            remote_row.get("fully_completed", False)
        )
        local_updated = local_row.get("updated_at")
        if updated_at and (not local_updated or updated_at > local_updated):
            merged["updated_at"] = updated_at
            merged["updated_at_iso"] = remote_row.get("updated_at_iso")
        merged["source"] = "both"
        combined[day] = merged

    return [combined[key] for key in sorted(combined)]


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


def build_calendar(rows, hf_days=None, hf_summaries=None, known_statuses=None):
    row_map = {row["date"]: row for row in rows}
    hf_day_set = set(hf_days or [])
    hf_summaries = hf_summaries or {}
    known_statuses = known_statuses or {}
    years = defaultdict(list)

    for current in iter_scope_dates():
        iso = current.isoformat()
        weekday = current.weekday()
        if weekday >= 5:
            continue
        row = row_map.get(iso)
        remote_row = hf_summaries.get(iso)
        calendar_row = row or remote_row

        if calendar_row:
            total_cases = calendar_row["total_cases"]
            scraped_cases = calendar_row["scraped_cases"]
            remaining_cases = max(0, total_cases - scraped_cases)
            if total_cases <= 0:
                status = "no_cases"
                shade = 0
            else:
                completion_ratio = scraped_cases / total_cases if total_cases else 0.0
                if calendar_row["fully_completed"]:
                    status = "complete"
                    shade = 4
                else:
                    status = "touched"
                    if scraped_cases == 0 or completion_ratio < 0.25:
                        shade = 1
                    elif completion_ratio < 0.5:
                        shade = 2
                    else:
                        shade = 3
            source = "local" if row else "hf"
        else:
            total_cases = 0
            scraped_cases = 0
            remaining_cases = 0
            status = known_statuses.get(iso, "untouched")
            shade = 0
            source = "log" if status != "untouched" else "none"

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
                "updated_at": (row["updated_at_iso"] if row else (remote_row.get("updated_at_iso") if remote_row else None)),
                "on_hf": iso in hf_day_set,
                "source": source,
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
            {"status": "untouched", "label": "Untouched"},
            {"status": "attempted_error", "label": "Attempted, unresolved"},
            {"status": "shade1", "label": "Many cases left"},
            {"status": "shade2", "label": "Some progress"},
            {"status": "shade3", "label": "Mostly scraped"},
            {"status": "shade4", "label": "Complete"},
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


def parse_upload_status():
    def compute():
        latest_path = None
        latest_mtime = None
        for name in list_bulk_upload_logs():
            path = LOG_ROOT / name
            if not path.exists():
                continue
            try:
                mtime = path.stat().st_mtime
            except FileNotFoundError:
                continue
            if latest_mtime is None or mtime > latest_mtime:
                latest_mtime = mtime
                latest_path = path

        if not latest_path:
            return {
                "active": False,
                "status": "idle",
                "stage": "idle",
                "batch_index": None,
                "batch_total": None,
                "batch_days": None,
                "batch_files": None,
                "batch_size_gb": None,
                "batches_finished": 0,
                "prepared_batches": None,
                "prepared_days": None,
                "files_done": None,
                "files_total": None,
                "files_pct": None,
                "current_day": None,
                "message": "No bulk upload log found.",
                "updated_at": None,
            }

        text = strip_ansi(latest_path.read_text(errors="replace"))
        lines = [line.strip() for line in text.splitlines() if line.strip()]
        prepared = None
        batch_plans = {}
        batch_finished = set()
        current_batch_index = None
        current_batch_total = None
        current_day = None
        files_done = None
        files_total = None
        files_pct = None
        stage = "planning"
        message = "Preparing bulk upload."

        for line in lines:
            match = PREPARED_RE.search(line)
            if match:
                prepared = {
                    "batch_count": int(match.group("batch_count")),
                    "day_count": int(match.group("day_count")),
                }
                continue

            match = BATCH_PLAN_RE.search(line)
            if match:
                idx = int(match.group("index"))
                batch_plans[idx] = {
                    "days": int(match.group("days")),
                    "files": int(match.group("files")),
                    "size_gb": float(match.group("gb")),
                }
                if current_batch_index is None:
                    current_batch_index = idx
                    current_batch_total = int(match.group("total"))
                continue

            match = BATCH_FINISHED_RE.search(line)
            if match:
                idx = int(match.group("index"))
                batch_finished.add(idx)
                current_batch_index = idx + 1
                current_batch_total = int(match.group("total"))
                stage = "starting_next_batch"
                message = f"Batch {idx}/{current_batch_total} finished."
                continue

            match = UPLOADING_DAY_RE.search(line)
            if match:
                current_day = match.group("day")
                stage = "uploading_commit"
                message = f"Uploading data for {current_day}."
                continue

            match = LFS_PROGRESS_RE.search(line)
            if match:
                files_total = int(match.group("total"))
                files_done = int(match.group("done"))
                files_pct = int(match.group("pct"))
                stage = "uploading_lfs" if files_done < files_total else "finalizing_commit"
                if current_batch_index and current_batch_total:
                    message = f"Batch {current_batch_index}/{current_batch_total}: {files_done}/{files_total} files transferred."
                else:
                    message = f"{files_done}/{files_total} files transferred."
                continue

            lower = line.lower()
            if lower.startswith("verified "):
                stage = "verifying"
                message = line
                continue
            if lower.startswith("deleted local "):
                stage = "pruning"
                message = line
                continue
            if (
                "gateway time-out" in lower
                or "too many requests" in lower
                or "precondition failed" in lower
                or "retrying in" in lower
            ):
                stage = "retrying"
                message = line
                continue

        current_batch = batch_plans.get(current_batch_index, {})
        updated_at = datetime.fromtimestamp(latest_path.stat().st_mtime, timezone.utc)
        active = any("upload_data_in_batches.py" in row["command"] for row in matching_processes("sync"))
        return {
            "active": active,
            "status": "active" if active else "idle",
            "stage": stage,
            "batch_index": current_batch_index,
            "batch_total": current_batch_total,
            "batch_days": current_batch.get("days"),
            "batch_files": current_batch.get("files"),
            "batch_size_gb": current_batch.get("size_gb"),
            "batches_finished": len(batch_finished),
            "prepared_batches": prepared["batch_count"] if prepared else None,
            "prepared_days": prepared["day_count"] if prepared else None,
            "files_done": files_done,
            "files_total": files_total,
            "files_pct": files_pct,
            "current_day": current_day,
            "message": message,
            "updated_at": format_dt(updated_at),
        }

    return cache_get_or_compute("upload_status", 10, compute)


def build_status():
    remote_state = cached_remote_state()
    hf_days = cached_remote_hf_day_folders()
    hf_summaries = cached_remote_hf_day_summary_map()
    known_statuses = known_attempt_statuses()
    rows = collect_day_rows()
    combined_rows = combine_day_rows(rows, hf_summaries)
    corpus = summarize_days(combined_rows)
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
    latest_sync_at = max(
        [dt for dt in [latest_sync_at, latest_log_activity(SYNC_LOGS + list_bulk_upload_logs())] if dt],
        default=None,
    )
    upload_status = parse_upload_status()

    return {
        "generated_at": format_dt(utc_now()),
        "scope": corpus["scope"],
        "corpus": corpus,
        "storage": {
            "data_bytes": total_data_size_bytes(),
            "data_human": human_bytes(total_data_size_bytes()),
        },
        "remote_cache": {
            "repo_id": remote_state.get("repo_id"),
            "generated_at": remote_state.get("generated_at"),
            "head_commit": remote_state.get("head_commit") or {},
            "day_count": int(remote_state.get("day_count", 0) or 0),
        },
        "services": {
            "scrape": derive_service_status(
                "scrape", scrape_processes, latest_scrape_at, scrape_logs
            ),
            "sync": derive_service_status("sync", sync_processes, latest_sync_at, sync_logs),
            "upload": upload_status,
        },
        "logs": {"scrape": scrape_logs, "sync": sync_logs + collect_logs(list_bulk_upload_logs(), line_count=35)},
        "prefixes": summarize_case_prefixes(),
        "calendar": build_calendar(rows, hf_days, hf_summaries, known_statuses),
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
