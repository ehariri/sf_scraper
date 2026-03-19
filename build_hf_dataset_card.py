import argparse
import json
from collections import defaultdict
from datetime import date
from pathlib import Path
import re

from huggingface_hub import HfApi, hf_hub_download
from huggingface_hub.errors import EntryNotFoundError
from hf_remote_state import DEFAULT_CACHE_PATH, load_remote_state


DEFAULT_REPO_ID = "please-the-bot/sf_superior_court"
DEFAULT_DATA_DIR = Path("data")
DEFAULT_OUTPUT = Path("HF_DATASET_CARD.md")
DEFAULT_CACHE_PATH_ARG = DEFAULT_CACHE_PATH

MONTH_NAMES = {
    1: "Jan",
    2: "Feb",
    3: "Mar",
    4: "Apr",
    5: "May",
    6: "Jun",
    7: "Jul",
    8: "Aug",
    9: "Sep",
    10: "Oct",
    11: "Nov",
    12: "Dec",
}


def load_day_summaries(data_dir: Path):
    rows = []
    for summary_path in sorted(data_dir.glob("20*/day_summary.json")):
        day = summary_path.parent.name
        try:
            payload = json.loads(summary_path.read_text())
        except Exception:
            continue
        total_cases = payload.get("total_cases", 0)
        if not total_cases:
            continue
        rows.append(
            {
                "day": day,
                "year": day[:4],
                "month": day[:7],
                "total_cases": total_cases,
                "scraped_cases": payload.get("scraped_cases", 0),
                "fully_completed": bool(payload.get("fully_completed", False)),
            }
        )
    return rows


def local_summary_map(rows):
    return {row["day"]: row for row in rows}


def summarize_local(rows):
    years = defaultdict(
        lambda: {"days": 0, "full_days": 0, "scraped_cases": 0, "total_cases": 0}
    )
    months = defaultdict(set)

    for row in rows:
        bucket = years[row["year"]]
        bucket["days"] += 1
        bucket["scraped_cases"] += row["scraped_cases"]
        bucket["total_cases"] += row["total_cases"]
        if row["fully_completed"]:
            bucket["full_days"] += 1
        months[row["year"]].add(int(row["month"][5:7]))

    return years, months


def remote_day_folders(api: HfApi, repo_id: str):
    days = []
    try:
        for entry in api.list_repo_tree(
            repo_id=repo_id,
            repo_type="dataset",
            path_in_repo="data",
            recursive=False,
            expand=True,
        ):
            if entry.path.startswith("data/") and entry.path.count("/") == 1:
                days.append(entry.path.split("/")[-1])
    except EntryNotFoundError:
        return []
    return sorted(days)


def remote_day_summary(repo_id: str, day: str):
    try:
        path = hf_hub_download(
            repo_id=repo_id,
            repo_type="dataset",
            filename=f"data/{day}/day_summary.json",
        )
    except Exception:
        return None
    try:
        return json.loads(Path(path).read_text())
    except Exception:
        return None


def summarize_remote(api: HfApi, repo_id: str, days):
    years = defaultdict(lambda: {"days": 0, "cases": 0})
    months = defaultdict(lambda: {"days": 0, "cases": 0})
    case_types = defaultdict(int)
    total_cases = 0
    total_discovered_cases = 0
    matched_days_with_totals = 0
    per_day_cases = []
    for day in days:
        case_count = 0
        try:
            for entry in api.list_repo_tree(
                repo_id=repo_id,
                repo_type="dataset",
                path_in_repo=f"data/{day}",
                recursive=True,
                expand=False,
            ):
                if entry.path.endswith("/register_of_actions.json"):
                    case_dir = entry.path.split("/")[-2]
                    case_types[case_type_prefix(case_dir)] += 1
                    case_count += 1
        except EntryNotFoundError:
            case_count = 0
        years[day[:4]]["days"] += 1
        years[day[:4]]["cases"] += case_count
        months[day[:7]]["days"] += 1
        months[day[:7]]["cases"] += case_count
        total_cases += case_count
        per_day_cases.append(case_count)
        remote_summary = remote_day_summary(repo_id, day)
        if remote_summary and remote_summary.get("total_cases"):
            total_discovered_cases += remote_summary["total_cases"]
            matched_days_with_totals += 1
    per_day_cases = sorted(per_day_cases)
    mean_cases_per_day = (total_cases / len(per_day_cases)) if per_day_cases else 0.0
    if not per_day_cases:
        median_cases_per_day = 0.0
    elif len(per_day_cases) % 2 == 1:
        median_cases_per_day = float(per_day_cases[len(per_day_cases) // 2])
    else:
        mid = len(per_day_cases) // 2
        median_cases_per_day = (per_day_cases[mid - 1] + per_day_cases[mid]) / 2.0
    scrape_rate = (
        (total_cases / total_discovered_cases) * 100 if total_discovered_cases else None
    )
    return {
        "years": years,
        "months": months,
        "total_cases": total_cases,
        "case_types": case_types,
        "total_discovered_cases": total_discovered_cases,
        "matched_days_with_totals": matched_days_with_totals,
        "mean_cases_per_day": mean_cases_per_day,
        "median_cases_per_day": median_cases_per_day,
        "scrape_rate": scrape_rate,
    }


def summarize_remote_from_cache(cache_payload, repo_id: str):
    if not cache_payload:
        return None
    if cache_payload.get("repo_id") != repo_id:
        return None

    days = cache_payload.get("days") or {}
    if not days:
        return None

    years = defaultdict(lambda: {"days": 0, "cases": 0})
    months = defaultdict(lambda: {"days": 0, "cases": 0})
    total_cases = 0
    total_discovered_cases = 0
    matched_days_with_totals = 0
    per_day_cases = []

    for day, payload in sorted(days.items()):
        scraped_cases = int(payload.get("scraped_cases", 0) or 0)
        total_cases_for_day = int(payload.get("total_cases", 0) or 0)

        years[day[:4]]["days"] += 1
        years[day[:4]]["cases"] += scraped_cases
        months[day[:7]]["days"] += 1
        months[day[:7]]["cases"] += scraped_cases
        total_cases += scraped_cases
        per_day_cases.append(scraped_cases)

        if total_cases_for_day:
            total_discovered_cases += total_cases_for_day
            matched_days_with_totals += 1

    per_day_cases = sorted(per_day_cases)
    mean_cases_per_day = (total_cases / len(per_day_cases)) if per_day_cases else 0.0
    if not per_day_cases:
        median_cases_per_day = 0.0
    elif len(per_day_cases) % 2 == 1:
        median_cases_per_day = float(per_day_cases[len(per_day_cases) // 2])
    else:
        mid = len(per_day_cases) // 2
        median_cases_per_day = (per_day_cases[mid - 1] + per_day_cases[mid]) / 2.0
    scrape_rate = (
        (total_cases / total_discovered_cases) * 100 if total_discovered_cases else None
    )

    return {
        "days": sorted(days),
        "years": years,
        "months": months,
        "total_cases": total_cases,
        "case_types": {},
        "total_discovered_cases": total_discovered_cases,
        "matched_days_with_totals": matched_days_with_totals,
        "mean_cases_per_day": mean_cases_per_day,
        "median_cases_per_day": median_cases_per_day,
        "scrape_rate": scrape_rate,
        "generated_at": cache_payload.get("generated_at"),
        "head_commit": (cache_payload.get("head_commit") or {}).get("commit_id"),
    }


def case_type_prefix(case_number: str):
    match = re.match(r"([A-Z]+)", case_number)
    return match.group(1) if match else "UNKNOWN"


def compress_months(month_numbers):
    if not month_numbers:
        return ""

    values = sorted(month_numbers)
    ranges = []
    start = prev = values[0]
    for current in values[1:]:
        if current == prev + 1:
            prev = current
            continue
        ranges.append((start, prev))
        start = prev = current
    ranges.append((start, prev))

    parts = []
    for start, end in ranges:
        if start == end:
            parts.append(MONTH_NAMES[start])
        else:
            parts.append(f"{MONTH_NAMES[start]}-{MONTH_NAMES[end]}")
    return ", ".join(parts)


def render_card(
    data_dir: Path,
    repo_id: str,
    rows,
    local_years,
    local_months,
    remote_days,
    remote_summary,
):
    remote_years = remote_summary["years"]
    remote_total_cases = remote_summary["total_cases"]
    today = date.today().isoformat()

    remote_year_lines = []
    for year in sorted(remote_years):
        bucket = remote_years[year]
        remote_year_lines.append(
            f"| {year} | {bucket['days']} | {bucket['cases']:,} |"
        )
    return f"""---
pretty_name: SF Superior Court Docket and ROA Scrape
language:
  - en
license: other
task_categories:
  - text-classification
  - token-classification
  - text-retrieval
tags:
  - courts
  - legal
  - dockets
  - register-of-actions
  - california
  - san-francisco
---

# SF Superior Court Docket and ROA Scrape

This dataset contains San Francisco Superior Court filing-day search results, case-level register-of-actions exports, and downloaded docket PDFs collected from the court's public portal.

This card describes the dataset currently present in the Hugging Face repo as of **{today}**.

## Overview

The Hugging Face dataset repo currently contains:

- **{len(remote_days)}** filing-day folders
- **{remote_total_cases:,}** uploaded case-level `register_of_actions.json` files
- Earliest uploaded day: **{remote_days[0] if remote_days else 'n/a'}**
- Latest uploaded day: **{remote_days[-1] if remote_days else 'n/a'}**
- Average uploaded cases per HF filing day: **{remote_summary['mean_cases_per_day']:.1f}**
- Median uploaded cases per HF filing day: **{remote_summary['median_cases_per_day']:.1f}**
{"- HF uploaded scrape rate across matched uploaded days: **" + format(remote_summary['scrape_rate'], '.1f') + "%** (" + str(remote_total_cases) + " uploaded / " + str(remote_summary['total_discovered_cases']) + " discovered across " + str(remote_summary['matched_days_with_totals']) + " days)" if remote_summary['scrape_rate'] is not None else "- HF uploaded scrape rate: unavailable (missing day-level totals for uploaded days)"}

In this project, a case counts as **uploaded** when a case directory with `register_of_actions.json` is present in the HF dataset repo.

## What Is In The Repo

Each filing day lives under `data/YYYY-MM-DD/`.

Typical contents:

- `day_summary.json`: filing-day level counts and scrape metadata
- `failed_cases.json`: cases discovered for that filing day that were not yet successfully scraped at the time of upload
- `CASE_NUMBER/register_of_actions.json`: case-level metadata and register-of-actions rows
- `CASE_NUMBER/*.pdf`: docket PDFs when document links were available

## Current HF Coverage

The table below summarizes the filing days and uploaded cases currently present in the HF repo.

| Year | Filing days in HF | Uploaded cases in HF |
| --- | ---: | ---: |
{chr(10).join(remote_year_lines)}
| Total | {len(remote_days)} | {remote_total_cases:,} |

## Limitations

- This is an **in-progress scrape**, not a final frozen release.
- Many filing days are only partially complete.
- Coverage is not uniform across case families.
- The presence of `failed_cases.json` in a day folder means that filing day still had unresolved cases at the time that version of the day was uploaded.
- This dataset is derived from a public court portal and should be used with appropriate care around privacy, legal process, and downstream publication.

## Intended Use

This corpus is being built to support:

- legal text collection
- docket-level and order-level AI-use analysis
- retrieval and triage experiments over court filings
- measurement of detectable AI-generated language in litigation materials
"""


def build_card(data_dir: Path, repo_id: str, cache_path: Path = DEFAULT_CACHE_PATH_ARG):
    rows = load_day_summaries(data_dir)
    local_years, local_months = summarize_local(rows)
    remote_cache = load_remote_state(cache_path)
    remote_summary = summarize_remote_from_cache(remote_cache, repo_id)
    if remote_summary is not None:
        remote_days = remote_summary["days"]
    else:
        api = HfApi()
        remote_days = remote_day_folders(api, repo_id)
        remote_summary = summarize_remote(api, repo_id, remote_days)
    return render_card(
        data_dir,
        repo_id,
        rows,
        local_years,
        local_months,
        remote_days,
        remote_summary,
    )


def main():
    parser = argparse.ArgumentParser(description="Build the HF dataset card from current data.")
    parser.add_argument("--repo-id", default=DEFAULT_REPO_ID)
    parser.add_argument("--data-dir", type=Path, default=DEFAULT_DATA_DIR)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--cache-path", type=Path, default=DEFAULT_CACHE_PATH_ARG)
    args = parser.parse_args()

    card = build_card(args.data_dir, args.repo_id, args.cache_path)
    args.output.write_text(card)
    print(f"Wrote dataset card to {args.output}")


if __name__ == "__main__":
    main()
