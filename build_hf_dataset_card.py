import argparse
import json
from collections import defaultdict
from datetime import date
from pathlib import Path
import re

from huggingface_hub import HfApi, hf_hub_download
from huggingface_hub.errors import EntryNotFoundError


DEFAULT_REPO_ID = "Arifov/sf_superior_court"
DEFAULT_DATA_DIR = Path("data")
DEFAULT_OUTPUT = Path("HF_DATASET_CARD.md")

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
    remote_months = remote_summary["months"]
    remote_total_cases = remote_summary["total_cases"]
    remote_case_types = remote_summary["case_types"]
    today = date.today().isoformat()

    local_day_count = len(rows)
    local_scraped = sum(row["scraped_cases"] for row in rows)
    local_total = sum(row["total_cases"] for row in rows)
    local_full_days = sum(1 for row in rows if row["fully_completed"])
    local_first = rows[0]["day"] if rows else "n/a"
    local_last = rows[-1]["day"] if rows else "n/a"

    remote_year_lines = []
    for year in sorted(remote_years):
        bucket = remote_years[year]
        remote_year_lines.append(
            f"| {year} | {bucket['days']} | {bucket['cases']:,} |"
        )
    remote_month_lines = []
    for month in sorted(remote_months):
        bucket = remote_months[month]
        remote_month_lines.append(
            f"| {month} | {bucket['days']} | {bucket['cases']:,} |"
        )
    remote_case_type_lines = []
    for prefix, count in sorted(
        remote_case_types.items(), key=lambda item: (-item[1], item[0])
    ):
        share = (count / remote_total_cases * 100) if remote_total_cases else 0.0
        remote_case_type_lines.append(f"| {prefix} | {count:,} | {share:.1f}% |")

    local_year_lines = []
    for year in sorted(local_years):
        bucket = local_years[year]
        rate = (
            (bucket["scraped_cases"] / bucket["total_cases"]) * 100
            if bucket["total_cases"]
            else 0.0
        )
        local_year_lines.append(
            f"| {year} | {bucket['days']} | {bucket['full_days']} | "
            f"{bucket['scraped_cases']:,} | {bucket['total_cases']:,} | {rate:.1f}% |"
        )
    total_rate = (local_scraped / local_total * 100) if local_total else 0.0
    local_month_lines = []
    for year in sorted(local_months):
        local_month_lines.append(f"- **{year}**: {compress_months(local_months[year])}")
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

This card reflects the current state of the scrape as of **{today}**.

## Dataset state

The Hugging Face dataset repo currently contains:

- **{len(remote_days)}** filing-day folders
- **{remote_total_cases:,}** uploaded case-level `register_of_actions.json` files
- Earliest uploaded day: **{remote_days[0] if remote_days else 'n/a'}**
- Latest uploaded day: **{remote_days[-1] if remote_days else 'n/a'}**
- Average uploaded cases per HF filing day: **{remote_summary['mean_cases_per_day']:.1f}**
- Median uploaded cases per HF filing day: **{remote_summary['median_cases_per_day']:.1f}**
{"- HF uploaded scrape rate across matched uploaded days: **" + format(remote_summary['scrape_rate'], '.1f') + "%** (" + str(remote_total_cases) + " uploaded / " + str(remote_summary['total_discovered_cases']) + " discovered across " + str(remote_summary['matched_days_with_totals']) + " days)" if remote_summary['scrape_rate'] is not None else "- HF uploaded scrape rate: unavailable (missing day-level totals for uploaded days)"}

In this project, a case counts as **uploaded** when a case directory with `register_of_actions.json` is present in the HF dataset repo.

## HF coverage by year

The table below summarizes the dataset that is actually present in the HF repo.

| Year | Filing days in HF | Uploaded cases in HF |
| --- | ---: | ---: |
{chr(10).join(remote_year_lines)}
| Total | {len(remote_days)} | {remote_total_cases:,} |

## HF coverage by month

Month-level coverage currently present in the HF repo:

| Month | Filing days in HF | Uploaded cases in HF |
| --- | ---: | ---: |
{chr(10).join(remote_month_lines)}

## HF case type distribution

Case types here are approximated from the leading alphabetic prefix of the case number stored in each uploaded case directory.

| Case prefix | Uploaded cases in HF | Share of HF uploaded cases |
| --- | ---: | ---: |
{chr(10).join(remote_case_type_lines)}

## Local backlog pending sync

The local working corpus is larger than the current HF upload:

- **{local_day_count:,}** filing days on local disk
- **{local_total:,}** discovered cases on local disk
- **{local_scraped:,}** scraped cases on local disk
- **{local_full_days:,}** fully completed local filing days
- Local coverage currently runs from **{local_first}** through **{local_last}**

Local year summary:

| Year | Filing days | Fully completed days | Scraped cases | Discovered cases | Scrape rate |
| --- | ---: | ---: | ---: | ---: | ---: |
{chr(10).join(local_year_lines)}
| Total | {local_day_count:,} | {local_full_days:,} | {local_scraped:,} | {local_total:,} | {total_rate:.1f}% |

Local month coverage:

{chr(10).join(local_month_lines)}

## Data layout

Each filing-day folder lives under `data/YYYY-MM-DD/`.

Typical contents:

- `day_summary.json`: filing-day level counts and run metadata
- `failed_cases.json`: cases that were discovered but not successfully scraped
- `sync_metadata.json`: upload/prune verification metadata when the day has been synced
- `CASE_NUMBER/register_of_actions.json`: case-level metadata and action rows
- `CASE_NUMBER/*.pdf`: downloaded docket PDFs when available

## Important limitations

- This is an **in-progress scrape**, not a final frozen release.
- The court site is operationally unstable under automation, so many days are only partially complete after the first pass.
- The Hugging Face repo may lag the local working corpus because upload and prune happen asynchronously.
- Coverage is not uniform across case families. Straight civil prefixes tend to scrape more reliably than some family, probate, or related case categories.
- This dataset is derived from a public court portal and should be used with appropriate care around privacy, legal process, and downstream publication.

## Intended use

This corpus is being built to support:

- legal text collection
- docket-level and order-level AI-use analysis
- retrieval and triage experiments over court filings
- measurement of detectable AI-generated language in litigation materials
"""


def build_card(data_dir: Path, repo_id: str):
    api = HfApi()
    rows = load_day_summaries(data_dir)
    local_years, local_months = summarize_local(rows)
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
    args = parser.parse_args()

    card = build_card(args.data_dir, args.repo_id)
    args.output.write_text(card)
    print(f"Wrote dataset card to {args.output}")


if __name__ == "__main__":
    main()
