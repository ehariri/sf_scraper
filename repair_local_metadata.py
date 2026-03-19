#!/usr/bin/env python3
"""Repair local scrape metadata after runs that counted unsaved PDFs as scraped."""

import argparse
import json
from pathlib import Path

from fast_scraper import scraper as scraper_mod


def iter_case_jsons(data_root: Path):
    for path in sorted(data_root.glob("20??-??-??/*/register_of_actions.json")):
        yield path


def actual_pdf_count(case_dir: Path) -> int:
    return sum(
        1
        for path in case_dir.iterdir()
        if path.is_file() and path.suffix.lower() == ".pdf"
    )


def rebuild_failed_cases_for_day(day_dir: Path, preserved_failed=None):
    failed_cases = []
    date_str = day_dir.name
    existing_failed = {}
    failed_path = day_dir / "failed_cases.json"
    if failed_path.exists():
        try:
            payload = json.loads(failed_path.read_text())
            existing_failed = {
                case.get("case_num"): case
                for case in payload.get("failed_cases", [])
                if case.get("case_num")
            }
        except Exception:
            existing_failed = {}
    if preserved_failed:
        for case_num, payload in preserved_failed.items():
            if case_num:
                existing_failed[case_num] = payload

    for case_dir in sorted(path for path in day_dir.iterdir() if path.is_dir()):
        json_path = case_dir / "register_of_actions.json"
        metadata = {}
        if json_path.exists():
            try:
                data = json.loads(json_path.read_text())
                metadata = data.get("metadata", {})
            except Exception:
                metadata = {}

        if json_path.exists() and scraper_mod.case_is_complete(date_str, case_dir.name):
            continue
        prior_failed = existing_failed.get(case_dir.name, {})
        failed_cases.append(
            {
                "case_num": case_dir.name,
                "title": metadata.get("case_title", "") or prior_failed.get("title", ""),
                "link": (
                    metadata.get("case_url")
                    or metadata.get("source", {}).get("search_result_link")
                    or prior_failed.get("link")
                ),
                "result_index": metadata.get("result_index", prior_failed.get("result_index")),
                "source_filing_date": metadata.get("filing_date", prior_failed.get("source_filing_date", date_str)),
            }
        )
    return scraper_mod.write_failed_cases(date_str, failed_cases), failed_cases


def rebuild_day_summary_for_day(day_dir: Path, failed_cases):
    date_str = day_dir.name
    current_summary = {}
    summary_path = day_dir / "day_summary.json"
    if summary_path.exists():
        try:
            current_summary = json.loads(summary_path.read_text())
        except Exception:
            current_summary = {}

    total_cases = current_summary.get("total_cases", 0)
    if not total_cases:
        total_cases = sum(1 for path in day_dir.iterdir() if path.is_dir())

    scraped_cases = 0
    cases_with_timing = 0
    total_scrape_elapsed_seconds = 0.0
    total_download_elapsed_seconds = 0.0
    total_downloaded_bytes = 0
    total_downloaded_docs = 0

    for case_dir in sorted(path for path in day_dir.iterdir() if path.is_dir()):
        json_path = case_dir / "register_of_actions.json"
        if not json_path.exists():
            continue
        try:
            data = json.loads(json_path.read_text())
        except Exception:
            continue
        metadata = data.get("metadata", {})
        timing = metadata.get("timing", {})
        if scraper_mod.case_is_complete(date_str, case_dir.name):
            scraped_cases += 1
        if timing:
            cases_with_timing += 1
            total_scrape_elapsed_seconds += timing.get("scrape_elapsed_seconds", 0.0)
            total_download_elapsed_seconds += timing.get(
                "download_elapsed_seconds", 0.0
            )
            total_downloaded_bytes += timing.get("downloaded_bytes", 0)
            total_downloaded_docs += timing.get("downloaded_docs", 0)

    summary = {
        "date": date_str,
        "total_cases": total_cases,
        "scraped_cases": scraped_cases,
        "fully_completed": total_cases > 0 and scraped_cases >= total_cases,
        "updated_at": scraper_mod.utc_now_iso(),
        "timing": {
            "cases_with_timing": cases_with_timing,
            "total_scrape_elapsed_seconds": round(total_scrape_elapsed_seconds, 3),
            "total_download_elapsed_seconds": round(
                total_download_elapsed_seconds, 3
            ),
            "total_downloaded_bytes": total_downloaded_bytes,
            "total_downloaded_docs": total_downloaded_docs,
        },
        "last_run": {
            "mode": "metadata_repair",
            "started_at": scraper_mod.utc_now_iso(),
            "finished_at": scraper_mod.utc_now_iso(),
            "elapsed_seconds": 0.0,
            "case_count": total_cases,
            "pending_case_count": len(failed_cases),
            "failed_case_count": len(failed_cases),
            "retry_rounds_run": 0,
            "max_concurrent_cases": 0,
            "max_concurrent_downloads": 0,
            "case_launch_stagger_ms": 0,
        },
    }
    summary_path.write_text(json.dumps(summary, indent=2))
    return summary


def repair_metadata(data_root: Path, rebuild_all_days: bool = False):
    scraper_mod.LOCAL_DATA_ROOT = data_root

    affected_days = set()
    affected_cases = 0
    preserved_failed_by_day = {}

    for json_path in iter_case_jsons(data_root):
        case_dir = json_path.parent
        try:
            data = json.loads(json_path.read_text())
        except Exception:
            continue

        metadata = data.get("metadata")
        if not isinstance(metadata, dict):
            continue

        if metadata.get("status") != "restricted" and not metadata.get("roa_source"):
            day_dir = case_dir.parent
            preserved_failed_by_day.setdefault(day_dir, {})[case_dir.name] = {
                "case_num": case_dir.name,
                "title": metadata.get("case_title", ""),
                "link": (
                    metadata.get("case_url")
                    or metadata.get("source", {}).get("search_result_link")
                ),
                "result_index": metadata.get("result_index"),
                "source_filing_date": metadata.get("filing_date", day_dir.name),
            }
            json_path.unlink(missing_ok=True)
            affected_days.add(day_dir)
            affected_cases += 1
            continue

        if metadata.get("storage") != "local":
            continue

        current_scraped = metadata.get("scraped_links", 0) or 0
        total_links = metadata.get("total_links", 0) or 0
        pdf_count = actual_pdf_count(case_dir)
        if current_scraped == pdf_count:
            continue

        metadata["scraped_links"] = pdf_count
        if total_links > 0 and pdf_count < total_links:
            metadata["repair_note"] = (
                "scraped_links adjusted to match PDFs present on local disk"
            )
        else:
            metadata.pop("repair_note", None)

        json_path.write_text(json.dumps(data, indent=2))
        affected_days.add(case_dir.parent)
        affected_cases += 1

    target_days = affected_days
    if rebuild_all_days:
        target_days = {
            path
            for path in data_root.iterdir()
            if path.is_dir() and path.name.startswith("20")
        }

    repaired_days = []
    for day_dir in sorted(target_days):
        failed_payload, failed_cases = rebuild_failed_cases_for_day(
            day_dir, preserved_failed=preserved_failed_by_day.get(day_dir)
        )
        summary = rebuild_day_summary_for_day(day_dir, failed_cases)
        repaired_days.append(
            {
                "date": day_dir.name,
                "failed_cases": len(failed_payload.get("failed_cases", [])),
                "scraped_cases": summary.get("scraped_cases", 0),
                "total_cases": summary.get("total_cases", 0),
                "fully_completed": summary.get("fully_completed", False),
            }
        )

    return affected_cases, repaired_days


def main():
    parser = argparse.ArgumentParser(
        description="Repair local scrape metadata to match PDFs present on disk."
    )
    parser.add_argument(
        "--data-root",
        type=Path,
        default=Path("data_2024"),
        help="Root directory containing local day folders",
    )
    parser.add_argument(
        "--rebuild-all-days",
        action="store_true",
        help="Rebuild failed_cases.json and day_summary.json for every day in the data root",
    )
    args = parser.parse_args()

    affected_cases, repaired_days = repair_metadata(
        args.data_root, rebuild_all_days=args.rebuild_all_days
    )
    print(
        json.dumps(
            {
                "data_root": str(args.data_root),
                "affected_cases": affected_cases,
                "affected_days": len(repaired_days),
                "sample_days": repaired_days[:10],
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
