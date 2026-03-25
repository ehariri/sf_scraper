#!/usr/bin/env python3
import argparse
import os
import shutil
import time
from pathlib import Path

from huggingface_hub import HfApi

from sync_existing_to_hf_and_prune import (
    DEFAULT_DATA_DIR,
    DEFAULT_REPO_ID,
    candidate_day_dirs,
    refresh_monitor_remote_cache,
    verify_day,
)
from upload_data_in_batches import (
    ROOT,
    acquire_repo_lock,
    batch_day_label,
    day_size_bytes,
    derive_batch_count,
    partition_days,
)


def ensure_clean_dir(path: Path):
    if path.exists():
        shutil.rmtree(path)
    path.mkdir(parents=True, exist_ok=True)


def hardlink_tree(src: Path, dst: Path):
    dst.mkdir(parents=True, exist_ok=True)
    for path in src.rglob("*"):
        rel = path.relative_to(src)
        target = dst / rel
        if path.is_dir():
            target.mkdir(parents=True, exist_ok=True)
            continue
        target.parent.mkdir(parents=True, exist_ok=True)
        os.link(path, target)


def stage_batch(batch, stage_root: Path):
    ensure_clean_dir(stage_root)
    data_root = stage_root / "data"
    for day_dir, _, _ in batch["days"]:
        hardlink_tree(day_dir, data_root / day_dir.name)
    return stage_root


def upload_batch_with_xet(api: HfApi, repo_id: str, stage_root: Path, num_workers: int, print_report_every: int):
    os.environ.pop("HF_HUB_DISABLE_XET", None)
    os.environ.setdefault("HF_XET_CACHE", str(ROOT / ".hf_xet_cache"))
    api.upload_large_folder(
        repo_id=repo_id,
        repo_type="dataset",
        folder_path=stage_root,
        num_workers=num_workers,
        print_report=True,
        print_report_every=print_report_every,
    )


def verify_batch(api: HfApi, repo_id: str, batch):
    for day_dir, _, _ in batch["days"]:
        result = verify_day(api, repo_id, day_dir)
        print(
            f"Verified {day_dir.name}: local={result['local_count']} "
            f"remote={result['remote_count']} extra_remote={result['extra_count']}"
        )
        if not result["ok"]:
            raise RuntimeError(
                f"Verification failed for {day_dir.name}: "
                f"missing={len(result['missing'])}, mismatched={len(result['mismatched'])}"
            )


def prune_batch(batch):
    for day_dir, _, _ in batch["days"]:
        shutil.rmtree(day_dir)
        print(f"Deleted local {day_dir}")


def main():
    parser = argparse.ArgumentParser(
        description="Upload local data to HF using the Xet-backed large-folder uploader."
    )
    parser.add_argument("--repo-id", default=DEFAULT_REPO_ID)
    parser.add_argument("--data-dir", type=Path, default=DEFAULT_DATA_DIR)
    parser.add_argument("--batch-count", type=int, default=10)
    parser.add_argument(
        "--target-batch-gb",
        type=float,
        default=None,
        help="Preferred batch size in GB; overrides --batch-count when set",
    )
    parser.add_argument("--start-date", type=str, default=None)
    parser.add_argument("--end-date", type=str, default=None)
    parser.add_argument(
        "--keep-local",
        action="store_true",
        help="Verify but do not delete local day folders after upload",
    )
    parser.add_argument(
        "--completed-only",
        action="store_true",
        help="Only include day folders marked fully completed",
    )
    parser.add_argument(
        "--num-workers",
        type=int,
        default=8,
        help="Number of upload_large_folder workers to use",
    )
    parser.add_argument(
        "--print-report-every",
        type=int,
        default=30,
        help="Seconds between upload_large_folder progress reports",
    )
    parser.add_argument(
        "--refresh-cache-every-batches",
        type=int,
        default=10,
        help="Refresh the monitor HF cache every N completed batches instead of after every batch",
    )
    args = parser.parse_args()

    api = HfApi()
    run_id = time.strftime("%Y%m%d-%H%M%S", time.gmtime())
    refresh_every = max(1, args.refresh_cache_every_batches)
    stage_root = ROOT / ".xet_stage" / run_id

    with acquire_repo_lock(args.repo_id):
        day_dirs = candidate_day_dirs(args.data_dir, completed_only=args.completed_only)
        if args.start_date:
            day_dirs = [day_dir for day_dir in day_dirs if day_dir.name >= args.start_date]
        if args.end_date:
            day_dirs = [day_dir for day_dir in day_dirs if day_dir.name <= args.end_date]
        batch_count = args.batch_count
        if args.target_batch_gb is not None:
            target_batch_bytes = int(args.target_batch_gb * (1024 ** 3))
            batch_count = derive_batch_count(day_dirs, target_batch_bytes)
        batches = partition_days(day_dirs, batch_count)

        print(
            f"Prepared {len(batches)} Xet batches from {len(day_dirs)} day folders "
            f"for repo {args.repo_id}"
        )
        print(f"Upload run id: {run_id}")
        for index, batch in enumerate(batches, start=1):
            print(
                f"Batch {index}/{len(batches)} [{batch_day_label(batch)}]: {len(batch['days'])} days, "
                f"{batch['files']} files, {batch['bytes'] / 1024 / 1024 / 1024:.2f} GB"
            )

        for index, batch in enumerate(batches, start=1):
            started = time.perf_counter()
            stage_batch(batch, stage_root)
            upload_batch_with_xet(
                api,
                args.repo_id,
                stage_root,
                num_workers=args.num_workers,
                print_report_every=args.print_report_every,
            )
            verify_batch(api, args.repo_id, batch)
            if not args.keep_local:
                prune_batch(batch)
            shutil.rmtree(stage_root, ignore_errors=True)
            if index % refresh_every == 0 or index == len(batches):
                refresh_monitor_remote_cache(args.repo_id)
            elapsed = time.perf_counter() - started
            print(f"Batch {index}/{len(batches)} finished in {elapsed:.1f}s")


if __name__ == "__main__":
    main()
