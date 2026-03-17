#!/usr/bin/env python3
import argparse
import json
import re
import shutil
import time
from pathlib import Path

from huggingface_hub import CommitOperationAdd, HfApi

from sync_existing_to_hf_and_prune import (
    DEFAULT_DATA_DIR,
    DEFAULT_REPO_ID,
    candidate_day_dirs,
    is_hf_commit_conflict,
    refresh_monitor_remote_cache,
    verify_day,
)


ROOT = Path(__file__).resolve().parent
SKIPPED_UPLOADS_ROOT = ROOT / "skipped_uploads"
SKIPPED_UPLOADS_LOG = ROOT / "logs" / "hf_skipped_uploads.json"
FAILED_UPLOAD_PATH_RE = re.compile(r"Error while uploading '([^']+)' to the Hub\.")


def day_size_bytes(day_dir: Path):
    total = 0
    files = 0
    for path in day_dir.rglob("*"):
        if path.is_file():
            files += 1
            total += path.stat().st_size
    return files, total


def partition_days(day_dirs, batch_count):
    sized = []
    for day_dir in day_dirs:
        files, total_bytes = day_size_bytes(day_dir)
        if files == 0:
            continue
        sized.append((day_dir, files, total_bytes))

    # Greedy bin packing by bytes.
    sized.sort(key=lambda item: item[2], reverse=True)
    batches = [
        {"days": [], "bytes": 0, "files": 0}
        for _ in range(batch_count)
    ]
    for day_dir, files, total_bytes in sized:
        batch = min(batches, key=lambda item: item["bytes"])
        batch["days"].append((day_dir, files, total_bytes))
        batch["bytes"] += total_bytes
        batch["files"] += files
    return [batch for batch in batches if batch["days"]]


def derive_batch_count(day_dirs, target_batch_bytes: int):
    total_bytes = 0
    counted_days = 0
    for day_dir in day_dirs:
        _, day_bytes = day_size_bytes(day_dir)
        if day_bytes <= 0:
            continue
        total_bytes += day_bytes
        counted_days += 1
    if counted_days == 0 or target_batch_bytes <= 0:
        return 1
    batch_count = max(1, round(total_bytes / target_batch_bytes))
    return min(batch_count, counted_days)


def run_hf_commit_with_retry(action, description: str, max_attempts: int = 8):
    for attempt in range(1, max_attempts + 1):
        try:
            return action()
        except Exception as exc:
            if not is_hf_commit_conflict(exc) or attempt == max_attempts:
                raise
            response = getattr(exc, "response", None)
            status_code = getattr(response, "status_code", None)
            if status_code == 429:
                delay = min(1800, 60 * attempt)
            elif status_code in {502, 503, 504}:
                delay = min(300, 15 * attempt)
            else:
                delay = min(30, 1.5 * (2 ** (attempt - 1)))
            print(
                f"HF commit conflict during {description} "
                f"(attempt {attempt}/{max_attempts}). Retrying in {delay:.1f}s..."
            )
            time.sleep(delay)


def build_operations(batch):
    operations = []
    for day_dir, _, _ in batch["days"]:
        for path in sorted(day_dir.rglob("*")):
            if not path.is_file():
                continue
            rel = path.relative_to(day_dir).as_posix()
            operations.append(
                CommitOperationAdd(
                    path_in_repo=f"data/{day_dir.name}/{rel}",
                    path_or_fileobj=str(path),
                )
            )
    return operations


def extract_failed_repo_path(exc: Exception):
    match = FAILED_UPLOAD_PATH_RE.search(str(exc))
    if not match:
        return None
    return match.group(1)


def record_skipped_upload(repo_path: str, quarantined_path: Path, reason: str):
    SKIPPED_UPLOADS_LOG.parent.mkdir(parents=True, exist_ok=True)
    try:
        payload = json.loads(SKIPPED_UPLOADS_LOG.read_text())
        if not isinstance(payload, list):
            payload = []
    except Exception:
        payload = []
    payload.append(
        {
            "repo_path": repo_path,
            "quarantined_path": str(quarantined_path),
            "reason": reason,
            "recorded_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        }
    )
    SKIPPED_UPLOADS_LOG.write_text(json.dumps(payload, indent=2) + "\n")


def quarantine_failed_local_file(data_dir: Path, repo_path: str, reason: str):
    if not repo_path.startswith("data/"):
        return None
    relative_path = Path(repo_path[len("data/") :])
    local_path = data_dir / relative_path
    if not local_path.exists():
        return None
    quarantined_path = SKIPPED_UPLOADS_ROOT / relative_path
    quarantined_path.parent.mkdir(parents=True, exist_ok=True)
    shutil.move(str(local_path), str(quarantined_path))
    record_skipped_upload(repo_path, quarantined_path, reason)
    print(f"Skipped failing upload file {repo_path} -> {quarantined_path}")
    return quarantined_path


def upload_batch(api, repo_id, data_dir: Path, batch_index, batch, batch_count):
    message = (
        f"Bulk sync SF Superior Court batch {batch_index}/{batch_count} "
        f"({len(batch['days'])} days)"
    )
    quarantined_repo_paths = set()
    while True:
        operations = build_operations(batch)
        if not operations:
            return
        try:
            run_hf_commit_with_retry(
                lambda: api.create_commit(
                    repo_id=repo_id,
                    repo_type="dataset",
                    operations=operations,
                    commit_message=message,
                ),
                f"bulk batch upload {batch_index}/{batch_count}",
            )
            return
        except RuntimeError as exc:
            failed_repo_path = extract_failed_repo_path(exc)
            if not failed_repo_path or failed_repo_path in quarantined_repo_paths:
                raise
            quarantined_path = quarantine_failed_local_file(data_dir, failed_repo_path, str(exc))
            if not quarantined_path:
                raise
            quarantined_repo_paths.add(failed_repo_path)


def verify_batch(api, repo_id, batch):
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
        description="Upload the current local data tree to HF in a fixed number of bulk commits"
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
    args = parser.parse_args()

    api = HfApi()
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
        f"Prepared {len(batches)} batches from {len(day_dirs)} day folders "
        f"for repo {args.repo_id}"
    )
    for index, batch in enumerate(batches, start=1):
        print(
            f"Batch {index}/{len(batches)}: {len(batch['days'])} days, "
            f"{batch['files']} files, {batch['bytes'] / 1024 / 1024 / 1024:.2f} GB"
        )

    for index, batch in enumerate(batches, start=1):
        started = time.perf_counter()
        upload_batch(api, args.repo_id, args.data_dir, index, batch, len(batches))
        verify_batch(api, args.repo_id, batch)
        if not args.keep_local:
            prune_batch(batch)
        refresh_monitor_remote_cache(args.repo_id)
        elapsed = time.perf_counter() - started
        print(f"Batch {index}/{len(batches)} finished in {elapsed:.1f}s")


if __name__ == "__main__":
    main()
