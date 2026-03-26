#!/usr/bin/env python3
import argparse
import fcntl
import json
import re
import shutil
import threading
import time
from contextlib import contextmanager
from pathlib import Path

from huggingface_hub import CommitOperationAdd, HfApi
from huggingface_hub.utils import disable_progress_bars

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


def repo_lock_path(repo_id: str):
    safe_name = repo_id.replace("/", "__")
    return ROOT / "logs" / f"hf_repo_lock_{safe_name}.lock"


@contextmanager
def acquire_repo_lock(repo_id: str):
    lock_path = repo_lock_path(repo_id)
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with open(lock_path, "w") as lock_file:
        try:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise RuntimeError(
                f"Another local HF writer is already active for {repo_id}. "
                f"Lock: {lock_path}"
            ) from exc
        lock_file.write(f"{time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())}\n")
        lock_file.flush()
        try:
            yield
        finally:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)


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


def derive_batch_count(day_dirs, target_batch_bytes: int = None, target_batch_files: int = None):
    total_bytes = 0
    total_files = 0
    counted_days = 0
    for day_dir in day_dirs:
        day_files, day_bytes = day_size_bytes(day_dir)
        if day_bytes <= 0 or day_files <= 0:
            continue
        total_bytes += day_bytes
        total_files += day_files
        counted_days += 1
    if counted_days == 0:
        return 1
    batch_count = 1
    if target_batch_bytes and target_batch_bytes > 0:
        batch_count = max(batch_count, round(total_bytes / target_batch_bytes))
    if target_batch_files and target_batch_files > 0:
        batch_count = max(batch_count, round(total_files / target_batch_files))
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


def commit_retry_delay(exc: Exception, attempt: int):
    response = getattr(exc, "response", None)
    status_code = getattr(response, "status_code", None)
    if status_code == 429:
        return min(1800, 60 * attempt)
    if status_code in {500, 502, 503, 504}:
        return min(300, 15 * attempt)
    return min(30, 1.5 * (2 ** (attempt - 1)))


def run_with_heartbeat(action, description: str, interval_seconds: float):
    if interval_seconds <= 0:
        return action()

    started = time.perf_counter()
    stop_event = threading.Event()

    def heartbeat():
        while not stop_event.wait(interval_seconds):
            elapsed = time.perf_counter() - started
            print(f"{description} still in progress after {elapsed:.1f}s...")

    reporter = threading.Thread(target=heartbeat, daemon=True)
    reporter.start()
    try:
        return action()
    finally:
        stop_event.set()
        reporter.join(timeout=1)


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


def record_skipped_upload(
    repo_path: str,
    quarantined_path: Path,
    reason: str,
    *,
    exception_type: str = None,
    exception_message: str = None,
    batch_label: str = None,
    batch_index: int = None,
    batch_count: int = None,
    run_id: str = None,
):
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
            "exception_type": exception_type,
            "exception_message": exception_message,
            "batch_label": batch_label,
            "batch_index": batch_index,
            "batch_count": batch_count,
            "run_id": run_id,
            "recorded_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        }
    )
    SKIPPED_UPLOADS_LOG.write_text(json.dumps(payload, indent=2) + "\n")


def quarantine_failed_local_file(
    data_dir: Path,
    repo_path: str,
    reason: str,
    *,
    exception: Exception = None,
    batch_label: str = None,
    batch_index: int = None,
    batch_count: int = None,
    run_id: str = None,
):
    if not repo_path.startswith("data/"):
        return None
    relative_path = Path(repo_path[len("data/") :])
    local_path = data_dir / relative_path
    if not local_path.exists():
        return None
    quarantined_path = SKIPPED_UPLOADS_ROOT / relative_path
    quarantined_path.parent.mkdir(parents=True, exist_ok=True)
    shutil.move(str(local_path), str(quarantined_path))
    exception_type = type(exception).__name__ if exception is not None else None
    exception_message = str(exception) if exception is not None else None
    record_skipped_upload(
        repo_path,
        quarantined_path,
        reason,
        exception_type=exception_type,
        exception_message=exception_message,
        batch_label=batch_label,
        batch_index=batch_index,
        batch_count=batch_count,
        run_id=run_id,
    )
    print(
        f"Skipped failing upload file {repo_path} -> {quarantined_path}. "
        f"batch={batch_index}/{batch_count} [{batch_label}] run={run_id} "
        f"exception_type={exception_type} exception_message={exception_message}"
    )
    return quarantined_path


def batch_day_label(batch):
    day_names = sorted(day_dir.name for day_dir, _, _ in batch["days"])
    if not day_names:
        return "empty"
    if len(day_names) == 1:
        return day_names[0]
    return f"{day_names[0]}..{day_names[-1]}"


def batch_commit_message(run_id: str, batch_index, batch, batch_count):
    day_count = len(batch["days"])
    day_word = "day" if day_count == 1 else "days"
    return (
        f"Bulk sync SF Superior Court {batch_day_label(batch)} "
        f"run {run_id} batch {batch_index}/{batch_count} ({day_count} {day_word})"
    )


def find_recent_commit(api, repo_id, commit_message: str, attempts: int = 4):
    for attempt in range(1, attempts + 1):
        try:
            commits = api.list_repo_commits(repo_id=repo_id, repo_type="dataset")
            for commit in commits[:50]:
                if commit.title == commit_message or commit.message == commit_message:
                    return commit
            return None
        except Exception as exc:
            if not is_hf_commit_conflict(exc) or attempt == attempts:
                raise
            delay = commit_retry_delay(exc, attempt)
            print(
                f"HF commit history lookup failed for '{commit_message}' "
                f"(attempt {attempt}/{attempts}). Retrying in {delay:.1f}s..."
            )
            time.sleep(delay)
    return None


def upload_batch(
    api,
    repo_id,
    data_dir: Path,
    batch_index,
    batch,
    batch_count,
    run_id: str,
    heartbeat_seconds: float,
):
    message = batch_commit_message(run_id, batch_index, batch, batch_count)
    quarantined_repo_paths = set()
    upload_description = (
        f"Uploading batch {batch_index}/{batch_count} [{batch_day_label(batch)}]"
    )
    while True:
        operations = build_operations(batch)
        if not operations:
            return
        for attempt in range(1, 9):
            try:
                commit_info = run_with_heartbeat(
                    lambda: api.create_commit(
                        repo_id=repo_id,
                        repo_type="dataset",
                        operations=operations,
                        commit_message=message,
                    ),
                    upload_description,
                    heartbeat_seconds,
                )
                return {
                    "verified_via": "create_commit",
                    "commit_message": message,
                    "commit_id": getattr(commit_info, "oid", None),
                }
            except RuntimeError as exc:
                failed_repo_path = extract_failed_repo_path(exc)
                if not failed_repo_path or failed_repo_path in quarantined_repo_paths:
                    raise
                quarantined_path = quarantine_failed_local_file(
                    data_dir,
                    failed_repo_path,
                    str(exc),
                    exception=exc,
                    batch_label=batch_day_label(batch),
                    batch_index=batch_index,
                    batch_count=batch_count,
                    run_id=run_id,
                )
                if not quarantined_path:
                    raise
                quarantined_repo_paths.add(failed_repo_path)
                break
            except Exception as exc:
                if not is_hf_commit_conflict(exc):
                    raise
                matching_commit = find_recent_commit(api, repo_id, message)
                if matching_commit is not None:
                    print(
                        f"Commit history already contains batch {batch_index}/{batch_count} "
                        f"[{batch_day_label(batch)}] after ambiguous upload; "
                        f"treating commit as successful. commit_id={matching_commit.commit_id}"
                    )
                    return {
                        "verified_via": "commit_history",
                        "commit_message": message,
                        "commit_id": matching_commit.commit_id,
                    }
                if attempt == 8:
                    raise
                delay = commit_retry_delay(exc, attempt)
                print(
                    f"HF commit conflict during bulk batch upload {batch_day_label(batch)} "
                    f"run {run_id} batch {batch_index}/{batch_count} "
                    f"(attempt {attempt}/8). Retrying in {delay:.1f}s..."
                )
                time.sleep(delay)


def prune_batch(batch):
    for day_dir, _, _ in batch["days"]:
        shutil.rmtree(day_dir)
        print(f"Deleted local {day_dir}")


def verify_batch_exact(api, repo_id, batch):
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
    parser.add_argument(
        "--target-batch-files",
        type=int,
        default=None,
        help="Preferred batch size in file count; increases batch count when average files per batch would exceed this",
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
        "--refresh-cache-every-batches",
        type=int,
        default=10,
        help="Refresh the monitor HF cache every N completed batches instead of after every batch",
    )
    parser.add_argument(
        "--upload-heartbeat-seconds",
        type=float,
        default=30.0,
        help="Print a heartbeat while an HF batch commit is in progress",
    )
    args = parser.parse_args()

    disable_progress_bars()
    api = HfApi()
    run_id = time.strftime("%Y%m%d-%H%M%S", time.gmtime())
    refresh_every = max(1, args.refresh_cache_every_batches)

    with acquire_repo_lock(args.repo_id):
        day_dirs = candidate_day_dirs(args.data_dir, completed_only=args.completed_only)
        if args.start_date:
            day_dirs = [day_dir for day_dir in day_dirs if day_dir.name >= args.start_date]
        if args.end_date:
            day_dirs = [day_dir for day_dir in day_dirs if day_dir.name <= args.end_date]
        batch_count = args.batch_count
        if args.target_batch_gb is not None or args.target_batch_files is not None:
            target_batch_bytes = (
                int(args.target_batch_gb * (1024 ** 3))
                if args.target_batch_gb is not None
                else None
            )
            batch_count = derive_batch_count(
                day_dirs,
                target_batch_bytes=target_batch_bytes,
                target_batch_files=args.target_batch_files,
            )
        batches = partition_days(day_dirs, batch_count)

        print(
            f"Prepared {len(batches)} batches from {len(day_dirs)} day folders "
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
            print(
                f"Starting batch {index}/{len(batches)} [{batch_day_label(batch)}]: "
                f"{len(batch['days'])} days, {batch['files']} files, "
                f"{batch['bytes'] / 1024 / 1024 / 1024:.2f} GB"
            )
            batch_result = upload_batch(
                api,
                args.repo_id,
                args.data_dir,
                index,
                batch,
                len(batches),
                run_id,
                args.upload_heartbeat_seconds,
            )
            print(
                f"Verified batch {index}/{len(batches)} [{batch_day_label(batch)}] "
                f"via {batch_result['verified_via']}. commit_id={batch_result['commit_id']}"
            )
            if index == 1:
                verify_batch_exact(api, args.repo_id, batch)
            if not args.keep_local:
                prune_batch(batch)
            if index % refresh_every == 0 or index == len(batches):
                refresh_monitor_remote_cache(args.repo_id)
            elapsed = time.perf_counter() - started
            print(f"Batch {index}/{len(batches)} finished in {elapsed:.1f}s")


if __name__ == "__main__":
    main()
