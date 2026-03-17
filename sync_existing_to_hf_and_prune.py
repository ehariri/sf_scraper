import argparse
import json
import shutil
import time
from pathlib import Path

from huggingface_hub import CommitOperationAdd, HfApi
from huggingface_hub.errors import EntryNotFoundError, HfHubHTTPError

from build_hf_dataset_card import build_card
from hf_remote_state import refresh_remote_state_cache


DEFAULT_REPO_ID = "please-the-bot/sf_superior_court"
DEFAULT_DATA_DIR = Path("data")
SYNC_METADATA_FILENAME = "sync_metadata.json"
DATASET_CARD_FILENAME = "HF_DATASET_CARD.md"
HF_COMMIT_MAX_ATTEMPTS = 8
DATASET_ROOT_FILENAMES = {"day_summary.json", "failed_cases.json"}


def utc_now_iso():
    from datetime import datetime

    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def is_hf_commit_conflict(exc: Exception):
    if not isinstance(exc, HfHubHTTPError):
        return False
    response = getattr(exc, "response", None)
    status_code = getattr(response, "status_code", None)
    if status_code in {412, 429}:
        return True
    text = str(exc).lower()
    return (
        "precondition failed" in text
        or "a commit has happened since" in text
        or "too many requests" in text
        or "rate limit" in text
    )


def run_hf_commit_with_retry(action, description: str, max_attempts: int = HF_COMMIT_MAX_ATTEMPTS):
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
            else:
                delay = min(30, 1.5 * (2 ** (attempt - 1)))
            print(
                f"HF commit conflict during {description} "
                f"(attempt {attempt}/{max_attempts}). Retrying in {delay:.1f}s..."
            )
            time.sleep(delay)


def load_day_summary(day_dir: Path):
    summary_path = day_dir / "day_summary.json"
    if not summary_path.exists():
        return None

    try:
        with open(summary_path, "r") as f:
            return json.load(f)
    except Exception:
        return None


def load_sync_metadata(day_dir: Path):
    sync_path = day_dir / SYNC_METADATA_FILENAME
    if not sync_path.exists():
        return {
            "date": day_dir.name,
            "updated_at": None,
            "runs": [],
            "cases": {},
            "root_files": {},
        }

    try:
        with open(sync_path, "r") as f:
            return json.load(f)
    except Exception:
        return {
            "date": day_dir.name,
            "updated_at": None,
            "runs": [],
            "cases": {},
            "root_files": {},
        }


def save_sync_metadata(day_dir: Path, payload):
    payload["updated_at"] = utc_now_iso()
    with open(day_dir / SYNC_METADATA_FILENAME, "w") as f:
        json.dump(payload, f, indent=2)


def local_files_for_day(day_dir: Path):
    files = {}
    for path in sorted(day_dir.rglob("*")):
        if path.is_file():
            rel = path.relative_to(day_dir).as_posix()
            files[rel] = path.stat().st_size
    return files


def local_files_for_path(path_root: Path):
    files = {}
    for path in sorted(path_root.rglob("*")):
        if path.is_file():
            rel = path.relative_to(path_root).as_posix()
            files[rel] = path.stat().st_size
    return files


def remote_files_for_prefix(api: HfApi, repo_id: str, prefix: str):
    files = {}
    try:
        for entry in api.list_repo_tree(
            repo_id=repo_id,
            repo_type="dataset",
            path_in_repo=prefix,
            recursive=True,
            expand=True,
        ):
            # `list_repo_tree` yields RepoFolder and RepoFile objects.
            # RepoFile carries a `size`; RepoFolder does not.
            if hasattr(entry, "size"):
                rel = entry.path[len(prefix) + 1 :]
                files[rel] = entry.size
    except EntryNotFoundError:
        return {}
    return files


def verify_day(api: HfApi, repo_id: str, day_dir: Path):
    local = local_files_for_day(day_dir)
    remote = remote_files_for_prefix(api, repo_id, f"data/{day_dir.name}")

    missing = sorted(set(local) - set(remote))
    mismatched = sorted(
        rel for rel, size in local.items() if rel in remote and remote[rel] != size
    )
    extra = sorted(set(remote) - set(local))

    return {
        "ok": not missing and not mismatched,
        "missing": missing,
        "mismatched": mismatched,
        "extra_count": len(extra),
        "local_count": len(local),
        "remote_count": len(remote),
    }


def verify_case_dir(api: HfApi, repo_id: str, day_name: str, case_dir: Path):
    local = local_files_for_path(case_dir)
    remote = remote_files_for_prefix(api, repo_id, f"data/{day_name}/{case_dir.name}")

    missing = sorted(set(local) - set(remote))
    mismatched = sorted(
        rel for rel, size in local.items() if rel in remote and remote[rel] != size
    )
    extra = sorted(set(remote) - set(local))

    return {
        "ok": not missing and not mismatched,
        "missing": missing,
        "mismatched": mismatched,
        "extra_count": len(extra),
        "local_count": len(local),
        "remote_count": len(remote),
    }


def remote_files_for_case_from_day_remote(day_remote, case_name: str):
    prefix = f"{case_name}/"
    files = {}
    for rel, size in day_remote.items():
        if rel.startswith(prefix):
            files[rel[len(prefix) :]] = size
    return files


def verify_case_dir_against_remote_map(case_dir: Path, remote_files: dict):
    local = local_files_for_path(case_dir)

    missing = sorted(set(local) - set(remote_files))
    mismatched = sorted(
        rel for rel, size in local.items() if rel in remote_files and remote_files[rel] != size
    )
    extra = sorted(set(remote_files) - set(local))

    return {
        "ok": not missing and not mismatched,
        "missing": missing,
        "mismatched": mismatched,
        "extra_count": len(extra),
        "local_count": len(local),
        "remote_count": len(remote_files),
    }


def verify_root_files_against_remote_map(day_dir: Path, filenames, day_remote: dict):
    local = {}
    remote = {}
    for filename in filenames:
        path = day_dir / filename
        if path.exists():
            local[filename] = path.stat().st_size
        if filename in day_remote:
            remote[filename] = day_remote[filename]

    missing = sorted(set(local) - set(remote))
    mismatched = sorted(
        rel for rel, size in local.items() if rel in remote and remote[rel] != size
    )
    extra = sorted(set(remote) - set(local))

    return {
        "ok": not missing and not mismatched,
        "missing": missing,
        "mismatched": mismatched,
        "extra_count": len(extra),
        "local_count": len(local),
        "remote_count": len(remote),
    }


def verify_root_files(api: HfApi, repo_id: str, day_dir: Path, filenames):
    local = {}
    remote = {}
    prefix = f"data/{day_dir.name}"
    for filename in filenames:
        path = day_dir / filename
        if path.exists():
            local[filename] = path.stat().st_size
    remote_tree = remote_files_for_prefix(api, repo_id, prefix)
    for filename in filenames:
        if filename in remote_tree:
            remote[filename] = remote_tree[filename]

    missing = sorted(set(local) - set(remote))
    mismatched = sorted(
        rel for rel, size in local.items() if rel in remote and remote[rel] != size
    )
    extra = sorted(set(remote) - set(local))

    return {
        "ok": not missing and not mismatched,
        "missing": missing,
        "mismatched": mismatched,
        "extra_count": len(extra),
        "local_count": len(local),
        "remote_count": len(remote),
    }


def upload_day(api: HfApi, repo_id: str, day_dir: Path):
    run_hf_commit_with_retry(
        lambda: api.upload_folder(
            repo_id=repo_id,
            repo_type="dataset",
            folder_path=day_dir,
            path_in_repo=f"data/{day_dir.name}",
            commit_message=f"Upload SF Superior Court day {day_dir.name}",
        ),
        f"day upload {day_dir.name}",
    )


def upload_case_dir(api: HfApi, repo_id: str, day_name: str, case_dir: Path):
    run_hf_commit_with_retry(
        lambda: api.upload_folder(
            repo_id=repo_id,
            repo_type="dataset",
            folder_path=case_dir,
            path_in_repo=f"data/{day_name}/{case_dir.name}",
            commit_message=f"Upload SF Superior Court case {day_name}/{case_dir.name}",
        ),
        f"case upload {day_name}/{case_dir.name}",
    )


def upload_case_batch(api: HfApi, repo_id: str, day_name: str, case_dirs, root_filenames=None):
    operations = []
    total_bytes = 0

    for case_dir in case_dirs:
        for path in sorted(case_dir.rglob("*")):
            if not path.is_file():
                continue
            rel = path.relative_to(case_dir).as_posix()
            operations.append(
                CommitOperationAdd(
                    path_in_repo=f"data/{day_name}/{case_dir.name}/{rel}",
                    path_or_fileobj=str(path),
                )
            )
            total_bytes += path.stat().st_size

    for filename in sorted(root_filenames or []):
        path = case_dirs[0].parent / filename
        if not path.exists():
            continue
        operations.append(
            CommitOperationAdd(
                path_in_repo=f"data/{day_name}/{filename}",
                path_or_fileobj=str(path),
            )
        )
        total_bytes += path.stat().st_size

    if not operations:
        return 0

    run_hf_commit_with_retry(
        lambda: api.create_commit(
            repo_id=repo_id,
            repo_type="dataset",
            operations=operations,
            commit_message=(
                f"Upload SF Superior Court cases {day_name} "
                f"({len(case_dirs)} cases)"
            ),
        ),
        f"case batch upload {day_name} ({len(case_dirs)} cases)",
    )
    return total_bytes


def upload_day_root_files(api: HfApi, repo_id: str, day_dir: Path, filenames):
    temp_root = day_dir / ".hf_root_upload"
    if temp_root.exists():
        shutil.rmtree(temp_root)
    temp_root.mkdir()

    try:
        for filename in filenames:
            source = day_dir / filename
            if source.exists():
                shutil.copy2(source, temp_root / filename)
        if not any(temp_root.iterdir()):
            return
        run_hf_commit_with_retry(
            lambda: api.upload_folder(
                repo_id=repo_id,
                repo_type="dataset",
                folder_path=temp_root,
                path_in_repo=f"data/{day_dir.name}",
                commit_message=f"Upload SF Superior Court day metadata {day_dir.name}",
            ),
            f"day metadata upload {day_dir.name}",
        )
    finally:
        shutil.rmtree(temp_root, ignore_errors=True)


def upload_dataset_card(api: HfApi, repo_id: str, data_dir: Path):
    card_path = Path(__file__).resolve().parent / DATASET_CARD_FILENAME
    card_path.write_text(build_card(data_dir, repo_id))
    run_hf_commit_with_retry(
        lambda: api.upload_file(
            path_or_fileobj=str(card_path),
            path_in_repo="README.md",
            repo_id=repo_id,
            repo_type="dataset",
            commit_message="Refresh dataset card after sync",
        ),
        "dataset card refresh",
    )


def refresh_monitor_remote_cache(repo_id: str):
    result = refresh_remote_state_cache(repo_id=repo_id)
    state = result["state"]
    print(
        f"Refreshed HF remote cache: changed={result['changed']} "
        f"days={state.get('day_count', 0)}"
    )


def candidate_day_dirs(data_dir: Path, explicit_days=None, completed_only=False):
    if explicit_days:
        day_dirs = [data_dir / day for day in explicit_days]
    else:
        day_dirs = sorted([p for p in data_dir.iterdir() if p.is_dir()])

    filtered = []
    for day_dir in day_dirs:
        if not day_dir.exists():
            raise FileNotFoundError(day_dir)

        if completed_only:
            summary = load_day_summary(day_dir)
            if not summary or not summary.get("fully_completed"):
                continue

        filtered.append(day_dir)

    return filtered


def append_run(sync_metadata, payload):
    runs = sync_metadata.setdefault("runs", [])
    runs.append(payload)
    if len(runs) > 25:
        del runs[:-25]


def record_case_sync(sync_metadata, case_name, payload):
    sync_metadata.setdefault("cases", {})[case_name] = payload


def record_root_sync(sync_metadata, filename, payload):
    sync_metadata.setdefault("root_files", {})[filename] = payload


def main():
    parser = argparse.ArgumentParser(
        description="Upload local SF Superior Court day folders to HF and prune verified local copies"
    )
    parser.add_argument("--repo-id", default=DEFAULT_REPO_ID, help="HF dataset repo id")
    parser.add_argument("--data-dir", type=Path, default=DEFAULT_DATA_DIR)
    parser.add_argument(
        "--days",
        nargs="*",
        default=None,
        help="Specific day folders to process; default is all day directories in data-dir",
    )
    parser.add_argument(
        "--keep-local",
        action="store_true",
        help="Upload and verify but do not delete the local day directory",
    )
    parser.add_argument(
        "--unit",
        choices=["day", "case"],
        default="case",
        help="Upload whole days or smaller per-case directories",
    )
    parser.add_argument(
        "--completed-only",
        action="store_true",
        help="Only process day folders whose day_summary.json marks them fully completed",
    )
    parser.add_argument(
        "--loop-seconds",
        type=int,
        default=0,
        help="Repeat scans every N seconds; 0 runs a single pass",
    )
    args = parser.parse_args()

    api = HfApi()
    while True:
        loop_started_at = utc_now_iso()
        loop_started_perf = time.perf_counter()
        loop_had_changes = False
        day_dirs = candidate_day_dirs(
            args.data_dir,
            explicit_days=args.days,
            completed_only=args.completed_only,
        )
        if not day_dirs:
            print("No eligible day folders found.")
        for day_dir in day_dirs:
            sync_metadata = load_sync_metadata(day_dir)
            day_remote = None
            if args.unit == "day":
                local = local_files_for_day(day_dir)
                upload_started_at = utc_now_iso()
                upload_started_perf = time.perf_counter()
                print(
                    f"Uploading {day_dir.name}: {len(local)} files, "
                    f"{sum(local.values()) / 1024 / 1024:.1f} MB"
                )
                upload_day(api, args.repo_id, day_dir)

                verify_started_perf = time.perf_counter()
                result = verify_day(api, args.repo_id, day_dir)
                verify_elapsed = round(time.perf_counter() - verify_started_perf, 3)
                upload_elapsed = round(
                    time.perf_counter() - upload_started_perf - verify_elapsed, 3
                )
                print(
                    f"Verified {day_dir.name}: local={result['local_count']} "
                    f"remote={result['remote_count']} extra_remote={result['extra_count']}"
                )
                if not result["ok"]:
                    raise RuntimeError(
                        f"Verification failed for {day_dir.name}: "
                        f"missing={len(result['missing'])}, mismatched={len(result['mismatched'])}"
                    )

                append_run(
                    sync_metadata,
                    {
                        "kind": "day",
                        "started_at": upload_started_at,
                        "finished_at": utc_now_iso(),
                        "upload_elapsed_seconds": upload_elapsed,
                        "verify_elapsed_seconds": verify_elapsed,
                        "file_count": len(local),
                        "total_bytes": sum(local.values()),
                        "verified_local_count": result["local_count"],
                        "verified_remote_count": result["remote_count"],
                    },
                )
                save_sync_metadata(day_dir, sync_metadata)

                if not args.keep_local:
                    shutil.rmtree(day_dir)
                    print(f"Deleted local {day_dir}")
                loop_had_changes = True
                continue

            case_dirs = sorted([p for p in day_dir.iterdir() if p.is_dir()])
            dataset_root_files = [
                path.name
                for path in sorted(day_dir.iterdir())
                if path.is_file() and path.name in DATASET_ROOT_FILENAMES
            ]
            if case_dirs:
                day_remote = remote_files_for_prefix(api, args.repo_id, f"data/{day_dir.name}")
            pending_case_dirs = []

            for case_dir in case_dirs:
                local = local_files_for_path(case_dir)
                existing_remote = remote_files_for_case_from_day_remote(day_remote or {}, case_dir.name)
                existing = verify_case_dir_against_remote_map(case_dir, existing_remote)
                if existing["ok"]:
                    print(
                        f"Already mirrored {day_dir.name}/{case_dir.name}: "
                        f"local={existing['local_count']} remote={existing['remote_count']}"
                    )
                    record_case_sync(
                        sync_metadata,
                        case_dir.name,
                        {
                            "started_at": utc_now_iso(),
                            "finished_at": utc_now_iso(),
                            "upload_elapsed_seconds": 0.0,
                            "verify_elapsed_seconds": 0.0,
                            "file_count": len(local),
                            "total_bytes": sum(local.values()),
                            "verified_local_count": existing["local_count"],
                            "verified_remote_count": existing["remote_count"],
                            "pruned_local": not args.keep_local,
                            "upload_skipped": True,
                        },
                    )
                    save_sync_metadata(day_dir, sync_metadata)
                    if not args.keep_local:
                        shutil.rmtree(case_dir)
                        print(f"Deleted local {case_dir}")
                    loop_had_changes = True
                    continue
                pending_case_dirs.append(case_dir)

            if pending_case_dirs:
                batch_started_at = utc_now_iso()
                batch_started_perf = time.perf_counter()
                pending_local = {
                    case_dir.name: local_files_for_path(case_dir) for case_dir in pending_case_dirs
                }
                batch_file_count = sum(len(files) for files in pending_local.values())
                batch_total_bytes = sum(
                    sum(files.values()) for files in pending_local.values()
                )
                print(
                    f"Uploading {day_dir.name}: {len(pending_case_dirs)} cases, "
                    f"{batch_file_count} files, {batch_total_bytes / 1024 / 1024:.1f} MB"
                )
                upload_case_batch(
                    api,
                    args.repo_id,
                    day_dir.name,
                    pending_case_dirs,
                    root_filenames=dataset_root_files,
                )
                verify_started_perf = time.perf_counter()
                day_remote = remote_files_for_prefix(api, args.repo_id, f"data/{day_dir.name}")
                verify_elapsed = round(time.perf_counter() - verify_started_perf, 3)
                upload_elapsed = round(
                    time.perf_counter() - batch_started_perf - verify_elapsed, 3
                )

                for case_dir in pending_case_dirs:
                    local = pending_local[case_dir.name]
                    updated_remote = remote_files_for_case_from_day_remote(day_remote, case_dir.name)
                    result = verify_case_dir_against_remote_map(case_dir, updated_remote)
                    print(
                        f"Verified {day_dir.name}/{case_dir.name}: local={result['local_count']} "
                        f"remote={result['remote_count']} extra_remote={result['extra_count']}"
                    )
                    if not result["ok"]:
                        raise RuntimeError(
                            f"Verification failed for {day_dir.name}/{case_dir.name}: "
                            f"missing={len(result['missing'])}, mismatched={len(result['mismatched'])}"
                        )
                    record_case_sync(
                        sync_metadata,
                        case_dir.name,
                        {
                            "started_at": batch_started_at,
                            "finished_at": utc_now_iso(),
                            "upload_elapsed_seconds": upload_elapsed,
                            "verify_elapsed_seconds": verify_elapsed,
                            "file_count": len(local),
                            "total_bytes": sum(local.values()),
                            "verified_local_count": result["local_count"],
                            "verified_remote_count": result["remote_count"],
                            "pruned_local": not args.keep_local,
                            "batch_case_count": len(pending_case_dirs),
                        },
                    )
                    save_sync_metadata(day_dir, sync_metadata)
                    if not args.keep_local:
                        shutil.rmtree(case_dir)
                        print(f"Deleted local {case_dir}")
                    loop_had_changes = True

                if dataset_root_files:
                    root_result = verify_root_files_against_remote_map(
                        day_dir, dataset_root_files, day_remote
                    )
                    print(
                        f"Verified day metadata {day_dir.name}: local={root_result['local_count']} "
                        f"remote={root_result['remote_count']} extra_remote={root_result['extra_count']}"
                    )
                    if not root_result["ok"]:
                        raise RuntimeError(
                            f"Verification failed for day metadata {day_dir.name}: "
                            f"missing={len(root_result['missing'])}, mismatched={len(root_result['mismatched'])}"
                        )
                    for filename in dataset_root_files:
                        local_path = day_dir / filename
                        if local_path.exists():
                            record_root_sync(
                                sync_metadata,
                                filename,
                                {
                                    "started_at": batch_started_at,
                                    "finished_at": utc_now_iso(),
                                    "upload_elapsed_seconds": upload_elapsed,
                                    "verify_elapsed_seconds": verify_elapsed,
                                    "size": local_path.stat().st_size,
                                    "pruned_local": not args.keep_local,
                                    "batched_with_cases": True,
                                },
                            )
                    save_sync_metadata(day_dir, sync_metadata)
                    if not args.keep_local:
                        for filename in dataset_root_files:
                            (day_dir / filename).unlink(missing_ok=True)
                    loop_had_changes = True

            root_files = [
                path.name
                for path in sorted(day_dir.iterdir())
                if path.is_file()
                and path.name in DATASET_ROOT_FILENAMES
            ]
            if root_files:
                if day_remote is None:
                    day_remote = remote_files_for_prefix(api, args.repo_id, f"data/{day_dir.name}")
                existing = verify_root_files_against_remote_map(day_dir, root_files, day_remote)
                if existing["ok"]:
                    print(
                        f"Already mirrored day metadata {day_dir.name}: "
                        f"local={existing['local_count']} remote={existing['remote_count']}"
                    )
                    timestamp = utc_now_iso()
                    for filename in root_files:
                        local_path = day_dir / filename
                        if local_path.exists():
                            record_root_sync(
                                sync_metadata,
                                filename,
                                {
                                    "started_at": timestamp,
                                    "finished_at": timestamp,
                                    "upload_elapsed_seconds": 0.0,
                                    "verify_elapsed_seconds": 0.0,
                                    "size": local_path.stat().st_size,
                                    "pruned_local": not args.keep_local,
                                    "upload_skipped": True,
                                },
                            )
                    save_sync_metadata(day_dir, sync_metadata)
                    if not args.keep_local:
                        for filename in root_files:
                            (day_dir / filename).unlink(missing_ok=True)
                    loop_had_changes = True
                else:
                    upload_started_at = utc_now_iso()
                    upload_started_perf = time.perf_counter()
                    print(f"Uploading day metadata {day_dir.name}: {', '.join(root_files)}")
                    upload_day_root_files(api, args.repo_id, day_dir, root_files)
                    verify_started_perf = time.perf_counter()
                    day_remote = remote_files_for_prefix(api, args.repo_id, f"data/{day_dir.name}")
                    result = verify_root_files_against_remote_map(day_dir, root_files, day_remote)
                    verify_elapsed = round(time.perf_counter() - verify_started_perf, 3)
                    upload_elapsed = round(
                        time.perf_counter() - upload_started_perf - verify_elapsed, 3
                    )
                    print(
                        f"Verified day metadata {day_dir.name}: local={result['local_count']} "
                        f"remote={result['remote_count']} extra_remote={result['extra_count']}"
                    )
                    if not result["ok"]:
                        raise RuntimeError(
                            f"Verification failed for day metadata {day_dir.name}: "
                            f"missing={len(result['missing'])}, mismatched={len(result['mismatched'])}"
                        )
                    for filename in root_files:
                        local_path = day_dir / filename
                        if local_path.exists():
                            record_root_sync(
                                sync_metadata,
                                filename,
                                {
                                    "started_at": upload_started_at,
                                    "finished_at": utc_now_iso(),
                                    "upload_elapsed_seconds": upload_elapsed,
                                    "verify_elapsed_seconds": verify_elapsed,
                                    "size": local_path.stat().st_size,
                                    "pruned_local": not args.keep_local,
                                },
                            )
                    save_sync_metadata(day_dir, sync_metadata)
                    if not args.keep_local:
                        for filename in root_files:
                            (day_dir / filename).unlink(missing_ok=True)
                    loop_had_changes = True

            if not args.keep_local and not any(day_dir.iterdir()):
                day_dir.rmdir()
                print(f"Deleted local day {day_dir}")
                loop_had_changes = True

        loop_elapsed = round(time.perf_counter() - loop_started_perf, 3)
        if loop_had_changes:
            print("Refreshing HF dataset card...")
            upload_dataset_card(api, args.repo_id, args.data_dir)
            print("Dataset card refreshed.")
            print("Refreshing HF remote cache...")
            refresh_monitor_remote_cache(args.repo_id)
        print(
            f"Sync pass finished: started_at={loop_started_at} "
            f"elapsed_seconds={loop_elapsed} day_count={len(day_dirs)}"
        )
        if args.loop_seconds <= 0:
            break

        time.sleep(args.loop_seconds)


if __name__ == "__main__":
    main()
