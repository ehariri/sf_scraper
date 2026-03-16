import argparse
import json
import shutil
import time
from pathlib import Path

from huggingface_hub import HfApi
from huggingface_hub.errors import EntryNotFoundError

from build_hf_dataset_card import build_card


DEFAULT_REPO_ID = "Arifov/sf_superior_court"
DEFAULT_DATA_DIR = Path("data")
SYNC_METADATA_FILENAME = "sync_metadata.json"
DATASET_CARD_FILENAME = "HF_DATASET_CARD.md"


def utc_now_iso():
    from datetime import datetime

    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


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
    api.upload_folder(
        repo_id=repo_id,
        repo_type="dataset",
        folder_path=day_dir,
        path_in_repo=f"data/{day_dir.name}",
        commit_message=f"Upload SF Superior Court day {day_dir.name}",
    )


def upload_case_dir(api: HfApi, repo_id: str, day_name: str, case_dir: Path):
    api.upload_folder(
        repo_id=repo_id,
        repo_type="dataset",
        folder_path=case_dir,
        path_in_repo=f"data/{day_name}/{case_dir.name}",
        commit_message=f"Upload SF Superior Court case {day_name}/{case_dir.name}",
    )


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
        api.upload_folder(
            repo_id=repo_id,
            repo_type="dataset",
            folder_path=temp_root,
            path_in_repo=f"data/{day_dir.name}",
            commit_message=f"Upload SF Superior Court day metadata {day_dir.name}",
        )
    finally:
        shutil.rmtree(temp_root, ignore_errors=True)


def upload_dataset_card(api: HfApi, repo_id: str, data_dir: Path):
    card_path = Path(__file__).resolve().parent / DATASET_CARD_FILENAME
    card_path.write_text(build_card(data_dir, repo_id))
    api.upload_file(
        path_or_fileobj=str(card_path),
        path_in_repo="README.md",
        repo_id=repo_id,
        repo_type="dataset",
        commit_message="Refresh dataset card after sync",
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
            for case_dir in case_dirs:
                local = local_files_for_path(case_dir)
                existing = verify_case_dir(api, args.repo_id, day_dir.name, case_dir)
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
                upload_started_at = utc_now_iso()
                upload_started_perf = time.perf_counter()
                print(
                    f"Uploading {day_dir.name}/{case_dir.name}: {len(local)} files, "
                    f"{sum(local.values()) / 1024 / 1024:.1f} MB"
                )
                upload_case_dir(api, args.repo_id, day_dir.name, case_dir)
                verify_started_perf = time.perf_counter()
                result = verify_case_dir(api, args.repo_id, day_dir.name, case_dir)
                verify_elapsed = round(time.perf_counter() - verify_started_perf, 3)
                upload_elapsed = round(
                    time.perf_counter() - upload_started_perf - verify_elapsed, 3
                )
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
                        "started_at": upload_started_at,
                        "finished_at": utc_now_iso(),
                        "upload_elapsed_seconds": upload_elapsed,
                        "verify_elapsed_seconds": verify_elapsed,
                        "file_count": len(local),
                        "total_bytes": sum(local.values()),
                        "verified_local_count": result["local_count"],
                        "verified_remote_count": result["remote_count"],
                        "pruned_local": not args.keep_local,
                    },
                )
                save_sync_metadata(day_dir, sync_metadata)
                if not args.keep_local:
                    shutil.rmtree(case_dir)
                    print(f"Deleted local {case_dir}")
                loop_had_changes = True

            root_files = [
                path.name
                for path in sorted(day_dir.iterdir())
                if path.is_file()
            ]
            if root_files:
                existing = verify_root_files(api, args.repo_id, day_dir, root_files)
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
                    result = verify_root_files(api, args.repo_id, day_dir, root_files)
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
        print(
            f"Sync pass finished: started_at={loop_started_at} "
            f"elapsed_seconds={loop_elapsed} day_count={len(day_dirs)}"
        )
        if args.loop_seconds <= 0:
            break

        time.sleep(args.loop_seconds)


if __name__ == "__main__":
    main()
