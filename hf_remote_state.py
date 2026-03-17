#!/usr/bin/env python3
import json
from datetime import datetime, timezone
from pathlib import Path

from huggingface_hub import HfApi, hf_hub_download


ROOT = Path(__file__).resolve().parent
DEFAULT_CACHE_PATH = ROOT / "cache" / "hf_remote_state.json"
DEFAULT_REPO_ID = "please-the-bot/sf_superior_court"
DEFAULT_SCOPE_START = "2020-01-01"
DEFAULT_SCOPE_END = "2025-12-31"


def utc_now_iso():
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def build_remote_state(
    repo_id: str = DEFAULT_REPO_ID,
    scope_start: str = DEFAULT_SCOPE_START,
    scope_end: str = DEFAULT_SCOPE_END,
):
    api = HfApi()
    commits = api.list_repo_commits(repo_id=repo_id, repo_type="dataset")
    head = commits[0] if commits else None

    days = {}
    for entry in api.list_repo_tree(
        repo_id=repo_id,
        repo_type="dataset",
        path_in_repo="data",
        recursive=False,
        expand=False,
    ):
        path = getattr(entry, "path", "")
        if not path.startswith("data/"):
            continue
        day = path.split("/", 1)[1]
        if not (scope_start <= day <= scope_end):
            continue
        try:
            summary_path = hf_hub_download(
                repo_id=repo_id,
                repo_type="dataset",
                filename=f"data/{day}/day_summary.json",
            )
            payload = json.loads(Path(summary_path).read_text())
        except Exception:
            payload = {}
        days[day] = {
            "total_cases": int(payload.get("total_cases", 0) or 0),
            "scraped_cases": int(payload.get("scraped_cases", 0) or 0),
            "fully_completed": bool(payload.get("fully_completed", False)),
            "updated_at": payload.get("updated_at"),
        }

    return {
        "schema_version": 1,
        "generated_at": utc_now_iso(),
        "repo_id": repo_id,
        "scope": {"start": scope_start, "end": scope_end},
        "head_commit": {
            "commit_id": getattr(head, "commit_id", None),
            "title": getattr(head, "title", None),
            "created_at": getattr(head, "created_at", None).isoformat().replace("+00:00", "Z")
            if getattr(head, "created_at", None)
            else None,
            "authors": getattr(head, "authors", None),
        },
        "recent_commits": [
            {
                "commit_id": commit.commit_id,
                "title": commit.title,
                "created_at": commit.created_at.isoformat().replace("+00:00", "Z")
                if commit.created_at
                else None,
                "authors": commit.authors,
            }
            for commit in commits[:20]
        ],
        "days": days,
        "day_count": len(days),
    }


def load_remote_state(cache_path: Path = DEFAULT_CACHE_PATH):
    if not cache_path.exists():
        return None
    try:
        return json.loads(cache_path.read_text())
    except Exception:
        return None


def write_remote_state(state, cache_path: Path = DEFAULT_CACHE_PATH):
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    cache_path.write_text(json.dumps(state, indent=2, sort_keys=True) + "\n")


def refresh_remote_state_cache(
    repo_id: str = DEFAULT_REPO_ID,
    cache_path: Path = DEFAULT_CACHE_PATH,
    scope_start: str = DEFAULT_SCOPE_START,
    scope_end: str = DEFAULT_SCOPE_END,
):
    previous = load_remote_state(cache_path)
    current = build_remote_state(repo_id=repo_id, scope_start=scope_start, scope_end=scope_end)
    write_remote_state(current, cache_path)
    changed = previous != current
    return {"changed": changed, "state": current}


def main():
    import argparse

    parser = argparse.ArgumentParser(
        description="Refresh the cached HF remote-state summary used by the monitor"
    )
    parser.add_argument("--repo-id", default=DEFAULT_REPO_ID)
    parser.add_argument("--cache-path", type=Path, default=DEFAULT_CACHE_PATH)
    parser.add_argument("--scope-start", default=DEFAULT_SCOPE_START)
    parser.add_argument("--scope-end", default=DEFAULT_SCOPE_END)
    args = parser.parse_args()

    result = refresh_remote_state_cache(
        repo_id=args.repo_id,
        cache_path=args.cache_path,
        scope_start=args.scope_start,
        scope_end=args.scope_end,
    )
    state = result["state"]
    print(
        f"HF cache refreshed: changed={result['changed']} "
        f"days={state.get('day_count', 0)} cache={args.cache_path}"
    )


if __name__ == "__main__":
    main()
