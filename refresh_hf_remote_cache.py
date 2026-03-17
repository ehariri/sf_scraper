#!/usr/bin/env python3
import argparse
from pathlib import Path

from hf_remote_state import (
    DEFAULT_CACHE_PATH,
    DEFAULT_REPO_ID,
    DEFAULT_SCOPE_END,
    DEFAULT_SCOPE_START,
    refresh_remote_state_cache,
)


def main():
    parser = argparse.ArgumentParser(
        description="Refresh the committed HF remote-state cache used by the monitor"
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
    head_commit = (state.get("head_commit") or {}).get("commit_id")
    print(
        f"HF remote cache refreshed: changed={result['changed']} "
        f"days={state.get('day_count', 0)} head_commit={head_commit or 'unknown'} "
        f"path={args.cache_path}"
    )


if __name__ == "__main__":
    main()
