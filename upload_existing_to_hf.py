import argparse
from pathlib import Path

from huggingface_hub import HfApi


DEFAULT_REPO_ID = "Arifov/sf_superior_court"


def main():
    parser = argparse.ArgumentParser(
        description="Upload an existing SF Superior Court data tree to Hugging Face"
    )
    parser.add_argument(
        "--repo-id",
        type=str,
        default=DEFAULT_REPO_ID,
        help="HF dataset repo id",
    )
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=Path("data"),
        help="Local data directory to upload",
    )
    parser.add_argument(
        "--path-in-repo",
        type=str,
        default="data",
        help="Target path inside the dataset repo",
    )
    parser.add_argument(
        "--allow-patterns",
        nargs="*",
        default=None,
        help="Optional allow patterns passed to upload_folder",
    )
    parser.add_argument(
        "--ignore-patterns",
        nargs="*",
        default=None,
        help="Optional ignore patterns passed to upload_folder",
    )
    args = parser.parse_args()

    api = HfApi()
    print(f"Uploading {args.data_dir} to {args.repo_id}:{args.path_in_repo}")
    api.upload_folder(
        repo_id=args.repo_id,
        repo_type="dataset",
        folder_path=args.data_dir,
        path_in_repo=args.path_in_repo,
        commit_message="Upload SF Superior Court scrape snapshot",
        allow_patterns=args.allow_patterns,
        ignore_patterns=args.ignore_patterns,
    )
    print("Upload complete.")


if __name__ == "__main__":
    main()
