#!/usr/bin/env python3
import argparse
import subprocess
import sys
import time
from pathlib import Path


ROOT = Path(__file__).resolve().parent
DEFAULT_PYTHON = ROOT / ".venv" / "bin" / "python"


def should_restart(output_text: str, exit_code: int) -> bool:
    lowered = output_text.lower()
    if "504 server error: gateway time-out" in lowered:
        return True
    if "commit/main" in lowered and "gateway time-out" in lowered:
        return True
    return exit_code != 0 and "hfhubhttperror" in lowered and "504" in lowered


def main():
    parser = argparse.ArgumentParser(
        description="Run upload_data_in_batches.py and automatically restart after HF 504 failures."
    )
    parser.add_argument(
        "--restart-delay-seconds",
        type=int,
        default=300,
        help="Seconds to wait before restarting after a detected HF 504 failure.",
    )
    parser.add_argument(
        "--python",
        type=Path,
        default=DEFAULT_PYTHON,
        help="Python interpreter to run upload_data_in_batches.py with.",
    )
    parser.add_argument(
        "upload_args",
        nargs=argparse.REMAINDER,
        help="Arguments forwarded to upload_data_in_batches.py. Prefix with -- before them if needed.",
    )
    args = parser.parse_args()

    upload_args = list(args.upload_args)
    if upload_args and upload_args[0] == "--":
        upload_args = upload_args[1:]

    cmd = [str(args.python), "-u", str(ROOT / "upload_data_in_batches.py"), *upload_args]

    attempt = 0
    while True:
        attempt += 1
        print(f"Starting bulk upload attempt {attempt}: {' '.join(cmd)}", flush=True)
        proc = subprocess.Popen(
            cmd,
            cwd=str(ROOT),
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )
        output_chunks = []
        assert proc.stdout is not None
        for line in proc.stdout:
            output_chunks.append(line)
            sys.stdout.write(line)
            sys.stdout.flush()
        proc.wait()
        combined_output = "".join(output_chunks)

        if proc.returncode == 0:
            print("Bulk upload completed successfully.", flush=True)
            return 0

        if not should_restart(combined_output, proc.returncode):
            print(
                f"Bulk upload exited with code {proc.returncode} and is not restartable.",
                flush=True,
            )
            return proc.returncode

        print(
            f"Detected restartable HF 504 failure. Waiting {args.restart_delay_seconds}s before restarting...",
            flush=True,
        )
        time.sleep(args.restart_delay_seconds)


if __name__ == "__main__":
    raise SystemExit(main())
