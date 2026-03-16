import argparse
import signal
import subprocess
import sys
import time


def kill_port(port):
    try:
        output = subprocess.check_output(["lsof", "-ti", f":{port}"], text=True)
    except subprocess.CalledProcessError:
        return

    for line in output.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            subprocess.run(["kill", "-TERM", line], check=False)
        except Exception:
            pass


def main():
    parser = argparse.ArgumentParser(
        description="Run the fast scraper for a bounded wall-clock duration."
    )
    parser.add_argument("--timeout-seconds", type=int, required=True)
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("scraper_args", nargs=argparse.REMAINDER)
    args = parser.parse_args()

    if args.scraper_args and args.scraper_args[0] == "--":
        scraper_args = args.scraper_args[1:]
    else:
        scraper_args = args.scraper_args

    if not scraper_args:
        raise SystemExit("No scraper args provided")

    cmd = [sys.executable, "-u", "fast_scraper/scraper.py", *scraper_args]
    print(f"Starting timed scraper: {' '.join(cmd)}", flush=True)
    print(
        f"Timeout window: {args.timeout_seconds}s on Chrome port {args.port}",
        flush=True,
    )

    proc = subprocess.Popen(cmd)
    deadline = time.time() + args.timeout_seconds

    try:
        while True:
            ret = proc.poll()
            if ret is not None:
                print(f"Scraper exited with code {ret}", flush=True)
                return ret

            if time.time() >= deadline:
                print("Timeout reached. Terminating scraper...", flush=True)
                proc.terminate()
                try:
                    proc.wait(timeout=20)
                except subprocess.TimeoutExpired:
                    print("Scraper did not terminate. Killing...", flush=True)
                    proc.kill()
                    proc.wait(timeout=10)
                kill_port(args.port)
                print("Timed scrape runner finished after timeout.", flush=True)
                return 0

            time.sleep(5)
    finally:
        if proc.poll() is None:
            proc.send_signal(signal.SIGTERM)
            try:
                proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                proc.kill()
        kill_port(args.port)


if __name__ == "__main__":
    raise SystemExit(main())
