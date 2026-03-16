import json
import shutil
import subprocess
import time
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
SCRAPER = ROOT / ".venv" / "bin" / "python"
SCRAPER_SCRIPT = ROOT / "fast_scraper" / "scraper.py"
OUT_ROOT = ROOT / "benchmarks" / "worker_matrix"

BASE_ARGS = [
    "--disable-hf-upload",
    "--keep-local-pdfs",
    "--max-concurrent-downloads",
    "6",
    "--retry-passes",
    "0",
    "--search-timeout-ms",
    "30000",
    "--table-idle-timeout-ms",
    "30000",
    "--case-ready-poll-attempts",
    "20",
]

# Fixed five-day workload with known case counts:
# 2022-01-03: 48
# 2022-01-04: 43
# 2022-01-05: 71
# 2022-01-06: 48
# 2022-01-07: 52
SCENARIOS = [
    {
        "name": "two_workers_cases_2",
        "max_concurrent_cases": 2,
        "workers": [
            {"name": "w1", "port": 9631, "start_date": "2022-01-03", "end_date": "2022-01-05"},
            {"name": "w2", "port": 9632, "start_date": "2022-01-06", "end_date": "2022-01-07"},
        ],
    },
    {
        "name": "three_workers_cases_1",
        "max_concurrent_cases": 1,
        "workers": [
            {"name": "w1", "port": 9641, "start_date": "2022-01-03", "end_date": "2022-01-03"},
            {"name": "w2", "port": 9642, "start_date": "2022-01-04", "end_date": "2022-01-05"},
            {"name": "w3", "port": 9643, "start_date": "2022-01-06", "end_date": "2022-01-07"},
        ],
    },
    {
        "name": "three_workers_cases_2",
        "max_concurrent_cases": 2,
        "workers": [
            {"name": "w1", "port": 9651, "start_date": "2022-01-03", "end_date": "2022-01-03"},
            {"name": "w2", "port": 9652, "start_date": "2022-01-04", "end_date": "2022-01-05"},
            {"name": "w3", "port": 9653, "start_date": "2022-01-06", "end_date": "2022-01-07"},
        ],
    },
    {
        "name": "four_workers_cases_1",
        "max_concurrent_cases": 1,
        "workers": [
            {"name": "w1", "port": 9661, "start_date": "2022-01-03", "end_date": "2022-01-03"},
            {"name": "w2", "port": 9662, "start_date": "2022-01-04", "end_date": "2022-01-04"},
            {"name": "w3", "port": 9663, "start_date": "2022-01-05", "end_date": "2022-01-05"},
            {"name": "w4", "port": 9664, "start_date": "2022-01-06", "end_date": "2022-01-07"},
        ],
    },
    {
        "name": "four_workers_cases_2",
        "max_concurrent_cases": 2,
        "workers": [
            {"name": "w1", "port": 9671, "start_date": "2022-01-03", "end_date": "2022-01-03"},
            {"name": "w2", "port": 9672, "start_date": "2022-01-04", "end_date": "2022-01-04"},
            {"name": "w3", "port": 9673, "start_date": "2022-01-05", "end_date": "2022-01-05"},
            {"name": "w4", "port": 9674, "start_date": "2022-01-06", "end_date": "2022-01-07"},
        ],
    },
    {
        "name": "five_workers_cases_1",
        "max_concurrent_cases": 1,
        "workers": [
            {"name": "w1", "port": 9681, "start_date": "2022-01-03", "end_date": "2022-01-03"},
            {"name": "w2", "port": 9682, "start_date": "2022-01-04", "end_date": "2022-01-04"},
            {"name": "w3", "port": 9683, "start_date": "2022-01-05", "end_date": "2022-01-05"},
            {"name": "w4", "port": 9684, "start_date": "2022-01-06", "end_date": "2022-01-06"},
            {"name": "w5", "port": 9685, "start_date": "2022-01-07", "end_date": "2022-01-07"},
        ],
    },
    {
        "name": "five_workers_cases_2",
        "max_concurrent_cases": 2,
        "workers": [
            {"name": "w1", "port": 9691, "start_date": "2022-01-03", "end_date": "2022-01-03"},
            {"name": "w2", "port": 9692, "start_date": "2022-01-04", "end_date": "2022-01-04"},
            {"name": "w3", "port": 9693, "start_date": "2022-01-05", "end_date": "2022-01-05"},
            {"name": "w4", "port": 9694, "start_date": "2022-01-06", "end_date": "2022-01-06"},
            {"name": "w5", "port": 9695, "start_date": "2022-01-07", "end_date": "2022-01-07"},
        ],
    },
]


def kill_port(port):
    try:
        output = subprocess.check_output(["lsof", "-ti", f":{port}"], text=True)
    except subprocess.CalledProcessError:
        return
    for pid in output.splitlines():
        pid = pid.strip()
        if pid:
            subprocess.run(["kill", "-TERM", pid], check=False)


def scenario_dirs(name):
    base = OUT_ROOT / name
    return {
        "base": base,
        "data": base / "data",
        "logs": base / "logs",
        "summary": base / "summary.json",
    }


def launch_worker(worker, data_root, log_path, max_concurrent_cases):
    cmd = [
        str(SCRAPER),
        "-u",
        str(SCRAPER_SCRIPT),
        "--port",
        str(worker["port"]),
        "--start-date",
        worker["start_date"],
        "--end-date",
        worker["end_date"],
        "--data-root",
        str(data_root),
        "--max-concurrent-cases",
        str(max_concurrent_cases),
        *BASE_ARGS,
    ]
    with open(log_path, "w") as log_file:
        proc = subprocess.Popen(cmd, cwd=ROOT, stdout=log_file, stderr=subprocess.STDOUT)
    return {"proc": proc, "cmd": cmd, "log_path": str(log_path)}


def collect_day_summaries(data_root):
    days = []
    for path in sorted(data_root.glob("*/day_summary.json")):
        with open(path, "r") as f:
            summary = json.load(f)
        days.append(
            {
                "date": path.parent.name,
                "scraped_cases": summary.get("scraped_cases", 0),
                "total_cases": summary.get("total_cases", 0),
                "fully_completed": summary.get("fully_completed", False),
                "last_run": summary.get("last_run"),
            }
        )
    return days


def run_scenario(scenario, timeout_seconds=1800):
    dirs = scenario_dirs(scenario["name"])
    if dirs["base"].exists():
        shutil.rmtree(dirs["base"])
    dirs["logs"].mkdir(parents=True, exist_ok=True)
    dirs["data"].mkdir(parents=True, exist_ok=True)

    for worker in scenario["workers"]:
        kill_port(worker["port"])

    launches = []
    started_at = time.time()
    for worker in scenario["workers"]:
        log_path = dirs["logs"] / f"{worker['name']}.log"
        launches.append(
            {
                "worker": worker,
                **launch_worker(
                    worker,
                    dirs["data"],
                    log_path,
                    scenario["max_concurrent_cases"],
                ),
            }
        )

    deadline = started_at + timeout_seconds
    timed_out = False
    while True:
        alive = [item for item in launches if item["proc"].poll() is None]
        if not alive:
            break
        if time.time() >= deadline:
            timed_out = True
            for item in alive:
                item["proc"].terminate()
            time.sleep(2)
            for item in alive:
                if item["proc"].poll() is None:
                    item["proc"].kill()
            break
        time.sleep(2)

    finished_at = time.time()
    for worker in scenario["workers"]:
        kill_port(worker["port"])

    days = collect_day_summaries(dirs["data"])
    scraped = sum(day["scraped_cases"] for day in days)
    total = sum(day["total_cases"] for day in days)
    wall_elapsed = round(finished_at - started_at, 3)
    result = {
        "scenario": scenario["name"],
        "worker_count": len(scenario["workers"]),
        "max_concurrent_cases": scenario["max_concurrent_cases"],
        "timed_out": timed_out,
        "wall_elapsed_seconds": wall_elapsed,
        "scraped_cases": scraped,
        "total_cases": total,
        "completed_cases_per_minute": round((scraped / wall_elapsed) * 60, 3)
        if wall_elapsed > 0
        else 0.0,
        "workers": [
            {
                "name": item["worker"]["name"],
                "port": item["worker"]["port"],
                "start_date": item["worker"]["start_date"],
                "end_date": item["worker"]["end_date"],
                "returncode": item["proc"].poll(),
                "command": item["cmd"],
                "log_path": item["log_path"],
            }
            for item in launches
        ],
        "days": days,
    }
    with open(dirs["summary"], "w") as f:
        json.dump(result, f, indent=2)
    return result


def main():
    OUT_ROOT.mkdir(parents=True, exist_ok=True)
    results = [run_scenario(scenario) for scenario in SCENARIOS]
    with open(OUT_ROOT / "all_results.json", "w") as f:
        json.dump(results, f, indent=2)
    print(json.dumps(results, indent=2))


if __name__ == "__main__":
    main()
