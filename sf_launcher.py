import subprocess
import sys
import time
from datetime import datetime, timedelta
import math

# --- Configuration ---
START_DATE = "2015-01-01"
END_DATE = "2015-01-10"
NUM_WORKERS = 3
BASE_PORT = 9222

def get_date_range(start_str, end_str):
    start = datetime.strptime(start_str, "%Y-%m-%d")
    end = datetime.strptime(end_str, "%Y-%m-%d")
    dates = []
    curr = start
    while curr <= end:
        dates.append(curr.strftime("%Y-%m-%d"))
        curr += timedelta(days=1)
    return dates

def split_dates(dates, n):
    k, m = divmod(len(dates), n)
    return [dates[i*k+min(i, m):(i+1)*k+min(i+1, m)] for i in range(n)]

def main():
    all_dates = get_date_range(START_DATE, END_DATE)
    print(f"Total dates to scrape: {len(all_dates)}")
    
    chunks = split_dates(all_dates, NUM_WORKERS)
    processes = []
    
    for i, chunk in enumerate(chunks):
        if not chunk:
            continue
            
        worker_start = chunk[0]
        worker_end = chunk[-1]
        port = BASE_PORT + i
        
        print(f"Launching Worker {i+1} on port {port}: {worker_start} to {worker_end} ({len(chunk)} days)")
        
        cmd = [
            sys.executable, "sf_multi.py",
            "--port", str(port),
            "--start-date", worker_start,
            "--end-date", worker_end
        ]
        
        # Launch process
        p = subprocess.Popen(cmd)
        processes.append(p)
        
        # Stagger launches slightly
        time.sleep(5)
        
    print(f"\nAll {len(processes)} workers launched.")
    print("Please solve the Cloudflare challenge in EACH Chrome window that opens.")
    print("Waiting for workers to complete...")
    
    try:
        for p in processes:
            p.wait()
    except KeyboardInterrupt:
        print("\nStopping all workers...")
        for p in processes:
            p.terminate()

    print("All workers finished.")

if __name__ == "__main__":
    main()
