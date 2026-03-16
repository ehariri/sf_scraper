# Multi-Machine Operation

This repo can be run across multiple machines, but only if the machines are coordinated deliberately. The scraper and sync tools are safe for parallel use when they operate on disjoint date ranges or disjoint local data roots.

Current project scope:

- Filing dates: `2020-01-01` through `2025-12-31`
- Canonical HF dataset repo: `please-the-bot/sf_superior_court`

## Source Of Truth

Use these sources in this order:

1. HF dataset repo: shared project output
2. Each machine's local `data/` tree: scratch space and resume state for that machine only
3. Logs and monitor output: operational state, not authoritative corpus state

Do not treat one machine's local `data/` tree as the project-wide source of truth.

## Hard Rules

These are the rules that keep a multi-machine run from corrupting itself:

1. Never assign the same filing day to two machines at the same time.
2. Never point two sync/prune jobs at the same local `data/` tree.
3. Never share one physical `data/` directory over Dropbox, network mounts, or rsync while the scraper is running.
4. Keep each machine on its own Chrome debug ports.
5. If a machine is using `--hf-only`, do not also run a sync/prune loop against the same dates from another machine.
6. If multiple machines write to HF at the same time, keep them on disjoint date ranges.

## Machine Prerequisites

Each machine needs:

- this repo at the same commit or same copied snapshot
- Python environment with repo dependencies installed
- Playwright and Chromium installed
- Google Chrome installed
- a Hugging Face login with write access to `please-the-bot/sf_superior_court`
- enough local space for the chosen mode:
  - local-first mode: tens of GB recommended
  - `--hf-only` mode: can run on a much smaller disk budget

Bootstrap:

```bash
cd sf_scraper_fork
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python -m playwright install chromium
hf auth login
```

Verify HF access:

```bash
python3 - <<'PY'
from huggingface_hub import HfApi
api = HfApi()
info = api.repo_info(repo_id="please-the-bot/sf_superior_court", repo_type="dataset")
print(info.id, "private=", info.private)
PY
```

## Recommended Topologies

### Option A: Local-First Shards

This is the safest setup.

Each machine:

- scrapes only its assigned date range
- writes to its own local `data/` tree
- optionally runs its own sync/prune loop afterward

Use this when local disk is available and you want the easiest recovery path.

You can use either:

- `launcher.py` for a multi-worker shard on one machine
- `fast_scraper/scraper.py` directly for one explicit worker

Launcher example:

```bash
source .venv/bin/activate
python launcher.py \
  --start-date 2022-01-03 \
  --end-date 2022-06-30 \
  --num-workers 3 \
  --base-port 9400 \
  --data-root data_machine_a \
  --max-concurrent-cases 2 \
  --max-concurrent-downloads 6 \
  --disable-hf-upload
```

First pass example:

```bash
source .venv/bin/activate
python fast_scraper/scraper.py \
  --start-date 2022-01-03 \
  --end-date 2022-06-30 \
  --port 9222 \
  --max-concurrent-cases 2 \
  --max-concurrent-downloads 6 \
  --retry-passes 0 \
  --disable-hf-upload \
  --data-root data_machine_a
```

Sync/prune example:

```bash
source .venv/bin/activate
env HF_HUB_DISABLE_XET=1 python sync_existing_to_hf_and_prune.py \
  --repo-id please-the-bot/sf_superior_court \
  --data-dir data_machine_a \
  --unit case \
  --loop-seconds 60
```

### Option B: HF-Only Shards

This is the low-disk setup.

Each machine:

- scrapes only its assigned date range
- uploads case bundles directly to HF
- keeps only lightweight local JSON state

Use this when the machine has limited storage.

Example:

```bash
source .venv/bin/activate
python fast_scraper/scraper.py \
  --start-date 2024-01-02 \
  --end-date 2024-06-28 \
  --port 9222 \
  --max-concurrent-cases 2 \
  --max-concurrent-downloads 6 \
  --max-concurrent-hf-uploads 1 \
  --retry-passes 0 \
  --hf-repo-id please-the-bot/sf_superior_court \
  --hf-only \
  --data-root data_machine_b
```

Tradeoff:

- lower disk use
- tighter coupling to HF/network health
- less forgiving if the machine loses connectivity mid-run

## Sharding Strategy

Assign non-overlapping date blocks. Good defaults for the current `2020-2025` scope:

- machine A: `2020-2021`
- machine B: `2022-2023`
- machine C: `2024-2025`

Or split more finely by quarter or half-year if machines are uneven.

For cleanup, shard the already-touched incomplete days the same way:

- machine A cleans only its own shard
- machine B cleans only its own shard
- machine C cleans only its own shard

Do not run one machine's cleanup over another machine's active first-pass shard.

## Recommended Runtime Settings

Best observed first-pass setup so far:

- `2` case tabs per worker
- `6` concurrent downloads
- `0` retry passes during the first sweep

Recommended pattern:

1. Broad first pass on each shard
2. `--failed-only` cleanup on each shard
3. sync/prune to HF

## Cloudflare And Browser Behavior

Every machine needs its own manual Cloudflare solve.

Operational notes:

- one Chrome window per worker
- solve the challenge once per worker when it appears
- after the challenge, the worker should continue on its own
- separate machines are better than trying to overstuff one browser context

## Monitor Usage

Each machine's local monitor only reflects that machine's local `data/` tree and process state.

It does not automatically show a project-wide merged view unless all machines write into the same shared status source, which this repo does not do.

Use the monitor per machine for:

- local scrape health
- local sync health
- local coverage and failure backlog

Use the HF dataset card for the shared published dataset state.

## Sync Coordination

If multiple machines are syncing to HF, this is the safest pattern:

- each machine syncs only its own local shard
- all machines target `please-the-bot/sf_superior_court`
- date ranges remain disjoint
- each machine keeps `--unit case`
- each machine leaves `HF_HUB_DISABLE_XET=1`

Notes:

- multiple machines can commit to HF concurrently when they operate on disjoint paths
- the dataset card may refresh frequently if several syncs complete near the same time
- if HF rate limiting appears, reduce concurrent HF activity before reducing scrape concurrency

## Failure Recovery

If one machine fails:

1. stop only that machine's scraper or sync process
2. keep the other machines running
3. resume the failed machine on the same date shard
4. prefer `--failed-only` if the first pass already wrote manifests

For local-first mode, recovery comes from:

- `day_summary.json`
- `failed_cases.json`
- `sync_metadata.json`

For HF-only mode, local metadata is still kept for resume, but PDFs are not.

## Commands By Task

First pass on one shard:

```bash
python fast_scraper/scraper.py \
  --start-date 2023-01-02 \
  --end-date 2023-06-30 \
  --port 9222 \
  --max-concurrent-cases 2 \
  --max-concurrent-downloads 6 \
  --retry-passes 0 \
  --data-root data_machine_c \
  --disable-hf-upload
```

Failed-only cleanup on the same shard:

```bash
python fast_scraper/scraper.py \
  --start-date 2023-01-02 \
  --end-date 2023-06-30 \
  --port 9222 \
  --max-concurrent-cases 2 \
  --max-concurrent-downloads 6 \
  --retry-passes 0 \
  --failed-only \
  --data-root data_machine_c \
  --disable-hf-upload
```

Continuous sync/prune on that shard:

```bash
env HF_HUB_DISABLE_XET=1 python sync_existing_to_hf_and_prune.py \
  --repo-id please-the-bot/sf_superior_court \
  --data-dir data_machine_c \
  --unit case \
  --loop-seconds 60
```

## What Not To Do

Avoid these patterns:

- two machines scraping the same date range
- one machine scraping while another machine prunes the same local directory
- using `--clear` on a shard another machine has already populated
- assuming the Vercel/tunnel monitor is project-wide; it only reflects the machine behind that monitor

## Recommended Team Procedure

1. Assign each machine a fixed date shard.
2. Decide whether each machine is `local-first` or `hf-only`.
3. Keep a separate `data-root` per machine.
4. Run first pass.
5. Run failed-only cleanup.
6. Run sync/prune.
7. Update any shared tracking sheet or runbook with shard ownership and current state.
