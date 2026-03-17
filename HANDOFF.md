# Handoff

This file is the operational handoff for continuing the SF Superior Court scrape on another device.

## Scope

- Active study window: `2020-01-02` through `2025-12-31`
- Do not prioritize new collection outside `2020-2025`
- `2015-2019` and `2026+` are out of current collection scope, except for already-uploaded pilot days

## Key repos

- Code repo to clone:
  - `https://github.com/J0V1K/sf_scraper_fork`
- Hugging Face dataset repo:
  - `Arifov/sf_superior_court`

## Current state

### Scraping

- Scraping is currently paused.
- HF sync loop may still be running on the original machine, but it was idle the last time checked.
- Most of the corpus is broad first-pass coverage, not complete cleanup coverage.

### Coverage summary

As of the latest handoff:

- Local working corpus total:
  - `922` filing days
  - `71,468` discovered cases
  - `7,691` scraped cases
- In-scope local `2020-2025` state:
  - `1090` incomplete filing days
  - effectively no fully completed `2020-2025` day folders left locally that were eligible for sync at the time of handoff
- `2020` now has broad first-pass coverage across the full year:
  - `2020-01-02` through `2020-12-29`
  - `242` day folders
  - `1,364 / 14,724` cases scraped
- HF dataset card recently showed:
  - `37` filing-day folders in HF
  - `555` uploaded case-level `register_of_actions.json` files

Treat the HF dataset card as the source of truth for what is currently uploaded.

## Best-known operating configuration

### Stable default

- `3` workers
- `2` concurrent case tabs per worker
- `6` concurrent downloads per worker
- `0` retry passes on first pass
- `0ms` launch stagger

This was the best repeatable setup.

### Faster but less stable

- `4` workers
- `2` concurrent case tabs per worker

This produced the best single-run cases/minute result, but was more variable and more likely to run into Cloudflare/session instability.

## Workflow

### 1. First pass

Use for broad date coverage:

```bash
python launcher.py \
  --start-date 2020-01-02 \
  --end-date 2020-12-29 \
  --num-workers 3 \
  --max-concurrent-cases 2 \
  --max-concurrent-downloads 6 \
  --disable-hf-upload \
  --keep-local-pdfs
```

### 2. Overnight bounded run

Use for fixed-duration runs:

```bash
python timed_scrape_runner.py \
  --timeout-seconds 36000 \
  --port 9741 \
  -- \
  --port 9741 \
  --start-date 2020-01-02 \
  --end-date 2020-04-30 \
  --disable-hf-upload \
  --keep-local-pdfs \
  --max-concurrent-cases 2 \
  --max-concurrent-downloads 6 \
  --retry-passes 0 \
  --search-timeout-ms 30000 \
  --table-idle-timeout-ms 30000 \
  --case-ready-poll-attempts 20
```

### 3. Failed-only cleanup

This is required to turn broad first-pass days into fully completed uploadable days.

Typical large-range cleanup:

```bash
python timed_scrape_runner.py \
  --timeout-seconds 36000 \
  --port 9751 \
  -- \
  --port 9751 \
  --start-date 2020-01-02 \
  --end-date 2021-12-31 \
  --failed-only \
  --disable-hf-upload \
  --keep-local-pdfs \
  --max-concurrent-cases 2 \
  --max-concurrent-downloads 6 \
  --retry-passes 0 \
  --search-timeout-ms 30000 \
  --table-idle-timeout-ms 30000 \
  --case-ready-poll-attempts 20
```

Shard cleanup helper:

```bash
python run_failed_cleanup_shard.py \
  --shard-index 0 \
  --shard-count 3 \
  --port 9711
```

### 4. Sync and prune to HF

Run this continuously once completed days exist:

```bash
HF_HUB_DISABLE_XET=1 python -u sync_existing_to_hf_and_prune.py \
  --repo-id Arifov/sf_superior_court \
  --data-dir data \
  --unit case \
  --completed-only \
  --loop-seconds 60
```

This script:

- uploads case directories
- uploads day-level metadata files
- verifies remote contents
- prunes local files after verification
- regenerates and uploads the HF dataset card after sync passes that change the dataset

## Cloudflare

This project is not truly headless.

- Chrome opens on the configured debug ports
- you must solve Cloudflare in each Chrome window when prompted
- after the challenge is solved, the scraper continues automatically

Without remote desktop/browser access, another device cannot operate this workflow unattended.

## Important operational notes

- HF sync is only useful when fully completed day folders exist locally.
- First-pass scraping alone will usually not produce many completed days.
- `failed_cases.json` is the key artifact for cleanup.
- `register_of_actions.json` is the success criterion for a truly scraped case.
- CGC and CUD prefixes scrape much better than many family/probate-related prefixes.
- The original local git repo had broken history objects; this GitHub repo is a clean export snapshot and should be treated as the runnable source repo.

## Recommended next actions on a new device

1. Clone the GitHub repo.
2. Create a Python venv and install `requirements.txt`.
3. Install Playwright Chromium.
4. Log into Hugging Face.
5. Start with failed-only cleanup on `2020-2025`.
6. Keep the HF sync loop running in parallel.
7. Monitor the HF dataset card to see what has actually uploaded.
