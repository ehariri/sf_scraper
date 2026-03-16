# Worker Matrix Results

Workload: `2022-01-03` through `2022-01-07` (`262` discovered cases)

Common settings:
- `--disable-hf-upload`
- `--keep-local-pdfs`
- `--max-concurrent-downloads 6`
- `--retry-passes 0`
- `--search-timeout-ms 30000`
- `--table-idle-timeout-ms 30000`
- `--case-ready-poll-attempts 20`
- `--case-launch-stagger-ms 0`

## Completed scenarios

| Scenario | Workers | Case tabs/worker | Scraped cases | Wall seconds | Cases/min |
| --- | ---: | ---: | ---: | ---: | ---: |
| `two_workers_cases_2` | 2 | 2 | 18 | 429.464 | 2.515 |
| `three_workers_cases_1` | 3 | 1 | 18 | 507.825 | 2.127 |
| `three_workers_cases_2` | 3 | 2 | 25 | 300.982 | 4.984 |
| `four_workers_cases_1` | 4 | 1 | 21 | 477.701 | 2.638 |
| `four_workers_cases_2` | 4 | 2 | 25 | 252.837 | 5.933 |
| `three_workers_cases_2_repeat` | 3 | 2 | 25 | 325.182 | 4.613 |
| `four_workers_cases_2_repeat` | 4 | 2 | 19 | 266.890 | 4.271 |

## Operational read

- Best single run: `four_workers_cases_2` at `5.933` cases/min.
- Best repeated configuration: `three_workers_cases_2`.
  - repeat results: `4.984`, `4.613`
  - average: `4.799`
  - range: `0.371`
- `four_workers_cases_2` was faster once but much less stable on repeat.
  - repeat results: `5.933`, `4.271`
  - average: `5.102`
  - range: `1.662`
- `5` workers was not a clean unattended configuration on this machine.
  - `five_workers_cases_1` stalled on a fresh Cloudflare challenge before producing a comparable summary.
  - `five_workers_cases_2` was not run after that because the five-worker branch had already crossed the browser/session stability threshold.

## Current recommendation

For unattended first-pass scraping on this machine:

- preferred default: `3` workers, `2` case tabs per worker, `6` concurrent downloads, `0` retry passes, `0ms` launch stagger
- aggressive option: `4` workers, `2` case tabs per worker
  - use this only when you can tolerate higher variance and occasional Cloudflare/session instability

The earlier stagger benchmark also held:

- `0ms` case launch stagger beat `150ms` and `300ms`
- adding launch delay reduced cases/min instead of improving it
