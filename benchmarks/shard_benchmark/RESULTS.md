# Local Shard Simulation Benchmark

Workload:

- `2022-01-03` (`48` discovered cases)
- `2022-01-04` (`43` discovered cases)
- `2022-01-05` (`71` discovered cases)
- Total discovered: `162`

All runs used the same scraper settings:

- `--max-concurrent-cases 2`
- `--max-concurrent-downloads 6`
- `--retry-passes 0`
- local-only storage (`--disable-hf-upload --keep-local-pdfs`)

## Results

| scenario | shard layout | completed cases | wall seconds | completed/min |
| --- | --- | ---: | ---: | ---: |
| `single_worker` | one worker over all 3 days | `14` | `332.575` | `2.526` |
| `two_workers` | `2022-01-03..04` + `2022-01-05` | `12` | `224.400` | `3.209` |
| `three_workers` | one day per worker | `11` | `136.247` | `4.844` |

## Interpretation

- Local sharding improved throughput materially even on one machine.
- `three_workers` produced the highest completed-cases-per-minute result.
- `three_workers` did not maximize completed cases on this sample; it won because
  wall-clock time dropped much faster than completions did.
- Relative to `single_worker`, the `three_workers` setup improved throughput by
  about `91.8%`.

## Recommended Fastest Setup

For first-pass extraction throughput on this machine:

- run `3` separate scraper workers
- keep each worker conservative:
  - `--max-concurrent-cases 2`
  - `--max-concurrent-downloads 6`
  - `--retry-passes 0`
- shard by date range/day across workers
- use `--failed-only` cleanup afterward instead of retrying inline

This benchmark is a local simulation of multi-machine sharding. Real separate
machines may perform better because they also isolate network/session behavior,
not just browser workers.
