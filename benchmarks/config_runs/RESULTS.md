# Scraper Configuration Benchmark

Dense sample days:

- `2021-07-16` (`56` discovered cases)
- `2022-01-21` (`61` discovered cases)

Metrics below use `day_summary.json:last_run.elapsed_seconds` and report completed
cases per minute for the first pass only (`--retry-passes 0`).

| config | settings | day | completed | failed | elapsed_s | completed/min |
| --- | --- | --- | ---: | ---: | ---: | ---: |
| `baseline` | `cases=2 downloads=6 search=10s table=10s polls=10` | `2021-07-16` | `5` | `51` | `112.227` | `2.67` |
| `baseline` | `cases=2 downloads=6 search=10s table=10s polls=10` | `2022-01-21` | `2` | `59` | `114.137` | `1.05` |
| `parallel_relaxed` | `cases=2 downloads=6 search=30s table=30s polls=20` | `2021-07-16` | `7` | `49` | `148.531` | `2.83` |
| `parallel_relaxed` | `cases=2 downloads=6 search=30s table=30s polls=20` | `2022-01-21` | `4` | `57` | `157.628` | `1.52` |
| `serial_relaxed` | `cases=1 downloads=4 search=45s table=45s polls=30` | `2021-07-16` | `8` | `48` | `335.324` | `1.43` |

Notes:

- `parallel_relaxed` beat `baseline` on both sample days for completed cases per minute.
- `serial_relaxed` recovered slightly more cases on `2021-07-16`, but the wall-clock
  penalty was too large, so throughput dropped materially.
- The `2022-01-21` `serial_relaxed` run was stopped after it had only matched the
  `parallel_relaxed` completion count (`4`) with materially more elapsed time.

Recommendation:

- Use `2` concurrent case tabs and `6` concurrent downloads.
- Use more patient waits (`30s` search/table timeouts, `20` case-ready polls).
- Keep the first sweep at `--retry-passes 0` and use `--failed-only` cleanup runs
  afterward for the long tail.
