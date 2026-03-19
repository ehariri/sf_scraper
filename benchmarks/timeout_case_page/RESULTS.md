# Case Page Timeout Experiment

Sample:

- Day: `2021-03-25`
- Mode: `--failed-only`
- Pending cases loaded from manifest: `62`
- Other settings held constant:
  - `--max-concurrent-cases 2`
  - `--max-concurrent-downloads 6`
  - `--retry-passes 0`
  - local-only

Question:

- Does raising `--case-ready-poll-attempts` recover more cases, or are we already waiting long enough?

Results:

| Config | Scraped Cases | Total Cases | Failed Cases | Elapsed Seconds | Cases / Min | Logged Timeout Errors | Logged Execution-Context Errors |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `poll20` | 10 | 67 | 57 | 140.812 | 4.26 | n/a | n/a |
| `poll40` | 9 | 67 | 58 | 138.814 | 3.89 | 2 | 56 |
| `poll60` | 9 | 67 | 58 | 365.092 | 1.48 | 7 | 51 |

Interpretation:

- Increasing the case-page wait budget did **not** improve completion on this sample.
- `poll40` was slightly worse than baseline.
- `poll60` was much slower and still did not recover more cases.
- The dominant failure mode remains `Execution context destroyed during case scrape`, not the explicit `Timeout waiting for case page load` error.

Conclusion:

- On this sample, the scraper is **not timing out too soon** on case-page readiness.
- Raising `--case-ready-poll-attempts` mostly burns wall-clock time on pages that never stabilize.
- The more valuable next target is handling page/context resets, not increasing this timeout.
