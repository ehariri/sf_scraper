# Live Benchmark Notes

Date run: March 12, 2026

Target filing date used for stable live sampling: `2020-12-24`

- Shared startup to case-list timing:
  - browser launch overhead: `2.07s`
  - session acquisition / Cloudflare pass: `5.15s`
  - search + case-list extraction (`47` cases): `2.28s`
  - total launch-to-case-list: `9.50s`

- Case-list extraction only on the live `47`-case results page:
  - old extractor mean: `0.509s`
  - new extractor mean: `0.0050s`
  - improvement: about `102x`

- End-to-end live sample on the first 3 real cases from `2020-12-24`:
  - old sequential path: `26.61s`, `3/3` successful cases, `44` docs
  - new sequential path: `22.79s`, `3/3` successful cases, `44` docs
  - new 2-tab parallel path: `12.12s`, `3/3` successful cases, `44` docs

Relative improvement on the 3-case live sample:

- new sequential vs old sequential: about `14.4%` faster
- new 2-tab parallel vs old sequential: about `54.4%` faster

## Important caveat

I also tried a larger live stress run on `2026-03-10` (`77` filings). The concurrent worker showed a real stability issue under heavier load and started throwing repeated `Execution context was destroyed` errors mid-run. So the small-sample benchmark is clean, but the faster concurrent path still needs another stability pass before I would trust a full-day production scrape.
