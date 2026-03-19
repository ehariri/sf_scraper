---
pretty_name: SF Superior Court Docket and ROA Scrape
language:
  - en
license: other
---

# SF Superior Court Docket and ROA Scrape

This dataset contains San Francisco Superior Court filing-day search results, case-level register-of-actions exports, and downloaded docket PDFs collected from the court's public portal.

This card reflects the current state of the scrape as of **2026-03-15**.

## Dataset state

The Hugging Face dataset repo currently contains:

- **37** filing-day folders
- **555** uploaded case-level `register_of_actions.json` files
- Earliest uploaded day: **2015-01-02**
- Latest uploaded day: **2026-03-10**
- Average uploaded cases per HF filing day: **15.0**
- Median uploaded cases per HF filing day: **1.0**
- HF uploaded scrape rate across matched uploaded days: **71.6%** (555 uploaded / 775 discovered across 37 days)

In this project, a case counts as **uploaded** when a case directory with `register_of_actions.json` is present in the HF dataset repo.

## HF coverage by year

The table below summarizes the dataset that is actually present in the HF repo.

| Year | Filing days in HF | Uploaded cases in HF |
| --- | ---: | ---: |
| 2015 | 4 | 129 |
| 2020 | 5 | 256 |
| 2021 | 7 | 136 |
| 2022 | 2 | 2 |
| 2023 | 6 | 7 |
| 2024 | 5 | 5 |
| 2025 | 4 | 4 |
| 2026 | 4 | 16 |
| Total | 37 | 555 |

## HF coverage by month

Month-level coverage currently present in the HF repo:

| Month | Filing days in HF | Uploaded cases in HF |
| --- | ---: | ---: |
| 2015-01 | 4 | 129 |
| 2020-12 | 5 | 256 |
| 2021-01 | 6 | 135 |
| 2021-07 | 1 | 1 |
| 2022-01 | 1 | 1 |
| 2022-09 | 1 | 1 |
| 2023-01 | 2 | 3 |
| 2023-02 | 1 | 1 |
| 2023-05 | 1 | 1 |
| 2023-06 | 1 | 1 |
| 2023-09 | 1 | 1 |
| 2024-02 | 2 | 2 |
| 2024-07 | 1 | 1 |
| 2024-09 | 1 | 1 |
| 2024-12 | 1 | 1 |
| 2025-01 | 1 | 1 |
| 2025-03 | 1 | 1 |
| 2025-06 | 1 | 1 |
| 2025-11 | 1 | 1 |
| 2026-01 | 1 | 1 |
| 2026-02 | 2 | 2 |
| 2026-03 | 1 | 13 |

## HF case type distribution

Case types here are approximated from the leading alphabetic prefix of the case number stored in each uploaded case directory.

| Case prefix | Uploaded cases in HF | Share of HF uploaded cases |
| --- | ---: | ---: |
| CGC | 244 | 44.0% |
| CSM | 59 | 10.6% |
| FDI | 56 | 10.1% |
| CUD | 40 | 7.2% |
| CNC | 28 | 5.0% |
| FCS | 26 | 4.7% |
| PDW | 21 | 3.8% |
| PES | 20 | 3.6% |
| FDV | 19 | 3.4% |
| CPF | 10 | 1.8% |
| FSD | 9 | 1.6% |
| PTR | 6 | 1.1% |
| FMS | 5 | 0.9% |
| APP | 4 | 0.7% |
| FPT | 4 | 0.7% |
| PCN | 2 | 0.4% |
| PGN | 2 | 0.4% |

## Local backlog pending sync

The local working corpus is larger than the current HF upload:

- **1,138** filing days on local disk
- **86,014** discovered cases on local disk
- **9,629** scraped cases on local disk
- **0** fully completed local filing days
- Local coverage currently runs from **2020-01-02** through **2026-03-13**

Local year summary:

| Year | Filing days | Fully completed days | Scraped cases | Discovered cases | Scrape rate |
| --- | ---: | ---: | ---: | ---: | ---: |
| 2020 | 242 | 0 | 1,364 | 14,724 | 9.3% |
| 2021 | 93 | 0 | 664 | 5,823 | 11.4% |
| 2022 | 173 | 0 | 1,683 | 11,058 | 15.2% |
| 2023 | 169 | 0 | 1,507 | 12,455 | 12.1% |
| 2024 | 169 | 0 | 1,726 | 14,398 | 12.0% |
| 2025 | 244 | 0 | 2,153 | 22,994 | 9.4% |
| 2026 | 48 | 0 | 532 | 4,562 | 11.7% |
| Total | 1,138 | 0 | 9,629 | 86,014 | 11.2% |

Local month coverage:

- **2020**: Jan-Dec
- **2021**: Jan-Mar, Jul-Sep
- **2022**: Jan-Mar, Jul-Dec
- **2023**: Jan-Sep
- **2024**: Jan-Mar, Jul-Dec
- **2025**: Jan-Dec
- **2026**: Jan-Mar

## Data layout

Each filing-day folder lives under `data/YYYY-MM-DD/`.

Typical contents:

- `day_summary.json`: filing-day level counts and run metadata
- `failed_cases.json`: cases that were discovered but not successfully scraped
- `sync_metadata.json`: upload/prune verification metadata when the day has been synced
- `CASE_NUMBER/register_of_actions.json`: case-level metadata and action rows
- `CASE_NUMBER/*.pdf`: downloaded docket PDFs when available

## Important limitations

- This is an **in-progress scrape**, not a final frozen release.
- The court site is operationally unstable under automation, so many days are only partially complete after the first pass.
- The Hugging Face repo may lag the local working corpus because upload and prune happen asynchronously.
- Coverage is not uniform across case families. Straight civil prefixes tend to scrape more reliably than some family, probate, or related case categories.
- This dataset is derived from a public court portal and should be used with appropriate care around privacy, legal process, and downstream publication.

## Intended use

This corpus is being built to support:

- legal text collection
- docket-level and order-level AI-use analysis
- retrieval and triage experiments over court filings
- measurement of detectable AI-generated language in litigation materials
