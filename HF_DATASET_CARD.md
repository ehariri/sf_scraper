---
pretty_name: SF Superior Court Docket and ROA Scrape
language:
  - en
license: other
---

# SF Superior Court Docket and ROA Scrape

This dataset contains San Francisco Superior Court filing-day search results, case-level register-of-actions exports, and downloaded docket PDFs collected from the court's public portal.

This card describes the dataset currently present in the Hugging Face repo as of **2026-03-19**.

## Overview

The Hugging Face dataset repo currently contains:

- **1259** filing-day folders
- **9,937** uploaded case-level `register_of_actions.json` files
- Earliest uploaded day: **2020-01-02**
- Latest uploaded day: **2025-12-31**
- Average uploaded cases per HF filing day: **7.9**
- Median uploaded cases per HF filing day: **7.0**
- HF uploaded scrape rate across matched uploaded days: **11.1%** (9937 uploaded / 89357 discovered across 1258 days)

In this project, a case counts as **uploaded** when a case directory with `register_of_actions.json` is present in the HF dataset repo.

## What Is In The Repo

Each filing day lives under `data/YYYY-MM-DD/`.

Typical contents:

- `day_summary.json`: filing-day level counts and scrape metadata
- `failed_cases.json`: cases discovered for that filing day that were not yet successfully scraped at the time of upload
- `CASE_NUMBER/register_of_actions.json`: case-level metadata and register-of-actions rows
- `CASE_NUMBER/*.pdf`: docket PDFs when document links were available

## Current HF Coverage

The table below summarizes the filing days and uploaded cases currently present in the HF repo.

| Year | Filing days in HF | Uploaded cases in HF |
| --- | ---: | ---: |
| 2020 | 230 | 1,401 |
| 2021 | 230 | 1,453 |
| 2022 | 229 | 1,967 |
| 2023 | 175 | 1,507 |
| 2024 | 159 | 1,568 |
| 2025 | 236 | 2,041 |
| Total | 1259 | 9,937 |

## Limitations

- This is an **in-progress scrape**, not a final frozen release.
- Many filing days are only partially complete.
- Coverage is not uniform across case families.
- The presence of `failed_cases.json` in a day folder means that filing day still had unresolved cases at the time that version of the day was uploaded.
- This dataset is derived from a public court portal and should be used with appropriate care around privacy, legal process, and downstream publication.

## Intended Use

This corpus is being built to support:

- legal text collection
- docket-level and order-level AI-use analysis
- retrieval and triage experiments over court filings
- measurement of detectable AI-generated language in litigation materials
