# HBFA Ops ERP – Weekly Mylar Runbook

This captures the exact, working steps you’re using today. Normal steps first, then optional backfill paths. Replace all `YYYY-MM-DD` with the actual report date you’re processing.

## Normal Steps

- Save email attachment
  - Save the weekly Excel as: `D:\Downloads\hbfa-ops-erp\HBFA Report-YYYY-MM-DD-hh-mm-ss.xlsx`

- Preprocess to CSV (legacy UI tool)
  - Double‑click your legacy processor on the saved Excel. It outputs:
    - `D:\Downloads\hbfa-ops-erp\Polaris_Processed.xlsx`
    - `D:\Downloads\hbfa-ops-erp\Polaris_Processed.csv`

- Import CSV → DynamoDB (raw + normalized)
  - Repo: `D:\Downloads\HBFASales\hbfa-sales-ui`
  - Command (Fusion excluded by default):
    - `node scripts\import-polaris-report.mjs --file="D:\Downloads\hbfa-ops-erp\Polaris_Processed.csv" --report-date=YYYY-MM-DD`
  - Notes:
    - Writes raw rows into `polaris_raw_weekly` and upserts normalized rows into `hbfa_sales_offers` (region `us-east-2`).
    - If you need to include Fusion in the import: set `POLARIS_INCLUDE_FUSION=true` for that run.

- Generate the Mylar PDF from canonical data
  - Repo: `D:\Downloads\hbfa-ops-erp`
  - Command:
    - `python -m tools.polaris.report_pdf_hso --output "D:\Downloads\hbfa-ops-erp\reports\mylar-YYYY-MM-DD.pdf"`

## Backfill / Recovery Steps

- Backfill Fusion from manual source
  - Repo: `D:\Downloads\HBFASales\hbfa-sales-ui`
  - Command:
    - `node scripts\backfill-hbfa-sales-offers.mjs`
  - Uses `fusion_offers` as source and writes into `hbfa_sales_offers`.

- Backfill Fusion from `polaris_raw_weekly` (optional helper)
  - Repo: `D:\Downloads\hbfa-ops-erp`
  - Command (dry‑run first):
    - `node scripts\backfill-fusion-from-praw.mjs --since=YYYY-MM-01 --until=YYYY-MM-DD --dry-run`
    - Remove `--dry-run` to write.
  - Filters `project = Fusion` by default; respects `AWS_REGION` (default `us-east-2`).

## Notes on Duplicates

- Unit numbers like `205.0` vs `205` can create dupes. The reporting path normalizes `Contract Unit Number` so that integer‑like floats (e.g., `205.0`) collapse to `"205"` before de‑duplication.
- If you ever see dupes in `hbfa_sales_offers`, delete the `".0"` variants and ensure future imports normalize unit numbers at write time.

## Future Consolidation (Prompt)

You are picking up the weekly Polaris → Mylar consolidation. Build a single desktop script/launcher that:
- Prompts for or detects the latest `HBFA Report-YYYY-MM-DD-*.xlsx`.
- Runs the legacy preprocessor to produce `Polaris_Processed.csv`.
- Imports the CSV to DynamoDB (`polaris_raw_weekly` + `hbfa_sales_offers`), optionally including Fusion.
- Generates the PDF via `tools.polaris.report_pdf_hso` and opens it.
- Logs steps and outcomes for easy auditing.

Start by wrapping the two existing commands and add Fusion as a toggle. Keep the initial email save as a manual step.

Open item to verify in code/tests:
- Confirm status mapping for `Offer - Out for signature` remains `StatusNumeric = 3` end-to-end (import → canonical table → report). Add/adjust test coverage if needed.
