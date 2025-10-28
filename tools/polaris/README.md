# Polaris Normalization Module (Draft)

## Purpose
- Extract Excel-to-structured-data normalization logic from `ExtractPolarisFull.V.8.4.py`.
- Provide a headless, scriptable entry point that works in CLI, Lambda, or Glue environments.
- Preserve Access report expectations (`StatusNumeric`, combined buyer names, SoMi unit renumbering, AltProjectName).
- Milestone ingestion tooling now lives under `tools/hbfa/milestones` (see that README for the successor CLI).

## Proposed Package Layout
```
tools/
  polaris/
    __init__.py
    aws.py                # S3 download + DynamoDB write helpers
    processing.py         # Core normalization logic (pandas-based)
    io.py                 # Utility helpers for reading Excel / writing JSON or Parquet
    cli.py                # Thin argparse wrapper for local runs and AWS Lambdas via awslambdaric
    combined.py           # hbfa_sales_offers + Polaris merger
    report_pdf_hso.py     # Canonical Mylar generator
    report_pdf.py         # Legacy Polaris-only PDF generator
    tests/
      test_processing.py  # Unit tests for status mapping, buyer combining, renumbering, etc.
      fixtures/
        sample_export.xlsx # Minimal replicate of Polaris export for regression tests
```

## Primary Entry Points
- `process_polaris_export(input_path: str, sheet_name: str = "HBFA Report") -> list[dict]`
  - Returns cleaned dictionaries ready for DynamoDB or JSON serialization.
- `process_s3_export(s3_uri: str)` downloads from S3, normalizes, and yields JSON-ready rows.
- `write_records_to_dynamodb(records, table_name, overwrite_keys=None)` performs a batch put with type conversion safety.
- **Combined dataset builder**
  - `python -m tools.polaris.combined --polaris <optional.xlsx> --output out.json`
  - Pulls the canonical `hbfa_sales_offers` table, optionally layers in a fresh Polaris export, and produces a unified dataframe with the standard column set. Use `--project` to filter by project_id and `--profile` to choose AWS credentials.
- CLI
  - `python -m tools.polaris.cli --input path_or_s3 --output out.json --format json|parquet`
  - `--dynamodb-table`/`--overwrite-keys` route cleaned rows directly into DynamoDB when boto3 is available.
- PDF generators
- `python -m tools.polaris.report_pdf_hso --output reports/mylar.pdf --profile <aws-profile>`
  - Primary flow: reads `hbfa_sales_offers`, optionally merges a Polaris export, applies `ops_milestones` overrides, and renders the Mylar PDF.
- `python -m tools.polaris.report_pdf --output reports/mylar.pdf --profile <aws-profile>`
  - Legacy flow: reads `hbfa_PolarisRaw`; kept for historical comparisons.
- Both require `boto3`, `pandas`, `fpdf2`, `matplotlib` (`pip install boto3 pandas fpdf2 matplotlib`).
- **Ops milestone key normalization**
  - `python tools/polaris/normalize_ops_keys.py --profile hbfa-secure-uploader`
    - Dry-run (default) scans `ops_milestones`, reporting any rows whose `pk`/`sk` do not match the canonical project/unit format (`SoMi Towns`, `SoMi A`, `SoMi B`, `Fusion`, `Aria`, `Vida`, plus `HayView-###` / `Fusion-###` unit numbers).
    - Use the PowerShell commands below to hydrate the canonical keys before Mylar pulls Ops milestones into the `Ops MS` / `MS Date` columns:
      ```powershell
      # Preview the planned rewrites
      python tools/polaris/normalize_ops_keys.py `
        --profile hbfa-secure-uploader `
        --region us-west-1

      # Apply changes and append a JSONL backup (repeatable any time)
      python tools/polaris/normalize_ops_keys.py `
        --profile hbfa-secure-uploader `
        --region us-west-1 `
        --apply `
        --backup ops-normalize-2025-10-23.jsonl
      ```
    - Canonical rows are created (or confirmed) and the legacy aliases are deleted so the Mylar exporter can join Ops milestones, set the two-character milestone code, and populate the matching milestone date.
    - Prompt snippet for future runs:
      ```text
      You are cleaning up the ops_milestones table so Mylar can hydrate Ops MS / MS Date. Run normalize_ops_keys.py in dry-run mode first, then apply with a dated JSONL backup once the summary shows the expected aliases (SoMi → Towns/A/B, Fusion, Aria, Vida). Confirm the follow-up dry-run reports “prepared 0 rewrites.”
      ```

## PDF Report Generators

- `report_pdf_hso.py` generates the weekly "Mylar" directly from `hbfa_sales_offers` (canonical source). Units marked "Available" remain green unless an explicit override in `ops_milestones` says otherwise.
- Optional Polaris exports can be merged when you need to backfill the canonical table or compare side-by-side; pass `--polaris path/to/export.xlsx`.
- `report_pdf.py` remains available as a fallback for the Polaris-only workflow.

### Workflow
- `report_pdf_hso.py` scans `hbfa_sales_offers` (default region us-east-2) using the supplied AWS profile (needs `dynamodb:Scan` on the table and CMK decrypt if applicable).
- Optional: merges `ops_milestones` (default region us-west-1) to apply Ops overrides.
- Optional: merges a Polaris export via `combine_sources`.
- Sort order: `AltProjectName`, `StatusNumeric`, then `Buyer Contract: COE Date` (status 1) or `Contract Unit Number` (statuses 2-5). Closed rows are limited to the current calendar year.
- Tabloid-landscape (11" x 17") PDF with a cover dashboard and the detailed table.

### Table formatting
- Columns (left → right):
  `AltProjectName`, `Contract Unit Number`, `Status`, `Buyer Contract: COE Date`, `Buyers Combined`, `Buyer Contract: Cash?`, `Buyer Contract: Investor/Owner`, `Buyer Contract: Initial Deposit Amount`, `List Price`, `Buyer Contract: Base Price`, `Final Price`, `Buyer Contract: Contract Sent Date`, `Buyer Contract: Appraiser Visit Date`, `Buyer Contract: Notes`.
- Highlight colors by `StatusNumeric`:
  1 → light red, 2-3 → light orange, 4 → light green, 5 → no fill.
- Currency/null handling avoids `$NaN`; boolean fields render `Yes/No`.
- Output is written to the requested path; parent directories are created automatically.

### Cover dashboard
Charts are rendered to `_charts_temp/` and layered via `_draw_cover_page`.

| Chart | Definition | Notes |
| --- | --- | --- |
| YTD Sales by Project | Count of rows with `Buyer Contract: Week Ratified Date` in the current year. | Contracts deduped by `pk`; latest `ExtractedAt` wins. |
| YTD Closed by Project | Status 1 rows with current-year COE. | |
| Total Closed by Project | All status 1 rows. | |
| Backlog by Project | Status 2 rows. | |
| Inventory by Project | Unique `Contract Unit Number` minus closed (`StatusNumeric == 1`) units. | Horizontal bar chart; x-axis fixed to 0-130. |

- Donuts stay on the first row (max width 75 mm, legend offset 48 mm). Any additional charts flow to the next row with spacing to avoid legend overlap.
- Inventory bar uses consistent typography with the donuts and prints counts to the right edge.

### Color palette & cycling
- Project color assignments originate in `tools/polaris/report_pdf.py` inside `BASE_PROJECT_COLORS` and `_build_project_palette`.
- Colors cycle in list order; when a new `AltProjectName` appears it receives the next color (wrapping when the list is exhausted).
- To keep long-term consistency, reorder or replace the hex values in `BASE_PROJECT_COLORS` so the first N entries match your preferred ramp, then regenerate the PDF to review. Existing projects reuse their assigned color for the session because the palette is rebuilt from the sorted project list each run.
- Quick reference for the current brand ramp (the first five slots align with key projects):
  `Fusion → #66c2a5`, `SoMi Condos → #fc8d62`, `SoMi Hayview → #b3b3b3`, `SoMi Towns → #fdbf6f`, `Vida → #8da0cb`.

### Running locally
```
pip install boto3 pandas fpdf2 matplotlib
python -m tools.polaris.report_pdf_hso ^
  --profile hbfa-secure-uploader ^
  --hso-table hbfa_sales_offers ^
  --hso-region us-east-2 ^
  --ops-table ops_milestones ^
  --ops-region us-west-1 ^
  --output reports/mylar.pdf ^
  --logo assets/bfa-logo.png
```
- `--polaris` can be supplied if you want to merge a new export on the fly.
- `--logo` is optional. The profile must also satisfy the relevant S3/KMS guardrails if you later upload the PDF.
- Temporary chart PNGs are deleted on success; failures leave them behind for inspection.

## Data Contracts
- Output dictionary keys: snake_case equivalents of current Excel columns plus derived fields:
  - `project_name`, `alt_project_name`, `status`, `status_numeric`, `buyers_combined`, `contract_unit_number`, `unit_name`, `coe_date`, etc.
  - Include metadata: `source_file`, `extracted_at`, `row_index`.
- Type coercion:
  - Dates via `pd.to_datetime(..., errors="coerce")`.
  - Currency via `Decimal`.
  - Text normalized with `.strip()` and uppercase/lowercase where needed.

## AWS Alignment
- Structured output aligns with DynamoDB `PolarisRaw` schema (`PK = project_name#contract_unit_number`, `SK = coe_date` or `status#<slug>` when the row has no activity dates).
- Parquet writer ensures `StatusNumeric` remains `INT`, buyer strings as `STRING`, dates as `TIMESTAMP`.
- Schema module describes Athena table columns for:
  - `polaris_raw`
  - `plan_milestone_summary`
  - `building_data`
  - `weekly_sales_agg`
- Reporting API: Lambda-backed endpoint that joins Dynamo projections with Athena views mirroring `qryTransactionReport_New` filters (status filters, date ranges, buyer search).

## Testing & Validation
- Golden-file regression: `tests/fixtures/sample_export.xlsx` powers pytest assertions for normalization and ensures "Total" summaries drop out.
- Focused unit tests cover `assign_status_numeric`, `renumber_units`, `generate_alt_project_name`, and `combine_buyers`.
- CLI smoke test: `python -m tools.polaris.cli --input tools/polaris/tests/fixtures/sample_export.xlsx --output /tmp/polaris.json`.
- Serialization checks: Confirm Parquet/JSON round-trips preserve `StatusNumeric` as integer and ISO-8601 datetime strings.

## AWS Pipeline Alignment
- Glue/Lambda wrapper should import `tools.polaris.cli.main` or call `process_polaris_export` directly with S3 object bytes.
- DynamoDB upsert contract: `pk = f"{project_name}#{contract_unit_number}"`, `sk = coe_date` when present, otherwise `status#<slug>` (e.g. `status#pending-release`) so repeat ingests update the same item, plus `ExtractedAt` timestamp for audit.
- Athena schema draft:
  - `polaris_raw(project_name string, alt_project_name string, contract_unit_number string, status string, status_numeric int, buyers_combined string, coe_date timestamp, ...)`
  - `plan_milestone_summary(alt_project_name string, milestone_name string, milestone_date timestamp, days_since_last_milestone int, updated_at timestamp)`
  - `building_data(alt_project_name string, unit_name string, square_footage int, hoa_dues decimal(10,2), phase string, updated_at timestamp)`
  - `weekly_sales_agg(alt_project_name string, year_week string, reservations int, ratifications int, cancellations int, backlog int, updated_at timestamp)`

## Immediate Next Actions
1. Produce infrastructure snippets (CDK/Terraform) defining S3 buckets, Glue catalog tables, and DynamoDB tables for review before implementation.
2. Align Sales/Ops project nomenclature (duplicate unit numbers in SoMi Hayward buildings) so canonical keys are unique in `hbfa_sales_offers`.

### 2025-10-22 Updates
- Added the combined dataset builder and `report_pdf_hso` so Mylar is generated from `hbfa_sales_offers` with optional Polaris backfill.
- Removed the legacy "inventory → pending release" fallback; inventory rows now retain their status unless an Ops override explicitly changes it.
- Legacy `report_pdf.py` remains for historical comparisons but no longer forces Fusion/inventory demotions.
