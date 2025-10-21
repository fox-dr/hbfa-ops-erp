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
    processing.py          # Core normalization logic (pandas-based)
    io.py                  # Utility helpers for reading Excel / writing JSON or Parquet
    cli.py                 # Thin argparse wrapper for local runs and AWS Lambdas via awslambdaric
    schemas.py             # pydantic/dataclass definitions for row payloads and Dynamo/Athena projections
    tests/
      test_processing.py   # Unit tests for status mapping, buyer combining, renumbering, etc.
      fixtures/
        sample_export.xlsx # Minimal replicate of Polaris export for regression tests
```

## Primary Entry Points
- `process_polaris_export(input_path: str, sheet_name: str = "HBFA Report") -> list[dict]`
  - Returns cleaned dictionaries ready for DynamoDB or JSON serialization.
- `process_s3_export(s3_uri: str)` downloads from S3, normalizes, and yields JSON-ready rows.
- `write_records_to_dynamodb(records, table_name, overwrite_keys=None)` performs a batch put with type conversion safety.
- CLI
  - `python -m tools.polaris.cli --input path_or_s3 --output out.json --format json|parquet`
  - `--dynamodb-table`/`--overwrite-keys` route cleaned rows directly into DynamoDB when boto3 is available.
- PDF generator (new)
  - `python -m tools.polaris.report_pdf --output reports/mylar.pdf --profile <aws-profile>`
  - Pulls `hbfa_PolarisRaw` from DynamoDB, applies the Access-style sort/filter, renders charts, and writes a tabloid-landscape PDF.
  - Requires `boto3`, `pandas`, `fpdf2`, `matplotlib` (`pip install boto3 pandas fpdf2 matplotlib`).

## PDF Report Generator

- Fusion is now excluded from the normalized dataset (both the Dynamo ingest and the PDF pipeline omit rows where `Project Name = "Fusion"` or the derived `AltProjectName` is `Fusion`).
- When Polaris reports a unit as *Available* but there is no manual override in `ops_milestones`, the Mylar output demotes the row to *Pending Release* and renders it in grey. Overrides recorded in Ops (e.g., marking a unit as true inventory or backlog) take precedence and restore the corresponding color.
- The PDF generator reads the `ops_milestones` table (default name) and applies any backlog status overrides before building charts or tables. Pass `--ops-table ""` to disable the lookup.

`tools/polaris/report_pdf.py` produces the weekly "Mylar" summary without manual Access exports.

### Workflow
- Scans DynamoDB table `hbfa_PolarisRaw` using the provided AWS profile (needs `dynamodb:Scan` on the table and `kms:Decrypt` on the `hbfa-pii-west` CMK).
- Normalizes columns via `build_dataframe`, reusing the Access-compatible transformations.
- Sort order: `AltProjectName`, `StatusNumeric`, then `Buyer Contract: COE Date` (status 1) or `Contract Unit Number` (statuses 2–5). Closed rows are limited to the current calendar year.
- Tabloid-landscape (11" × 17") PDF with a cover dashboard and the detailed table.

### Table formatting
- Columns (left → right):  
  `AltProjectName`, `Contract Unit Number`, `Status`, `Buyer Contract: COE Date`, `Buyers Combined`, `Buyer Contract: Cash?`, `Buyer Contract: Investor/Owner`, `Buyer Contract: Initial Deposit Amount`, `List Price`, `Buyer Contract: Base Price`, `Final Price`, `Buyer Contract: Contract Sent Date`, `Buyer Contract: Appraiser Visit Date`, `Buyer Contract: Notes`.
- Highlight colors by `StatusNumeric`:  
  1 → light red, 2–3 → light orange, 4 → light green, 5 → no fill.
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

- Donuts stay on the first row (max width 75 mm, legend offset 48 mm). Any additional charts flow to the next row with spacing to avoid legend overlap.
- Inventory bar uses consistent typography with the donuts and prints counts to the right edge.

### Color palette & cycling
- Project color assignments originate in `tools/polaris/report_pdf.py` inside `BASE_PROJECT_COLORS` and `_build_project_palette`.
- Colors cycle in list order; when a new `AltProjectName` appears it receives the next color (wrapping when the list is exhausted).
- To keep long-term consistency, reorder or replace the hex values in `BASE_PROJECT_COLORS` so the first N entries match your preferred ramp, then regenerate the PDF to review. Existing projects reuse their assigned color for the session because the palette is rebuilt from the sorted project list each run.
- Quick reference for the current brand ramp (the first five slots align with key projects):  
  `Fusion → #66c2a5`, `SoMi Condos → #fc8d62`, `SoMi Hayview → #b3b3b3`, `SoMi Towns → #fdbf6f`, `Vida → #8da0cb`. Update the list order as new flagship communities are added so colors stay consistent over time.

### Running locally
```
pip install boto3 pandas fpdf2 matplotlib
python -m tools.polaris.report_pdf ^
  --profile hbfa-secure-uploader ^
  --region us-west-1 ^
  --output reports/mylar.pdf ^
  --logo assets/bfa-logo.png
```
- `--logo` is optional. The profile must also satisfy the S3/KMS guardrails if you later upload the PDF with the secure uploader CLI.
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
  - `PolarisRaw`
  - `PlanMilestoneSummary`
  - `BuildingData`
  - `WeeklySalesAgg`

## Testing & Validation
- Golden-file regression: `tests/fixtures/sample_export.xlsx` powers pytest assertions for normalization and ensures “Total” summaries drop out.
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
- Reporting API: Lambda-backed endpoint that joins Dynamo projections with Athena views mirroring `qryTransactionReport_New` filters (status filters, date ranges, buyer search).

## Immediate Next Actions
1. Produce infrastructure snippets (CDK/Terraform) defining S3 buckets, Glue catalog tables, and DynamoDB tables for review before implementation.



### 2025-10-20 Updates
- Fusion rows are filtered out because Polaris stopped publishing accurate data for that project; the Ops milestones table remains our source of truth.
- When Polaris reports a unit as Available but no Ops override exists, the Mylar now treats it as Pending Release (grey) unless an override explicitly sets a different status.
- The PDF generator now reads ops_milestones (configurable via --ops-table) before rendering so timeline and Mylar share the same backlog overrides.

