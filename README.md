# HBFA Ops ERP - Weekly Mylar Runbook

This captures the exact, working steps you're using today. Normal steps first, then optional backfill paths. Replace all `YYYY-MM-DD` with the actual report date you're processing.

## Chore 2025-11-06: Pre-kickoff downstream wiring

- Scope: `chore/2025-11-06`. Track the wiring so the new `pre_kickoff` flag suppresses downstream milestones without deleting overrides.
- Behavior gap (resolved): the React editor already blanked inputs, but the Netlify timeline/Mylar loaders still emitted stored overrides, so `pre_kickoff = true` buildings (e.g., SOMI B3 with a 2025-10-22 B3 anchor) continued to surface downstream.
- Timeline function (`netlify/functions/ops-milestones-timeline.js` in hbfa-ops): building-level `pre_kickoff` now blanks anchors/overrides before computing schedules, tags each unit response with the flag, and removes flagged buildings from anchor-gap counts.
- Polaris Mylar pipeline (`tools/polaris/report_pdf.py`, `tools/polaris/report_pdf_hso.py` in this repo): override index stores `pre_kickoff`, `_apply_ops_overrides`/HSO reducer drop Ops milestones whenever the flag is present, so staged overrides stay dormant until release.
- Mylar layout tweak (2025-11-06): appended an `Ops COE` column to the far right of the PDF (following `MS Date`) and plumbed the exporter so Ops-projected COE dates appear when overrides are active.
- Status: code and docs updated locally on 2025-11-06; pushes pending confirmation below.

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
## Work plan (2025-11-04)
- Get Ops milestones into Fusion's Mylar without re-importing stale Fusion status rows.
- Explore an orchestrator-style flow that accepts the Fusion-filtered `Polaris_Processed` output so normalization/import stay optional.
- Update TrackingForm and the Netlify offers handler so entering `week_ratified_date` also sets `StatusNumeric = 2` and `is_immutable = 1`.
- Document the Monday checklist (file path, command, timeline endpoint check) so the weekly runner can verify Fusion quickly.
- Add guardrails/tests to keep Fusion excluded unless explicitly toggled back on.


- Confirm status mapping for `Offer - Out for signature` remains `StatusNumeric = 3` end-to-end (import → canonical table → report). Add/adjust test coverage if needed.

## Status Ownership and Ops Overrides

- Current behavior: The report layer can override Status/StatusNumeric via `ops_milestones` based on the priority order `[closed, backlog, offer, inventory, unreleased, projected_coe]`.
- Policy going forward: Only the Sales UI should set Status and StatusNumeric. The PDF builder should only populate Ops milestone code/date (the B/U series), not change Status/SN.
- Action items:
  - Sales UI: ensure StatusNumeric is assigned consistently from Status during import/update.
  - Ops data: stop using override flags that imply Status (e.g., `inventory`, `unreleased`) and migrate legacy rows away from those flags. If an "Ops Status" is needed, add it explicitly rather than overloading Sales status.
  - Cleanup: legacy `ops_milestones` entries that still drive Status should be reviewed/normalized. Until then, manual corrections in Ops may be needed to avoid unintended flips (e.g., Pending Release vs Available).

## HSO Report: Ops Milestones Selection Logic

- Scope: Applies to `tools/polaris/report_pdf_hso.py` when reading `ops_milestones`.
- Building-only: Only building-level milestones (`unit_key = "#building"`) are considered; unit-level entries are ignored for the HSO report.
- As-of-today filter: From all building milestones, select the latest milestone whose date is not greater than today (on-the-ground view).
- Blank if none: If no building milestone date is ≤ today, the "Ops MS" and "MS Date" cells are left blank.
- No status changes: This logic only populates the Ops milestone code/date columns and does not alter Status/StatusNumeric.

- Formatting:
  - Boolean fields (e.g., `Cash?`) render as `Y`/`N` in both the standard and HSO PDFs. Numeric variants like `0/1` or `0.0/1.0` are coerced accordingly.

## PDF Columns Update

- Added a new `Building` column between `Homesite` and `Status`.
- Renamed `Unit` column header to `Homesite`.
- The `Building` value is sourced from `ops_milestones` (`building_id`) and is populated when available.
- There is sufficient space on the page; other columns keep their widths. No change to data export commands.
- The `Cash?` column now renders as `Y`/`N` instead of numeric `1.0`/`0.0`.
  - Accepts booleans, numeric `0/1` (including `0.0/1.0`, `Decimal`), and common string forms (`yes/no`, `true/false`, `y/n`).
  - Any unrecognized value is passed through unchanged.

## Ops Milestones + Building ID Details

- As‑of‑today milestones:
  - Only milestones with a date ≤ today are considered to reflect on‑the‑ground status.
  - B→U handoff: if the latest building milestone ≤ today is B11, unit milestones (U1–U6) take precedence per unit; otherwise, show the latest qualifying building milestone; if none qualify, cells remain blank (construction not started).

- Building ID hydration in PDF:
  - The PDF column key is `building_id` (header shows “Building”).
  - `building_id` is pulled from `ops_milestones` for each unit, even when there are no milestone overrides, so Building appears without requiring a milestone.
  - A compatibility field `builder` is also populated with the same value for downstream consumers expecting that key.

- Robust DynamoDB parsing:
  - The report unpacks low‑level DynamoDB attribute maps (e.g., `{ "S": "…" }`) for fields like `pk`, `sk`, `project_id`, `unit_number`, `building_id`, `data`, and `updated_at` before processing.
  - Milestone payloads in `data` are JSON‑decoded after unwrapping.

- Unit key matching:
  - When looking up unit overrides and building metadata, the report tries multiple unit key variants to match ops data: the full unit key, the suffix after the last dash, and trailing digits (e.g., `HayView-306` → `306`).

- HSO reducer behavior (when generating via `report_pdf_hso.py`):
  - Applies the as‑of‑today B→U handoff.
  - Preserves and emits `building_id` for units even if the project has no building milestones ≤ today, so Building still shows in the PDF while milestone cells remain blank.

- Debugging lookups (optional):
  - Set `HBFA_DEBUG_OPS=1` to print per‑row diagnostics to stderr showing candidate project keys, matched override keys, and whether a `building_id` was found.

## Bootstrap or Reload Ops Milestones from CSV

- Convert CSV → PutRequests JSON:
  - Command:
    - `python -m tools.polaris.csv_to_ops_puts d:\\downloads\\hbfa-ops\\aria_ops_milestones.csv d:\\downloads\\hbfa-ops-erp\\aria_ops_puts.json --project Aria`
  - CSV headers supported: `project_id` (optional if `--project` given), `building_id`, `unit_number`.
  - Optional per‑row milestone/date: add `milestone` and `date` to include a single override; omit to leave milestones blank (not started).

- Load into DynamoDB:
  - Command:
    - `python -m tools.polaris.load_ops_milestones d:\\downloads\\hbfa-ops-erp\\aria_ops_puts.json --table-name ops_milestones --region us-west-1 --profile <aws-profile>`
  - Dry‑run suggestion: inspect `aria_ops_puts.json` before loading. If you need a full dry‑run, load to a temporary table name (e.g., `ops_milestones_staging`) and verify via the report with `--ops-table ops_milestones_staging`.

- Verify in PDF:
  - Run `report_pdf_hso`; Building hydrates from `building_id` even if milestones are blank.

## Import Canonical Rows into hbfa_sales_offers (Aria)

- Import CSV using the existing HBFASales importer:
  - Repo: `D:\Downloads\HBFASales\hbfa-sales-ui`
  - Command (example):
    - `node scripts\import-hbfa-sales-offers.mjs --file="D:\Downloads\HBFASales\hbfa-sales-ui\aria_hbfa_sales_offers.csv" --project=Aria`
  - Environment:
    - `AWS_REGION` defaults to `us-east-2` (set if needed).
    - `AWS_PROFILE` if you use profiles.
  - CSV expectations (minimum):
    - `Project Name` or `AltProjectName` = `Aria`
    - `Contract Unit Number` = homesite (numeric)
    - `Status` and `StatusNumeric` (e.g., `Pending Release`, `5`)
  - Dry‑run guidance:
    - If the importer supports `--dry-run`, use it first. Otherwise, test with a small subset CSV (2–3 rows), verify in the console, then load the full file.

- Generate the Mylar after imports:
  - Command:
    - `python -m tools.polaris.report_pdf_hso --output "D:\\Downloads\\hbfa-ops-erp\\reports\\mylar-YYYY-MM-DD.pdf"`
  - Notes:
    - The report combines hbfa_sales_offers (canonical) with ops_milestones (Building/milestones). Ensure unit numbers and project names align (unit numbers numeric; project `Aria`).

### Quick Subset Generator (for dry-run tests)

- Create a small (e.g., 3-row) subset of a larger CSV while keeping headers:
  - Command (example):
    - `python -m tools.polaris.csv_subset D:\\Downloads\\HBFASales\\hbfa-sales-ui\\aria_hbfa_sales_offers.csv D:\\Downloads\\hbfa-ops-erp\\aria_subset.csv --rows 3`
  - Use the subset file with your importer’s dry-run (if supported) or as a minimal live test before loading the full dataset.
`## Legacy Polaris Preprocessor Scripts

Reference copies of the historical Excel processors live under `C:\Users\foxda\OneDrive\Documents\PythonScripts`. Keep these handy while the new orchestration is wired up:
- `ExtractPolaris.py`
- `ExtractPolaris2Files.py`
- `ExtractPolarisConst.py`
- `ExtractPolarisContract.py`
- `ExtractPolarisFull.V.1.py`
- `ExtractPolarisFull.V.2.py`
- `ExtractPolarisFull.V.3.py`
- `ExtractPolarisFull.V.4.py`
- `ExtractPolarisFull.V.5.py`
- `ExtractPolarisFull.V.7.py`
- `ExtractPolarisFull.V.7.1.py`
- `ExtractPolarisFull.V.7.2.py`
- `ExtractPolarisFull.V.7.3.py`
- `ExtractPolarisFull.V.7.4.py`
- `ExtractPolarisFull.V.8.py`
- `ExtractPolarisFull.V.8.1.py`
- `ExtractPolarisFull.V.8.2.noover 8.1.py`
- `ExtractPolarisFull.V.8.3.py`
- `ExtractPolarisUnit.py`
- `ExtractPolarisUnit.V.2.py`
- `ExtractPolarisUnit.V.3.py`
- `ExtractPolarisWeekUpdate.py`
- `ExtractPolarisWeekUpdate.V.2.py`
- `ExtractPolarisWeekUpdate.V.3.py`


