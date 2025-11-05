from __future__ import annotations

import argparse
from datetime import datetime, timezone, date
from pathlib import Path
from typing import List, Optional, Sequence
import sys

import pandas as pd

from .combined import combine_sources
from .processing import DEFAULT_COLUMNS, DEFAULT_SHEET_NAME, DEFAULT_SKIPROWS
from . import report_pdf as legacy_report


def _merge_columns(
    base_columns: Sequence[str], extras: Optional[Sequence[str]]
) -> List[str]:
    if not extras:
        return list(base_columns)
    merged = list(base_columns)
    for col in extras:
        if col not in merged:
            merged.append(col)
    return merged


def _build_pk(df: pd.DataFrame) -> pd.Series:
    project = (
        df.get("Project Name", pd.Series("", dtype="object"))
        .fillna("")
        .astype(str)
        .str.strip()
    )
    unit = (
        df.get("Contract Unit Number", pd.Series("", dtype="object"))
        .fillna("")
        .astype(str)
        .str.strip()
    )
    pk = project + "#" + unit
    pk = pk.str.strip("#")
    pk = pk.where(pk != "", pd.NA)
    return pk


def _load_ops_overrides(
    table_name: str,
    region: str,
    profile: Optional[str],
) -> dict[tuple[str, str], dict]:
    if not table_name:
        return {}
    try:
        items = legacy_report._load_items(  # type: ignore[attr-defined]
            table_name,
            region=region,
            profile=profile,
        )
        return legacy_report._build_ops_override_index(items)  # type: ignore[attr-defined]
    except Exception as exc:  # pylint: disable=broad-except
        print(
            f"Warning: unable to load ops milestones from {table_name}: {exc}",
            file=sys.stderr,
        )
        return {}


def _select_latest_milestone_for_today(
    overrides_map: dict,
    codes: tuple[str, ...],
) -> Optional[tuple[str, str, str]]:
    """
    From an overrides map and a sequence of milestone codes (e.g., BUILDING or UNIT),
    pick the latest milestone whose date is not greater than today.

    Returns (code, key, value) or None if none qualify.
    """
    if not isinstance(overrides_map, dict) or not overrides_map:
        return None
    today = date.today()
    candidates: list[tuple[pd.Timestamp, str, str, str]] = []  # (ts, code, key, value)
    key_map = getattr(legacy_report, "MILESTONE_KEY_MAP", {})  # type: ignore[attr-defined]
    for code in codes:
        for key in key_map.get(code, ()):  # type: ignore[index]
            if key not in overrides_map:
                continue
            value = overrides_map.get(key)
            if value in (None, "", False):
                continue
            parsed = pd.to_datetime(value, errors="coerce", utc=True)
            if pd.isna(parsed):
                continue
            parsed_date = parsed.tz_convert("UTC").date()
            if parsed_date <= today:
                candidates.append((parsed.tz_convert("UTC"), code, key, str(value)))
    if not candidates:
        return None
    candidates.sort(key=lambda t: t[0])
    _, sel_code, sel_key, sel_val = candidates[-1]
    return sel_code, sel_key, sel_val


def _reduce_overrides_asof_today(    overrides: dict[tuple[str, str], dict]) -> dict[tuple[str, str], dict]:    """    For each project/unit pair, compute the as-of-today milestone with handoff rules:    - If no building milestone (B1..B11) has a date <= today: no construction started -> blank.    - If latest building milestone <= today is before B11: use that building milestone.    - If latest building milestone is B11: prefer the latest unit milestone (U1..U6) with date <= today; if none, keep B11.    The returned overrides map is reduced so legacy selection picks exactly one milestone per row.    """    if not overrides:        return {}    result: dict[tuple[str, str], dict] = {}    # Group by project for convenience    projects: dict[str, dict[str, dict]] = {}    for (project_key, unit_key), payload in overrides.items():        projects.setdefault(project_key, {})[unit_key] = payload    building_codes = tuple(getattr(legacy_report, "BUILDING_MILESTONE_CODES", ()))  # type: ignore[attr-defined]    unit_codes = tuple(getattr(legacy_report, "UNIT_MILESTONE_CODES", ()))  # type: ignore[attr-defined]    build_key_factory = getattr(        legacy_report,        "_build_building_lookup_key",        lambda normalized: f"#building::{normalized or 'unknown'}",    )    for project_key, entries in projects.items():        building_entries = {            key: payload            for key, payload in entries.items()            if isinstance(key, str) and key.startswith("#building")        }        building_selection: dict[str, dict] = {}        for b_key, b_payload in building_entries.items():            overrides_dict = b_payload.get("overrides") if isinstance(b_payload, dict) else None            b_sel = _select_latest_milestone_for_today(overrides_dict or {}, building_codes)            timestamp = b_payload.get("timestamp") if isinstance(b_payload, dict) else None            building_id = b_payload.get("building_id") if isinstance(b_payload, dict) else None            normalized_building = b_payload.get("normalized_building_id") if isinstance(b_payload, dict) else None            if b_sel is None:                result[(project_key, b_key)] = {                    "overrides": {},                    "timestamp": timestamp,                    "building_id": building_id,                    "normalized_building_id": normalized_building,                }                building_selection[b_key] = {                    "code": None,                    "payload": b_payload,                }                continue            b_code, b_milestone_key, b_value = b_sel            result[(project_key, b_key)] = {                "overrides": {b_milestone_key: b_value},                "timestamp": timestamp,                "building_id": building_id,                "normalized_building_id": normalized_building,            }            building_selection[b_key] = {                "code": b_code,                "milestone_key": b_milestone_key,                "milestone_value": b_value,                "payload": b_payload,            }        if not building_selection:            fallback_key = build_key_factory(None)            building_selection[fallback_key] = {                "code": None,                "payload": {},            }            result[(project_key, fallback_key)] = {                "overrides": {},                "timestamp": None,                "building_id": None,                "normalized_building_id": None,            }        for unit_key, u_payload in entries.items():            if isinstance(unit_key, str) and unit_key.startswith("#building"):                continue            if not isinstance(u_payload, dict):                continue            unit_building_norm = u_payload.get("normalized_building_id")            preferred_building_key = (                build_key_factory(unit_building_norm) if unit_building_norm else None            )            building_entry = None            building_lookup_key = None            if preferred_building_key and preferred_building_key in building_selection:                building_entry = building_selection[preferred_building_key]                building_lookup_key = preferred_building_key            else:                building_lookup_key, building_entry = next(iter(building_selection.items()))            building_payload = result.get((project_key, building_lookup_key), {}) if building_lookup_key else {}            building_code = building_entry.get("code") if building_entry else None            result[(project_key, unit_key)] = {                "overrides": {},                "timestamp": u_payload.get("timestamp"),                "building_id": u_payload.get("building_id") or building_payload.get("building_id"),                "normalized_building_id": unit_building_norm or building_payload.get("normalized_building_id"),            }            if building_code != "B11":                continue            unit_overrides = u_payload.get("overrides") or {}            u_sel = _select_latest_milestone_for_today(unit_overrides, unit_codes)            if u_sel is None:                continue            _, u_milestone_key, u_value = u_sel            result[(project_key, unit_key)] = {                "overrides": {u_milestone_key: u_value},                "timestamp": u_payload.get("timestamp"),                "building_id": u_payload.get("building_id") or building_payload.get("building_id"),                "normalized_building_id": unit_building_norm or building_payload.get("normalized_building_id"),            }    return result

def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Generate the HBFA Mylar PDF using the canonical "
            "hbfa_sales_offers dataset (with optional Polaris supplement)."
        )
    )
    parser.add_argument(
        "--polaris",
        help="Optional path to a Polaris Excel export (.xlsx) to merge in.",
    )
    parser.add_argument(
        "--sheet-name",
        default=DEFAULT_SHEET_NAME,
        help=f"Worksheet name when reading a Polaris export (default: {DEFAULT_SHEET_NAME}).",
    )
    parser.add_argument(
        "--skiprows",
        type=int,
        default=DEFAULT_SKIPROWS,
        help=f"Header rows to skip in Polaris export (default: {DEFAULT_SKIPROWS}).",
    )
    parser.add_argument(
        "--hso-table",
        default="hbfa_sales_offers",
        help="hbfa_sales_offers DynamoDB table name (default: hbfa_sales_offers).",
    )
    parser.add_argument(
        "--hso-region",
        default="us-east-2",
        help="AWS region for hbfa_sales_offers (default: us-east-2).",
    )
    parser.add_argument(
        "--project",
        dest="projects",
        action="append",
        help="Optional project_id filter; repeat to include multiple projects.",
    )
    parser.add_argument(
        "--include-column",
        dest="extra_columns",
        action="append",
        help="Additional columns to retain in the dataset.",
    )
    parser.add_argument(
        "--ops-table",
        default="ops_milestones",
        help="DynamoDB table for ops milestones overrides (default: ops_milestones).",
    )
    parser.add_argument(
        "--ops-region",
        default="us-west-1",
        help="AWS region for ops milestones table (default: us-west-1).",
    )
    parser.add_argument(
        "--profile",
        help="Optional AWS profile for DynamoDB access.",
    )
    parser.add_argument(
        "--output",
        help=(
            "Path to the PDF file that will be written. "
            "Defaults to reports/mylar-<today>.pdf when omitted."
        ),
    )
    parser.add_argument(
        "--logo",
        help="Optional path to a logo image (PNG/JPG) to render in the header.",
    )
    parser.add_argument(
        "--title",
        default="Sales Transactions & Construction Sequence Summary",
        help="Override the report title.",
    )
    parser.add_argument(
        "--subtitle",
        help="Override the report subtitle. Defaults to generated timestamp.",
    )
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = build_argument_parser()
    args = parser.parse_args(argv)

    output_path: Path
    if args.output:
        output_path = Path(args.output)
    else:
        today = datetime.now().strftime("%Y-%m-%d")
        output_path = Path("reports") / f"mylar-{today}.pdf"
        print(f"No --output provided; writing to {output_path}")

    columns_to_keep = _merge_columns(DEFAULT_COLUMNS, args.extra_columns)

    combined_df = combine_sources(
        polaris_path=args.polaris,
        sheet_name=args.sheet_name,
        skiprows=args.skiprows,
        columns_to_keep=columns_to_keep,
        include_projects=args.projects,
        table_name=args.hso_table,
        region=args.hso_region,
        profile=args.profile,
    )

    if combined_df.empty:
        print("No records found for the requested parameters.", file=sys.stderr)
        return 1

    combined_df = combined_df.copy()
    combined_df["pk"] = _build_pk(combined_df)
    combined_df["ExtractedAt"] = datetime.now(timezone.utc).isoformat()

    records = combined_df.to_dict("records")

    ops_table = (args.ops_table or "").strip()
    raw_ops_overrides = _load_ops_overrides(
        ops_table,
        region=args.ops_region,
        profile=args.profile,
    )
    # Apply as-of-today milestone reduction with B→U handoff logic
    ops_overrides = _reduce_overrides_asof_today(raw_ops_overrides)

    original_excluded = getattr(legacy_report, "EXCLUDED_PROJECTS", set())
    legacy_report.EXCLUDED_PROJECTS = set()

    try:
        table_df, summary_df = legacy_report.build_dataframe(  # type: ignore[attr-defined]
            records,
            overrides=ops_overrides,
        )
    finally:
        legacy_report.EXCLUDED_PROJECTS = original_excluded

    charts_dir = output_path.parent / "_charts_temp"
    chart_paths = legacy_report.generate_summary_charts(  # type: ignore[attr-defined]
        summary_df,
        charts_dir,
    )

    subtitle = args.subtitle or f"Generated {datetime.now():%m/%d/%Y %I:%M %p}"
    legacy_report.generate_pdf(  # type: ignore[attr-defined]
        table_df,
        output_path,
        title=args.title,
        subtitle=subtitle,
        logo_path=args.logo,
        chart_images=chart_paths,
    )

    for chart_path in chart_paths:
        try:
            chart_path.unlink(missing_ok=True)
        except Exception:
            pass
    if chart_paths and charts_dir.exists():
        try:
            charts_dir.rmdir()
        except OSError:
            pass

    print(f"Wrote report to {output_path}")

    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())


