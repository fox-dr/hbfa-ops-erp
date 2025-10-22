from __future__ import annotations

import argparse
from datetime import datetime
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
        "--profile",
        help="Optional AWS profile for DynamoDB access.",
    )
    parser.add_argument(
        "--output",
        required=True,
        help="Path to the PDF file that will be written.",
    )
    parser.add_argument(
        "--logo",
        help="Optional path to a logo image (PNG/JPG) to render in the header.",
    )
    parser.add_argument(
        "--title",
        default="Sales Summary and Transaction Report",
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
    combined_df["ExtractedAt"] = datetime.utcnow().isoformat()

    records = combined_df.to_dict("records")

    ops_table = (args.ops_table or "").strip()
    ops_overrides = _load_ops_overrides(
        ops_table,
        region=args.hso_region,
        profile=args.profile,
    )

    original_excluded = getattr(legacy_report, "EXCLUDED_PROJECTS", set())
    legacy_report.EXCLUDED_PROJECTS = set()

    try:
        table_df, summary_df = legacy_report.build_dataframe(  # type: ignore[attr-defined]
            records,
            overrides=ops_overrides,
        )
    finally:
        legacy_report.EXCLUDED_PROJECTS = original_excluded

    charts_dir = Path(args.output).parent / "_charts_temp"
    chart_paths = legacy_report.generate_summary_charts(  # type: ignore[attr-defined]
        summary_df,
        charts_dir,
    )

    subtitle = args.subtitle or f"Generated {datetime.now():%m/%d/%Y %I:%M %p}"
    legacy_report.generate_pdf(  # type: ignore[attr-defined]
        table_df,
        args.output,
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

    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
