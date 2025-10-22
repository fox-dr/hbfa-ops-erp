from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from decimal import Decimal
from typing import Iterable, List, Optional, Sequence

import boto3
import pandas as pd
from boto3.dynamodb.conditions import Attr

from . import DEFAULT_COLUMNS, process_polaris_export
from .io import write_records
from .processing import (
    DATE_COLUMNS,
    DEFAULT_SHEET_NAME,
    DEFAULT_SKIPROWS,
    assign_status_numeric,
    combine_buyers,
    _coerce_dates,
    _ensure_columns,
    _finalize_records,
)

log = logging.getLogger(__name__)

DEFAULT_HSO_TABLE = os.environ.get("TARGET_TABLE", "hbfa_sales_offers")
DEFAULT_HSO_REGION = (
    os.environ.get("AWS_REGION")
    or os.environ.get("AWS_DEFAULT_REGION")
    or "us-east-2"
)


def _merge_columns(
    base_columns: Sequence[str], extras: Optional[Iterable[str]]
) -> Sequence[str]:
    if not extras:
        return list(base_columns)
    merged = list(base_columns)
    for col in extras:
        if col not in merged:
            merged.append(col)
    return merged


def _convert_decimal(value):
    if isinstance(value, Decimal):
        if value % 1 == 0:
            return int(value)
        return float(value)
    if isinstance(value, list):
        return [_convert_decimal(v) for v in value]
    if isinstance(value, dict):
        return {k: _convert_decimal(v) for k, v in value.items()}
    return value


def _cash_display(value: object) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, bool):
        return "Yes" if value else "No"
    text = str(value).strip().lower()
    if not text:
        return None
    if text in {"yes", "y", "true", "1"}:
        return "Yes"
    if text in {"no", "n", "false", "0"}:
        return "No"
    return str(value)


def _buyers_combined(item: dict) -> Optional[str]:
    buyers = item.get("buyers_combined")
    if buyers:
        return buyers
    series = pd.Series(
        {
            "Buyer Contract: Buyer 1: Full Name": item.get("buyer_1__full_name"),
            "Buyer Contract: Buyer 2: Full Name": item.get("buyer_2_full_name"),
        }
    )
    return combine_buyers(series)


def _map_hso_item(item: dict, columns: Sequence[str]) -> dict:
    record = {col: None for col in columns}

    project_name = item.get("project_name") or item.get("project_id")
    contract_unit_number = item.get("contract_unit_number") or item.get("unit_number")
    unit_name = item.get("unit_name") or item.get("unit_number")

    record["Project Name"] = project_name
    record["AltProjectName"] = (
        item.get("alt_project_name") or project_name or item.get("project_id")
    )
    record["Contract Unit Number"] = contract_unit_number
    record["Unit Name"] = unit_name
    record["Buyer Contract: Unit Name"] = unit_name
    record["Buyer Contract: Base Price"] = item.get("base_price")
    record["Buyer Contract: Buyer 1: Full Name"] = item.get("buyer_1__full_name")
    record["Buyer Contract: Buyer 2: Full Name"] = item.get("buyer_2_full_name")
    record["Buyer Contract: Buyer 2 Email"] = item.get("buyer_2_email")
    record["Buyer Contract: Appraiser Visit Date"] = item.get("appraiser_visit_date")
    record["Buyer Contract: COE Date"] = item.get("coe_date")
    record["Buyer Contract: Extended/Adjusted COE"] = (
        item.get("extended_adjusted_coe") or item.get("adjusted_coe")
    )
    record["Buyer Contract: Primary Lender"] = item.get("primary_lender")
    record[
        "Buyer Contract: Primary Loan Officer: Full Name"
    ] = item.get("primary_loan_officer_full_name")
    record["Buyer Contract: Projected Closing Date"] = item.get(
        "projected_closing_date"
    )
    record["Buyer Contract: Total Credits"] = item.get("total_credits")
    record["Buyer Contract: Week Ratified Date"] = item.get("week_ratified_date")
    record["Buyer Contract: Buyer - Email"] = (
        item.get("buyer1_email")
        or item.get("buyer_email")
        or item.get("buyer_primary_email")
    )
    record["Buyer Contract: Buyer - Mobile Phone"] = item.get("buyer_mobile_phone")
    record["Buyer Contract: Cash?"] = _cash_display(
        item.get("cash") or item.get("cash_purchase")
    )
    record["Buyer Contract: Contract Sent Date"] = item.get("contract_sent_date")
    record["Buyer Contract: Deposits Received to Date"] = item.get(
        "deposits_received_to_date"
    )
    record["Escrow Number"] = item.get("escrow_number")
    record["Final Price"] = item.get("final_price")
    record["Buyer Contract: Financing Contingency Date"] = item.get(
        "financing_contingency_date"
    )
    record["Buyer Contract: Fully Executed Date"] = item.get("fully_executed_date")
    record["Buyer Contract: HOA Credit"] = item.get("hoa_credit")
    record["Buyer Contract: Initial Deposit Amount"] = item.get(
        "initial_deposit_amount"
    )
    record["Buyer Contract: Initial Deposit Receipt Date"] = item.get(
        "initial_deposit_receipt_date"
    )
    record["Buyer Contract: Investor/Owner"] = _cash_display(
        item.get("investor_owner")
    )
    record["List Price"] = item.get("list_price")
    record["Lot Number"] = item.get("lot_number")
    record["Buyer Contract: Notes"] = item.get("notes")
    record["Buyer Contract: Unit Phase"] = item.get("unit_phase")
    record["Buyer Contract: Agent Brokerage"] = item.get("agent_brokerage")
    record["Buyer Contract: Referring Agent: Email"] = item.get(
        "referring_agent_email"
    )
    record["Buyer Contract: Referring Agent: Full Name"] = item.get(
        "referring_agent_full_name"
    )
    record["Buyer Contract: Seller Credit"] = item.get("seller_credit")
    record["Buyer Contract: Total Upgrades + Solar"] = item.get(
        "total_upgrades_solar"
    )
    record["Unit Number"] = item.get("unit_number") or contract_unit_number
    record["Buyer Contract: Upgrade Credit"] = item.get("upgrade_credit")
    record["Status"] = item.get("status")
    record["StatusNumeric"] = item.get("statusnumeric")
    record["Buyers Combined"] = _buyers_combined(item)

    return {k: _convert_decimal(v) for k, v in record.items()}


def _scan_hso(
    table_name: str,
    region: str,
    include_projects: Optional[Iterable[str]] = None,
) -> List[dict]:
    resource = boto3.resource("dynamodb", region_name=region)
    table = resource.Table(table_name)
    scan_kwargs = {}
    if include_projects:
        projects = [p for p in include_projects if p]
        if projects:
            scan_kwargs["FilterExpression"] = Attr("project_id").is_in(projects)

    items: List[dict] = []
    last_key = None
    while True:
        if last_key:
            scan_kwargs["ExclusiveStartKey"] = last_key
        response = table.scan(**scan_kwargs)
        items.extend(response.get("Items", []))
        last_key = response.get("LastEvaluatedKey")
        if not last_key:
            break
    return items


def load_hso_dataframe(
    *,
    table_name: str = DEFAULT_HSO_TABLE,
    region: str = DEFAULT_HSO_REGION,
    columns_to_keep: Optional[Sequence[str]] = None,
    include_projects: Optional[Iterable[str]] = None,
) -> pd.DataFrame:
    columns_to_keep = list(columns_to_keep or DEFAULT_COLUMNS)
    items = _scan_hso(table_name, region, include_projects)
    if not items:
        return pd.DataFrame(columns=columns_to_keep)

    rows = [_map_hso_item(_convert_decimal(item), columns_to_keep) for item in items]
    df = pd.DataFrame(rows)
    for col in columns_to_keep:
        if col not in df.columns:
            df[col] = pd.NA
    df = df[columns_to_keep]
    df = _coerce_dates(df, DATE_COLUMNS)

    if "StatusNumeric" in df.columns:
        needs_status = df["StatusNumeric"].isna() & df["Status"].notna()
        if needs_status.any():
            df.loc[needs_status, "StatusNumeric"] = df.loc[
                needs_status, "Status"
            ].map(assign_status_numeric)

    return df


def combine_sources(
    *,
    polaris_path: Optional[str] = None,
    sheet_name: str = DEFAULT_SHEET_NAME,
    skiprows: int = DEFAULT_SKIPROWS,
    columns_to_keep: Optional[Sequence[str]] = None,
    include_projects: Optional[Iterable[str]] = None,
    table_name: str = DEFAULT_HSO_TABLE,
    region: str = DEFAULT_HSO_REGION,
) -> pd.DataFrame:
    columns_to_keep = list(columns_to_keep or DEFAULT_COLUMNS)
    frames: List[pd.DataFrame] = []

    if polaris_path:
        polaris_df = process_polaris_export(
            polaris_path,
            sheet_name=sheet_name,
            skiprows=skiprows,
            columns_to_keep=columns_to_keep,
            as_records=False,
        )
        polaris_df = _ensure_columns(polaris_df, columns_to_keep)
        frames.append(polaris_df)

    hso_df = load_hso_dataframe(
        table_name=table_name,
        region=region,
        columns_to_keep=columns_to_keep,
        include_projects=include_projects,
    )
    if not hso_df.empty:
        frames.append(hso_df)

    if not frames:
        return pd.DataFrame(columns=columns_to_keep)

    combined = pd.concat(frames, ignore_index=True)
    for col in ("Project Name", "Contract Unit Number"):
        if col in combined.columns:
            combined[col] = combined[col].fillna("").astype(str).str.strip()

    combined = combined.drop_duplicates(
        subset=["Project Name", "Contract Unit Number"], keep="last"
    )

    combined = combined[columns_to_keep]
    combined = _coerce_dates(combined, DATE_COLUMNS)

    return combined


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Generate a consolidated HBFA report that merges Polaris data with "
            "the canonical hbfa_sales_offers DynamoDB table."
        )
    )
    parser.add_argument(
        "--polaris",
        help="Optional path to a Polaris Excel export (.xlsx).",
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
        default=DEFAULT_HSO_TABLE,
        help=f"DynamoDB table containing hbfa_sales_offers data (default: {DEFAULT_HSO_TABLE}).",
    )
    parser.add_argument(
        "--hso-region",
        default=DEFAULT_HSO_REGION,
        help=f"AWS region for the hbfa_sales_offers table (default: {DEFAULT_HSO_REGION}).",
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
        help="Additional columns to retain in the final dataset.",
    )
    parser.add_argument(
        "--output",
        help="Optional output path. If omitted, records are printed to stdout as JSON.",
    )
    parser.add_argument(
        "--format",
        choices=("json", "csv", "parquet"),
        help="Output format (inferred from output extension if not provided).",
    )
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = build_parser()
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
    )

    records = _finalize_records(combined_df)

    if args.output:
        write_records(records, args.output, args.format)
    else:
        json.dump(records, sys.stdout, indent=2)
        sys.stdout.write("\n")

    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
