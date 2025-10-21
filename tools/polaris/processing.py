from __future__ import annotations

import datetime as dt
import logging
import re
from pathlib import Path
from typing import Iterable, List, Optional, Sequence, Union

import pandas as pd

log = logging.getLogger(__name__)

# Mapping status to numeric values for sorting
STATUS_ORDER = {
    "Closed": 1,
    "Ratified - Fully executed": 2,
    "Offer - Out for signature": 3,
    "Available": 4,
    "Pending Release": 5,
}

EXCLUDED_PROJECTS = {"fusion"}

DEFAULT_SKIPROWS = 11
DEFAULT_SHEET_NAME = "HBFA Report"

DEFAULT_COLUMNS = [
    "Project Name",
    "AltProjectName",
    "Contract Unit Number",
    "Unit Name",
    "Buyer Contract: Unit Name",
    "Buyer Contract: Base Price",
    "Buyer Contract: Buyer 1: Full Name",
    "Buyer Contract: Buyer 2: Full Name",
    "Buyer Contract: Buyer 2 Email",
    "Buyer Contract: Appraiser Visit Date",
    "Buyer Contract: COE Date",
    "Buyer Contract: Extended/Adjusted COE",
    "Buyer Contract: Primary Lender",
    "Buyer Contract: Primary Loan Officer: Full Name",
    "Buyer Contract: Projected Closing Date",
    "Buyer Contract: Total Credits",
    "Buyer Contract: Week Ratified Date",
    "Buyer Contract: Buyer - Email",
    "Buyer Contract: Buyer - Mobile Phone",
    "Buyer Contract: Cash?",
    "Buyer Contract: Contract Sent Date",
    "Buyer Contract: Deposits Received to Date",
    "Escrow Number",
    "Final Price",
    "Buyer Contract: Financing Contingency Date",
    "Buyer Contract: Fully Executed Date",
    "Buyer Contract: HOA Credit",
    "Buyer Contract: Initial Deposit Amount",
    "Buyer Contract: Initial Deposit Receipt Date",
    "Buyer Contract: Investor/Owner",
    "List Price",
    "Lot Number",
    "Buyer Contract: Notes",
    "Buyer Contract: Unit Phase",
    "Buyer Contract: Agent Brokerage",
    "Buyer Contract: Referring Agent: Email",
    "Buyer Contract: Referring Agent: Full Name",
    "Buyer Contract: Seller Credit",
    "Buyer Contract: Total Upgrades + Solar",
    "Unit Number",
    "Buyer Contract: Upgrade Credit",
    "Status",
    "StatusNumeric",
    "Buyers Combined",
]

DATE_COLUMNS = {
    "Buyer Contract: Appraiser Visit Date",
    "Buyer Contract: COE Date",
    "Buyer Contract: Extended/Adjusted COE",
    "Buyer Contract: Projected Closing Date",
    "Buyer Contract: Week Ratified Date",
    "Buyer Contract: Contract Sent Date",
    "Buyer Contract: Financing Contingency Date",
    "Buyer Contract: Fully Executed Date",
    "Buyer Contract: Initial Deposit Receipt Date",
}


def assign_status_numeric(status: str) -> int:
    """Assign numeric value based on status ranking."""
    return STATUS_ORDER.get(status, 99)


def renumber_units(unit_name: object, contract_unit_number: object) -> object:
    """Renumber SoMi Haypark condo units >= 200 to avoid Access collisions."""
    if isinstance(unit_name, str) and "somi condos" in unit_name.lower():
        try:
            unit_number = int(contract_unit_number)
        except (TypeError, ValueError):
            return contract_unit_number
        if unit_number >= 200:
            return str(1000 + unit_number)
    return contract_unit_number


def generate_alt_project_name(row: pd.Series) -> str:
    """Derive AltProjectName for SoMi Hayward projects to retain Access compatibility."""
    project_name = row.get("Project Name", "")
    unit_name = row.get("Unit Name", "")

    if project_name != "SoMi Hayward":
        return project_name

    if isinstance(unit_name, str):
        if "SoMi HayPark" in unit_name:
            return "SoMi Towns"
        if "SoMi Haypark" in unit_name:
            return "SoMi Condos"
        if "SoMi HayView" in unit_name:
            return "SoMi HayView"
    return "SoMi Hayward"


def combine_buyers(row: pd.Series) -> str:
    """Combine buyer names into a single string."""
    buyer1_raw = row.get("Buyer Contract: Buyer 1: Full Name", "")
    buyer2_raw = row.get("Buyer Contract: Buyer 2: Full Name", "")

    buyer1 = str(buyer1_raw).strip() if pd.notna(buyer1_raw) else ""
    buyer2 = str(buyer2_raw).strip() if pd.notna(buyer2_raw) else ""

    if buyer1 and buyer2:
        return f"{buyer1} and {buyer2}"
    return buyer1 or buyer2


def _drop_totals(df: pd.DataFrame) -> pd.DataFrame:
    """Remove summary rows marked as Total."""
    if df.empty:
        return df
    first_col_name = df.columns[0]
    df[first_col_name] = df[first_col_name].fillna("").astype(str).str.strip()
    total_row_index = (
        df[df[first_col_name].str.contains(r"\bTotal\b", case=False, na=False)]
        .index.min()
    )
    if total_row_index is not None:
        df = df.loc[: total_row_index - 1].copy()
    for col in ("Project Name", "AltProjectName"):
        if col in df.columns:
            df = df[
                ~df[col]
                .fillna("")
                .astype(str)
                .str.contains(r"\bTotal\b", case=False, na=False)
            ]
    return df


def _ensure_columns(df: pd.DataFrame, columns_to_keep: Sequence[str]) -> pd.DataFrame:
    present_columns = [col for col in columns_to_keep if col in df.columns]
    missing = [col for col in columns_to_keep if col not in present_columns]
    if missing:
        log.debug("Missing columns in export: %s", ", ".join(missing))
    return df[present_columns]


def _coerce_dates(df: pd.DataFrame, date_columns: Iterable[str]) -> pd.DataFrame:
    for col in date_columns:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")
    return df


def _finalize_records(df: pd.DataFrame) -> List[dict]:
    """Convert dataframe to JSON-ready dictionaries."""
    records: List[dict] = []
    extraction_time = dt.datetime.utcnow().replace(tzinfo=dt.timezone.utc)
    for idx, row in df.iterrows():
        record = {}
        for key, value in row.items():
            if pd.isna(value):
                record[key] = None
            elif isinstance(value, (pd.Timestamp, dt.datetime)):
                record[key] = value.isoformat()
            else:
                record[key] = value
        record["RowIndex"] = int(idx)
        record["ExtractedAt"] = extraction_time.isoformat()
        project_name = str(record.get("AltProjectName") or record.get("Project Name") or "").strip()
        unit_candidate = (
            record.get("Contract Unit Number")
            or record.get("Unit Number")
            or record.get("Lot Number")
        )
        if pd.isna(unit_candidate):
            unit_candidate = None
        if isinstance(unit_candidate, float) and float(unit_candidate).is_integer():
            unit_candidate = int(unit_candidate)
        unit_str = str(unit_candidate).strip() if unit_candidate not in (None, "") else ""
        pk_components: list[str] = []
        if project_name:
            pk_components.append(project_name)
        if unit_str:
            pk_components.append(unit_str)
        if not pk_components:
            pk_components.extend(["UNKNOWN", f"row{record['RowIndex']}"])
        record["pk"] = "#".join(pk_components)

        sk_fields = [
            "Buyer Contract: COE Date",
            "Buyer Contract: Projected Closing Date",
            "Buyer Contract: Week Ratified Date",
            "Buyer Contract: Contract Sent Date",
        ]
        sk_value: Optional[str] = None
        for field in sk_fields:
            candidate = record.get(field)
            if candidate:
                sk_value = str(candidate)
                break
        if not sk_value:
            status_value = record.get("Status")
            status_numeric = record.get("StatusNumeric")
            normalized_status = ""
            if isinstance(status_value, str):
                normalized_status = re.sub(r"[^0-9A-Za-z]+", "-", status_value.strip().lower()).strip("-")
            if normalized_status:
                sk_value = f"status#{normalized_status}"
            else:
                numeric_component: Optional[str] = None
                if isinstance(status_numeric, (int, float)) and not pd.isna(status_numeric):
                    try:
                        numeric_component = f"{int(status_numeric):02d}"
                    except (TypeError, ValueError):
                        numeric_component = None
                sk_value = f"status#{numeric_component}" if numeric_component else "status#unknown"
        record["sk"] = sk_value
        records.append(record)
    return records


def process_dataframe(
    df: pd.DataFrame, columns_to_keep: Optional[Sequence[str]] = None
) -> pd.DataFrame:
    """Normalize a Polaris export dataframe."""
    if columns_to_keep is None:
        columns_to_keep = DEFAULT_COLUMNS

    df = df.copy()
    df.columns = df.columns.str.strip()

    df = _drop_totals(df)

    df = df[df["Project Name"].notna()]
    df = df[df["Project Name"].astype(str).str.strip() != ""]
    df = df[
        ~df["Project Name"]
        .astype(str)
        .str.strip()
        .str.lower()
        .isin(EXCLUDED_PROJECTS)
    ]

    if "Status" in df.columns:
        df["StatusNumeric"] = df["Status"].map(assign_status_numeric)

    if {"Unit Name", "Contract Unit Number"}.issubset(df.columns):
        df["Contract Unit Number"] = df.apply(
            lambda row: renumber_units(row["Unit Name"], row["Contract Unit Number"]),
            axis=1,
        )

    df["AltProjectName"] = df.apply(generate_alt_project_name, axis=1)

    df["Buyers Combined"] = df.apply(combine_buyers, axis=1)

    df = _coerce_dates(df, DATE_COLUMNS)

    required_cols = {"AltProjectName", "Buyers Combined", "StatusNumeric"}
    missing_required = required_cols - set(columns_to_keep)
    if missing_required:
        columns_to_keep = list(columns_to_keep) + sorted(missing_required)

    df = _ensure_columns(df, columns_to_keep)
    return df


def process_polaris_export(
    input_path: Union[str, Path],
    *,
    sheet_name: str = DEFAULT_SHEET_NAME,
    skiprows: int = DEFAULT_SKIPROWS,
    columns_to_keep: Optional[Sequence[str]] = None,
    as_records: bool = True,
) -> Union[pd.DataFrame, List[dict]]:
    """Load an Excel export and return normalized records or dataframe."""
    path = Path(input_path)
    if not path.exists():
        raise FileNotFoundError(f"Input file not found: {path}")

    df = pd.read_excel(path, sheet_name=sheet_name, skiprows=skiprows, engine="openpyxl")

    normalized_df = process_dataframe(df, columns_to_keep=columns_to_keep)

    if as_records:
        return _finalize_records(normalized_df)
    return normalized_df
