from __future__ import annotations

import argparse
import math
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Callable, Iterable, List, Optional, Sequence, Tuple
import json
import logging
import sys
import boto3
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib import colors as mcolors
from fpdf import FPDF

plt.switch_backend("Agg")

HIGHLIGHT_COLORS = {
    1: (244, 121, 131),   # light red
    2: (255, 196, 79),   # light orange
    3: (255, 196, 79),   # same orange
    4: (173, 221, 142),  # light green
    5: (200, 200, 200),  # light grey for pending release
}

BASE_PROJECT_COLORS = [
    "#66c2a5",
    "#fc8d62",
    "#b3b3b3",
    "#fdbf6f",
    "#8da0cb",  
    "#a6d854",
    "#e78ac3",  
    "#e5c494",
    "#1f78b4",
    "#b2df8a",
    "#fb9a99",  
    "#ffd92f",
]

log = logging.getLogger(__name__)

EXCLUDED_PROJECTS = {"fusion"}

ALT_PROJECT_TO_OPS: dict[str, str] = {
    "aria": "Aria",
    "fusion": "Fusion",
    "somi towns": "SoMi Haypark",
    "somi condos": "SoMi HayView",
    "somi hayview": "SoMi HayView",
    "somi haypark": "SoMi Haypark",
    "vida": "Vida",
    "vida 2": "Vida 2",
}

STATUS_KEY_PRIORITY = ["closed", "backlog", "offer", "inventory", "unreleased", "projected_coe"]

STATUS_KEY_TO_NUMERIC = {
    "closed": 1,
    "backlog": 2,
    "offer": 3,
    "inventory": 4,
    "unreleased": 5,
    "projected_coe": 6,
}

STATUS_KEY_TO_LABEL = {
    "closed": "Closed",
    "backlog": "Ratified - Fully executed",
    "offer": "Offer - Out for signature",
    "inventory": "Available",
    "unreleased": "Pending Release",
    "projected_coe": "Projected COE",
}

def _normalize_unit_number(value: object) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, (int,)):
        return str(value)
    if isinstance(value, float):
        if math.isnan(value):
            return None
        if value.is_integer():
            return str(int(value))
        return str(value).strip()
    text = str(value).strip()
    if not text:
        return None
    if text.isdigit():
        return str(int(text))
    try:
        numeric = float(text)
    except ValueError:
        return text
    if math.isnan(numeric):
        return None
    if numeric.is_integer():
        return str(int(numeric))
    return text


def _normalize_project_id(value: object) -> str:
    if value is None:
        return ""
    return str(value).strip().lower()


def _map_alt_to_ops_project(alt_project: object, project_name: object) -> str:
    for candidate in (alt_project, project_name):
        if not candidate:
            continue
        project_text = str(candidate).strip()
        if not project_text:
            continue
        mapped = ALT_PROJECT_TO_OPS.get(project_text.lower())
        return mapped or project_text
    return ""


def _parse_iso8601(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    candidate = value.strip()
    if not candidate:
        return None
    if candidate.endswith("Z"):
        candidate = candidate[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(candidate)
    except ValueError:
        return None


def _resolve_override_status(overrides: Optional[dict]) -> Optional[str]:
    if not overrides or not isinstance(overrides, dict):
        return None
    for key in STATUS_KEY_PRIORITY:
        value = overrides.get(key)
        if value not in (None, "", False):
            return key
    return None


def _build_ops_override_index(items: Iterable[dict]) -> dict[tuple[str, str], dict]:
    index: dict[tuple[str, str], dict] = {}
    for raw in items:
        item_type = raw.get("type")
        if item_type != "unit":
            continue
        project_id = raw.get("project_id") or ""
        unit_number = raw.get("unit_number") or raw.get("sk")
        if not project_id or not unit_number:
            pk = raw.get("pk")
            if pk and "#" in pk:
                project_id = project_id or pk.split("#", 1)[0]
            unit_number = unit_number or raw.get("sk")
        project_key = _normalize_project_id(project_id)
        unit_key = _normalize_unit_number(unit_number)
        if not project_key or not unit_key:
            continue
        data = raw.get("data")
        if isinstance(data, str):
            try:
                data = json.loads(data)
            except Exception:
                continue
        if not isinstance(data, dict):
            continue
        unit_data = data.get("unit")
        if not isinstance(unit_data, dict):
            continue
        overrides = unit_data.get("overrides")
        if not isinstance(overrides, dict) or not overrides:
            continue
        timestamp = _parse_iso8601(raw.get("updated_at"))
        key = (project_key, unit_key)
        existing = index.get(key)
        if existing:
            existing_ts = existing.get("timestamp")
            if existing_ts and timestamp and existing_ts >= timestamp:
                continue
        index[key] = {
            "overrides": overrides,
            "timestamp": timestamp,
        }
    return index


def _apply_ops_overrides(df: pd.DataFrame, overrides: dict[tuple[str, str], dict]) -> pd.DataFrame:
    if not overrides:
        return df
    df = df.copy()
    if "Status" not in df.columns:
        df["Status"] = ""
    df["StatusNumeric"] = (
        pd.to_numeric(df.get("StatusNumeric", 99), errors="coerce")
        .fillna(99)
        .astype(int)
    )
    for idx, row in df.iterrows():
        ops_project = _map_alt_to_ops_project(row.get("AltProjectName"), row.get("Project Name"))
        project_key = _normalize_project_id(ops_project)
        unit_key = _normalize_unit_number(row.get("Contract Unit Number"))
        if not project_key or not unit_key:
            continue
        override_entry = overrides.get((project_key, unit_key))
        status_key = _resolve_override_status(override_entry.get("overrides") if override_entry else None)
        if status_key:
            df.at[idx, "Status"] = STATUS_KEY_TO_LABEL[status_key]
            df.at[idx, "StatusNumeric"] = STATUS_KEY_TO_NUMERIC[status_key]
            continue
    return df


def _filter_excluded_projects(df: pd.DataFrame, excluded: set[str]) -> pd.DataFrame:
    if not excluded:
        return df
    alt_col = df.get("AltProjectName", pd.Series(dtype="object")).astype(str).str.strip()
    project_col = df.get("Project Name", pd.Series(dtype="object")).astype(str).str.strip()
    excluded_lower = {name.lower() for name in excluded}
    mask = alt_col.str.lower().isin(excluded_lower) | project_col.str.lower().isin(excluded_lower)
    if mask.any():
        return df.loc[~mask].copy()
    return df

def _build_project_palette(projects: Sequence[str]) -> dict[str, str]:
    palette: dict[str, str] = {}
    if not projects:
        return palette
    for idx, project in enumerate(projects):
        color = BASE_PROJECT_COLORS[idx % len(BASE_PROJECT_COLORS)]
        palette[project] = color
    return palette

def _series_for_projects(df: pd.DataFrame, mask: pd.Series) -> pd.Series:
    series = df.loc[mask, "AltProjectName"].dropna()
    counts = series.value_counts().sort_values(ascending=False)
    return counts

def _autopct_factory(total: int) -> Callable[[float], str]:
    def formatter(pct: float) -> str:
        if pct <= 0:
            return ""
        value = int(round(pct * total / 100))
        return str(value) if value > 0 else ""
    return formatter

def _render_donut_chart(series: pd.Series, title: str, center_text: str, palette: dict[str, str], output_path: Path) -> Path:
    values = series.values.astype(float)
    labels = [str(label) for label in series.index]
    colors = [palette.get(label, "#cccccc") for label in labels]
    total = int(series.sum())

    fig, ax = plt.subplots(figsize=(3.0, 3.0), dpi=160)
    fig.subplots_adjust(left=0.08, right=0.92, top=0.88, bottom=0.32)
    wedges, texts, autotexts = ax.pie(
        values,
        labels=labels,
        colors=colors,
        startangle=45,
        autopct=_autopct_factory(total),
        pctdistance=0.78,
        labeldistance=1.05,
        wedgeprops=dict(width=0.38, edgecolor="white"),
        textprops=dict(color="black", fontsize=8),
    )

    for autotext in autotexts:
        autotext.set_color("black")
        autotext.set_fontsize(8)

    ax.text(0, 0, center_text, ha="center", va="center", fontsize=11, fontweight="bold")
    ax.set_title(title, fontsize=12, pad=14)
    ax.axis("equal")

    legend_labels = [f"{label} ({int(val)})" for label, val in zip(labels, values)]
    legend_handles = [
        plt.Line2D([0], [0], marker="o", color="w", markerfacecolor=palette.get(label, "#cccccc"), markersize=8)
        for label in labels
    ]
    fig.legend(legend_handles, legend_labels, loc="lower center", bbox_to_anchor=(0.5, 0.06), ncol=2, fontsize=8)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path)
    plt.close(fig)
    return output_path


def _render_inventory_bar_chart(series: pd.Series, title: str, palette: dict[str, str], output_path: Path) -> Path:
    sorted_series = series.sort_values(ascending=True)
    labels = [str(label) for label in sorted_series.index]
    values = sorted_series.values.astype(float)
    colors = [palette.get(label, "#6baed6") for label in sorted_series.index]

    fig, ax = plt.subplots(figsize=(5.25, 4.0), dpi=160)
    bars = ax.barh(labels, values, color=colors)

    ax.set_title(title, fontsize=20, pad=12)
    ax.set_xlabel("Units")
    ax.xaxis.set_tick_params(labelsize=12)
    ax.yaxis.set_tick_params(labelsize=12)
    ax.grid(axis="x", linestyle="--", alpha=0.3)
    ax.set_xlim(0, 123)

    for bar, value in zip(bars, values):
        ax.text(123 - 50, bar.get_y() + bar.get_height() / 2, f"{int(value)}", va="center", fontsize=12, ha="right", color="black")

    fig.tight_layout()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, bbox_inches="tight")
    plt.close(fig)
    return output_path


    values = series.values.astype(float)
    labels = [str(label) for label in series.index]
    colors = [palette.get(label, "#cccccc") for label in labels]
    total = int(series.sum())

    fig, ax = plt.subplots(figsize=(3.0, 3.0), dpi=160)
    wedges, texts, autotexts = ax.pie(
        values,
        labels=labels,
        colors=colors,
        startangle=90,
        autopct=_autopct_factory(total),
        pctdistance=0.78,
        labeldistance=1.12,
        wedgeprops=dict(width=0.38, edgecolor="white"),
        textprops=dict(color="black", fontsize=8),
    )

    for autotext in autotexts:
        autotext.set_color("black")
        autotext.set_fontsize(8)

    ax.text(0, 0, center_text, ha="center", va="center", fontsize=11, fontweight="bold")
    ax.set_title(title, fontsize=12, pad=14)
    ax.axis("equal")

    legend_labels = [f"{label} ({int(val)})" for label, val in zip(labels, values)]
    legend_handles = [
        plt.Line2D([0], [0], marker="o", color="w", markerfacecolor=palette.get(label, "#cccccc"), markersize=8)
        for label in labels
    ]
    ax.legend(legend_handles, legend_labels, loc="upper center", bbox_to_anchor=(0.5, -0.15), ncol=2, fontsize=8)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, bbox_inches="tight")
    plt.close(fig)
    return output_path

def generate_summary_charts(df: pd.DataFrame, output_dir: Path) -> List[Path]:
    if df.empty:
        return []

    projects = sorted({str(p).strip() for p in df["AltProjectName"].dropna() if str(p).strip()})
    palette = _build_project_palette(projects)

    df = df.copy()
    df["Buyer Contract: COE Date"] = pd.to_datetime(df.get("Buyer Contract: COE Date"), errors="coerce")
    df["Buyer Contract: Week Ratified Date"] = pd.to_datetime(df.get("Buyer Contract: Week Ratified Date"), errors="coerce")

    current_year = datetime.now().year

    ytd_sales_series = _series_for_projects(
        df,
        df["Buyer Contract: Week Ratified Date"].dt.year == current_year,
    )
    ytd_closed_series = _series_for_projects(
        df,
        (df["StatusNumeric"] == 1)
        & (df["Buyer Contract: COE Date"].dt.year == current_year),
    )
    total_closed_series = _series_for_projects(df, df["StatusNumeric"] == 1)
    backlog_series = _series_for_projects(df, df["StatusNumeric"] == 2)

    # Inventory calculation: total unique units minus closed units (Status 1)
    df["__unit_key"] = df["Contract Unit Number"].astype(str).where(df["Contract Unit Number"].notna())
    total_units = (
        df.dropna(subset=["__unit_key"])
        .groupby("AltProjectName")["__unit_key"]
        .nunique()
    )
    closed_units = (
        df[(df["StatusNumeric"] == 1) & df["__unit_key"].notna()]
        .groupby("AltProjectName")["__unit_key"]
        .nunique()
    )
    inventory_series = (total_units - closed_units).fillna(total_units)
    inventory_series = inventory_series[inventory_series > 0].sort_values(ascending=False)
    if not inventory_series.empty:
        inventory_series = inventory_series.astype(int)

    df.drop(columns=["__unit_key"], inplace=True, errors="ignore")

    chart_defs = [
        ("YTD Sales by Project", ytd_sales_series, "YTD Sales", "donut"),
        ("YTD Closed by Project", ytd_closed_series, "YTD Closed", "donut"),
        ("Total Closed by Project", total_closed_series, "Total Closed", "donut"),
        ("Backlog by Project", backlog_series, "Backlog", "donut"),
        ("Inventory by Project", inventory_series, "Inventory", "bar"),
    ]

    chart_paths: List[Path] = []
    for idx, (title, series, label, chart_type) in enumerate(chart_defs):
        if series.empty or int(series.sum()) == 0:
            continue
        path = output_dir / f"chart_{idx}.png"
        if chart_type == "donut":
            center_text = f"{int(series.sum()):,}\n{label}"
            chart_paths.append(_render_donut_chart(series, title, center_text, palette, path))
        elif chart_type == "bar":
            chart_paths.append(_render_inventory_bar_chart(series, title, palette, path))

    return chart_paths


@dataclass(frozen=True)
class ColumnConfig:
    key: str
    header: str
    width: float
    formatter: Callable[[object], str] = lambda value: "" if value is None else str(value)
    align: str = "L"


DATE_FIELDS = {
    "Buyer Contract: COE Date",
    "Buyer Contract: Contract Sent Date",
    "Buyer Contract: Appraiser Visit Date",
    "Ops Milestone Date",
}

CURRENCY_FIELDS = {
    "Buyer Contract: Initial Deposit Amount",
    "List Price",
    "Buyer Contract: Base Price",
    "Final Price",
}

BOOLEAN_FIELDS = {
    "Buyer Contract: Cash?",
}

# Include Ops milestone placeholders at the far right; widths still leave room for future fields.
TABLE_COLUMNS: Sequence[ColumnConfig] = (
    ColumnConfig("AltProjectName", "Project", 22.0),
    ColumnConfig("Contract Unit Number", "Unit", 19.0),
    ColumnConfig("Status", "Status", 22.0),
    ColumnConfig("Buyer Contract: COE Date", "COE Date", 17.0),
    ColumnConfig("Buyers Combined", "Buyers Combined", 34.0),
    ColumnConfig("Buyer Contract: Cash?", "Cash?", 12.0, align="C"),
    ColumnConfig("Buyer Contract: Investor/Owner", "Investor/Owner", 22.0),
    ColumnConfig("Buyer Contract: Initial Deposit Amount", "Initial Deposit", 20.0, align="R"),
    ColumnConfig("List Price", "List Price", 18.0, align="R"),
    ColumnConfig("Buyer Contract: Base Price", "Base Price", 20.0, align="R"),
    ColumnConfig("Final Price", "Final Price", 20.0, align="R"),
    ColumnConfig("Buyer Contract: Contract Sent Date", "Contract Sent Date", 20.0),
    ColumnConfig("Buyer Contract: Appraiser Visit Date", "Appraiser Visit Date", 20.0),
    ColumnConfig("Buyer Contract: Notes", "Notes", 60.0),
    ColumnConfig("Ops Milestone Code", "Ops MS", 14.0, align="C"),
    ColumnConfig("Ops Milestone Date", "MS Date", 18.0),
)


def _load_items(
    table_name: str,
    *,
    region: str,
    profile: Optional[str],
) -> List[dict]:
    session = boto3.Session(profile_name=profile) if profile else boto3.Session()
    table = session.resource("dynamodb", region_name=region).Table(table_name)

    results: List[dict] = []
    response = table.scan()
    results.extend(response.get("Items", []))
    while "LastEvaluatedKey" in response:
        response = table.scan(ExclusiveStartKey=response["LastEvaluatedKey"])
        results.extend(response.get("Items", []))
    return results


def _decimal_to_number(value: Decimal) -> float | int:
    if value % 1 == 0:
        return int(value)
    return float(value)


def _coerce_dynamo_types(item: dict) -> dict:
    coerced: dict = {}
    for key, value in item.items():
        if isinstance(value, list):
            coerced[key] = [_coerce_dynamo_types(v) if isinstance(v, dict) else v for v in value]
        elif isinstance(value, dict):
            coerced[key] = _coerce_dynamo_types(value)
        elif isinstance(value, Decimal):
            coerced[key] = _decimal_to_number(value)
        else:
            coerced[key] = value
    return coerced


def _format_currency(value: object) -> str:
    if value in (None, "", 0) or (isinstance(value, float) and not math.isfinite(value)):
        return ""
    try:
        amount = Decimal(str(value))
    except Exception:
        return str(value)
    if amount.is_nan():
        return ""
    return f"${amount:,.0f}"


def _format_boolean(value: object) -> str:
    if value in (None, ""):
        return ""
    if isinstance(value, bool):
        return "Yes" if value else "No"
    lowered = str(value).strip().lower()
    if lowered in {"y", "yes", "true", "1"}:
        return "Yes"
    if lowered in {"n", "no", "false", "0"}:
        return "No"
    return str(value)


def _format_date(value: object) -> str:
    if value is None:
        return ""
    if value is pd.NA or (hasattr(pd, "isna") and pd.isna(value)):
        return ""
    if isinstance(value, datetime):
        return value.strftime("%m/%d/%Y")
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return ""
        parsed = pd.to_datetime(text, errors="coerce", utc=True)
        if pd.isna(parsed):
            return text
        return parsed.tz_convert("UTC").strftime("%m/%d/%Y")
    parsed = pd.to_datetime(value, errors="coerce", utc=True)
    if pd.isna(parsed):
        try:
            text = str(value).strip()
        except Exception:
            return ""
        if not text:
            return ""
        parsed = pd.to_datetime(text, errors="coerce", utc=True)
        if pd.isna(parsed):
            return text
    return parsed.tz_convert("UTC").strftime("%m/%d/%Y")


def _format_value(column_key: str, value: object) -> str:
    if column_key in DATE_FIELDS:
        return _format_date(value)
    if column_key in CURRENCY_FIELDS:
        return _format_currency(value)
    if column_key in BOOLEAN_FIELDS:
        return _format_boolean(value)
    if value is None:
        return ""
    return str(value)


class ReportPDF(FPDF):
    def __init__(
        self,
        columns: Sequence[ColumnConfig],
        *,
        title: str,
        subtitle: str,
        logo_path: Optional[str] = None,
    ) -> None:
        super().__init__(orientation="L", unit="mm", format=(279.4, 431.8))
        self.columns = columns
        self.title_text = title
        self.subtitle_text = subtitle
        self.logo_path = logo_path
        self.logo_height = 16.0
        self.render_table_header = True
        self.left_margin = 10
        self.right_margin = 10
        self.top_margin = 12
        self.set_margins(self.left_margin, self.top_margin, self.right_margin)
        self.set_auto_page_break(auto=False, margin=12)

    def header(self) -> None:
        self.set_font("Helvetica", "B", 16)
        x = self.left_margin
        if self.logo_path:
            self.image(self.logo_path, x=self.left_margin, y=10, h=self.logo_height)
            x += 22
        self.set_xy(x, 10)
        self.cell(0, 6, self.title_text)
        self.ln(6)
        self.set_x(x)

        self.set_font("Helvetica", "", 10)
        self.cell(0, 5, self.subtitle_text)
        bottom = 10 + (self.logo_height if self.logo_path else 0)
        if self.render_table_header:
            next_y = max(self.get_y(), bottom) + 2
            self.set_y(next_y)
            self._draw_header_row()
        else:
            self.set_y(bottom + 6)

    def _draw_header_row(self) -> None:
        header_y = self.get_y()
        header_x = self.left_margin
        self.set_font("Helvetica", "B", 7)
        self.set_fill_color(230, 230, 230)
        for column in self.columns:
            self.set_xy(header_x, header_y)
            self.multi_cell(
                column.width,
                4,
                column.header,
                border=1,
                align="C",
                fill=True,
            )
            header_x += column.width
        self.set_xy(self.left_margin, header_y + 8)

    def _calc_cell_lines(self, text: str, width: float) -> int:
        if not text:
            return 1
        available = max(width - 1, 1)
        total_lines = 0
        for paragraph in text.split("\n"):
            if not paragraph:
                total_lines += 1
                continue
            paragraph_width = self.get_string_width(paragraph)
            lines = max(1, math.ceil(paragraph_width / available))
            total_lines += lines
        return total_lines

    def draw_row(self, row: dict) -> None:
        self.set_font("Helvetica", "", 7)
        formatted_values: List[str] = []
        max_lines = 1
        for column in self.columns:
            value = _format_value(column.key, row.get(column.key))
            formatted_values.append(value)
            max_lines = max(max_lines, self._calc_cell_lines(value, column.width))

        line_height = 4.2
        row_height = max_lines * line_height

        if self.get_y() + row_height > self.page_break_trigger:
            self.add_page()

        fill_color = HIGHLIGHT_COLORS.get(int(row.get("StatusNumeric", 0)))
        fill = fill_color is not None
        if fill:
            self.set_fill_color(*fill_color)

        x = self.left_margin
        y = self.get_y()
        for column, text in zip(self.columns, formatted_values):
            self.set_xy(x, y)
            self.multi_cell(
                column.width,
                line_height,
                text,
                border=1,
                align=column.align,
                fill=fill,
            )
            x += column.width
        self.set_xy(self.left_margin, y + row_height)


def build_dataframe(
    items: Iterable[dict],
    overrides: Optional[dict[tuple[str, str], dict]] = None,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    rows = [_coerce_dynamo_types(item) for item in items]
    if not rows:
        empty = pd.DataFrame(columns=[col.key for col in TABLE_COLUMNS])
        return empty, empty.copy()

    df = pd.DataFrame(rows)
    for column in TABLE_COLUMNS:
        if column.key not in df.columns:
            df[column.key] = pd.NA

    df["AltProjectName"] = df["AltProjectName"].fillna("")
    df["StatusNumeric"] = (
        pd.to_numeric(df.get("StatusNumeric", 99), errors="coerce")
        .fillna(99)
        .astype(int)
    )

    if "Contract Unit Number" in df.columns:
        numeric_units = pd.to_numeric(df["Contract Unit Number"], errors="coerce")
        df["UnitSortKey"] = numeric_units
        df["UnitSortFallback"] = df["Contract Unit Number"].astype(str)
    else:
        df["UnitSortKey"] = pd.NA
        df["UnitSortFallback"] = ""

    df["Buyers Combined"] = df.get("Buyers Combined")
    if "Buyers Combined" not in df.columns or df["Buyers Combined"].isna().all():
        df["Buyers Combined"] = df.get("Buyers Combined", pd.NA)

    df["Buyers Combined"] = df["Buyers Combined"].fillna("")

    df["Buyer Contract: COE Date"] = pd.to_datetime(
        df.get("Buyer Contract: COE Date"), errors="coerce"
    )

    df = _filter_excluded_projects(df, EXCLUDED_PROJECTS)

    summary_df = df.copy()
    if "pk" in summary_df.columns:
        if "ExtractedAt" in summary_df.columns:
            summary_df["ExtractedAt"] = pd.to_datetime(summary_df["ExtractedAt"], errors="coerce")
            summary_df = summary_df.sort_values(["ExtractedAt", "Buyer Contract: COE Date"]).drop_duplicates("pk", keep="last")
        else:
            summary_df = summary_df.sort_values("Buyer Contract: COE Date").drop_duplicates("pk", keep="last")

    if overrides:
        df = _apply_ops_overrides(df, overrides)
        summary_df = _apply_ops_overrides(summary_df, overrides)

    current_year = pd.Timestamp.now(tz=None).year
    mask_closed = df["StatusNumeric"] == 1
    mask_current_year = df["Buyer Contract: COE Date"].dt.year == current_year
    df = df.loc[~mask_closed | mask_current_year].copy()

    df["COESortKey"] = df["Buyer Contract: COE Date"]
    max_date = pd.Timestamp("2262-04-11")
    df["COESortKey"] = df["COESortKey"].fillna(max_date)
    df.loc[df["StatusNumeric"] != 1, "COESortKey"] = max_date

    df["UnitSortKey"] = df["UnitSortKey"].where(df["StatusNumeric"] != 1, pd.NA)

    df = df.sort_values(
        by=[
            "AltProjectName",
            "StatusNumeric",
            "COESortKey",
            "UnitSortKey",
            "UnitSortFallback",
        ]
    )

    df = df.drop(columns=["UnitSortKey", "UnitSortFallback", "COESortKey"], errors="ignore")
    df = df.reset_index(drop=True)

    return df, summary_df


def _draw_cover_page(pdf: ReportPDF, chart_images: Sequence[Path]) -> None:
    if not chart_images:
        return

    start_y = pdf.get_y() + 6
    available_width = pdf.w - pdf.left_margin - pdf.right_margin
    count = len(chart_images)
    if count == 0:
        return

    max_columns = 4
    gap_x = 14
    columns = min(max_columns, count)
    max_chart_width = 85.0
    row_width = min(max_chart_width, (available_width - gap_x * (columns - 1)) / columns)
    chart_height = row_width * 0.65
    legend_offset = 48
    row_height = chart_height + legend_offset

    for idx, image_path in enumerate(chart_images):
        row = idx // columns
        col = idx % columns
        x = pdf.left_margin + col * (row_width + gap_x)
        y = start_y + row * row_height
        if image_path.exists():
            pdf.image(str(image_path), x=x, y=y, w=row_width)

    total_rows = (count + columns - 1) // columns
    bottom_y = start_y + total_rows * row_height + 12
    pdf.set_y(bottom_y)

def generate_pdf(
    df: pd.DataFrame,
    output_path: Path | str,
    *,
    title: str,
    subtitle: str,
    logo_path: Optional[str] = None,
    chart_images: Optional[Sequence[Path]] = None,
) -> None:
    pdf = ReportPDF(
        TABLE_COLUMNS,
        title=title,
        subtitle=subtitle,
        logo_path=logo_path,
    )

    charts = [Path(img) for img in (chart_images or []) if Path(img).exists()]

    pdf.render_table_header = False
    pdf.add_page()
    _draw_cover_page(pdf, charts)

    pdf.render_table_header = True
    pdf.add_page()

    for _, row in df.iterrows():
        pdf.draw_row(row)

    out_path = Path(output_path)
    parent = out_path.parent
    if parent and parent != Path('.'):
        parent.mkdir(parents=True, exist_ok=True)
    pdf.output(str(out_path))


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Generate the Polaris Mylar PDF report from DynamoDB contents."
    )
    parser.add_argument(
        "--table-name",
        default="hbfa_PolarisRaw",
        help="DynamoDB table name to scan (default: hbfa_PolarisRaw).",
    )
    parser.add_argument(
        "--ops-table",
        default="ops_milestones",
        help="DynamoDB table for ops milestones overrides (default: ops_milestones). Set to empty to skip.",
    )
    parser.add_argument(
        "--region",
        default="us-west-1",
        help="AWS region for DynamoDB (default: us-west-1).",
    )
    parser.add_argument(
        "--profile",
        help="Optional AWS profile name to use for credentials.",
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

    items = _load_items(
        args.table_name,
        region=args.region,
        profile=args.profile,
    )

    ops_overrides: dict[tuple[str, str], dict] = {}
    ops_table = (args.ops_table or "").strip()
    if ops_table:
        try:
            ops_items = _load_items(
                ops_table,
                region=args.region,
                profile=args.profile,
            )
            ops_overrides = _build_ops_override_index(ops_items)
        except Exception as exc:  # pylint: disable=broad-except
            print(f"Warning: unable to load ops milestones from {ops_table}: {exc}", file=sys.stderr)

    table_df, summary_df = build_dataframe(items, overrides=ops_overrides)

    charts_dir = Path(args.output).parent / "_charts_temp"
    chart_paths = generate_summary_charts(summary_df, charts_dir)

    subtitle = args.subtitle or f"Generated {datetime.now():%m/%d/%Y %I:%M %p}"
    generate_pdf(
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




