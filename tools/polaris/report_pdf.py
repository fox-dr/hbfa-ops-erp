from __future__ import annotations

import argparse
import math
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Callable, Iterable, List, Optional, Sequence, Tuple
import json
import os
import logging
import sys
import boto3
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib import colors as mcolors
from fpdf import FPDF

plt.switch_backend("Agg")

HIGHLIGHT_COLORS = {
    1: (244, 121, 131),   # light red (Closed)
    2: (255, 196, 79),    # light orange (Ratified)
    # 3 intentionally has no fill (Offer - Out for signature)
    4: (173, 221, 142),   # light green (Available)
    5: (200, 200, 200),   # light grey (Pending Release)
}

LEGEND_CELL_WIDTH = 36.0
LEGEND_CELL_HEIGHT = 8.0
LEGEND_SPACING = 6.0
LEGEND_GRID: List[List[tuple[str, Optional[tuple[int, int, int]]]]] = [
    [
        ("Closed YTD", HIGHLIGHT_COLORS[1]),
        ("B1 Construction Release(Cut 1)", None),
        ("B5 1st Floor Frame Complete", None),
        ("B9 Roof Truss Delivery", None),
        ("U1 Unit Frame Inspection(Cut 3)", None),
        ("U5 Appliance Delivery", None),
    ],
    [
        ("Backlog", HIGHLIGHT_COLORS[2]),
        ("B2 Foundation Start", None),
        ("B6 2nd Floor Frame Complete", None),
        ("B10 Roof/Shear Nail Inspection", None),
        ("U2 Drywall Nail Inspection", None),
        ("U6 Buyer Orientation", None),
    ],
    [
        ("Available", HIGHLIGHT_COLORS[4]),
        ("B3 Ground Plumbing Inspection", None),
        ("B7 3rd Floor Frame Complete", None),
        ("B11 Install Windows Ext. Doors", None),
        ("U3 Drywall Texture", None),
        ("", None),
    ],
    [
        ("Pending", None),
        ("B4 Foundation Pour(Cut 2)", None),
        ("B8 4th Floor Frame Complete", None),
        ("", None),
        ("U4 Install Cabinets", None),
        ("", None),
    ],
]

UNIT_MILESTONE_CODES: Sequence[str] = (
    "U6",
    "U5",
    "U4",
    "U3",
    "U2",
    "U1",
)

BUILDING_MILESTONE_CODES: Sequence[str] = (
    "B11",
    "B10",
    "B9",
    "B8",
    "B7",
    "B6",
    "B5",
    "B4",
    "B3",
    "B2",
    "B1",
)

MILESTONE_KEY_MAP: dict[str, tuple[str, ...]] = {
    "B1": (
        "construction_release",
        "construction_release_cut1",
        "release_construction_cut1",
    ),
    "B2": (
        "foundation_start",
        "start_foundation",
        "start_foundation_cut2",
    ),
    "B3": (
        "ground_plumbing_inspection",
        "lower_foundation_ground_plumbing_inspection",
        "upper_foundation_ground_plumbing_inspection",
    ),
    "B4": (
        "foundation_pour",
        "lower_foundation_pour",
        "upper_foundation_pour",
        "pour_foundation_slab",
    ),
    "B5": (
        "first_floor_frame_complete",
        "first_floor_structural_steel_100_percent",
        "first_floor_frame_inspection",
        "raise_walls_first_floor",
        "set_second_floor_beams",
    ),
    "B6": (
        "second_floor_frame_complete",
        "frame_walls_second_floor",
        "second_floor_plumbing_hvac_complete",
        "second_floor_frame_inspection_first_floor_drywall_inspection",
    ),
    "B7": (
        "third_floor_frame_complete",
        "frame_third_floor_walls",
        "third_floor_plumbing_hvac_complete",
        "third_floor_frame_inspection_start",
        "plumb_line_third_floor_walls",
    ),
    "B8": (
        "fourth_floor_frame_inspection_start",
        "fourth_floor_plumbing_hvac_complete",
        "finish_framing_inspection",
    ),
    "B9": (
        "roof_truss_delivery",
        "deliver_roof_trusses",
        "load_roof_trusses",
    ),
    "B10": (
        "roof_nail_shear_nail_inspection",
        "sheathing_inspection",
    ),
    "B11": (
        "install_windows_exterior_doors",
        "set_window_door_frames",
    ),
    "U1": (
        "finish_framing_inspection",
        "unit_frame_inspection",
        "first_floor_frame_inspection",
        "second_floor_frame_inspection_first_floor_drywall_inspection",
        "third_floor_frame_inspection_start",
        "fourth_floor_frame_inspection_start",
    ),
    "U2": (
        "drywall_nail_inspection",
    ),
    "U3": (
        "drywall_texture",
        "texture_drywall",
    ),
    "U4": (
        "install_cabinets",
    ),
    "U5": (
        "appliance_delivery",
    ),
    "U6": (
        "buyer_orientation",
    ),
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

# EXCLUDED_PROJECTS = {"fusion"}

ALT_PROJECT_TO_OPS: dict[str, str] = {
    "aria": "Aria",
    "fusion": "Fusion",
    "somi towns": "SoMi Towns",
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


def _normalize_building_id(value: object) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    normalized = "".join(ch for ch in text.lower() if ch.isalnum())
    return normalized or None


def _build_building_lookup_key(normalized: Optional[str]) -> str:
    suffix = normalized or "unknown"
    return f"#building::{suffix}"


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


def _unwrap_attr(value: object) -> object:
    """
    Best-effort unwrap for DynamoDB low-level attribute maps, e.g., {"S": "text"}.
    If the value is a dict with a single key among common DynamoDB types, return the inner value.
    Leaves complex types (M/L) unchanged.
    """
    if isinstance(value, dict) and len(value) == 1:
        key = next(iter(value))
        if key in {"S", "N", "BOOL", "NULL"}:
            return value[key]
    return value

def _resolve_override_status(overrides: Optional[dict]) -> Optional[str]:
    if not overrides or not isinstance(overrides, dict):
        return None
    for key in STATUS_KEY_PRIORITY:
        value = overrides.get(key)
        if value not in (None, "", False):
            return key
    return None


def _resolve_milestone(
    unit_overrides: Optional[dict], building_overrides: Optional[dict]
) -> tuple[Optional[str], Optional[str]]:
    def first_match(codes: Sequence[str], dictionaries: Sequence[dict]) -> tuple[Optional[str], Optional[str]]:
        for code in codes:
            keys = MILESTONE_KEY_MAP.get(code, ())
            if not keys:
                continue
            for overrides in dictionaries:
                for key in keys:
                    if key in overrides:
                        value = overrides[key]
                        if value not in (None, "", False):
                            return code, value
        return None, None

    unit_dicts: List[dict] = []
    if isinstance(unit_overrides, dict):
        unit_dicts.append(unit_overrides)
    if unit_dicts:
        code, date = first_match(UNIT_MILESTONE_CODES, unit_dicts)
        if code:
            return code, date

    building_dicts: List[dict] = []
    if isinstance(building_overrides, dict):
        building_dicts.append(building_overrides)
    # Fall back to unit overrides for building milestones when data is stored there
    if unit_dicts:
        building_dicts.extend(unit_dicts)
    if building_dicts:
        return first_match(BUILDING_MILESTONE_CODES, building_dicts)
    return None, None


def _resolve_ops_coe(
    unit_overrides: Optional[dict], building_overrides: Optional[dict]
) -> Optional[str]:
    for source in (unit_overrides, building_overrides):
        if isinstance(source, dict):
            value = source.get("projected_coe")
            if value not in (None, "", False):
                return str(value)
    return None


def _extract_pre_kickoff_flag(*payloads: object) -> bool:
    for payload in payloads:
        if isinstance(payload, dict):
            value = payload.get("pre_kickoff")
            if isinstance(value, bool):
                return value
    return False


def _build_ops_override_index(items: Iterable[dict]) -> dict[tuple[str, str], dict]:
    index: dict[tuple[str, str], dict] = {}
    for raw in items:
        project_id = _unwrap_attr(raw.get("project_id")) or ""
        unit_number = _unwrap_attr(raw.get("unit_number")) or _unwrap_attr(raw.get("sk"))
        pk = _unwrap_attr(raw.get("pk"))
        if not project_id and pk and "#" in pk:
            project_id = pk.split("#", 1)[0]
        project_key = _normalize_project_id(project_id)
        if not project_key:
            continue
        data = _unwrap_attr(raw.get("data"))
        if isinstance(data, str):
            try:
                data = json.loads(data)
            except Exception:
                continue
        if not isinstance(data, dict):
            continue
        timestamp = _parse_iso8601(_unwrap_attr(raw.get("updated_at")))
        building_payload = data.get("building")
        unit_payload = data.get("unit")
        building_id_value = None
        try:
            if isinstance(building_payload, dict):
                building_id_value = building_payload.get("building_id") or building_payload.get("buildingId")
            if not building_id_value and isinstance(unit_payload, dict):
                building_id_value = unit_payload.get("building_id") or unit_payload.get("buildingId")
            if not building_id_value:
                building_id_value = _unwrap_attr(raw.get("building_id") or raw.get("buildingId"))
        except Exception:
            building_id_value = None
        building_id_value = _unwrap_attr(building_id_value)
        normalized_building_id = _normalize_building_id(building_id_value)
        building_lookup_key = _build_building_lookup_key(normalized_building_id)
        building_pre_kickoff = _extract_pre_kickoff_flag(building_payload)

        # Capture building-level overrides per building_id to avoid collisions across buildings.
        if isinstance(building_payload, dict):
            building_overrides = building_payload.get("overrides")
            if isinstance(building_overrides, dict) and building_overrides:
                key = (project_key, building_lookup_key)
                existing = index.get(key)
                existing_ts = existing.get("timestamp") if existing else None
                if not existing or (existing_ts and timestamp and existing_ts < timestamp):
                    index[key] = {
                        "overrides": building_overrides,
                        "timestamp": timestamp,
                        "building_id": building_id_value,
                        "normalized_building_id": normalized_building_id,
                        "pre_kickoff": building_pre_kickoff,
                    }
        key = (project_key, building_lookup_key)
        existing = index.get(key)
        if existing and isinstance(existing, dict):
            existing["pre_kickoff"] = building_pre_kickoff
            existing_ts = existing.get("timestamp")
            is_newer = bool(timestamp and (not existing_ts or (existing_ts and existing_ts < timestamp)))
            if building_id_value and (not existing.get("building_id") or is_newer):
                existing["building_id"] = building_id_value
            if normalized_building_id and (not existing.get("normalized_building_id") or is_newer):
                existing["normalized_building_id"] = normalized_building_id
            if is_newer:
                existing["timestamp"] = timestamp
        elif building_pre_kickoff:
            index[key] = {
                "overrides": {},
                "timestamp": timestamp,
                "building_id": building_id_value,
                "normalized_building_id": normalized_building_id,
                "pre_kickoff": True,
            }

        unit_entry_recorded = False
        if isinstance(unit_payload, dict):
            unit_overrides = unit_payload.get("overrides")
            if isinstance(unit_overrides, dict) and unit_overrides:
                unit_id = unit_payload.get("unit_number") or unit_number
                if unit_id is None:
                    unit_id = _unwrap_attr(raw.get("sk"))
                unit_key = _normalize_unit_number(unit_id)
                if unit_key:
                    unit_pre_kickoff = _extract_pre_kickoff_flag(unit_payload, building_payload)
                    key = (project_key, unit_key)
                    existing = index.get(key)
                    existing_ts = existing.get("timestamp") if existing else None
                    if not existing or (existing_ts and timestamp and existing_ts < timestamp):
                        index[key] = {
                            "overrides": unit_overrides,
                            "timestamp": timestamp,
                            "building_id": building_id_value,
                            "normalized_building_id": normalized_building_id,
                            "pre_kickoff": unit_pre_kickoff,
                        }
                        unit_entry_recorded = True

        # If there are no milestone overrides but we have a building_id and a unit_number,
        # create a metadata-only entry so the report can hydrate the Building column.
        if not unit_entry_recorded and building_id_value:
            unit_fallback = _normalize_unit_number(unit_number)
            if unit_fallback:
                unit_pre_kickoff = _extract_pre_kickoff_flag(unit_payload, building_payload)
                key = (project_key, unit_fallback)
                existing = index.get(key)
                existing_ts = existing.get("timestamp") if existing else None
                if not existing or (existing_ts and timestamp and existing_ts < timestamp):
                    index[key] = {
                        "overrides": {},
                        "timestamp": timestamp,
                        "building_id": building_id_value,
                        "normalized_building_id": normalized_building_id,
                        "pre_kickoff": unit_pre_kickoff,
                    }
    return index


def _apply_ops_overrides(df: pd.DataFrame, overrides: dict[tuple[str, str], dict]) -> pd.DataFrame:
    if not overrides:
        return df
    df = df.copy()
    if "Ops Milestone Code" not in df.columns:
        df["Ops Milestone Code"] = ""
    if "Ops Milestone Date" not in df.columns:
        df["Ops Milestone Date"] = ""
    if "Ops COE" not in df.columns:
        df["Ops COE"] = ""
    # Ensure columns for display and compatibility
    if "building_id" not in df.columns:
        df["building_id"] = ""
    # Also support legacy/lowercase field name if present in downstream consumers
    if "builder" not in df.columns:
        df["builder"] = ""
    df["Ops Milestone Code"] = df["Ops Milestone Code"].fillna("")
    df["Ops Milestone Date"] = df["Ops Milestone Date"].fillna("")
    df["Ops COE"] = df["Ops COE"].fillna("")
    df["building_id"] = df["building_id"].fillna("")
    df["builder"] = df["builder"].fillna("")
    # Respect existing Status/StatusNumeric from Sales; do not override Status here.
    df["StatusNumeric"] = pd.to_numeric(df.get("StatusNumeric", 99), errors="coerce").fillna(99).astype(int)
    debug_ops = bool(os.getenv("HBFA_DEBUG_OPS"))
    module = sys.modules[__name__]
    build_key_factory = getattr(
        module,
        "_build_building_lookup_key",
        lambda normalized: f"#building::{normalized or 'unknown'}",
    )
    for idx, row in df.iterrows():
        # Build a robust set of possible project keys to match overrides, to avoid mismatches
        # from name mapping differences (e.g., "SoMi Towns" vs "SoMi Haypark").
        raw_alt = row.get("AltProjectName")
        raw_name = row.get("Project Name")
        mapped = _map_alt_to_ops_project(raw_alt, raw_name)
        candidate_projects = []
        for candidate in (raw_alt, raw_name, mapped):
            key = _normalize_project_id(candidate)
            if key and key not in candidate_projects:
                candidate_projects.append(key)
        unit_key = _normalize_unit_number(row.get("Contract Unit Number"))
        if not candidate_projects or not unit_key:
            continue
        # Build candidate unit keys to match ops (strip prefixes like "HayView-306")
        candidate_units: List[str] = []
        candidate_units.append(unit_key)
        unit_text = str(unit_key)
        # after last hyphen
        if "-" in unit_text:
            tail = unit_text.split("-")[-1]
            if tail not in candidate_units:
                candidate_units.append(tail)
        # extract last numeric run
        import re as _re
        m = _re.search(r"(\d+)$", unit_text)
        if m:
            digits = m.group(1)
            if digits not in candidate_units:
                candidate_units.append(digits)
        # Try to find a matching overrides entry using any candidate project key
        override_entry = None
        override_project_key = None
        for pj in candidate_projects:
            for uk in candidate_units:
                candidate_entry = overrides.get((pj, uk))
                if candidate_entry:
                    override_entry = candidate_entry
                    override_project_key = pj
                    break
            if override_entry:
                break

        normalized_building = (
            override_entry.get("normalized_building_id")
            if override_entry and isinstance(override_entry, dict)
            else None
        )
        building_entry = None
        if candidate_projects:
            search_projects = [override_project_key] if override_project_key else []
            search_projects.extend([pj for pj in candidate_projects if pj not in search_projects])
            for pj in search_projects:
                if normalized_building:
                    key = build_key_factory(normalized_building)
                    building_entry = overrides.get((pj, key))
                    if building_entry:
                        break
                building_entry = overrides.get((pj, "#building"))
                if building_entry:
                    break
        unit_overrides = override_entry.get("overrides") if override_entry else None
        building_overrides = building_entry.get("overrides") if building_entry else None
        unit_pre_kickoff = bool(override_entry.get("pre_kickoff")) if isinstance(override_entry, dict) else False
        building_pre_kickoff = bool(building_entry.get("pre_kickoff")) if isinstance(building_entry, dict) else False
        skip_milestones = unit_pre_kickoff or building_pre_kickoff
        milestone_code = milestone_date = None
        ops_coe = None
        if not skip_milestones:
            milestone_code, milestone_date = _resolve_milestone(unit_overrides, building_overrides)
            ops_coe = _resolve_ops_coe(unit_overrides, building_overrides)
        if milestone_code:
            df.at[idx, "Ops Milestone Code"] = milestone_code
            df.at[idx, "Ops Milestone Date"] = milestone_date
        if skip_milestones:
            df.at[idx, "Ops COE"] = ""
        else:
            df.at[idx, "Ops COE"] = ops_coe or ""
        # populate Building column from overrides metadata if available
        building_id = None
        if override_entry and isinstance(override_entry, dict):
            building_id = override_entry.get("building_id") or building_id
        if not building_id and building_entry and isinstance(building_entry, dict):
            building_id = building_entry.get("building_id")
        if building_id:
            df.at[idx, "building_id"] = str(building_id)
            df.at[idx, "builder"] = str(building_id)
        elif debug_ops:
            found_keys = []
            for pj in candidate_projects:
                for uk in candidate_units:
                    if (pj, uk) in overrides:
                        found_keys.append(f"{pj}#{uk}")
                for key_tuple in overrides.keys():
                    if key_tuple[0] == pj and isinstance(key_tuple[1], str) and key_tuple[1].startswith("#building"):
                        found_keys.append(f"{pj}#{key_tuple[1]}")
            print(
                f"[OPS DEBUG] idx={idx} unit={unit_key} alt={raw_alt} proj={raw_name} mapped={mapped} candidates={candidate_projects} matched={found_keys} building_id=None ms=({milestone_code},{milestone_date})",
                file=sys.stderr,
            )
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
    "Ops COE",
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
    ColumnConfig("Contract Unit Number", "Homesite", 19.0),
    ColumnConfig("building_id", "Building", 18.0),
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
    ColumnConfig("Ops COE", "Ops COE", 20.0),
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
    # Render booleans as Y/N, handling numeric 0/1 and string variants.
    if value in (None, ""):
        return ""
    # Numeric values (including Decimals)
    if isinstance(value, (int, float, Decimal)):
        try:
            return "Y" if float(value) != 0.0 else "N"
        except Exception:
            pass
    # Actual booleans
    if isinstance(value, bool):
        return "Y" if value else "N"
    # String representations
    lowered = str(value).strip().lower()
    if lowered in {"y", "yes", "true", "t", "1", "1.0"}:
        return "Y"
    if lowered in {"n", "no", "false", "f", "0", "0.0"}:
        return "N"
    # Fallback to original text if unrecognized
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
        legend_columns = len(LEGEND_GRID[0])
        legend_width = legend_columns * LEGEND_CELL_WIDTH
        legend_height = len(LEGEND_GRID) * LEGEND_CELL_HEIGHT
        legend_x = self.w - self.right_margin - legend_width
        legend_y = 10
        available_title_width = self.w - self.right_margin - x
        if legend_x - x > LEGEND_SPACING:
            available_title_width = legend_x - x - LEGEND_SPACING
        self.set_xy(x, 10)
        self.cell(available_title_width, 6, self.title_text)
        self.ln(6)
        self.set_x(x)

        self.set_font("Helvetica", "", 10)
        self.cell(available_title_width, 5, self.subtitle_text)
        legend_bottom = self._draw_header_legend(legend_x, legend_y, legend_height)
        bottom = max(10 + (self.logo_height if self.logo_path else 0), legend_bottom)
        if self.render_table_header:
            next_y = max(self.get_y(), bottom) + 2
            self.set_y(next_y)
            self._draw_header_row()
        else:
            self.set_y(bottom + 6)

    def _draw_header_legend(self, x: float, y: float, legend_height: float) -> float:
        self.set_draw_color(0, 0, 0)
        self.set_text_color(0, 0, 0)
        self.set_font("Helvetica", "", 6)
        for row_index, row in enumerate(LEGEND_GRID):
            for col_index, (label, color) in enumerate(row):
                cell_x = x + col_index * LEGEND_CELL_WIDTH
                cell_y = y + row_index * LEGEND_CELL_HEIGHT
                style = "D"
                if color:
                    self.set_fill_color(*color)
                    style = "FD"
                self.rect(cell_x, cell_y, LEGEND_CELL_WIDTH, LEGEND_CELL_HEIGHT, style=style)
                if label:
                    self.set_xy(cell_x + 1.5, cell_y + 1.4)
                    self.multi_cell(LEGEND_CELL_WIDTH - 3.0, 2.6, label, border=0)
        return y + legend_height

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




