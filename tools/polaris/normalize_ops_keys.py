from __future__ import annotations

import argparse
import json
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, Optional, Tuple

import boto3
from botocore.exceptions import ClientError

# Canonical project names already in use by hbfa_sales_offers / Mylar.
CANONICAL_PROJECTS = {"SoMi Towns", "SoMi A", "SoMi B", "Fusion", "Aria", "Vida"}

# Direct alias map (case-insensitive) when building context is not required.
PROJECT_ALIAS_MAP = {
    "somi haypark": "SoMi Towns",
    "somi hayview": "SoMi B",  # default, overridden by building-specific map below
    "fusion": "Fusion",
    "aria": "Aria",
    "vida": "Vida",
}

# Building-aware overrides. Keys are (base project alias, normalized building_id).
PROJECT_BY_BUILDING = {
    ("somi hayview", "building a"): "SoMi A",
    ("somi hayview", "building b"): "SoMi B",
    ("somi hayview", "bldg a"): "SoMi A",
    ("somi hayview", "bldg b"): "SoMi B",
    ("somi hayview", "tower a"): "SoMi A",
    ("somi hayview", "tower b"): "SoMi B",
}

# For projects that require a normalized unit prefix.
UNIT_PREFIX_BY_PROJECT = {
    "SoMi A": "HayView-",
    "SoMi B": "HayView-",
}


@dataclass
class ItemChange:
    pk_old: str
    sk_old: str
    pk_new: str
    sk_new: str
    project_before: Optional[str]
    project_after: Optional[str]
    unit_before: Optional[str]
    unit_after: Optional[str]


def _session(region: str, profile: Optional[str]) -> boto3.Session:
    if profile:
        return boto3.Session(profile_name=profile, region_name=region)
    return boto3.Session(region_name=region)


def _normalize_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _normalize_lower(value: Any) -> str:
    return _normalize_text(value).lower()


def _digits_only(value: str) -> str:
    return "".join(ch for ch in value if ch.isdigit())


def _guess_canonical_project(pk: str, building_id: str, record: Dict[str, Any]) -> str:
    base, _, suffix = pk.partition("#")
    base_norm = _normalize_lower(base)
    bldg_source = building_id or suffix
    if not bldg_source and isinstance(record.get("data"), dict):
        building_obj = record["data"].get("building")
        if isinstance(building_obj, dict):
            bldg_source = (
                building_obj.get("building_id")
                or building_obj.get("buildingId")
                or building_obj.get("id")
            )
    bldg_norm = _normalize_lower(bldg_source)
    candidate = None
    if (base_norm, bldg_norm) in PROJECT_BY_BUILDING:
        candidate = PROJECT_BY_BUILDING[(base_norm, bldg_norm)]
    elif base_norm in PROJECT_ALIAS_MAP:
        candidate = PROJECT_ALIAS_MAP[base_norm]
    else:
        candidate = base
    return candidate or base


def _normalize_unit_sk(project: str, sk: str, record: Dict[str, Any]) -> str:
    if sk == "#building":
        return sk
    text = _normalize_text(sk)
    if not text:
        text = _normalize_text(record.get("unit_number"))
    if not text and isinstance(record.get("data"), dict):
        unit_data = record["data"].get("unit")
        if isinstance(unit_data, dict):
            text = _normalize_text(
                unit_data.get("unit_number")
                or unit_data.get("unit_id")
                or unit_data.get("unit_label")
            )
    if not text:
        return sk
    prefix = UNIT_PREFIX_BY_PROJECT.get(project)
    if prefix:
        if text.lower().startswith(prefix.lower()):
            return text
        digits = _digits_only(text)
        if digits:
            return f"{prefix}{digits.zfill(3)}"
    return text


def _update_payload_metadata(record: Dict[str, Any], project: str, unit_sk: str) -> None:
    if record.get("project_id") != project:
        record["project_id"] = project
    if isinstance(record.get("data"), dict):
        data = record["data"]
        building = data.get("building")
        if isinstance(building, dict):
            if building.get("project_id") != project:
                building["project_id"] = project
        unit = data.get("unit")
        if isinstance(unit, dict):
            if unit.get("project_id") != project:
                unit["project_id"] = project
            if unit_sk != "#building" and unit.get("unit_number") != unit_sk:
                unit["unit_number"] = unit_sk
            if unit_sk != "#building" and unit.get("unit_id") not in (None, unit_sk):
                unit["unit_id"] = unit_sk


def _scan_table(table, *, page_limit: Optional[int] = None) -> Iterable[Dict[str, Any]]:
    kwargs: Dict[str, Any] = {}
    scanned = 0
    while True:
        resp = table.scan(**kwargs)
        items = resp.get("Items", [])
        for item in items:
            yield item
            scanned += 1
            if page_limit is not None and scanned >= page_limit:
                return
        last_key = resp.get("LastEvaluatedKey")
        if not last_key:
            return
        kwargs["ExclusiveStartKey"] = last_key


def _convert_to_put_item(item: Dict[str, Any]) -> Dict[str, Any]:
    # boto3 resource returns Decimal for numeric types already compatible with put_item.
    return item


def normalize_ops_milestones(
    *,
    table_name: str,
    region: str,
    profile: Optional[str],
    apply_changes: bool,
    backup_path: Optional[Path],
    limit: Optional[int],
) -> Tuple[int, int]:
    session = _session(region, profile)
    dynamo = session.resource("dynamodb", region_name=region)
    table = dynamo.Table(table_name)
    backup_file = None
    if backup_path:
        backup_path = backup_path.expanduser().resolve()
        if apply_changes:
            backup_file = backup_path.open("a", encoding="utf-8")
    total = 0
    updated = 0
    for item in _scan_table(table, page_limit=limit):
        total += 1
        pk = _normalize_text(item.get("pk"))
        sk = _normalize_text(item.get("sk"))
        if not pk or not sk:
            continue
        building_id_value: Optional[str] = None
        building_id_value = _normalize_text(item.get("building_id") or item.get("buildingId"))
        canonical_project = _guess_canonical_project(pk, building_id_value, item)
        if canonical_project not in CANONICAL_PROJECTS:
            # Skip unrelated projects; no rewrite needed.
            continue
        target_pk = canonical_project
        target_sk = _normalize_unit_sk(canonical_project, sk, item)
        project_before = item.get("project_id") or item.get("project")
        unit_before = sk if sk != "#building" else None
        if target_pk == pk and target_sk == sk:
            continue
        cloned = deepcopy(item)
        cloned["pk"] = target_pk
        cloned["sk"] = target_sk
        _update_payload_metadata(cloned, canonical_project, target_sk)
        change = ItemChange(
            pk_old=pk,
            sk_old=sk,
            pk_new=target_pk,
            sk_new=target_sk,
            project_before=project_before,
            project_after=canonical_project,
            unit_before=unit_before,
            unit_after=target_sk if target_sk != "#building" else None,
        )
        print(
            f"[{'APPLY' if apply_changes else 'DRY'}] {pk}#{sk} -> {target_pk}#{target_sk}"
        )
        updated += 1
        if not apply_changes:
            continue
        # Backup original item
        if backup_file:
            snapshot = {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "original": item,
                "change": change.__dict__,
            }
            backup_file.write(json.dumps(snapshot, default=str) + "\n")
        put_item = _convert_to_put_item(cloned)
        can_delete_alias = False
        try:
            table.put_item(
                Item=put_item,
                ConditionExpression="attribute_not_exists(pk) AND attribute_not_exists(sk)",
            )
            can_delete_alias = True
        except ClientError as exc:
            error_code = exc.response.get("Error", {}).get("Code", "")
            if error_code == "ConditionalCheckFailedException":
                can_delete_alias = True
                print(
                    f"  ! put_item skipped for {target_pk}#{target_sk}: canonical key already present"
                )
            else:
                print(f"  ! put_item failed for {target_pk}#{target_sk}: {exc}")
                continue
        if not can_delete_alias:
            continue
        try:
            table.delete_item(
                Key={"pk": pk, "sk": sk},
                ConditionExpression="attribute_exists(pk)",
            )
        except ClientError as exc:
            print(f"  ! delete_item failed for {pk}#{sk}: {exc}")
            continue
    if backup_file:
        backup_file.close()
    return total, updated


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Normalize ops_milestones partition/sort keys to match canonical "
            "hbfa_sales_offers project & unit identifiers."
        )
    )
    parser.add_argument(
        "--table-name",
        default="ops_milestones",
        help="DynamoDB table name (default: ops_milestones).",
    )
    parser.add_argument(
        "--region",
        default="us-west-1",
        help="AWS region for the ops_milestones table (default: us-west-1).",
    )
    parser.add_argument(
        "--profile",
        help="Optional AWS profile name for credentials.",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Apply the mutations. Without this flag the script runs in dry-run mode.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="Optional maximum number of items to scan (useful for smoke tests).",
    )
    parser.add_argument(
        "--backup",
        help="Optional path to append JSONL snapshots of original items before mutation (only when --apply).",
    )
    return parser


def main(argv: Optional[Iterable[str]] = None) -> int:
    parser = build_argument_parser()
    args = parser.parse_args(list(argv) if argv is not None else None)
    backup_path = Path(args.backup) if args.backup else None
    total, updated = normalize_ops_milestones(
        table_name=args.table_name,
        region=args.region,
        profile=args.profile,
        apply_changes=args.apply,
        backup_path=backup_path,
        limit=args.limit,
    )
    mode = "APPLY" if args.apply else "DRY-RUN"
    print(f"{mode} complete: scanned {total} items, prepared {updated} rewrites")
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
