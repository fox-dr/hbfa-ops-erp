from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from tools.polaris.aws import parse_s3_uri
from tools.polaris.processing import (
    assign_status_numeric,
    combine_buyers,
    generate_alt_project_name,
    _finalize_records,
    process_dataframe,
    process_polaris_export,
    renumber_units,
)


def fixture_path() -> Path:
    return Path(__file__).parent / "fixtures" / "sample_export.xlsx"


def test_assign_status_numeric_known_and_unknown():
    assert assign_status_numeric("Closed") == 1
    assert assign_status_numeric("Ratified - Fully executed") == 2
    assert assign_status_numeric("Something Else") == 99


def test_renumber_units_for_somi_condos_only():
    assert renumber_units("SoMi Condos Phase 2", "205") == "1205"
    assert renumber_units("SoMi Condos Phase 2", "150") == "150"
    assert renumber_units("Other Project", "205") == "205"
    assert renumber_units("SoMi Condos Phase 2", "ABC") == "ABC"


def test_generate_alt_project_name_variants():
    row = pd.Series({"Project Name": "SoMi Hayward", "Unit Name": "SoMi HayPark Unit"})
    assert generate_alt_project_name(row) == "SoMi Towns"

    row = pd.Series({"Project Name": "SoMi Hayward", "Unit Name": "SoMi Haypark Unit"})
    assert generate_alt_project_name(row) == "SoMi Condos"

    row = pd.Series({"Project Name": "SoMi Hayward", "Unit Name": "SoMi HayView #12"})
    assert generate_alt_project_name(row) == "SoMi HayView"

    row = pd.Series({"Project Name": "SoMi Hayward", "Unit Name": "Random"})
    assert generate_alt_project_name(row) == "SoMi Hayward"

    row = pd.Series({"Project Name": "New Village", "Unit Name": "Anything"})
    assert generate_alt_project_name(row) == "New Village"


def test_combine_buyers_handles_single_and_double_names():
    row = pd.Series(
        {
            "Buyer Contract: Buyer 1: Full Name": "Ada Lovelace",
            "Buyer Contract: Buyer 2: Full Name": "Alan Turing",
        }
    )
    assert combine_buyers(row) == "Ada Lovelace and Alan Turing"

    row = pd.Series({"Buyer Contract: Buyer 1: Full Name": "Grace Hopper"})
    assert combine_buyers(row) == "Grace Hopper"

    row = pd.Series({"Buyer Contract: Buyer 2: Full Name": "Katherine Johnson"})
    assert combine_buyers(row) == "Katherine Johnson"

    row = pd.Series({"Buyer Contract: Buyer 1: Full Name": float("nan")})
    assert combine_buyers(row) == ""


def test_process_polaris_export_normalizes_fixture():
    records = process_polaris_export(fixture_path(), as_records=True)

    assert len(records) == 2

    first = records[0]
    assert first["Project Name"] == "SoMi Hayward"
    assert first["AltProjectName"] == "SoMi Towns"
    assert first["Contract Unit Number"] == "1205"
    assert first["Status"] == "Ratified - Fully executed"
    assert first["StatusNumeric"] == 2
    assert first["Buyers Combined"] == "Ada Lovelace and Alan Turing"
    assert first["Buyer Contract: COE Date"] == "2025-08-15T00:00:00"
    assert first["pk"] == "SoMi Towns#1205"
    assert first["sk"] == "2025-08-15T00:00:00"

    second = records[1]
    assert second["Project Name"] == "Bay Village"
    assert second["Contract Unit Number"] in (12, 12.0, "12")
    assert second["Buyers Combined"] == "Grace Hopper"
    assert second["pk"] == "Bay Village#12"
    assert second["sk"] == "2025-09-30T00:00:00"

    for record in records:
        assert "Total" not in (record["Project Name"] or "")


def test_process_dataframe_filters_fusion_projects():
    df = pd.DataFrame(
        [
            {
                "Project Name": "Fusion",
                "Unit Name": "Fusion Building 1 - 101",
                "Contract Unit Number": "101",
                "Status": "Available",
            },
            {
                "Project Name": "Bay Village",
                "Unit Name": "Bay Village - 1",
                "Contract Unit Number": "1",
                "Status": "Closed",
            },
        ]
    )

    processed = process_dataframe(df, columns_to_keep=["Project Name", "Unit Name", "Contract Unit Number", "Status"])
    assert "Fusion" not in set(processed["Project Name"])
    assert len(processed) == 1
    assert processed.iloc[0]["Project Name"] == "Bay Village"


def test_finalize_records_uses_status_sort_key_when_dates_missing():
    df = pd.DataFrame(
        [
            {
                "Project Name": "Fusion",
                "Contract Unit Number": "101",
                "Status": "Pending Release",
                "StatusNumeric": 5,
            }
        ]
    )
    records = _finalize_records(df)
    assert records[0]["sk"] == "status#pending-release"


def test_parse_s3_uri_success_and_failure():
    bucket, key = parse_s3_uri("s3://polaris-exports/2025/10/export.xlsx")
    assert bucket == "polaris-exports"
    assert key == "2025/10/export.xlsx"

    with pytest.raises(ValueError):
        parse_s3_uri("https://example.com/file.xlsx")

    with pytest.raises(ValueError):
        parse_s3_uri("s3://bucket")
