import json

import polars as pl

from faersdb.warehouse import (
    MANIFEST_FILENAME,
    REQUIRED_WAREHOUSE_FILES,
    build_manifest,
    validate_warehouse,
    write_manifest,
)


def _write_minimal_warehouse(warehouse):
    warehouse.mkdir()
    for filename in REQUIRED_WAREHOUSE_FILES:
        data = {"id": [1]}
        if filename == "demo.parquet":
            data["source_quarter"] = ["2024q1"]
        if filename == "latest_demo.parquet":
            data["age_years"] = [45.0]
        pl.DataFrame(data).write_parquet(warehouse / filename)


def test_validate_warehouse_reports_missing_files(tmp_path):
    result = validate_warehouse(tmp_path / "warehouse")

    assert not result.ready
    assert set(result.missing_files) == set(REQUIRED_WAREHOUSE_FILES)
    assert result.errors


def test_build_and_write_manifest_for_complete_warehouse(tmp_path):
    warehouse = tmp_path / "warehouse"
    _write_minimal_warehouse(warehouse)

    result = validate_warehouse(warehouse)
    assert result.ready

    manifest = build_manifest(
        warehouse,
        dataset_version="2024q1-test",
        source_url="https://example.test/faers",
        include_hashes=True,
    )
    assert manifest["dataset_version"] == "2024q1-test"
    assert manifest["quarters"] == ["2024q1"]
    assert len(manifest["files"]) == len(REQUIRED_WAREHOUSE_FILES)
    assert all("sha256" in file_info for file_info in manifest["files"])

    manifest_path = write_manifest(warehouse, manifest)
    assert manifest_path.name == MANIFEST_FILENAME
    assert json.loads(manifest_path.read_text(encoding="utf-8"))["dataset_version"] == "2024q1-test"
