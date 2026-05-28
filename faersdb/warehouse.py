"""Runtime warehouse validation and release manifest helpers."""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
from typing import Any

import duckdb

from faersdb.db import (
    RAW_WAREHOUSE_TABLES,
    QUERY_WAREHOUSE_TABLES,
    missing_query_tables,
)

MANIFEST_FILENAME = "warehouse-manifest.json"
REQUIRED_WAREHOUSE_TABLES = (*RAW_WAREHOUSE_TABLES, *QUERY_WAREHOUSE_TABLES)
REQUIRED_WAREHOUSE_FILES = tuple(
    f"{table_name}.parquet" for table_name in REQUIRED_WAREHOUSE_TABLES
)


@dataclass(frozen=True)
class WarehouseFileStatus:
    name: str
    exists: bool
    size_bytes: int | None = None
    readable: bool = False
    error: str | None = None


@dataclass(frozen=True)
class WarehouseValidationResult:
    warehouse_dir: str
    ready: bool
    manifest_path: str | None
    missing_files: list[str]
    unreadable_files: list[str]
    missing_query_tables: list[str]
    files: list[WarehouseFileStatus]

    @property
    def errors(self) -> list[str]:
        errors: list[str] = []
        if self.missing_files:
            errors.append("Missing required warehouse files: " + ", ".join(self.missing_files))
        if self.unreadable_files:
            errors.append("Unreadable warehouse files: " + ", ".join(self.unreadable_files))
        if self.missing_query_tables:
            errors.append(
                "Missing or incompatible derived query tables: "
                + ", ".join(self.missing_query_tables)
            )
        return errors

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["errors"] = self.errors
        return data


def app_version() -> str:
    try:
        return version("faers-db")
    except PackageNotFoundError:
        return "0.0.0+local"


def validate_warehouse(warehouse_dir: Path) -> WarehouseValidationResult:
    """Check that a warehouse bundle has all required, readable Parquet files."""
    root = warehouse_dir.expanduser().resolve()
    file_statuses: list[WarehouseFileStatus] = []
    missing_files: list[str] = []
    unreadable_files: list[str] = []

    for filename in REQUIRED_WAREHOUSE_FILES:
        path = root / filename
        if not path.exists():
            missing_files.append(filename)
            file_statuses.append(WarehouseFileStatus(name=filename, exists=False))
            continue

        try:
            with duckdb.connect(database=":memory:") as conn:
                conn.execute("DESCRIBE SELECT * FROM read_parquet(?)", [str(path)]).fetchall()
        except duckdb.Error as exc:
            unreadable_files.append(filename)
            file_statuses.append(
                WarehouseFileStatus(
                    name=filename,
                    exists=True,
                    size_bytes=path.stat().st_size,
                    readable=False,
                    error=str(exc),
                )
            )
            continue

        file_statuses.append(
            WarehouseFileStatus(
                name=filename,
                exists=True,
                size_bytes=path.stat().st_size,
                readable=True,
            )
        )

    missing_query = missing_query_tables(root)
    manifest_path = root / MANIFEST_FILENAME
    ready = not missing_files and not unreadable_files and not missing_query

    return WarehouseValidationResult(
        warehouse_dir=str(root),
        ready=ready,
        manifest_path=str(manifest_path) if manifest_path.exists() else None,
        missing_files=missing_files,
        unreadable_files=unreadable_files,
        missing_query_tables=missing_query,
        files=file_statuses,
    )


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as file:
        for chunk in iter(lambda: file.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _table_row_count(conn: duckdb.DuckDBPyConnection, path: Path) -> int:
    return int(conn.execute("SELECT count(*) FROM read_parquet(?)", [str(path)]).fetchone()[0])


def build_manifest(
    warehouse_dir: Path,
    *,
    dataset_version: str,
    source_url: str | None = None,
    notes: str | None = None,
    include_hashes: bool = True,
) -> dict[str, Any]:
    """Build a release manifest for a validated warehouse directory."""
    root = warehouse_dir.expanduser().resolve()
    validation = validate_warehouse(root)
    if not validation.ready:
        raise ValueError("; ".join(validation.errors))

    generated_at = datetime.now(UTC).replace(microsecond=0).isoformat()
    files: list[dict[str, Any]] = []
    quarters: list[str] = []

    with duckdb.connect(database=":memory:") as conn:
        demo_path = root / "demo.parquet"
        quarters = [
            row[0]
            for row in conn.execute(
                """
                SELECT DISTINCT source_quarter
                FROM read_parquet(?)
                WHERE source_quarter IS NOT NULL
                ORDER BY source_quarter
                """,
                [str(demo_path)],
            ).fetchall()
        ]

        for filename in REQUIRED_WAREHOUSE_FILES:
            path = root / filename
            file_info: dict[str, Any] = {
                "name": filename,
                "size_bytes": path.stat().st_size,
                "rows": _table_row_count(conn, path),
            }
            if include_hashes:
                file_info["sha256"] = file_sha256(path)
            files.append(file_info)

    return {
        "schema_version": 1,
        "dataset_version": dataset_version,
        "app_version": app_version(),
        "generated_at": generated_at,
        "source_url": source_url,
        "notes": notes,
        "quarters": quarters,
        "required_files": list(REQUIRED_WAREHOUSE_FILES),
        "files": files,
    }


def write_manifest(warehouse_dir: Path, manifest: dict[str, Any]) -> Path:
    output_path = warehouse_dir.expanduser().resolve() / MANIFEST_FILENAME
    output_path.write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return output_path
