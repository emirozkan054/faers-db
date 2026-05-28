"""DuckDB connection manager that reads Parquet warehouse files."""

from __future__ import annotations

import threading
from pathlib import Path

import duckdb

from faersdb.config import settings, sql_string

# Table names that map to Parquet files in the warehouse directory.
RAW_WAREHOUSE_TABLES = ("demo", "drug", "reac", "outc", "ther", "indi", "rpsr")
QUERY_WAREHOUSE_TABLES = (
    "latest_demo",
    "latest_drug",
    "latest_reac",
    "latest_outc",
    "latest_ther",
    "latest_indi",
    "latest_rpsr",
    "case_summary",
    "filter_metadata",
)
WAREHOUSE_TABLES = (*RAW_WAREHOUSE_TABLES, *QUERY_WAREHOUSE_TABLES)
REQUIRED_QUERY_TABLES = frozenset(QUERY_WAREHOUSE_TABLES)
REQUIRED_QUERY_TABLE_COLUMNS = {
    "latest_demo": ("age_years",),
}

# Module-level singleton connection for the query layer.
_lock = threading.Lock()
_shared_conn: duckdb.DuckDBPyConnection | None = None
_shared_warehouse_dir: Path | None = None


def _register_warehouse(conn: duckdb.DuckDBPyConnection, warehouse_dir: Path) -> None:
    """Register each Parquet file as a DuckDB view for transparent querying."""
    for table_name in WAREHOUSE_TABLES:
        parquet_path = warehouse_dir / f"{table_name}.parquet"
        if parquet_path.exists():
            conn.execute(
                f"CREATE OR REPLACE VIEW {table_name} AS "
                f"SELECT * FROM read_parquet({sql_string(str(parquet_path))})"
            )


def missing_query_tables(warehouse_dir: Path | None = None) -> list[str]:
    """Return required derived query tables missing from the warehouse."""
    root = warehouse_dir or settings.warehouse_path
    missing = [
        table_name
        for table_name in QUERY_WAREHOUSE_TABLES
        if not (root / f"{table_name}.parquet").exists()
    ]
    for table_name, required_columns in REQUIRED_QUERY_TABLE_COLUMNS.items():
        parquet_path = root / f"{table_name}.parquet"
        if not parquet_path.exists():
            continue
        conn = duckdb.connect(database=":memory:")
        try:
            columns = {
                row[0]
                for row in conn.execute(
                    "DESCRIBE SELECT * FROM read_parquet(?)", [str(parquet_path)]
                ).fetchall()
            }
        except duckdb.Error:
            missing.append(table_name)
            continue
        finally:
            conn.close()
        missing.extend(
            f"{table_name}.{column}"
            for column in required_columns
            if column not in columns
        )
    return missing


def get_conn() -> duckdb.DuckDBPyConnection:
    """Return the shared DuckDB connection with warehouse views registered.

    Uses a module-level singleton connection that persists across requests.
    DuckDB Parquet views over immutable files are safe to reuse.
    """
    global _shared_conn, _shared_warehouse_dir

    warehouse_dir = settings.warehouse_path
    with _lock:
        if _shared_conn is not None and _shared_warehouse_dir == warehouse_dir:
            return _shared_conn

        # Close any stale connection (e.g. warehouse_dir changed during tests)
        if _shared_conn is not None:
            try:
                _shared_conn.close()
            except Exception:
                pass

        conn = duckdb.connect(database=":memory:")
        conn.execute(f"SET memory_limit = {sql_string(settings.memory_limit)}")
        conn.execute(f"SET threads = {settings.threads}")
        _register_warehouse(conn, warehouse_dir)
        _shared_conn = conn
        _shared_warehouse_dir = warehouse_dir
        return conn


def reset_shared_conn() -> None:
    """Close the shared connection (for testing or warehouse rebuilds)."""
    global _shared_conn, _shared_warehouse_dir
    with _lock:
        if _shared_conn is not None:
            try:
                _shared_conn.close()
            except Exception:
                pass
            _shared_conn = None
            _shared_warehouse_dir = None
