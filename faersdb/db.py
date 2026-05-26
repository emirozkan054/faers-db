"""DuckDB connection manager that reads Parquet warehouse files."""

from __future__ import annotations

from pathlib import Path

import duckdb

from faersdb.config import settings

# Table names that map to Parquet files in the warehouse directory.
WAREHOUSE_TABLES = ("demo", "drug", "reac", "outc", "ther", "indi", "rpsr")


def _register_warehouse(conn: duckdb.DuckDBPyConnection, warehouse_dir: Path) -> None:
    """Register each Parquet file as a DuckDB view for transparent querying."""
    for table_name in WAREHOUSE_TABLES:
        parquet_path = warehouse_dir / f"{table_name}.parquet"
        if parquet_path.exists():
            conn.execute(
                f"CREATE OR REPLACE VIEW {table_name} AS "
                f"SELECT * FROM read_parquet('{parquet_path}')"
            )


def get_conn() -> duckdb.DuckDBPyConnection:
    """Return a fresh in-memory DuckDB connection with warehouse views registered."""
    conn = duckdb.connect(database=":memory:")
    conn.execute(f"SET memory_limit = '{settings.memory_limit}'")
    conn.execute(f"SET threads = {settings.threads}")
    _register_warehouse(conn, settings.warehouse_path)
    return conn