"""CLI for the FAERS-DB warehouse.

Commands:
    build    — Build the Parquet warehouse from FAERS ASCII data
    serve    — Start the API server
    info     — Show warehouse statistics
    scan     — List discovered quarter folders
"""

from __future__ import annotations

from pathlib import Path

import typer

from faersdb.config import settings

app = typer.Typer(
    help="FAERS-DB: DuckDB + Parquet warehouse for FDA adverse event data.",
)


@app.command()
def scan():
    """List all discovered FAERS quarter folders."""
    from faersdb.manifest import discover_quarters

    quarters = discover_quarters(Path(settings.data_root))
    if not quarters:
        typer.echo("No quarters found.")
        raise typer.Exit(1)

    for q in quarters:
        typer.echo(
            f"  {q['source_quarter']}  {q['source_system']:5s}  "
            f"{q['schema_era']:20s}  {q['folder_name']}"
        )
    typer.echo(f"\n{len(quarters)} quarters found.")


@app.command()
def build(
    quarter: str | None = typer.Option(
        None,
        "--quarter", "-q",
        help="Build only a specific quarter (e.g. 2024q1). Default: all.",
    ),
    warehouse: str | None = typer.Option(
        None,
        "--warehouse", "-w",
        help="Override warehouse directory.",
    ),
    memory_limit: str = typer.Option(
        settings.memory_limit,
        "--memory-limit",
        help="DuckDB memory limit for final deduplication (e.g. 2GB, 512MB).",
    ),
    threads: int = typer.Option(
        settings.threads,
        "--threads",
        min=1,
        help="Worker threads used by DuckDB finalization.",
    ),
):
    """Build the Parquet warehouse from FAERS ASCII data.

    Reads all quarter folders under DATA_ROOT, normalizes the data,
    and writes compressed Parquet files to the warehouse directory.
    """
    from faersdb.etl import build_warehouse

    data_root = Path(settings.data_root)
    warehouse_dir = Path(warehouse) if warehouse else settings.warehouse_path

    if not data_root.exists():
        typer.echo(f"Data root not found: {data_root}")
        raise typer.Exit(1)

    results = build_warehouse(
        data_root=data_root,
        warehouse_dir=warehouse_dir,
        quarter_filter=quarter,
        memory_limit=memory_limit,
        threads=threads,
    )

    if not results:
        raise typer.Exit(1)


@app.command()
def info(
    warehouse: str | None = typer.Option(
        None,
        "--warehouse", "-w",
        help="Override warehouse directory.",
    ),
):
    """Show warehouse statistics (row counts, sizes, columns)."""
    from faersdb.etl import warehouse_info

    warehouse_dir = Path(warehouse) if warehouse else settings.warehouse_path

    if not warehouse_dir.exists():
        typer.echo(f"Warehouse not found: {warehouse_dir}")
        typer.echo("Run 'build' first to create the warehouse.")
        raise typer.Exit(1)

    stats = warehouse_info(warehouse_dir)
    if not stats:
        typer.echo("No Parquet files found in warehouse.")
        raise typer.Exit(1)

    total_rows = 0
    total_mb = 0.0

    for table_name, table_info in stats.items():
        if "error" in table_info:
            typer.echo(f"  {table_name:8s}  ERROR: {table_info['error']}")
            continue
        rows = table_info["rows"]
        size_mb = table_info["size_mb"]
        total_rows += rows
        total_mb += size_mb
        typer.echo(
            f"  {table_name:8s}  {rows:>12,} rows  {size_mb:>8.1f} MB  "
            f"cols: {', '.join(table_info['columns'])}"
        )

    typer.echo(f"\n  {'TOTAL':8s}  {total_rows:>12,} rows  {total_mb:>8.1f} MB")


@app.command()
def serve(
    host: str = typer.Option("127.0.0.1", help="Bind host"),
    port: int = typer.Option(8000, help="Bind port"),
    reload: bool = typer.Option(False, "--reload", help="Enable hot-reload"),
):
    """Start the FastAPI server."""
    import uvicorn

    uvicorn.run(
        "faersdb.api:app",
        host=host,
        port=port,
        reload=reload,
    )


if __name__ == "__main__":
    app()
