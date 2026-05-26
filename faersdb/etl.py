"""ETL pipeline: read FAERS ASCII files → normalize → write Parquet.

Single-pass, streaming pipeline using Polars for all transforms.
Replaces direct_load.py, staging_load.py, and normalize/ modules.
"""

from __future__ import annotations

import time
import shutil
from pathlib import Path

import duckdb
import polars as pl
import typer

from faersdb.detect import discover_files
from faersdb.manifest import discover_quarters

# ─── Constants ────────────────────────────────────────────────────────────────

DELIMITER = "$"

# ─── Helpers ──────────────────────────────────────────────────────────────────


def _fmt(seconds: float) -> str:
    if seconds < 60:
        return f"{seconds:.1f}s"
    minutes, secs = divmod(seconds, 60)
    if minutes < 60:
        return f"{int(minutes)}m {secs:.0f}s"
    hours, minutes = divmod(minutes, 60)
    return f"{int(hours)}h {int(minutes)}m {secs:.0f}s"


def _fmt_mb(size: int) -> str:
    return f"{size / 1024 / 1024:.1f} MB"


def _clean_colname(name: str) -> str:
    return name.replace("\ufeff", "").replace("ï»¿", "").strip().upper()


def _read_ascii_file(file_path: Path) -> pl.LazyFrame:
    """Read a single FAERS '$'-delimited file into a Polars LazyFrame.

    All columns are read as Utf8 (strings). Column names are uppercased and
    cleaned of BOM artifacts.
    """
    df = pl.scan_csv(
        file_path,
        separator=DELIMITER,
        encoding="utf8-lossy",
        infer_schema=False,       # keep everything as string
        quote_char=None,          # do not treat double-quotes specially
        ignore_errors=True,
        truncate_ragged_lines=True,
        null_values=[""],
        low_memory=True,
    )
    # Normalize column names
    rename_map = {col: _clean_colname(col) for col in df.collect_schema().names()}
    return df.rename(rename_map)


def _coalesce_cols(lf: pl.LazyFrame, *names: str) -> pl.Expr:
    """COALESCE across multiple column names that may or may not exist."""
    present = [name for name in names if name in lf.collect_schema().names()]
    if not present:
        return pl.lit(None, dtype=pl.Utf8)
    if len(present) == 1:
        return pl.col(present[0]).str.strip_chars()
    return pl.coalesce([pl.col(c).str.strip_chars() for c in present])


def _clean_text(lf: pl.LazyFrame, name: str) -> pl.Expr:
    """Clean a text column: strip whitespace, convert empty to null."""
    if name not in lf.collect_schema().names():
        return pl.lit(None, dtype=pl.Utf8)
    return (
        pl.col(name)
        .str.strip_chars()
        .replace("", None)
    )


def _parse_date(expr: pl.Expr) -> pl.Expr:
    """Parse YYYYMMDD date strings. Only keep full 8-digit dates."""
    return (
        pl.when(expr.str.len_chars() == 8)
        .then(expr.str.to_date("%Y%m%d", strict=False))
        .otherwise(pl.lit(None, dtype=pl.Date))
    )


def _parse_int(expr: pl.Expr) -> pl.Expr:
    """Parse an integer from a string, returning null on failure."""
    return expr.cast(pl.Int32, strict=False)


def _parse_float(expr: pl.Expr) -> pl.Expr:
    """Parse a float from a string, returning null on failure."""
    return expr.cast(pl.Float64, strict=False)


def _norm_sex(expr: pl.Expr) -> pl.Expr:
    """Normalize sex codes to M/F/UNK."""
    upper = expr.str.to_uppercase()
    return (
        pl.when(upper.is_in(["M", "MALE", "1"])).then(pl.lit("M"))
        .when(upper.is_in(["F", "FEMALE", "2"])).then(pl.lit("F"))
        .otherwise(pl.lit("UNK"))
    )


# ─── Per-Table Processors ────────────────────────────────────────────────────


def _process_demo(
    lf: pl.LazyFrame, meta: dict
) -> pl.LazyFrame:
    """Normalize a DEMO file into the target schema."""
    primaryid = _coalesce_cols(lf, "PRIMARYID", "ISR", "REPORT_ID")
    caseid = _coalesce_cols(lf, "CASEID", "CASE_ID", "CASE")
    sex_raw = _coalesce_cols(lf, "SEX", "GNDR_COD")
    wt_raw = _clean_text(lf, "WT")
    wt_cod = _clean_text(lf, "WT_COD")
    i_f = _coalesce_cols(lf, "I_F_COD", "I_F_CODE")

    return lf.select(
        primaryid.alias("primaryid"),
        caseid.alias("caseid"),
        pl.lit(meta["source_quarter"]).alias("source_quarter"),
        pl.lit(meta["source_system"]).alias("source_system"),
        _parse_int(_clean_text(lf, "CASEVERSION")).alias("caseversion"),
        _clean_text(lf, "REPT_COD").alias("report_type"),
        i_f.alias("i_f_code"),
        _parse_date(_clean_text(lf, "EVENT_DT")).alias("event_dt"),
        _parse_date(_clean_text(lf, "MFR_DT")).alias("mfr_dt"),
        _parse_date(_clean_text(lf, "FDA_DT")).alias("fda_dt"),
        _parse_float(_clean_text(lf, "AGE")).alias("age"),
        _clean_text(lf, "AGE_COD").alias("age_cod"),
        _clean_text(lf, "AGE_GRP").alias("age_grp"),
        _norm_sex(sex_raw).alias("sex"),
        # Weight: only keep KG
        (
            pl.when(
                wt_cod.str.to_uppercase().is_in(["KG", ""]).fill_null(True)
            )
            .then(_parse_float(wt_raw))
            .otherwise(pl.lit(None, dtype=pl.Float64))
        ).alias("wt_kg"),
        _clean_text(lf, "REPORTER_COUNTRY").alias("reporter_country"),
        _clean_text(lf, "AUTH_NUM").alias("auth_num"),
        _clean_text(lf, "LIT_REF").alias("lit_ref"),
    ).filter(
        pl.col("primaryid").is_not_null() & pl.col("caseid").is_not_null()
    )


def _process_drug(
    lf: pl.LazyFrame, meta: dict
) -> pl.LazyFrame:
    """Normalize a DRUG file."""
    primaryid = _coalesce_cols(lf, "PRIMARYID", "ISR", "REPORT_ID")

    return lf.select(
        primaryid.alias("primaryid"),
        pl.lit(meta["source_quarter"]).alias("source_quarter"),
        _parse_int(_clean_text(lf, "DRUG_SEQ")).alias("drug_seq"),
        _clean_text(lf, "ROLE_COD").alias("role_cod"),
        _clean_text(lf, "DRUGNAME").alias("drugname"),
        _clean_text(lf, "PROD_AI").alias("prod_ai"),
        _clean_text(lf, "ROUTE").alias("route"),
        _clean_text(lf, "DOSE_VBM").alias("dose_vbm"),
        _parse_float(_clean_text(lf, "DOSE_AMT")).alias("dose_amt"),
        _clean_text(lf, "DOSE_UNIT").alias("dose_unit"),
        _parse_date(_clean_text(lf, "START_DT")).alias("start_dt"),
        _parse_date(_clean_text(lf, "END_DT")).alias("end_dt"),
    ).filter(
        pl.col("primaryid").is_not_null() & pl.col("drugname").is_not_null()
    )


def _process_reac(
    lf: pl.LazyFrame, meta: dict
) -> pl.LazyFrame:
    """Normalize a REAC file."""
    primaryid = _coalesce_cols(lf, "PRIMARYID", "ISR", "REPORT_ID")
    pt = _coalesce_cols(lf, "PT", "REAC_PT")

    return lf.select(
        primaryid.alias("primaryid"),
        pl.lit(meta["source_quarter"]).alias("source_quarter"),
        pt.alias("pt"),
        _clean_text(lf, "DRUG_REC_ACT").alias("drug_rec_act"),
    ).filter(
        pl.col("primaryid").is_not_null() & pl.col("pt").is_not_null()
    )


def _process_outc(
    lf: pl.LazyFrame, meta: dict
) -> pl.LazyFrame:
    """Normalize an OUTC file."""
    primaryid = _coalesce_cols(lf, "PRIMARYID", "ISR", "REPORT_ID")
    outcome = _coalesce_cols(lf, "OUTC_COD", "OUTCOME")

    return lf.select(
        primaryid.alias("primaryid"),
        pl.lit(meta["source_quarter"]).alias("source_quarter"),
        outcome.alias("outc_cod"),
    ).filter(
        pl.col("primaryid").is_not_null() & pl.col("outc_cod").is_not_null()
    )


def _process_ther(
    lf: pl.LazyFrame, meta: dict
) -> pl.LazyFrame:
    """Normalize a THER file."""
    primaryid = _coalesce_cols(lf, "PRIMARYID", "ISR", "REPORT_ID")
    drug_seq = _coalesce_cols(lf, "DSG_DRUG_SEQ", "DRUG_SEQ")

    return lf.select(
        primaryid.alias("primaryid"),
        pl.lit(meta["source_quarter"]).alias("source_quarter"),
        _parse_int(drug_seq).alias("drug_seq"),
        _parse_date(_clean_text(lf, "START_DT")).alias("start_dt"),
        _parse_date(_clean_text(lf, "END_DT")).alias("end_dt"),
        _parse_int(_clean_text(lf, "DUR")).alias("dur"),
        _clean_text(lf, "DUR_COD").alias("dur_cod"),
    ).filter(
        pl.col("primaryid").is_not_null()
    )


def _process_indi(
    lf: pl.LazyFrame, meta: dict
) -> pl.LazyFrame:
    """Normalize an INDI file."""
    primaryid = _coalesce_cols(lf, "PRIMARYID", "ISR", "REPORT_ID")
    drug_seq = _coalesce_cols(lf, "INDI_DRUG_SEQ", "DRUG_SEQ")
    indi_pt = _coalesce_cols(lf, "INDI_PT", "INDICATION")

    return lf.select(
        primaryid.alias("primaryid"),
        pl.lit(meta["source_quarter"]).alias("source_quarter"),
        _parse_int(drug_seq).alias("drug_seq"),
        indi_pt.alias("indi_pt"),
    ).filter(
        pl.col("primaryid").is_not_null() & pl.col("indi_pt").is_not_null()
    )


def _process_rpsr(
    lf: pl.LazyFrame, meta: dict
) -> pl.LazyFrame:
    """Normalize an RPSR file."""
    primaryid = _coalesce_cols(lf, "PRIMARYID", "ISR", "REPORT_ID")
    rpsr_cod = _coalesce_cols(lf, "RPSR_COD", "REPORTER_TYPE")

    return lf.select(
        primaryid.alias("primaryid"),
        pl.lit(meta["source_quarter"]).alias("source_quarter"),
        rpsr_cod.alias("rpsr_cod"),
    ).filter(
        pl.col("primaryid").is_not_null() & pl.col("rpsr_cod").is_not_null()
    )


# ─── Table Specs ──────────────────────────────────────────────────────────────

TABLE_SPECS: dict[str, dict] = {
    "DEMO": {
        "processor": _process_demo,
        "output": "demo.parquet",
    },
    "DRUG": {
        "processor": _process_drug,
        "output": "drug.parquet",
    },
    "REAC": {
        "processor": _process_reac,
        "output": "reac.parquet",
    },
    "OUTC": {
        "processor": _process_outc,
        "output": "outc.parquet",
    },
    "THER": {
        "processor": _process_ther,
        "output": "ther.parquet",
    },
    "INDI": {
        "processor": _process_indi,
        "output": "indi.parquet",
    },
    "RPSR": {
        "processor": _process_rpsr,
        "output": "rpsr.parquet",
    },
}

# ─── Delete File Handling ─────────────────────────────────────────────────────


def _collect_deleted_ids(
    quarters: list[dict],
    data_root: Path,
) -> set[str]:
    """Collect all primaryids listed in DELETE files across all quarters."""
    deleted: set[str] = set()
    delete_files: list[Path] = []
    for q in quarters:
        folder = Path(q["folder_path"])
        files = discover_files(folder)
        delete_files.extend(p for kind, p in files if kind == "DELETE")

    for index, fp in enumerate(delete_files, start=1):
        typer.echo(f"  DELETE: [{index}/{len(delete_files)}] {fp.name}")
        with open(fp, "r", encoding="utf-8-sig", errors="replace") as f:
            for line in f:
                pid = line.strip()
                if pid:
                    deleted.add(pid)
    return deleted


# ─── Core Build Logic ─────────────────────────────────────────────────────────


def _sql_string(value: str) -> str:
    """Return a single-quoted DuckDB SQL string literal."""
    return "'" + value.replace("'", "''") + "'"


def _sql_string_list(values: list[str]) -> str:
    return "[" + ", ".join(_sql_string(value) for value in values) + "]"


def _finalize_shards(
    shard_paths: list[Path],
    output_path: Path,
    warehouse_dir: Path,
    *,
    memory_limit: str,
    threads: int,
) -> int:
    """Deduplicate parquet shards into one output file using DuckDB spill logic."""
    temp_dir = warehouse_dir / "_duckdb_tmp"
    temp_dir.mkdir(parents=True, exist_ok=True)

    temp_output = output_path.with_name(f".{output_path.name}.tmp")
    temp_output.unlink(missing_ok=True)

    con = duckdb.connect()
    try:
        con.execute(f"SET memory_limit={_sql_string(memory_limit)}")
        con.execute(f"SET threads={max(1, threads)}")
        con.execute(f"SET temp_directory={_sql_string(str(temp_dir))}")
        paths_sql = _sql_string_list([str(path) for path in shard_paths])
        con.execute(
            "COPY ("
            f"SELECT DISTINCT * FROM read_parquet({paths_sql}, union_by_name=true)"
            f") TO {_sql_string(str(temp_output))} "
            "(FORMAT PARQUET, COMPRESSION SNAPPY)"
        )
    finally:
        con.close()

    temp_output.replace(output_path)
    return pl.scan_parquet(output_path).select(pl.len()).collect().item()


def build_table(
    table_kind: str,
    quarters: list[dict],
    warehouse_dir: Path,
    *,
    quarter_filter: str | None = None,
    memory_limit: str = "2GB",
    threads: int = 4,
) -> int:
    """Build one Parquet table from all quarter files.

    Returns the total number of rows written.
    """
    spec = TABLE_SPECS[table_kind]
    processor = spec["processor"]
    output_path = warehouse_dir / spec["output"]

    work_items: list[tuple[dict, Path]] = []
    for q in quarters:
        if quarter_filter and q["source_quarter"] != quarter_filter:
            continue
        folder = Path(q["folder_path"])
        files = discover_files(folder)
        kind_files = [p for kind, p in files if kind == table_kind]
        work_items.extend((q, fp) for fp in kind_files)

    if not work_items:
        typer.echo(f"  {table_kind}: no files found")
        return 0

    input_size = sum(fp.stat().st_size for _, fp in work_items)
    typer.echo(
        f"  {table_kind}: processing {len(work_items)} files "
        f"({_fmt_mb(input_size)} input)"
    )

    temp_dir = warehouse_dir / "_build_tmp" / table_kind.lower()
    if temp_dir.exists():
        shutil.rmtree(temp_dir)
    temp_dir.mkdir(parents=True, exist_ok=True)

    shard_paths: list[Path] = []
    shard_count = 0

    try:
        t0 = time.perf_counter()
        for index, (q, fp) in enumerate(work_items, start=1):
            meta = {
                "source_quarter": q["source_quarter"],
                "source_system": q["source_system"],
                "schema_era": q["schema_era"],
            }

            try:
                typer.echo(
                    f"  {table_kind}: [{index}/{len(work_items)}] "
                    f"{q['source_quarter']} {fp.name} ({_fmt_mb(fp.stat().st_size)})"
                )
                lf = _read_ascii_file(fp)
                processed = processor(lf, meta)
                shard_path = (
                    temp_dir
                    / f"{q['source_quarter']}_{shard_count:03d}_{fp.stem}.parquet"
                )
                processed.sink_parquet(
                    shard_path,
                    compression="snappy",
                    statistics=True,
                )
                shard_paths.append(shard_path)
                shard_count += 1
            except Exception as exc:
                typer.echo(f"  WARNING: skipping {fp.name}: {exc}")

        if not shard_paths:
            typer.echo(f"  {table_kind}: no files processed")
            return 0

        typer.echo(
            f"  {table_kind}: finalizing {len(shard_paths)} files "
            f"with DuckDB memory_limit={memory_limit}..."
        )
        row_count = _finalize_shards(
            shard_paths,
            output_path,
            warehouse_dir,
            memory_limit=memory_limit,
            threads=threads,
        )

        typer.echo(
            f"  {table_kind}: {row_count:,} rows → {output_path.name} "
            f"({output_path.stat().st_size / 1024 / 1024:.1f} MB) "
            f"in {_fmt(time.perf_counter() - t0)}"
        )
        return row_count
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)


def apply_deletes(
    quarters: list[dict],
    data_root: Path,
    warehouse_dir: Path,
) -> int:
    """Mark deleted primaryids in demo.parquet."""
    demo_path = warehouse_dir / "demo.parquet"
    if not demo_path.exists():
        return 0

    t0 = time.perf_counter()
    typer.echo("  DELETE: collecting deleted IDs...")
    deleted_ids = _collect_deleted_ids(quarters, data_root)
    if not deleted_ids:
        # Add is_deleted column as all false if no deletes
        df = pl.read_parquet(demo_path)
        if "is_deleted" not in df.columns:
            df = df.with_columns(pl.lit(False).alias("is_deleted"))
            df.write_parquet(demo_path, compression="snappy", statistics=True)
        typer.echo("  DELETE: 0 deleted IDs found")
        return 0

    typer.echo(f"  DELETE: {len(deleted_ids):,} deleted IDs found, marking in demo...")
    df = pl.read_parquet(demo_path)
    df = df.with_columns(
        pl.col("primaryid").is_in(list(deleted_ids)).alias("is_deleted")
    )
    df.write_parquet(demo_path, compression="snappy", statistics=True)

    marked = df.filter(pl.col("is_deleted")).height
    typer.echo(
        f"  DELETE: marked {marked:,} rows as deleted "
        f"in {_fmt(time.perf_counter() - t0)}"
    )
    return marked


def build_warehouse(
    data_root: Path,
    warehouse_dir: Path,
    *,
    quarter_filter: str | None = None,
    memory_limit: str = "2GB",
    threads: int = 4,
) -> dict[str, int]:
    """Build the complete Parquet warehouse from FAERS ASCII data.

    Returns a dict of table_kind → row_count.
    """
    t0 = time.perf_counter()
    warehouse_dir.mkdir(parents=True, exist_ok=True)

    typer.echo(f"Building warehouse at {warehouse_dir}")
    quarters = discover_quarters(data_root)
    if not quarters:
        typer.echo("No quarter folders found.")
        return {}

    if quarter_filter:
        matching = [q for q in quarters if q["source_quarter"] == quarter_filter]
        if not matching:
            typer.echo(f"Quarter {quarter_filter} not found.")
            return {}
        typer.echo(f"Building only quarter: {quarter_filter}")
    else:
        typer.echo(f"Found {len(quarters)} quarters: {quarters[0]['source_quarter']} → {quarters[-1]['source_quarter']}")

    results: dict[str, int] = {}

    for table_kind in TABLE_SPECS:
        results[table_kind] = build_table(
            table_kind,
            quarters,
            warehouse_dir,
            quarter_filter=quarter_filter,
            memory_limit=memory_limit,
            threads=threads,
        )

    shutil.rmtree(warehouse_dir / "_build_tmp", ignore_errors=True)

    # Apply deletes
    results["DELETE"] = apply_deletes(quarters, data_root, warehouse_dir)

    total_rows = sum(results.values())
    total_size_mb = sum(
        f.stat().st_size
        for f in warehouse_dir.iterdir()
        if f.suffix == ".parquet"
    ) / 1024 / 1024

    typer.echo(
        f"\nWarehouse built: {total_rows:,} total rows, "
        f"{total_size_mb:.1f} MB on disk, "
        f"elapsed {_fmt(time.perf_counter() - t0)}"
    )
    return results


def warehouse_info(warehouse_dir: Path) -> dict[str, dict]:
    """Return stats about each Parquet file in the warehouse."""
    info: dict[str, dict] = {}
    for table_name in list(TABLE_SPECS.keys()) + ["DELETE"]:
        fname = f"{table_name.lower()}.parquet"
        fpath = warehouse_dir / fname
        if fpath.exists():
            try:
                df = pl.scan_parquet(fpath)
                schema = df.collect_schema()
                row_count = pl.scan_parquet(fpath).select(pl.len()).collect().item()
                info[table_name] = {
                    "file": fname,
                    "rows": row_count,
                    "size_mb": round(fpath.stat().st_size / 1024 / 1024, 1),
                    "columns": list(schema.names()),
                }
            except Exception as exc:
                info[table_name] = {"file": fname, "error": str(exc)}
    return info
