"""Streaming, normalized-only loader for laptop-safe historical backfills."""

from __future__ import annotations

from dataclasses import dataclass
import re
import time
from pathlib import Path

import typer
from psycopg import sql

from faersdb.db import get_conn
from faersdb.staging_load import clean_colname, clean_value

DELIMITER = "$"
DEFAULT_COPY_CHUNK_MB = 4
DEFAULT_WORK_MEM = "128MB"
COPY_OPTIONS = "WITH (FORMAT csv, DELIMITER '$', QUOTE E'\\b', NULL '')"
SAFE_COL_RE = re.compile(r"[^a-z0-9_]+")


@dataclass(frozen=True)
class TempColumn:
    source_name: str
    sql_name: str


@dataclass(frozen=True)
class LoadedTempTable:
    name: str
    columns: tuple[TempColumn, ...]
    rows_loaded: int

    @property
    def by_source(self) -> dict[str, TempColumn]:
        out: dict[str, TempColumn] = {}
        for column in self.columns:
            out.setdefault(column.source_name, column)
        return out


def _fmt(seconds: float) -> str:
    if seconds < 60:
        return f"{seconds:.1f}s"
    minutes, secs = divmod(seconds, 60)
    return f"{int(minutes)}m {secs:.0f}s"


def _quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _quote_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def safe_sql_name(name: str, fallback: str = "col") -> str:
    cleaned = clean_colname(name).lower()
    safe = SAFE_COL_RE.sub("_", cleaned).strip("_")
    if not safe:
        safe = fallback
    if safe[0].isdigit():
        safe = f"c_{safe}"
    return safe


def build_temp_columns(raw_header: list[str]) -> tuple[TempColumn, ...]:
    columns: list[TempColumn] = []
    used: dict[str, int] = {}

    for idx, raw_name in enumerate(raw_header, start=1):
        source_name = clean_colname(raw_name)
        base_name = safe_sql_name(source_name, fallback=f"col_{idx}")
        count = used.get(base_name, 0) + 1
        used[base_name] = count
        sql_name = base_name if count == 1 else f"{base_name}_{count}"
        columns.append(TempColumn(source_name=source_name, sql_name=sql_name))

    return tuple(columns)


def _column_expr(table: LoadedTempTable, source_name: str, alias: str = "t") -> str | None:
    column = table.by_source.get(clean_colname(source_name))
    if not column:
        return None
    return f"{alias}.{_quote_ident(column.sql_name)}"


def clean_text_expr(table: LoadedTempTable, source_name: str, alias: str = "t") -> str:
    expr = _column_expr(table, source_name, alias)
    if not expr:
        return "NULL::text"
    return f"NULLIF(BTRIM({expr}), '')"


def first_present_expr(
    table: LoadedTempTable,
    source_names: list[str] | tuple[str, ...],
    alias: str = "t",
) -> str:
    exprs = [
        clean_text_expr(table, source_name, alias)
        for source_name in source_names
        if _column_expr(table, source_name, alias)
    ]
    if not exprs:
        return "NULL::text"
    if len(exprs) == 1:
        return exprs[0]
    return f"COALESCE({', '.join(exprs)})"


def int_expr(expr: str) -> str:
    return f"CASE WHEN {expr} ~ '^\\d+$' THEN {expr}::int END"


def decimal_expr(expr: str) -> str:
    return f"CASE WHEN {expr} ~ '^[+-]?\\d+(\\.\\d+)?$' THEN {expr}::numeric END"


def date_expr(expr: str) -> str:
    return f"CASE WHEN {expr} ~ '^\\d{{8}}$' THEN TO_DATE({expr}, 'YYYYMMDD') END"


def row_hash_expr(table: LoadedTempTable, alias: str = "t") -> str:
    if not table.columns:
        return "MD5('[]')"
    values = [
        f"NULLIF(BTRIM({alias}.{_quote_ident(column.sql_name)}), '')"
        for column in table.columns
    ]
    return f"MD5(JSONB_BUILD_ARRAY({', '.join(values)})::text)"


def _normalize_copy_line(raw_line: str, expected_fields: int) -> str | None:
    line = raw_line.rstrip("\r\n")
    if not line or not line.strip(DELIMITER + " \t"):
        return None

    parts = line.split(DELIMITER)
    if len(parts) < expected_fields:
        parts.extend([""] * (expected_fields - len(parts)))
    elif len(parts) > expected_fields:
        parts = parts[:expected_fields]

    return DELIMITER.join(parts) + "\n"


def _copy_text_lines(cur, copy_sql, lines, chunk_bytes: int) -> int:
    rows_loaded = 0
    pending: list[str] = []
    pending_bytes = 0

    with cur.copy(copy_sql) as copy:
        for line in lines:
            if line is None:
                continue
            pending.append(line)
            pending_bytes += len(line)
            rows_loaded += 1
            if pending_bytes >= chunk_bytes:
                copy.write("".join(pending))
                pending.clear()
                pending_bytes = 0

        if pending:
            copy.write("".join(pending))

    return rows_loaded


def copy_ascii_file_to_temp(
    cur,
    file_path: Path,
    temp_table_name: str,
    *,
    copy_chunk_mb: int = DEFAULT_COPY_CHUNK_MB,
) -> LoadedTempTable:
    """COPY one FAERS '$'-delimited file into a temp text table."""
    with open(file_path, "r", encoding="utf-8-sig", errors="replace", newline="") as f:
        header_line = f.readline()
        if not header_line:
            return LoadedTempTable(temp_table_name, tuple(), 0)

        raw_header = header_line.rstrip("\r\n").split(DELIMITER)
        columns = build_temp_columns(raw_header)
        if not columns:
            return LoadedTempTable(temp_table_name, tuple(), 0)

        create_cols = sql.SQL(", ").join(
            sql.SQL("{} text").format(sql.Identifier(column.sql_name))
            for column in columns
        )
        cur.execute(sql.SQL("DROP TABLE IF EXISTS {}").format(sql.Identifier(temp_table_name)))
        cur.execute(
            sql.SQL("CREATE TEMP TABLE {} ({}) ON COMMIT DROP").format(
                sql.Identifier(temp_table_name),
                create_cols,
            )
        )

        copy_sql = sql.SQL("COPY {} ({}) FROM STDIN " + COPY_OPTIONS).format(
            sql.Identifier(temp_table_name),
            sql.SQL(", ").join(sql.Identifier(column.sql_name) for column in columns),
        )
        expected_fields = len(columns)
        rows_loaded = _copy_text_lines(
            cur,
            copy_sql,
            (_normalize_copy_line(line, expected_fields) for line in f),
            max(1, copy_chunk_mb) * 1024 * 1024,
        )

    loaded = LoadedTempTable(temp_table_name, columns, rows_loaded)
    cur.execute(sql.SQL("ANALYZE {}").format(sql.Identifier(temp_table_name)))
    return loaded


def copy_delete_file_to_temp(
    cur,
    file_path: Path,
    temp_table_name: str,
    *,
    copy_chunk_mb: int = DEFAULT_COPY_CHUNK_MB,
) -> int:
    cur.execute(sql.SQL("DROP TABLE IF EXISTS {}").format(sql.Identifier(temp_table_name)))
    cur.execute(
        sql.SQL("CREATE TEMP TABLE {} (source_report_id text) ON COMMIT DROP").format(
            sql.Identifier(temp_table_name)
        )
    )
    copy_sql = sql.SQL("COPY {} (source_report_id) FROM STDIN " + COPY_OPTIONS).format(
        sql.Identifier(temp_table_name)
    )

    def lines():
        with open(file_path, "r", encoding="utf-8-sig", errors="replace", newline="") as f:
            for line in f:
                source_report_id = clean_value(line)
                if source_report_id:
                    yield source_report_id + "\n"

    rows_loaded = _copy_text_lines(
        cur,
        copy_sql,
        lines(),
        max(1, copy_chunk_mb) * 1024 * 1024,
    )
    cur.execute(sql.SQL("ANALYZE {}").format(sql.Identifier(temp_table_name)))
    return rows_loaded


def _configure_session(cur, work_mem: str):
    cur.execute("SET synchronous_commit = off")
    cur.execute("SELECT set_config('work_mem', %s, false)", (work_mem,))


def _meta_literals(meta: dict) -> tuple[str, str, str]:
    return (
        _quote_literal(meta["source_quarter"]),
        _quote_literal(meta["source_system"]),
        _quote_literal(meta["schema_era"]),
    )


def _source_report_expr(table: LoadedTempTable) -> str:
    return first_present_expr(table, ("PRIMARYID", "ISR", "REPORT_ID"))


def _source_case_expr(table: LoadedTempTable) -> str:
    return first_present_expr(table, ("CASE_ID", "CASEID", "CASE"))


def _load_all_demo(
    conn,
    quarter_work: list[tuple[dict, list[tuple[str, Path]]]],
    *,
    work_mem: str = DEFAULT_WORK_MEM,
    copy_chunk_mb: int = DEFAULT_COPY_CHUNK_MB,
) -> int:
    t0 = time.perf_counter()
    total = 0

    with conn.cursor() as cur:
        _configure_session(cur, work_mem)
        cur.execute(
            """
            CREATE TEMP TABLE _tmp_demo_norm (
                source_quarter text,
                source_system text,
                schema_era text,
                source_case_id text,
                source_report_id text,
                case_version_num int,
                report_type text,
                initial_or_followup text,
                event_dt date,
                mfr_dt date,
                fda_dt date,
                age_value numeric,
                age_unit text,
                age_group text,
                sex_std text,
                weight_kg numeric,
                reporter_country text,
                auth_num text,
                lit_ref text
            ) ON COMMIT DROP
            """
        )

        for quarter_info, files in quarter_work:
            meta = {
                "source_quarter": quarter_info["source_quarter"],
                "source_system": quarter_info["source_system"],
                "schema_era": quarter_info["schema_era"],
            }
            demo_files = [path for kind, path in files if kind == "DEMO"]
            for file_path in demo_files:
                table = copy_ascii_file_to_temp(
                    cur,
                    file_path,
                    "_tmp_demo_raw",
                    copy_chunk_mb=copy_chunk_mb,
                )
                if table.rows_loaded == 0:
                    continue
                total += table.rows_loaded
                _insert_demo_norm(cur, table, meta)

        cur.execute("ANALYZE _tmp_demo_norm")
        cur.execute(
            """
            INSERT INTO core.case_master (
                canonical_case_id,
                source_case_id,
                source_system,
                first_seen_quarter,
                latest_seen_quarter
            )
            SELECT
                source_system || ':' || source_case_id,
                source_case_id,
                source_system,
                MIN(source_quarter),
                MAX(source_quarter)
            FROM _tmp_demo_norm
            WHERE source_case_id IS NOT NULL
              AND source_report_id IS NOT NULL
            GROUP BY source_system, source_case_id
            ORDER BY 1
            """
        )
        cur.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS idx_case_master_canonical_case_id_uq
            ON core.case_master (canonical_case_id)
            """
        )
        cur.execute(
            """
            INSERT INTO core.case_version (
                case_pk,
                source_quarter,
                source_system,
                schema_era,
                source_report_id,
                source_case_id,
                case_version_num,
                report_type,
                initial_or_followup,
                event_dt,
                mfr_dt,
                fda_dt,
                age_value,
                age_unit,
                age_group,
                sex_std,
                weight_kg,
                reporter_country,
                auth_num,
                lit_ref
            )
            SELECT
                cm.case_pk,
                d.source_quarter,
                d.source_system,
                d.schema_era,
                d.source_report_id,
                d.source_case_id,
                d.case_version_num,
                d.report_type,
                d.initial_or_followup,
                d.event_dt,
                d.mfr_dt,
                d.fda_dt,
                d.age_value,
                d.age_unit,
                d.age_group,
                d.sex_std,
                d.weight_kg,
                d.reporter_country,
                d.auth_num,
                d.lit_ref
            FROM (
                SELECT DISTINCT ON (source_system, source_report_id, source_quarter)
                    *
                FROM _tmp_demo_norm
                WHERE source_case_id IS NOT NULL
                  AND source_report_id IS NOT NULL
                ORDER BY
                    source_system,
                    source_report_id,
                    source_quarter,
                    case_version_num DESC NULLS LAST
            ) d
            JOIN core.case_master cm
              ON cm.canonical_case_id = d.source_system || ':' || d.source_case_id
            ORDER BY d.source_system, d.source_report_id, d.source_quarter
            """
        )

    conn.commit()
    typer.echo(f"[backfill] DEMO {total} source rows in {_fmt(time.perf_counter() - t0)}")
    return total


def load_all_demo(
    quarter_work: list[tuple[dict, list[tuple[str, Path]]]],
    *,
    work_mem: str = DEFAULT_WORK_MEM,
    copy_chunk_mb: int = DEFAULT_COPY_CHUNK_MB,
) -> int:
    conn = get_conn()
    try:
        return _load_all_demo(
            conn,
            quarter_work,
            work_mem=work_mem,
            copy_chunk_mb=copy_chunk_mb,
        )
    finally:
        conn.close()


def _insert_demo_norm(cur, table: LoadedTempTable, meta: dict):
    quarter, source_system, schema_era = _meta_literals(meta)
    source_case_id = _source_case_expr(table)
    source_report_id = _source_report_expr(table)
    case_version_num = int_expr(clean_text_expr(table, "CASEVERSION"))
    report_type = clean_text_expr(table, "REPT_COD")
    initial_or_followup = first_present_expr(table, ("I_F_COD", "I_F_CODE"))
    event_dt = date_expr(clean_text_expr(table, "EVENT_DT"))
    mfr_dt = date_expr(clean_text_expr(table, "MFR_DT"))
    fda_dt = date_expr(clean_text_expr(table, "FDA_DT"))
    age_value = decimal_expr(clean_text_expr(table, "AGE"))
    age_unit = clean_text_expr(table, "AGE_COD")
    age_group = clean_text_expr(table, "AGE_GRP")
    sex_raw = first_present_expr(table, ("SEX", "GNDR_COD"))
    weight_raw = clean_text_expr(table, "WT")
    weight_unit = clean_text_expr(table, "WT_COD")

    cur.execute(
        f"""
        INSERT INTO _tmp_demo_norm (
            source_quarter,
            source_system,
            schema_era,
            source_case_id,
            source_report_id,
            case_version_num,
            report_type,
            initial_or_followup,
            event_dt,
            mfr_dt,
            fda_dt,
            age_value,
            age_unit,
            age_group,
            sex_std,
            weight_kg,
            reporter_country,
            auth_num,
            lit_ref
        )
        SELECT
            {quarter}::text,
            {source_system}::text,
            {schema_era}::text,
            {source_case_id},
            {source_report_id},
            {case_version_num},
            {report_type},
            {initial_or_followup},
            {event_dt},
            {mfr_dt},
            {fda_dt},
            {age_value},
            {age_unit},
            {age_group},
            CASE
                WHEN UPPER(COALESCE({sex_raw}, '')) IN ('M', 'MALE', '1') THEN 'M'
                WHEN UPPER(COALESCE({sex_raw}, '')) IN ('F', 'FEMALE', '2') THEN 'F'
                ELSE 'UNK'
            END,
            CASE
                WHEN {weight_raw} ~ '^[+-]?\\d+(\\.\\d+)?$'
                 AND UPPER(COALESCE({weight_unit}, '')) IN ('KG', '')
                THEN {weight_raw}::numeric
            END,
            {clean_text_expr(table, "REPORTER_COUNTRY")},
            {clean_text_expr(table, "AUTH_NUM")},
            {clean_text_expr(table, "LIT_REF")}
        FROM {_quote_ident(table.name)} t
        """
    )


def load_quarter_demo(
    quarter_info: dict,
    files: list[tuple[str, Path]],
    *,
    work_mem: str = DEFAULT_WORK_MEM,
    copy_chunk_mb: int = DEFAULT_COPY_CHUNK_MB,
) -> dict:
    count = load_all_demo(
        [(quarter_info, files)],
        work_mem=work_mem,
        copy_chunk_mb=copy_chunk_mb,
    )
    return {"quarter": quarter_info["source_quarter"], "demo": count}


def load_quarter_links(
    quarter_info: dict,
    files: list[tuple[str, Path]],
    *,
    work_mem: str = DEFAULT_WORK_MEM,
    copy_chunk_mb: int = DEFAULT_COPY_CHUNK_MB,
) -> dict:
    quarter = quarter_info["source_quarter"]
    meta = {
        "source_quarter": quarter_info["source_quarter"],
        "source_system": quarter_info["source_system"],
        "schema_era": quarter_info["schema_era"],
    }
    files_by_kind: dict[str, list[Path]] = {}
    for kind, path in files:
        if kind != "DEMO" and kind != "DELETE":
            files_by_kind.setdefault(kind, []).append(path)

    results: dict = {"quarter": quarter}
    t0 = time.perf_counter()
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            _configure_session(cur, work_mem)

            for kind, loader in [
                ("DRUG", _load_drug),
                ("REAC", _load_reac),
                ("OUTC", _load_outc),
                ("THER", _load_ther),
                ("INDI", _load_indi),
                ("RPSR", _load_rpsr),
            ]:
                results[kind.lower()] = loader(
                    cur,
                    meta,
                    files_by_kind.get(kind, []),
                    copy_chunk_mb=copy_chunk_mb,
                )
        conn.commit()
    finally:
        conn.close()

    results["elapsed"] = time.perf_counter() - t0
    typer.echo(f"  [{quarter}] links finished in {_fmt(results['elapsed'])}")
    return results


def load_quarter_delete(
    quarter_info: dict,
    files: list[tuple[str, Path]],
    *,
    work_mem: str = DEFAULT_WORK_MEM,
    copy_chunk_mb: int = DEFAULT_COPY_CHUNK_MB,
) -> dict:
    quarter = quarter_info["source_quarter"]
    meta = {
        "source_quarter": quarter_info["source_quarter"],
        "source_system": quarter_info["source_system"],
        "schema_era": quarter_info["schema_era"],
    }
    delete_files = [path for kind, path in files if kind == "DELETE"]

    conn = get_conn()
    try:
        with conn.cursor() as cur:
            _configure_session(cur, work_mem)
            count = _load_delete(cur, meta, delete_files, copy_chunk_mb=copy_chunk_mb)
        conn.commit()
    finally:
        conn.close()

    return {"quarter": quarter, "delete": count}


def _load_link_table(
    cur,
    meta: dict,
    file_paths: list[Path],
    *,
    table_kind: str,
    insert_builder,
    copy_chunk_mb: int = DEFAULT_COPY_CHUNK_MB,
) -> int:
    if not file_paths:
        return 0

    t0 = time.perf_counter()
    total = 0
    for file_path in file_paths:
        table = copy_ascii_file_to_temp(
            cur,
            file_path,
            f"_tmp_{table_kind.lower()}_raw",
            copy_chunk_mb=copy_chunk_mb,
        )
        if table.rows_loaded == 0:
            continue
        total += table.rows_loaded
        cur.execute(insert_builder(table, meta))
        cur.execute(sql.SQL("DROP TABLE IF EXISTS {}").format(sql.Identifier(table.name)))

    typer.echo(f"  [{meta['source_quarter']}] {table_kind} {total} source rows in {_fmt(time.perf_counter() - t0)}")
    return total


def _cv_join(alias: str = "d") -> str:
    return f"""
    JOIN core.case_version cv
      ON cv.source_system = {alias}.source_system
     AND cv.source_quarter = {alias}.source_quarter
     AND cv.source_report_id = {alias}.source_report_id
    """


def _load_drug(cur, meta: dict, file_paths: list[Path], *, copy_chunk_mb: int) -> int:
    return _load_link_table(
        cur,
        meta,
        file_paths,
        table_kind="DRUG",
        insert_builder=_drug_insert_sql,
        copy_chunk_mb=copy_chunk_mb,
    )


def _drug_insert_sql(table: LoadedTempTable, meta: dict) -> str:
    quarter, source_system, _ = _meta_literals(meta)
    source_report_id = _source_report_expr(table)
    drug_seq = int_expr(clean_text_expr(table, "DRUG_SEQ"))
    dose_amt = decimal_expr(clean_text_expr(table, "DOSE_AMT"))
    start_dt = date_expr(clean_text_expr(table, "START_DT"))
    end_dt = date_expr(clean_text_expr(table, "END_DT"))
    row_hash = row_hash_expr(table)

    return f"""
    WITH norm AS (
        SELECT
            {quarter}::text AS source_quarter,
            {source_system}::text AS source_system,
            {source_report_id} AS source_report_id,
            {drug_seq} AS drug_seq,
            {clean_text_expr(table, "ROLE_COD")} AS role_cod,
            {clean_text_expr(table, "DRUGNAME")} AS drugname,
            {clean_text_expr(table, "PROD_AI")} AS prod_ai,
            {clean_text_expr(table, "ROUTE")} AS route,
            {clean_text_expr(table, "DOSE_VBM")} AS dose_vbm,
            {dose_amt} AS dose_amt,
            {clean_text_expr(table, "DOSE_UNIT")} AS dose_unit,
            {start_dt} AS start_dt,
            {end_dt} AS end_dt,
            {row_hash} AS row_hash
        FROM {_quote_ident(table.name)} t
    ),
    deduped AS (
        SELECT DISTINCT *
        FROM norm
        WHERE source_report_id IS NOT NULL
          AND drugname IS NOT NULL
    )
    INSERT INTO core.case_drug (
        case_version_pk,
        source_system,
        source_quarter,
        source_report_id,
        drug_seq,
        role_cod,
        drugname,
        prod_ai,
        route,
        dose_vbm,
        dose_amt,
        dose_unit,
        start_dt,
        end_dt,
        row_hash
    )
    SELECT
        cv.case_version_pk,
        d.source_system,
        d.source_quarter,
        d.source_report_id,
        d.drug_seq,
        d.role_cod,
        d.drugname,
        d.prod_ai,
        d.route,
        d.dose_vbm,
        d.dose_amt,
        d.dose_unit,
        d.start_dt,
        d.end_dt,
        d.row_hash
    FROM deduped d
    {_cv_join("d")}
    ORDER BY d.source_system, d.source_quarter, d.source_report_id, d.row_hash
    """


def _load_reac(cur, meta: dict, file_paths: list[Path], *, copy_chunk_mb: int) -> int:
    return _load_link_table(
        cur,
        meta,
        file_paths,
        table_kind="REAC",
        insert_builder=_reac_insert_sql,
        copy_chunk_mb=copy_chunk_mb,
    )


def _reac_insert_sql(table: LoadedTempTable, meta: dict) -> str:
    quarter, source_system, _ = _meta_literals(meta)
    source_report_id = _source_report_expr(table)
    reaction_pt = first_present_expr(table, ("PT", "REAC_PT"))
    row_hash = row_hash_expr(table)

    return f"""
    WITH norm AS (
        SELECT
            {quarter}::text AS source_quarter,
            {source_system}::text AS source_system,
            {source_report_id} AS source_report_id,
            {reaction_pt} AS reaction_pt,
            {clean_text_expr(table, "OUTC_COD")} AS outcome,
            {row_hash} AS row_hash
        FROM {_quote_ident(table.name)} t
    ),
    deduped AS (
        SELECT DISTINCT *
        FROM norm
        WHERE source_report_id IS NOT NULL
          AND reaction_pt IS NOT NULL
    )
    INSERT INTO core.case_reaction (
        case_version_pk,
        source_system,
        source_quarter,
        source_report_id,
        reaction_pt,
        outcome,
        row_hash
    )
    SELECT
        cv.case_version_pk,
        d.source_system,
        d.source_quarter,
        d.source_report_id,
        d.reaction_pt,
        d.outcome,
        d.row_hash
    FROM deduped d
    {_cv_join("d")}
    ORDER BY d.source_system, d.source_quarter, d.source_report_id, d.row_hash
    """


def _load_outc(cur, meta: dict, file_paths: list[Path], *, copy_chunk_mb: int) -> int:
    return _load_link_table(
        cur,
        meta,
        file_paths,
        table_kind="OUTC",
        insert_builder=_outc_insert_sql,
        copy_chunk_mb=copy_chunk_mb,
    )


def _outc_insert_sql(table: LoadedTempTable, meta: dict) -> str:
    quarter, source_system, _ = _meta_literals(meta)
    source_report_id = _source_report_expr(table)
    outcome = first_present_expr(table, ("OUTC_COD", "OUTCOME"))
    row_hash = row_hash_expr(table)

    return f"""
    WITH norm AS (
        SELECT
            {quarter}::text AS source_quarter,
            {source_system}::text AS source_system,
            {source_report_id} AS source_report_id,
            {outcome} AS outcome,
            {row_hash} AS row_hash
        FROM {_quote_ident(table.name)} t
    ),
    deduped AS (
        SELECT DISTINCT *
        FROM norm
        WHERE source_report_id IS NOT NULL
          AND outcome IS NOT NULL
    )
    INSERT INTO core.case_outcome (
        case_version_pk,
        source_system,
        source_quarter,
        source_report_id,
        outcome,
        row_hash
    )
    SELECT
        cv.case_version_pk,
        d.source_system,
        d.source_quarter,
        d.source_report_id,
        d.outcome,
        d.row_hash
    FROM deduped d
    {_cv_join("d")}
    ORDER BY d.source_system, d.source_quarter, d.source_report_id, d.row_hash
    """


def _load_ther(cur, meta: dict, file_paths: list[Path], *, copy_chunk_mb: int) -> int:
    return _load_link_table(
        cur,
        meta,
        file_paths,
        table_kind="THER",
        insert_builder=_ther_insert_sql,
        copy_chunk_mb=copy_chunk_mb,
    )


def _ther_insert_sql(table: LoadedTempTable, meta: dict) -> str:
    quarter, source_system, _ = _meta_literals(meta)
    source_report_id = _source_report_expr(table)
    drug_seq = int_expr(first_present_expr(table, ("DSG_DRUG_SEQ", "DRUG_SEQ")))
    start_dt = date_expr(clean_text_expr(table, "START_DT"))
    end_dt = date_expr(clean_text_expr(table, "END_DT"))
    dur = int_expr(clean_text_expr(table, "DUR"))
    row_hash = row_hash_expr(table)

    return f"""
    WITH norm AS (
        SELECT
            {quarter}::text AS source_quarter,
            {source_system}::text AS source_system,
            {source_report_id} AS source_report_id,
            {drug_seq} AS drug_seq,
            {start_dt} AS start_dt,
            {end_dt} AS end_dt,
            {dur} AS dur,
            {clean_text_expr(table, "DUR_COD")} AS dur_cod,
            {row_hash} AS row_hash
        FROM {_quote_ident(table.name)} t
    ),
    deduped AS (
        SELECT DISTINCT *
        FROM norm
        WHERE source_report_id IS NOT NULL
    )
    INSERT INTO core.case_therapy (
        case_version_pk,
        source_system,
        source_quarter,
        source_report_id,
        drug_seq,
        start_dt,
        end_dt,
        dur,
        dur_cod,
        row_hash
    )
    SELECT
        cv.case_version_pk,
        d.source_system,
        d.source_quarter,
        d.source_report_id,
        d.drug_seq,
        d.start_dt,
        d.end_dt,
        d.dur,
        d.dur_cod,
        d.row_hash
    FROM deduped d
    {_cv_join("d")}
    ORDER BY d.source_system, d.source_quarter, d.source_report_id, d.row_hash
    """


def _load_indi(cur, meta: dict, file_paths: list[Path], *, copy_chunk_mb: int) -> int:
    return _load_link_table(
        cur,
        meta,
        file_paths,
        table_kind="INDI",
        insert_builder=_indi_insert_sql,
        copy_chunk_mb=copy_chunk_mb,
    )


def _indi_insert_sql(table: LoadedTempTable, meta: dict) -> str:
    quarter, source_system, _ = _meta_literals(meta)
    source_report_id = _source_report_expr(table)
    drug_seq = int_expr(first_present_expr(table, ("INDI_DRUG_SEQ", "DRUG_SEQ")))
    indi_pt = first_present_expr(table, ("INDI_PT", "INDICATION"))
    row_hash = row_hash_expr(table)

    return f"""
    WITH norm AS (
        SELECT
            {quarter}::text AS source_quarter,
            {source_system}::text AS source_system,
            {source_report_id} AS source_report_id,
            {drug_seq} AS drug_seq,
            {indi_pt} AS indi_pt,
            {row_hash} AS row_hash
        FROM {_quote_ident(table.name)} t
    ),
    deduped AS (
        SELECT DISTINCT *
        FROM norm
        WHERE source_report_id IS NOT NULL
          AND indi_pt IS NOT NULL
    )
    INSERT INTO core.case_indication (
        case_version_pk,
        source_system,
        source_quarter,
        source_report_id,
        drug_seq,
        indi_pt,
        row_hash
    )
    SELECT
        cv.case_version_pk,
        d.source_system,
        d.source_quarter,
        d.source_report_id,
        d.drug_seq,
        d.indi_pt,
        d.row_hash
    FROM deduped d
    {_cv_join("d")}
    ORDER BY d.source_system, d.source_quarter, d.source_report_id, d.row_hash
    """


def _load_rpsr(cur, meta: dict, file_paths: list[Path], *, copy_chunk_mb: int) -> int:
    return _load_link_table(
        cur,
        meta,
        file_paths,
        table_kind="RPSR",
        insert_builder=_rpsr_insert_sql,
        copy_chunk_mb=copy_chunk_mb,
    )


def _rpsr_insert_sql(table: LoadedTempTable, meta: dict) -> str:
    quarter, source_system, _ = _meta_literals(meta)
    source_report_id = _source_report_expr(table)
    reporter_type = first_present_expr(table, ("RPSR_COD", "REPORTER_TYPE"))
    row_hash = row_hash_expr(table)

    return f"""
    WITH norm AS (
        SELECT
            {quarter}::text AS source_quarter,
            {source_system}::text AS source_system,
            {source_report_id} AS source_report_id,
            {reporter_type} AS reporter_type,
            {row_hash} AS row_hash
        FROM {_quote_ident(table.name)} t
    ),
    deduped AS (
        SELECT DISTINCT *
        FROM norm
        WHERE source_report_id IS NOT NULL
          AND reporter_type IS NOT NULL
    )
    INSERT INTO core.case_report_source (
        case_version_pk,
        source_system,
        source_quarter,
        source_report_id,
        reporter_type,
        row_hash
    )
    SELECT
        cv.case_version_pk,
        d.source_system,
        d.source_quarter,
        d.source_report_id,
        d.reporter_type,
        d.row_hash
    FROM deduped d
    {_cv_join("d")}
    ORDER BY d.source_system, d.source_quarter, d.source_report_id, d.row_hash
    """


def _load_delete(
    cur,
    meta: dict,
    file_paths: list[Path],
    *,
    copy_chunk_mb: int = DEFAULT_COPY_CHUNK_MB,
) -> int:
    if not file_paths:
        return 0

    t0 = time.perf_counter()
    source_system = meta["source_system"]
    total = 0
    for file_path in file_paths:
        rows_loaded = copy_delete_file_to_temp(
            cur,
            file_path,
            "_tmp_delete_raw",
            copy_chunk_mb=copy_chunk_mb,
        )
        total += rows_loaded
        if rows_loaded == 0:
            continue
        cur.execute(
            """
            UPDATE core.case_version cv
            SET is_deleted = true,
                is_latest_known = false
            FROM (
                SELECT DISTINCT source_report_id
                FROM _tmp_delete_raw
                WHERE source_report_id IS NOT NULL
            ) d
            WHERE cv.source_system = %s
              AND cv.source_report_id = d.source_report_id
              AND cv.is_deleted = false
            """,
            (source_system,),
        )
        cur.execute("DROP TABLE IF EXISTS _tmp_delete_raw")

    typer.echo(f"  [{meta['source_quarter']}] DELETE {total} rows in {_fmt(time.perf_counter() - t0)}")
    return total


def purge_deleted_link_rows(conn):
    with conn.cursor() as cur:
        for table_name in [
            "core.case_drug",
            "core.case_reaction",
            "core.case_outcome",
            "core.case_therapy",
            "core.case_indication",
            "core.case_report_source",
        ]:
            cur.execute(
                f"""
                DELETE FROM {table_name} child
                USING core.case_version cv
                WHERE child.case_version_pk = cv.case_version_pk
                  AND cv.is_deleted = true
                """
            )
    conn.commit()


def recompute_latest_case_flags_global(conn, *, work_mem: str = "512MB"):
    typer.echo("Recomputing is_latest_known flags globally...")
    t0 = time.perf_counter()

    with conn.cursor() as cur:
        _configure_session(cur, work_mem)
        cur.execute("UPDATE core.case_version SET is_latest_known = false WHERE is_latest_known = true")
        cur.execute(
            """
            WITH ranked AS (
                SELECT
                    case_version_pk,
                    ROW_NUMBER() OVER (
                        PARTITION BY case_pk
                        ORDER BY
                            case_version_num DESC NULLS LAST,
                            COALESCE(fda_dt, event_dt, mfr_dt) DESC NULLS LAST,
                            source_quarter DESC,
                            case_version_pk DESC
                    ) AS rn
                FROM core.case_version
                WHERE is_deleted = false
            )
            UPDATE core.case_version cv
            SET is_latest_known = true
            FROM ranked
            WHERE cv.case_version_pk = ranked.case_version_pk
              AND ranked.rn = 1
            """
        )

    conn.commit()
    typer.echo(f"  is_latest_known recomputed in {_fmt(time.perf_counter() - t0)}")
