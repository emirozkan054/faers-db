from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
import os
from pathlib import Path
import re
import time
from typing import Annotated

import typer

from faersdb.config import settings
from faersdb.db import get_conn
from faersdb.detect import discover_files
from faersdb.manifest import discover_quarters
from faersdb.staging_load import (
    insert_delete_raw_rows,
    insert_demo_raw_rows,
    insert_drug_raw_rows,
    insert_indi_raw_rows,
    insert_outc_raw_rows,
    insert_reac_raw_rows,
    insert_rpsr_raw_rows,
    insert_ther_raw_rows,
)

app = typer.Typer()
PROJECT_ROOT = Path(__file__).resolve().parent.parent
INIT_SQL_PATH = PROJECT_ROOT / "sql" / "001_init.sql"
PARALLEL_NORMALIZE_KINDS = ("DRUG", "REAC", "OUTC", "THER", "INDI", "RPSR")
VALID_PIPELINE_PROFILES = {"standard", "fast_backfill"}
TABLE_STATEMENT_RE = re.compile(
    r"^create table if not exists ((?:etl|staging|core)\.[a-z_]+)\s*\(",
    re.IGNORECASE,
)
ALL_BACKFILL_TABLES = (
    "etl.load_batch",
    "etl.pipeline_run",
    "etl.pipeline_step_run",
    "etl.source_file",
    "staging.demo_raw",
    "staging.drug_raw",
    "staging.reac_raw",
    "staging.outc_raw",
    "staging.ther_raw",
    "staging.indi_raw",
    "staging.rpsr_raw",
    "staging.delete_raw",
    "core.case_master",
    "core.case_version",
    "core.case_drug",
    "core.case_reaction",
    "core.case_outcome",
    "core.case_therapy",
    "core.case_indication",
    "core.case_report_source",
)


def format_duration(seconds: float) -> str:
    if seconds < 60:
        return f"{seconds:.2f}s"
    minutes, secs = divmod(seconds, 60)
    if minutes < 60:
        return f"{int(minutes)}m {secs:.1f}s"
    hours, minutes = divmod(minutes, 60)
    return f"{int(hours)}h {int(minutes)}m {secs:.1f}s"


def format_metric(value: int | None) -> str:
    return str(value) if value is not None else "n/a"


def default_parallel_workers(max_workers: int) -> int:
    if max_workers > 0:
        return max_workers

    cpu_count = os.cpu_count() or 4
    return max(1, min(2, cpu_count))


def resolve_pipeline_profile(profile: str | None = None) -> str:
    resolved = (profile or settings.pipeline_profile).strip().lower()
    if resolved not in VALID_PIPELINE_PROFILES:
        raise typer.BadParameter(
            f"Unsupported profile '{resolved}'. Use one of: {', '.join(sorted(VALID_PIPELINE_PROFILES))}"
        )
    return resolved


def is_fast_backfill_profile(profile: str | None = None) -> bool:
    return resolve_pipeline_profile(profile) == "fast_backfill"


def iter_sql_statements(sql_text: str) -> list[str]:
    return [statement.strip() for statement in sql_text.split(";") if statement.strip()]


def init_sql_statements(profile: str | None = None) -> list[str]:
    resolved_profile = resolve_pipeline_profile(profile)
    statements: list[str] = []

    for statement in iter_sql_statements(INIT_SQL_PATH.read_text()):
        if resolved_profile == "fast_backfill" and statement.lower().startswith("create index if not exists"):
            continue

        match = TABLE_STATEMENT_RE.match(statement)
        if resolved_profile == "fast_backfill" and match:
            statement = TABLE_STATEMENT_RE.sub(
                lambda m: f"create unlogged table if not exists {m.group(1)} (",
                statement,
                count=1,
            )

        statements.append(statement)

    return statements


def deferred_index_statements() -> list[str]:
    return [
        statement
        for statement in iter_sql_statements(INIT_SQL_PATH.read_text())
        if statement.lower().startswith("create index if not exists")
    ]


def apply_fast_backfill_table_settings(cur):
    for table_name in ALL_BACKFILL_TABLES:
        cur.execute(
            f"alter table {table_name} set (autovacuum_enabled = false, toast.autovacuum_enabled = false)"
        )
        cur.execute(f"alter table {table_name} disable trigger all")


def reset_backfill_table_settings(cur):
    for table_name in ALL_BACKFILL_TABLES:
        cur.execute(f"alter table {table_name} enable trigger all")
        cur.execute(
            f"alter table {table_name} reset (autovacuum_enabled, toast.autovacuum_enabled)"
        )


def set_tables_logged(cur):
    for table_name in ALL_BACKFILL_TABLES:
        cur.execute(f"alter table {table_name} set logged")


def analyze_backfill_tables(cur):
    for table_name in ALL_BACKFILL_TABLES:
        cur.execute(f"analyze {table_name}")


CORE_BACKFILL_TABLES = tuple(
    table_name for table_name in ALL_BACKFILL_TABLES if table_name.startswith("core.")
)


def core_tables_have_rows() -> bool:
    with get_conn() as conn:
        with conn.cursor() as cur:
            for table_name in CORE_BACKFILL_TABLES:
                cur.execute("select to_regclass(%s)", (table_name,))
                if cur.fetchone()[0] is None:
                    continue
                cur.execute(f"select exists (select 1 from {table_name} limit 1)")
                if cur.fetchone()[0]:
                    return True
    return False


def drop_backfill_schemas():
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("drop schema if exists mart cascade")
            cur.execute("drop schema if exists core cascade")
            cur.execute("drop schema if exists staging cascade")
            cur.execute("drop schema if exists etl cascade")
        conn.commit()


def restore_backfill_table_settings_safely():
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                reset_backfill_table_settings(cur)
            conn.commit()
    except Exception as exc:  # pragma: no cover - best-effort failure cleanup
        typer.echo(f"[backfill] WARNING: could not restore table settings: {exc}")


def assert_backfill_referential_integrity(cur):
    checks = [
        (
            "case_version.case_pk",
            """
            select count(*)
            from core.case_version cv
            left join core.case_master cm on cm.case_pk = cv.case_pk
            where cm.case_pk is null
            """,
        ),
        (
            "case_drug.case_version_pk",
            """
            select count(*)
            from core.case_drug child
            left join core.case_version cv on cv.case_version_pk = child.case_version_pk
            where cv.case_version_pk is null
            """,
        ),
        (
            "case_reaction.case_version_pk",
            """
            select count(*)
            from core.case_reaction child
            left join core.case_version cv on cv.case_version_pk = child.case_version_pk
            where cv.case_version_pk is null
            """,
        ),
        (
            "case_outcome.case_version_pk",
            """
            select count(*)
            from core.case_outcome child
            left join core.case_version cv on cv.case_version_pk = child.case_version_pk
            where cv.case_version_pk is null
            """,
        ),
        (
            "case_therapy.case_version_pk",
            """
            select count(*)
            from core.case_therapy child
            left join core.case_version cv on cv.case_version_pk = child.case_version_pk
            where cv.case_version_pk is null
            """,
        ),
        (
            "case_indication.case_version_pk",
            """
            select count(*)
            from core.case_indication child
            left join core.case_version cv on cv.case_version_pk = child.case_version_pk
            where cv.case_version_pk is null
            """,
        ),
        (
            "case_report_source.case_version_pk",
            """
            select count(*)
            from core.case_report_source child
            left join core.case_version cv on cv.case_version_pk = child.case_version_pk
            where cv.case_version_pk is null
            """,
        ),
        (
            "case_version.latest_unique",
            """
            select count(*)
            from (
                select case_pk
                from core.case_version
                where is_latest_known = true
                group by case_pk
                having count(*) > 1
            ) duplicate_latest
            """,
        ),
    ]

    failures: list[str] = []
    for name, query in checks:
        cur.execute(query)
        count = cur.fetchone()[0]
        if count:
            failures.append(f"{name}={count}")

    if failures:
        raise RuntimeError(
            "Backfill integrity check failed: " + ", ".join(failures)
        )


LOADERS = {
    "DEMO": insert_demo_raw_rows,
    "DRUG": insert_drug_raw_rows,
    "REAC": insert_reac_raw_rows,
    "OUTC": insert_outc_raw_rows,
    "THER": insert_ther_raw_rows,
    "INDI": insert_indi_raw_rows,
    "RPSR": insert_rpsr_raw_rows,
    "DELETE": insert_delete_raw_rows,
}

def recompute_latest_case_flags_for_quarter(cur, quarter: str | None = None):
    quarter_where = ""
    params: list[str] = []
    if quarter:
        quarter_where = "where source_quarter = %s"
        params.append(quarter.lower())

    cur.execute(
        f"""
        with affected_cases as (
            select distinct case_pk
            from core.case_version
            {quarter_where}
        ),
        reset_flags as (
            update core.case_version
            set is_latest_known = false
            where case_pk in (select case_pk from affected_cases)
            returning case_version_pk
        ),
        ranked as (
            select
                case_version_pk,
                row_number() over (
                    partition by case_pk
                    order by
                        case_version_num desc nulls last,
                        coalesce(fda_dt, event_dt, mfr_dt) desc nulls last,
                        source_quarter desc,
                        case_version_pk desc
                ) as rn
            from core.case_version
            where is_deleted = false
              and case_pk in (select case_pk from affected_cases)
        )
        update core.case_version cv
        set is_latest_known = true
        from ranked
        where cv.case_version_pk = ranked.case_version_pk
          and ranked.rn = 1
        """,
        params,
    )


def build_quarter_filter(column: str, quarter: str | None) -> tuple[str, list[str]]:
    if quarter:
        return f" where {column} = %s", [quarter.lower()]
    return "", []


def quarter_pipeline_steps() -> list[tuple[str, str]]:
    return [
        ("load", "DEMO"),
        ("load", "DRUG"),
        ("load", "REAC"),
        ("load", "OUTC"),
        ("load", "THER"),
        ("load", "INDI"),
        ("load", "RPSR"),
        ("load", "DELETE"),
        ("normalize", "DEMO"),
        ("normalize", "DELETE"),
        ("normalize", "DRUG"),
        ("normalize", "REAC"),
        ("normalize", "OUTC"),
        ("normalize", "THER"),
        ("normalize", "INDI"),
        ("normalize", "RPSR"),
    ]


def normalize_command_map():
    return {
        "DEMO": normalize_demo_cmd,
        "DELETE": normalize_delete_cmd,
        "DRUG": normalize_drug_cmd,
        "REAC": normalize_reac_cmd,
        "OUTC": normalize_outc_cmd,
        "THER": normalize_ther_cmd,
        "INDI": normalize_indi_cmd,
        "RPSR": normalize_rpsr_cmd,
    }


def create_pipeline_run(quarter: str) -> int:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                insert into etl.pipeline_run (quarter, status)
                values (%s, 'running')
                returning pipeline_run_id
                """,
                (quarter,),
            )
            pipeline_run_id = cur.fetchone()[0]
        conn.commit()
    return pipeline_run_id


def create_pipeline_step(pipeline_run_id: int, step_order: int, phase: str, kind: str) -> int:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                insert into etl.pipeline_step_run (pipeline_run_id, step_order, phase, kind, status)
                values (%s, %s, %s, %s, 'running')
                returning pipeline_step_run_id
                """,
                (pipeline_run_id, step_order, phase, kind),
            )
            pipeline_step_run_id = cur.fetchone()[0]
        conn.commit()
    return pipeline_step_run_id


def finish_pipeline_step(pipeline_step_run_id: int, result: dict | None = None):
    result = result or {}
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                update etl.pipeline_step_run
                set status = 'finished',
                    finished_at = now(),
                    files_count = %s,
                    rows_inserted = %s,
                    processed = %s,
                    skipped = %s
                where pipeline_step_run_id = %s
                """,
                (
                    result.get("files"),
                    result.get("rows_inserted"),
                    result.get("processed"),
                    result.get("skipped"),
                    pipeline_step_run_id,
                ),
            )
        conn.commit()


def fail_pipeline_step(pipeline_step_run_id: int, error_text: str):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                update etl.pipeline_step_run
                set status = 'failed',
                    finished_at = now(),
                    error_text = %s
                where pipeline_step_run_id = %s
                """,
                (error_text[:4000], pipeline_step_run_id),
            )
        conn.commit()


def finish_pipeline_run(pipeline_run_id: int, status: str, notes: str | None = None):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                update etl.pipeline_run
                set status = %s,
                    finished_at = now(),
                    notes = %s
                where pipeline_run_id = %s
                """,
                (status, notes, pipeline_run_id),
            )
        conn.commit()


@app.command()
def scan():
    quarters = discover_quarters(Path(settings.data_root))
    for q in quarters:
        typer.echo(q)


@app.command()
def init_db(profile: str | None = None):
    started_at = time.perf_counter()
    resolved_profile = resolve_pipeline_profile(profile)
    with get_conn() as conn:
        with conn.cursor() as cur:
            for statement in init_sql_statements(resolved_profile):
                cur.execute(statement)
            if resolved_profile == "fast_backfill":
                apply_fast_backfill_table_settings(cur)
        conn.commit()
    typer.echo(
        "Database initialized. "
        f"profile={resolved_profile}, "
        f"elapsed={format_duration(time.perf_counter() - started_at)}."
    )


@app.command()
def finalize_backfill(
    durable: Annotated[
        bool,
        typer.Option(
            "--durable/--keep-unlogged",
            help="Convert fast-backfill tables from UNLOGGED to LOGGED for crash recovery.",
        ),
    ] = False,
    run_qa: Annotated[
        bool,
        typer.Option(
            "--run-qa/--no-run-qa",
            help="Run a full-database QA pass after finalization.",
        ),
    ] = False,
    maintenance_work_mem: Annotated[
        str,
        typer.Option(
            "--maintenance-work-mem",
            help="PostgreSQL maintenance_work_mem for deferred index builds.",
        ),
    ] = "512MB",
):
    started_at = time.perf_counter()
    index_statements = deferred_index_statements()
    typer.echo("Finalizing backfill: restoring table settings")
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("set local synchronous_commit = off")
            cur.execute("select set_config('maintenance_work_mem', %s, true)", (maintenance_work_mem,))
            cur.execute("set local max_parallel_maintenance_workers = 2")
            reset_backfill_table_settings(cur)
            typer.echo("Finalizing backfill: checking referential integrity")
            assert_backfill_referential_integrity(cur)
            if durable:
                typer.echo("Finalizing backfill: converting tables back to logged mode")
                set_tables_logged(cur)
            else:
                typer.echo(
                    "Finalizing backfill: keeping tables UNLOGGED for speed. "
                    "Use --durable later if you want crash recovery."
                )
            typer.echo(f"Finalizing backfill: building {len(index_statements)} deferred indexes")
            for idx, statement in enumerate(index_statements, start=1):
                typer.echo(f"  index {idx}/{len(index_statements)}")
                cur.execute(statement)
            typer.echo("Finalizing backfill: running analyze")
            analyze_backfill_tables(cur)
        conn.commit()

    if run_qa:
        typer.echo("[qa] full database")
        qa_summary()
    else:
        typer.echo(
            "Finalizing backfill: skipping full database QA. "
            "Run `qa-summary` separately when convenient."
        )

    typer.echo(
        "Backfill finalized. "
        f"durable={'yes' if durable else 'no'}, "
        f"elapsed={format_duration(time.perf_counter() - started_at)}"
    )


@app.command()
def load_manifest():
    started_at = time.perf_counter()
    root = Path(settings.data_root)
    quarters = discover_quarters(root)

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                insert into etl.load_batch (root_path, status)
                values (%s, %s)
                returning load_batch_id
                """,
                (str(root), "running"),
            )
            load_batch_id = cur.fetchone()[0]

            inserted = 0

            for q in quarters:
                folder = Path(q["folder_path"])
                files = discover_files(folder)

                for table_kind, file_path in files:
                    header_line = None
                    try:
                        with open(file_path, "r", encoding="latin1", errors="ignore") as f:
                            header_line = f.readline().strip()
                    except Exception:
                        header_line = None

                    cur.execute(
                        """
                        insert into etl.source_file (
                            load_batch_id,
                            source_quarter,
                            source_year,
                            source_qtr,
                            source_system,
                            schema_era,
                            folder_name,
                            table_kind,
                            file_path,
                            file_name,
                            file_size_bytes,
                            header_line
                        )
                        values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        on conflict (source_quarter, table_kind, file_path) do nothing
                        """,
                        (
                            load_batch_id,
                            q["source_quarter"],
                            q["source_year"],
                            q["source_qtr"],
                            q["source_system"],
                            q["schema_era"],
                            q["folder_name"],
                            table_kind,
                            str(file_path),
                            file_path.name,
                            file_path.stat().st_size,
                            header_line,
                        ),
                    )
                    inserted += cur.rowcount

            cur.execute(
                """
                update etl.load_batch
                set status = %s, finished_at = now()
                where load_batch_id = %s
                """,
                ("finished", load_batch_id),
            )

        conn.commit()

    typer.echo(
        "Manifest loaded. "
        f"load_batch_id={load_batch_id}, files_inserted={inserted}, "
        f"elapsed={format_duration(time.perf_counter() - started_at)}"
    )
    return {"files": inserted}


@app.command()
def load_staging(kind: str = "DEMO", quarter: str | None = None, profile: str | None = None):
    started_at = time.perf_counter()
    resolved_profile = resolve_pipeline_profile(profile)
    append_only = resolved_profile == "fast_backfill"
    kind = kind.upper()
    if kind not in LOADERS:
        raise typer.BadParameter(
            "Supported kinds: DEMO, DRUG, REAC, OUTC, THER, INDI, RPSR, DELETE"
        )

    sql = """
        select source_file_id, source_quarter, file_path
        from etl.source_file
        where table_kind = %s
    """
    params = [kind]

    if quarter:
        quarter = quarter.lower()
        sql += " and source_quarter = %s"
        params.append(quarter)

    sql += " order by source_quarter"

    total_rows = 0
    file_count = 0
    loader = LOADERS[kind]

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("set local synchronous_commit = off")
        with conn.cursor() as cur:
            cur.execute(sql, params)
            files = cur.fetchall()

        for source_file_id, source_quarter, file_path in files:
            inserted = loader(conn, source_file_id, Path(file_path), append_only=append_only)
            typer.echo(
                f"{source_quarter} -> inserted {inserted} {kind} rows from {file_path}"
            )
            total_rows += inserted
            file_count += 1

    typer.echo(
        f"Done. files={file_count}, rows_inserted={total_rows}, "
        f"elapsed={format_duration(time.perf_counter() - started_at)}"
    )
    return {"files": file_count, "rows_inserted": total_rows}


@app.command()
def normalize_demo_cmd(quarter: str | None = None, profile: str | None = None):
    started_at = time.perf_counter()
    append_only = is_fast_backfill_profile(profile)
    quarter_where, params = build_quarter_filter("f.source_quarter", quarter)
    demo_norm_sql = f"""
        with demo_norm as (
            select
                f.source_quarter,
                f.source_system,
                f.schema_era,
                nullif(btrim(coalesce(
                    s.raw_record->>'CASE_ID',
                    s.raw_record->>'CASEID',
                    s.raw_record->>'CASE'
                )), '') as source_case_id,
                nullif(btrim(coalesce(
                    s.raw_record->>'PRIMARYID',
                    s.raw_record->>'ISR',
                    s.raw_record->>'REPORT_ID'
                )), '') as source_report_id,
                case
                    when coalesce(s.raw_record->>'CASEVERSION', '') ~ '^\\d+$'
                    then (s.raw_record->>'CASEVERSION')::int
                end as case_version_num,
                nullif(btrim(s.raw_record->>'REPT_COD'), '') as report_type,
                nullif(btrim(coalesce(s.raw_record->>'I_F_COD', s.raw_record->>'I_F_CODE')), '') as initial_or_followup,
                case
                    when coalesce(s.raw_record->>'EVENT_DT', '') ~ '^\\d{{8}}$'
                    then to_date(s.raw_record->>'EVENT_DT', 'YYYYMMDD')
                end as event_dt,
                case
                    when coalesce(s.raw_record->>'MFR_DT', '') ~ '^\\d{{8}}$'
                    then to_date(s.raw_record->>'MFR_DT', 'YYYYMMDD')
                end as mfr_dt,
                case
                    when coalesce(s.raw_record->>'FDA_DT', '') ~ '^\\d{{8}}$'
                    then to_date(s.raw_record->>'FDA_DT', 'YYYYMMDD')
                end as fda_dt,
                case
                    when coalesce(s.raw_record->>'AGE', '') ~ '^[+-]?\\d+(\\.\\d+)?$'
                    then (s.raw_record->>'AGE')::numeric
                end as age_value,
                nullif(btrim(s.raw_record->>'AGE_COD'), '') as age_unit,
                nullif(btrim(s.raw_record->>'AGE_GRP'), '') as age_group,
                case
                    when upper(coalesce(nullif(btrim(coalesce(s.raw_record->>'SEX', s.raw_record->>'GNDR_COD')), ''), '')) in ('M', 'MALE', '1') then 'M'
                    when upper(coalesce(nullif(btrim(coalesce(s.raw_record->>'SEX', s.raw_record->>'GNDR_COD')), ''), '')) in ('F', 'FEMALE', '2') then 'F'
                    else 'UNK'
                end as sex_std,
                case
                    when coalesce(s.raw_record->>'WT', '') ~ '^[+-]?\\d+(\\.\\d+)?$'
                     and upper(coalesce(nullif(btrim(s.raw_record->>'WT_COD'), ''), '')) in ('KG', '')
                    then (s.raw_record->>'WT')::numeric
                end as weight_kg,
                nullif(btrim(s.raw_record->>'REPORTER_COUNTRY'), '') as reporter_country,
                nullif(btrim(s.raw_record->>'AUTH_NUM'), '') as auth_num,
                nullif(btrim(s.raw_record->>'LIT_REF'), '') as lit_ref,
                s.raw_record as raw_demo
            from staging.demo_raw s
            join etl.source_file f
              on f.source_file_id = s.source_file_id
            {quarter_where}
        )
    """

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("set local synchronous_commit = off")
            if append_only:
                processed = None
                skipped = None
            else:
                cur.execute(
                    demo_norm_sql
                    + """
                    select count(*)
                    from demo_norm
                    where source_case_id is not null
                      and source_report_id is not null
                    """,
                    params,
                )
                processed = cur.fetchone()[0]

                cur.execute(
                    demo_norm_sql
                    + """
                    select count(*)
                    from demo_norm
                    where source_case_id is null
                       or source_report_id is null
                    """,
                    params,
                )
                skipped = cur.fetchone()[0]

            cur.execute(
                demo_norm_sql
                + """
                insert into core.case_master (
                    canonical_case_id,
                    source_case_id,
                    source_system,
                    first_seen_quarter,
                    latest_seen_quarter
                )
                select distinct
                    source_system || ':' || source_case_id as canonical_case_id,
                    source_case_id,
                    source_system,
                    source_quarter,
                    source_quarter
                from demo_norm
                where source_case_id is not null
                  and source_report_id is not null
                on conflict (canonical_case_id) do update
                set first_seen_quarter = least(core.case_master.first_seen_quarter, excluded.first_seen_quarter),
                    latest_seen_quarter = greatest(core.case_master.latest_seen_quarter, excluded.latest_seen_quarter)
                """,
                params,
            )

            cur.execute(
                demo_norm_sql
                + """
                insert into core.case_version (
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
                    lit_ref,
                    raw_demo
                )
                select
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
                    d.lit_ref,
                    d.raw_demo
                from demo_norm d
                join core.case_master cm
                  on cm.canonical_case_id = d.source_system || ':' || d.source_case_id
                where d.source_case_id is not null
                  and d.source_report_id is not null
                on conflict (source_system, source_report_id, source_quarter) do update
                set
                    case_pk = excluded.case_pk,
                    schema_era = excluded.schema_era,
                    source_case_id = excluded.source_case_id,
                    case_version_num = excluded.case_version_num,
                    report_type = excluded.report_type,
                    initial_or_followup = excluded.initial_or_followup,
                    event_dt = excluded.event_dt,
                    mfr_dt = excluded.mfr_dt,
                    fda_dt = excluded.fda_dt,
                    age_value = excluded.age_value,
                    age_unit = excluded.age_unit,
                    age_group = excluded.age_group,
                    sex_std = excluded.sex_std,
                    weight_kg = excluded.weight_kg,
                    reporter_country = excluded.reporter_country,
                    auth_num = excluded.auth_num,
                    lit_ref = excluded.lit_ref,
                    raw_demo = excluded.raw_demo,
                    is_deleted = false
                """,
                params,
            )
            if append_only:
                processed = cur.rowcount
                skipped = None

            recompute_latest_case_flags_for_quarter(cur, quarter)
        conn.commit()

    typer.echo(
        "Normalized DEMO rows. "
        f"processed={format_metric(processed)}, skipped={format_metric(skipped)}, "
        f"elapsed={format_duration(time.perf_counter() - started_at)}"
    )
    return {"processed": processed, "skipped": skipped}


@app.command()
def normalize_delete_cmd(quarter: str | None = None, profile: str | None = None):
    started_at = time.perf_counter()
    resolve_pipeline_profile(profile)
    quarter_where, params = build_quarter_filter("f.source_quarter", quarter)
    delete_source_sql = f"""
        with delete_targets as (
            select distinct
                d.source_report_id,
                f.source_system
            from staging.delete_raw d
            join etl.source_file f
              on f.source_file_id = d.source_file_id
            {quarter_where}
        )
    """

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("set local synchronous_commit = off")
            cur.execute(delete_source_sql + "select count(*) from delete_targets", params)
            processed = cur.fetchone()[0]

            cur.execute(
                delete_source_sql
                + """
                , updated as (
                    update core.case_version cv
                    set is_deleted = true,
                        is_latest_known = false
                    from delete_targets d
                    where cv.source_system = d.source_system
                      and cv.source_report_id = d.source_report_id
                      and cv.is_deleted = false
                    returning cv.case_version_pk, d.source_system, d.source_report_id
                )
                select
                    count(*) as marked_deleted,
                    count(distinct (source_system, source_report_id)) as matched_targets
                from updated
                """,
                params,
            )
            marked_deleted, matched_targets = cur.fetchone()
            skipped = processed - matched_targets
            if marked_deleted:
                cur.execute(
                    delete_source_sql
                    + """
                    , affected_cases as (
                        select distinct cv.case_pk
                        from core.case_version cv
                        join delete_targets d
                          on cv.source_system = d.source_system
                         and cv.source_report_id = d.source_report_id
                    ),
                    reset_flags as (
                        update core.case_version
                        set is_latest_known = false
                        where case_pk in (select case_pk from affected_cases)
                        returning case_version_pk
                    ),
                    ranked as (
                        select
                            case_version_pk,
                            row_number() over (
                                partition by case_pk
                                order by
                                    case_version_num desc nulls last,
                                    coalesce(fda_dt, event_dt, mfr_dt) desc nulls last,
                                    source_quarter desc,
                                    case_version_pk desc
                            ) as rn
                        from core.case_version
                        where is_deleted = false
                          and case_pk in (select case_pk from affected_cases)
                    )
                    update core.case_version cv
                    set is_latest_known = true
                    from ranked
                    where cv.case_version_pk = ranked.case_version_pk
                      and ranked.rn = 1
                    """,
                    params,
                )
        conn.commit()

    typer.echo(
        "Applied DELETE rows. "
        f"processed={processed}, skipped={skipped}, marked_deleted={marked_deleted}, "
        f"elapsed={format_duration(time.perf_counter() - started_at)}"
    )
    return {
        "processed": processed,
        "skipped": skipped,
        "rows_inserted": marked_deleted,
    }


@app.command()
def normalize_drug_cmd(quarter: str | None = None, profile: str | None = None):
    started_at = time.perf_counter()
    append_only = is_fast_backfill_profile(profile)
    quarter_where, params = build_quarter_filter("f.source_quarter", quarter)
    drug_norm_sql = f"""
        with drug_norm as (
            select
                f.source_quarter,
                f.source_system,
                nullif(btrim(coalesce(
                    s.raw_record->>'PRIMARYID',
                    s.raw_record->>'ISR',
                    s.raw_record->>'REPORT_ID'
                )), '') as source_report_id,
                case
                    when coalesce(s.raw_record->>'DRUG_SEQ', '') ~ '^\\d+$'
                    then (s.raw_record->>'DRUG_SEQ')::int
                end as drug_seq,
                nullif(btrim(s.raw_record->>'ROLE_COD'), '') as role_cod,
                nullif(btrim(s.raw_record->>'DRUGNAME'), '') as drugname,
                nullif(btrim(s.raw_record->>'PROD_AI'), '') as prod_ai,
                nullif(btrim(s.raw_record->>'ROUTE'), '') as route,
                nullif(btrim(s.raw_record->>'DOSE_VBM'), '') as dose_vbm,
                case
                    when coalesce(s.raw_record->>'DOSE_AMT', '') ~ '^[+-]?\\d+(\\.\\d+)?$'
                    then (s.raw_record->>'DOSE_AMT')::numeric
                end as dose_amt,
                nullif(btrim(s.raw_record->>'DOSE_UNIT'), '') as dose_unit,
                case
                    when coalesce(s.raw_record->>'START_DT', '') ~ '^\\d{{8}}$'
                    then to_date(s.raw_record->>'START_DT', 'YYYYMMDD')
                end as start_dt,
                case
                    when coalesce(s.raw_record->>'END_DT', '') ~ '^\\d{{8}}$'
                    then to_date(s.raw_record->>'END_DT', 'YYYYMMDD')
                end as end_dt,
                s.row_hash,
                s.raw_record as raw_drug
            from staging.drug_raw s
            join etl.source_file f
              on f.source_file_id = s.source_file_id
            {quarter_where}
        ),
        deduped as (
            select distinct *
            from drug_norm
            where source_report_id is not null
              and drugname is not null
        )
    """

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("set local synchronous_commit = off")
            cur.execute("set local work_mem = '256MB'")
            if append_only:
                selected = None
            else:
                cur.execute(
                    drug_norm_sql
                    + """
                    select count(*)
                    from deduped
                    """,
                    params,
                )
                selected = cur.fetchone()[0]

            insert_sql = (
                drug_norm_sql
                + """
                insert into core.case_drug (
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
                    row_hash,
                    raw_drug
                )
                select
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
                    d.row_hash,
                    d.raw_drug
                from deduped d
                join core.case_version cv
                  on cv.source_system = d.source_system
                 and cv.source_quarter = d.source_quarter
                 and cv.source_report_id = d.source_report_id
                 and cv.is_deleted = false
                """
            )
            if not append_only:
                insert_sql += """
                on conflict (source_system, source_quarter, source_report_id, row_hash) do update
                set
                    case_version_pk = excluded.case_version_pk,
                    drug_seq = excluded.drug_seq,
                    role_cod = excluded.role_cod,
                    drugname = excluded.drugname,
                    prod_ai = excluded.prod_ai,
                    route = excluded.route,
                    dose_vbm = excluded.dose_vbm,
                    dose_amt = excluded.dose_amt,
                    dose_unit = excluded.dose_unit,
                    start_dt = excluded.start_dt,
                    end_dt = excluded.end_dt,
                    raw_drug = excluded.raw_drug
                """

            cur.execute(insert_sql, params)
            processed = cur.rowcount
            skipped = None if selected is None else selected - processed
        conn.commit()

    typer.echo(
        "Normalized DRUG rows. "
        f"processed={format_metric(processed)}, skipped={format_metric(skipped)}, "
        f"elapsed={format_duration(time.perf_counter() - started_at)}"
    )
    return {"processed": processed, "skipped": skipped}


@app.command()
def normalize_reac_cmd(quarter: str | None = None, profile: str | None = None):
    started_at = time.perf_counter()
    append_only = is_fast_backfill_profile(profile)
    quarter_where, params = build_quarter_filter("f.source_quarter", quarter)
    reac_norm_sql = f"""
        with reac_norm as (
            select
                f.source_quarter,
                f.source_system,
                nullif(btrim(coalesce(
                    s.raw_record->>'PRIMARYID',
                    s.raw_record->>'ISR',
                    s.raw_record->>'REPORT_ID'
                )), '') as source_report_id,
                nullif(btrim(coalesce(s.raw_record->>'PT', s.raw_record->>'REAC_PT')), '') as reaction_pt,
                nullif(btrim(s.raw_record->>'OUTC_COD'), '') as outcome,
                s.row_hash,
                s.raw_record as raw_reac
            from staging.reac_raw s
            join etl.source_file f
              on f.source_file_id = s.source_file_id
            {quarter_where}
        ),
        deduped as (
            select distinct *
            from reac_norm
            where source_report_id is not null
              and reaction_pt is not null
        )
    """

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("set local synchronous_commit = off")
            cur.execute("set local work_mem = '256MB'")
            if append_only:
                selected = None
            else:
                cur.execute(
                    reac_norm_sql
                    + """
                    select count(*)
                    from deduped
                    """,
                    params,
                )
                selected = cur.fetchone()[0]

            insert_sql = (
                reac_norm_sql
                + """
                insert into core.case_reaction (
                    case_version_pk,
                    source_system,
                    source_quarter,
                    source_report_id,
                    reaction_pt,
                    outcome,
                    row_hash,
                    raw_reac
                )
                select
                    cv.case_version_pk,
                    r.source_system,
                    r.source_quarter,
                    r.source_report_id,
                    r.reaction_pt,
                    r.outcome,
                    r.row_hash,
                    r.raw_reac
                from deduped r
                join core.case_version cv
                  on cv.source_system = r.source_system
                 and cv.source_quarter = r.source_quarter
                 and cv.source_report_id = r.source_report_id
                 and cv.is_deleted = false
                """
            )
            if not append_only:
                insert_sql += """
                on conflict (source_system, source_quarter, source_report_id, row_hash) do update
                set
                    case_version_pk = excluded.case_version_pk,
                    reaction_pt = excluded.reaction_pt,
                    outcome = excluded.outcome,
                    raw_reac = excluded.raw_reac
                """

            cur.execute(insert_sql, params)
            processed = cur.rowcount
            skipped = None if selected is None else selected - processed
        conn.commit()

    typer.echo(
        "Normalized REAC rows. "
        f"processed={format_metric(processed)}, skipped={format_metric(skipped)}, "
        f"elapsed={format_duration(time.perf_counter() - started_at)}"
    )
    return {"processed": processed, "skipped": skipped}


@app.command()
def normalize_outc_cmd(quarter: str | None = None, profile: str | None = None):
    started_at = time.perf_counter()
    append_only = is_fast_backfill_profile(profile)
    quarter_where, params = build_quarter_filter("f.source_quarter", quarter)
    outc_norm_sql = f"""
        with outc_norm as (
            select
                f.source_quarter,
                f.source_system,
                nullif(btrim(coalesce(
                    s.raw_record->>'PRIMARYID',
                    s.raw_record->>'ISR',
                    s.raw_record->>'REPORT_ID'
                )), '') as source_report_id,
                nullif(btrim(coalesce(s.raw_record->>'OUTC_COD', s.raw_record->>'OUTCOME')), '') as outcome,
                s.row_hash,
                s.raw_record as raw_outc
            from staging.outc_raw s
            join etl.source_file f
              on f.source_file_id = s.source_file_id
            {quarter_where}
        ),
        deduped as (
            select distinct *
            from outc_norm
            where source_report_id is not null
              and outcome is not null
        )
    """

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("set local synchronous_commit = off")
            cur.execute("set local work_mem = '256MB'")
            if append_only:
                selected = None
            else:
                cur.execute(
                    outc_norm_sql
                    + """
                    select count(*)
                    from deduped
                    """,
                    params,
                )
                selected = cur.fetchone()[0]

            insert_sql = (
                outc_norm_sql
                + """
                insert into core.case_outcome (
                    case_version_pk,
                    source_system,
                    source_quarter,
                    source_report_id,
                    outcome,
                    row_hash,
                    raw_outc
                )
                select
                    cv.case_version_pk,
                    o.source_system,
                    o.source_quarter,
                    o.source_report_id,
                    o.outcome,
                    o.row_hash,
                    o.raw_outc
                from deduped o
                join core.case_version cv
                  on cv.source_system = o.source_system
                 and cv.source_quarter = o.source_quarter
                 and cv.source_report_id = o.source_report_id
                 and cv.is_deleted = false
                """
            )
            if not append_only:
                insert_sql += """
                on conflict (source_system, source_quarter, source_report_id, row_hash) do update
                set
                    case_version_pk = excluded.case_version_pk,
                    outcome = excluded.outcome,
                    raw_outc = excluded.raw_outc
                """

            cur.execute(insert_sql, params)
            processed = cur.rowcount
            skipped = None if selected is None else selected - processed
        conn.commit()

    typer.echo(
        "Normalized OUTC rows. "
        f"processed={format_metric(processed)}, skipped={format_metric(skipped)}, "
        f"elapsed={format_duration(time.perf_counter() - started_at)}"
    )
    return {"processed": processed, "skipped": skipped}


@app.command()
def normalize_ther_cmd(quarter: str | None = None, profile: str | None = None):
    started_at = time.perf_counter()
    append_only = is_fast_backfill_profile(profile)
    quarter_where, params = build_quarter_filter("f.source_quarter", quarter)
    ther_norm_sql = f"""
        with ther_norm as (
            select
                f.source_quarter,
                f.source_system,
                nullif(btrim(coalesce(
                    s.raw_record->>'PRIMARYID',
                    s.raw_record->>'ISR',
                    s.raw_record->>'REPORT_ID'
                )), '') as source_report_id,
                case
                    when coalesce(coalesce(s.raw_record->>'DSG_DRUG_SEQ', s.raw_record->>'DRUG_SEQ'), '') ~ '^\\d+$'
                    then coalesce(s.raw_record->>'DSG_DRUG_SEQ', s.raw_record->>'DRUG_SEQ')::int
                end as drug_seq,
                case
                    when coalesce(s.raw_record->>'START_DT', '') ~ '^\\d{{8}}$'
                    then to_date(s.raw_record->>'START_DT', 'YYYYMMDD')
                end as start_dt,
                case
                    when coalesce(s.raw_record->>'END_DT', '') ~ '^\\d{{8}}$'
                    then to_date(s.raw_record->>'END_DT', 'YYYYMMDD')
                end as end_dt,
                case
                    when coalesce(s.raw_record->>'DUR', '') ~ '^\\d+$'
                    then (s.raw_record->>'DUR')::int
                end as dur,
                nullif(btrim(s.raw_record->>'DUR_COD'), '') as dur_cod,
                s.row_hash,
                s.raw_record as raw_ther
            from staging.ther_raw s
            join etl.source_file f
              on f.source_file_id = s.source_file_id
            {quarter_where}
        ),
        deduped as (
            select distinct *
            from ther_norm
            where source_report_id is not null
        )
    """

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("set local synchronous_commit = off")
            cur.execute("set local work_mem = '256MB'")
            if append_only:
                selected = None
            else:
                cur.execute(
                    ther_norm_sql
                    + """
                    select count(*)
                    from deduped
                    """,
                    params,
                )
                selected = cur.fetchone()[0]

            insert_sql = (
                ther_norm_sql
                + """
                insert into core.case_therapy (
                    case_version_pk,
                    source_system,
                    source_quarter,
                    source_report_id,
                    drug_seq,
                    start_dt,
                    end_dt,
                    dur,
                    dur_cod,
                    row_hash,
                    raw_ther
                )
                select
                    cv.case_version_pk,
                    t.source_system,
                    t.source_quarter,
                    t.source_report_id,
                    t.drug_seq,
                    t.start_dt,
                    t.end_dt,
                    t.dur,
                    t.dur_cod,
                    t.row_hash,
                    t.raw_ther
                from deduped t
                join core.case_version cv
                  on cv.source_system = t.source_system
                 and cv.source_quarter = t.source_quarter
                 and cv.source_report_id = t.source_report_id
                 and cv.is_deleted = false
                """
            )
            if not append_only:
                insert_sql += """
                on conflict (source_system, source_quarter, source_report_id, row_hash) do update
                set
                    case_version_pk = excluded.case_version_pk,
                    drug_seq = excluded.drug_seq,
                    start_dt = excluded.start_dt,
                    end_dt = excluded.end_dt,
                    dur = excluded.dur,
                    dur_cod = excluded.dur_cod,
                    raw_ther = excluded.raw_ther
                """

            cur.execute(insert_sql, params)
            processed = cur.rowcount
            skipped = None if selected is None else selected - processed
        conn.commit()

    typer.echo(
        "Normalized THER rows. "
        f"processed={format_metric(processed)}, skipped={format_metric(skipped)}, "
        f"elapsed={format_duration(time.perf_counter() - started_at)}"
    )
    return {"processed": processed, "skipped": skipped}


@app.command()
def normalize_indi_cmd(quarter: str | None = None, profile: str | None = None):
    started_at = time.perf_counter()
    append_only = is_fast_backfill_profile(profile)
    quarter_where, params = build_quarter_filter("f.source_quarter", quarter)
    indi_norm_sql = f"""
        with indi_norm as (
            select
                f.source_quarter,
                f.source_system,
                nullif(btrim(coalesce(
                    s.raw_record->>'PRIMARYID',
                    s.raw_record->>'ISR',
                    s.raw_record->>'REPORT_ID'
                )), '') as source_report_id,
                case
                    when coalesce(coalesce(s.raw_record->>'INDI_DRUG_SEQ', s.raw_record->>'DRUG_SEQ'), '') ~ '^\\d+$'
                    then coalesce(s.raw_record->>'INDI_DRUG_SEQ', s.raw_record->>'DRUG_SEQ')::int
                end as drug_seq,
                nullif(btrim(coalesce(s.raw_record->>'INDI_PT', s.raw_record->>'INDICATION')), '') as indi_pt,
                s.row_hash,
                s.raw_record as raw_indi
            from staging.indi_raw s
            join etl.source_file f
              on f.source_file_id = s.source_file_id
            {quarter_where}
        ),
        deduped as (
            select distinct *
            from indi_norm
            where source_report_id is not null
              and indi_pt is not null
        )
    """

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("set local synchronous_commit = off")
            cur.execute("set local work_mem = '256MB'")
            if append_only:
                selected = None
            else:
                cur.execute(
                    indi_norm_sql
                    + """
                    select count(*)
                    from deduped
                    """,
                    params,
                )
                selected = cur.fetchone()[0]

            insert_sql = (
                indi_norm_sql
                + """
                insert into core.case_indication (
                    case_version_pk,
                    source_system,
                    source_quarter,
                    source_report_id,
                    drug_seq,
                    indi_pt,
                    row_hash,
                    raw_indi
                )
                select
                    cv.case_version_pk,
                    i.source_system,
                    i.source_quarter,
                    i.source_report_id,
                    i.drug_seq,
                    i.indi_pt,
                    i.row_hash,
                    i.raw_indi
                from deduped i
                join core.case_version cv
                  on cv.source_system = i.source_system
                 and cv.source_quarter = i.source_quarter
                 and cv.source_report_id = i.source_report_id
                 and cv.is_deleted = false
                """
            )
            if not append_only:
                insert_sql += """
                on conflict (source_system, source_quarter, source_report_id, row_hash) do update
                set
                    case_version_pk = excluded.case_version_pk,
                    drug_seq = excluded.drug_seq,
                    indi_pt = excluded.indi_pt,
                    raw_indi = excluded.raw_indi
                """

            cur.execute(insert_sql, params)
            processed = cur.rowcount
            skipped = None if selected is None else selected - processed
        conn.commit()

    typer.echo(
        "Normalized INDI rows. "
        f"processed={format_metric(processed)}, skipped={format_metric(skipped)}, "
        f"elapsed={format_duration(time.perf_counter() - started_at)}"
    )
    return {"processed": processed, "skipped": skipped}


@app.command()
def normalize_rpsr_cmd(quarter: str | None = None, profile: str | None = None):
    started_at = time.perf_counter()
    append_only = is_fast_backfill_profile(profile)
    quarter_where, params = build_quarter_filter("f.source_quarter", quarter)
    rpsr_norm_sql = f"""
        with rpsr_norm as (
            select
                f.source_quarter,
                f.source_system,
                nullif(btrim(coalesce(
                    s.raw_record->>'PRIMARYID',
                    s.raw_record->>'ISR',
                    s.raw_record->>'REPORT_ID'
                )), '') as source_report_id,
                nullif(btrim(coalesce(s.raw_record->>'RPSR_COD', s.raw_record->>'REPORTER_TYPE')), '') as reporter_type,
                s.row_hash,
                s.raw_record as raw_rpsr
            from staging.rpsr_raw s
            join etl.source_file f
              on f.source_file_id = s.source_file_id
            {quarter_where}
        ),
        deduped as (
            select distinct *
            from rpsr_norm
            where source_report_id is not null
              and reporter_type is not null
        )
    """

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("set local synchronous_commit = off")
            cur.execute("set local work_mem = '256MB'")
            if append_only:
                selected = None
            else:
                cur.execute(
                    rpsr_norm_sql
                    + """
                    select count(*)
                    from deduped
                    """,
                    params,
                )
                selected = cur.fetchone()[0]

            insert_sql = (
                rpsr_norm_sql
                + """
                insert into core.case_report_source (
                    case_version_pk,
                    source_system,
                    source_quarter,
                    source_report_id,
                    reporter_type,
                    row_hash,
                    raw_rpsr
                )
                select
                    cv.case_version_pk,
                    r.source_system,
                    r.source_quarter,
                    r.source_report_id,
                    r.reporter_type,
                    r.row_hash,
                    r.raw_rpsr
                from deduped r
                join core.case_version cv
                  on cv.source_system = r.source_system
                 and cv.source_quarter = r.source_quarter
                 and cv.source_report_id = r.source_report_id
                 and cv.is_deleted = false
                """
            )
            if not append_only:
                insert_sql += """
                on conflict (source_system, source_quarter, source_report_id, row_hash) do update
                set
                    case_version_pk = excluded.case_version_pk,
                    reporter_type = excluded.reporter_type,
                    raw_rpsr = excluded.raw_rpsr
                """

            cur.execute(insert_sql, params)
            processed = cur.rowcount
            skipped = None if selected is None else selected - processed
        conn.commit()

    typer.echo(
        "Normalized RPSR rows. "
        f"processed={format_metric(processed)}, skipped={format_metric(skipped)}, "
        f"elapsed={format_duration(time.perf_counter() - started_at)}"
    )
    return {"processed": processed, "skipped": skipped}


@app.command()
def qa_summary(quarter: str | None = None):
    started_at = time.perf_counter()
    quarter_filter = quarter.lower() if quarter else None

    with get_conn() as conn:
        with conn.cursor() as cur:
            staging_where = ""
            core_where = ""
            delete_where = ""
            params: list[str] = []

            if quarter_filter:
                staging_where = "where source_quarter = %s"
                core_where = "where source_quarter = %s"
                delete_where = "where f.source_quarter = %s"
                params = [quarter_filter]

            staging_sql = """
                select table_kind, source_quarter, sum(row_count)
                from (
                    select 'DEMO'::text as table_kind, f.source_quarter, count(*)::bigint as row_count
                    from staging.demo_raw s
                    join etl.source_file f on f.source_file_id = s.source_file_id
                    group by f.source_quarter
                    union all
                    select 'DRUG', f.source_quarter, count(distinct s.row_hash)::bigint
                    from staging.drug_raw s
                    join etl.source_file f on f.source_file_id = s.source_file_id
                    group by f.source_quarter
                    union all
                    select 'REAC', f.source_quarter, count(distinct s.row_hash)::bigint
                    from staging.reac_raw s
                    join etl.source_file f on f.source_file_id = s.source_file_id
                    group by f.source_quarter
                    union all
                    select 'OUTC', f.source_quarter, count(distinct s.row_hash)::bigint
                    from staging.outc_raw s
                    join etl.source_file f on f.source_file_id = s.source_file_id
                    group by f.source_quarter
                    union all
                    select 'THER', f.source_quarter, count(distinct s.row_hash)::bigint
                    from staging.ther_raw s
                    join etl.source_file f on f.source_file_id = s.source_file_id
                    group by f.source_quarter
                    union all
                    select 'INDI', f.source_quarter, count(distinct s.row_hash)::bigint
                    from staging.indi_raw s
                    join etl.source_file f on f.source_file_id = s.source_file_id
                    group by f.source_quarter
                    union all
                    select 'RPSR', f.source_quarter, count(distinct s.row_hash)::bigint
                    from staging.rpsr_raw s
                    join etl.source_file f on f.source_file_id = s.source_file_id
                    group by f.source_quarter
                    union all
                    select 'DELETE', f.source_quarter, count(distinct s.source_report_id)::bigint
                    from staging.delete_raw s
                    join etl.source_file f on f.source_file_id = s.source_file_id
                    group by f.source_quarter
                ) x
                {staging_where}
                group by table_kind, source_quarter
                order by source_quarter, table_kind
            """.format(staging_where=staging_where)
            cur.execute(staging_sql, params)
            staging_rows = cur.fetchall()

            core_sql = """
                select kind, source_quarter, count(*)
                from (
                    select 'DEMO'::text as kind, source_quarter
                    from core.case_version
                    where is_deleted = false
                    union all
                    select 'DRUG', d.source_quarter
                    from core.case_drug d
                    join core.case_version cv on cv.case_version_pk = d.case_version_pk
                    where cv.is_deleted = false
                    union all
                    select 'REAC', r.source_quarter
                    from core.case_reaction r
                    join core.case_version cv on cv.case_version_pk = r.case_version_pk
                    where cv.is_deleted = false
                    union all
                    select 'OUTC', o.source_quarter
                    from core.case_outcome o
                    join core.case_version cv on cv.case_version_pk = o.case_version_pk
                    where cv.is_deleted = false
                    union all
                    select 'THER', t.source_quarter
                    from core.case_therapy t
                    join core.case_version cv on cv.case_version_pk = t.case_version_pk
                    where cv.is_deleted = false
                    union all
                    select 'INDI', i.source_quarter
                    from core.case_indication i
                    join core.case_version cv on cv.case_version_pk = i.case_version_pk
                    where cv.is_deleted = false
                    union all
                    select 'RPSR', rs.source_quarter
                    from core.case_report_source rs
                    join core.case_version cv on cv.case_version_pk = rs.case_version_pk
                    where cv.is_deleted = false
                ) x
                {core_where}
                group by kind, source_quarter
                order by source_quarter, kind
            """.format(core_where=core_where)
            cur.execute(core_sql, params)
            core_rows = {(kind, source_quarter): count for kind, source_quarter, count in cur.fetchall()}

            typer.echo("Staging distinct vs Core counts:")
            for kind, source_quarter, staging_count in staging_rows:
                core_count = core_rows.get((kind, source_quarter), 0)
                gap = staging_count - core_count
                typer.echo(
                    f"  {source_quarter} {kind}: staging={staging_count} core={core_count} gap={gap}"
                )

            cur.execute(
                """
                select count(*)
                from core.case_version
                where is_deleted = true
                """
                + (" and source_quarter = %s" if quarter_filter else ""),
                params,
            )
            deleted_case_versions = cur.fetchone()[0]
            typer.echo(f"Deleted case_version rows: {deleted_case_versions}")

            orphan_sql = """
                select 'case_drug' t, count(*)
                from core.case_drug d
                left join core.case_version cv on cv.case_version_pk = d.case_version_pk
                where cv.case_version_pk is null
                union all
                select 'case_reaction', count(*)
                from core.case_reaction r
                left join core.case_version cv on cv.case_version_pk = r.case_version_pk
                where cv.case_version_pk is null
                union all
                select 'case_outcome', count(*)
                from core.case_outcome o
                left join core.case_version cv on cv.case_version_pk = o.case_version_pk
                where cv.case_version_pk is null
                union all
                select 'case_therapy', count(*)
                from core.case_therapy t
                left join core.case_version cv on cv.case_version_pk = t.case_version_pk
                where cv.case_version_pk is null
                union all
                select 'case_indication', count(*)
                from core.case_indication i
                left join core.case_version cv on cv.case_version_pk = i.case_version_pk
                where cv.case_version_pk is null
                union all
                select 'case_report_source', count(*)
                from core.case_report_source rs
                left join core.case_version cv on cv.case_version_pk = rs.case_version_pk
                where cv.case_version_pk is null
                order by 1
            """
            cur.execute(orphan_sql)
            typer.echo("Orphan link checks:")
            for table_name, count in cur.fetchall():
                typer.echo(f"  {table_name}: {count}")

            collision_sql = """
                with ther as (
                  select
                    count(*) as rows_total,
                    count(distinct (
                      f.source_system,
                      f.source_quarter,
                      s.source_report_id,
                      case when s.source_report_id ~ '^\\d+$' then s.source_report_id end,
                      s.row_num
                    )) as staging_rows
                  from staging.delete_raw s
                  join etl.source_file f on f.source_file_id = s.source_file_id
                  {delete_where}
                )
                select rows_total, staging_rows, rows_total - staging_rows as estimated_duplicates
                from ther
            """.format(delete_where=delete_where)
            cur.execute(collision_sql, params)
            delete_profile = cur.fetchone()
            if delete_profile:
                typer.echo(
                    "DELETE profile: "
                    f"rows={delete_profile[0]} unique_rows={delete_profile[1]} "
                    f"estimated_duplicates={delete_profile[2]}"
                )

    typer.echo(f"QA summary elapsed={format_duration(time.perf_counter() - started_at)}")
    return {"deleted_case_versions": deleted_case_versions}


@app.command()
def run_quarter(
    quarter: str,
    run_qa: bool = True,
    parallel_normalize: bool = True,
    max_workers: int = 0,
    profile: str | None = None,
):
    started_at = time.perf_counter()
    resolved_profile = resolve_pipeline_profile(profile)
    quarter = quarter.lower()
    typer.echo(f"Running pipeline for {quarter} with profile={resolved_profile}")

    pipeline_run_id = create_pipeline_run(quarter)

    try:
        deferred_parallel_steps: list[tuple[int, str, str]] = []

        for step_order, (phase, kind) in enumerate(quarter_pipeline_steps(), start=1):
            if phase == "normalize" and parallel_normalize and kind in PARALLEL_NORMALIZE_KINDS:
                deferred_parallel_steps.append((step_order, phase, kind))
                continue

            pipeline_step_run_id = create_pipeline_step(pipeline_run_id, step_order, phase, kind)
            try:
                step_started_at = time.perf_counter()
                if phase == "load":
                    typer.echo(f"[load] {kind} {quarter}")
                    result = load_staging(kind=kind, quarter=quarter, profile=resolved_profile)
                else:
                    typer.echo(f"[normalize] {kind} {quarter}")
                    result = normalize_command_map()[kind](quarter=quarter, profile=resolved_profile)

                finish_pipeline_step(pipeline_step_run_id, result)
                typer.echo(
                    f"[done] {phase} {kind} {quarter} in "
                    f"{format_duration(time.perf_counter() - step_started_at)}"
                )
            except Exception as exc:
                fail_pipeline_step(pipeline_step_run_id, str(exc))
                raise

        if deferred_parallel_steps:
            worker_count = default_parallel_workers(max_workers)
            typer.echo(
                f"[normalize] parallel phase for {quarter} with {worker_count} workers"
            )
            normalize_commands = normalize_command_map()
            future_map = {}

            with ThreadPoolExecutor(max_workers=worker_count) as executor:
                for step_order, phase, kind in deferred_parallel_steps:
                    pipeline_step_run_id = create_pipeline_step(
                        pipeline_run_id, step_order, phase, kind
                    )
                    step_started_at = time.perf_counter()
                    typer.echo(f"[normalize] {kind} {quarter} (parallel)")
                    future = executor.submit(
                        normalize_commands[kind], quarter=quarter, profile=resolved_profile
                    )
                    future_map[future] = (pipeline_step_run_id, phase, kind, step_started_at)

                errors: list[Exception] = []
                for future in as_completed(future_map):
                    pipeline_step_run_id, phase, kind, step_started_at = future_map[future]
                    try:
                        result = future.result()
                        finish_pipeline_step(pipeline_step_run_id, result)
                        typer.echo(
                            f"[done] {phase} {kind} {quarter} in "
                            f"{format_duration(time.perf_counter() - step_started_at)}"
                        )
                    except Exception as exc:
                        fail_pipeline_step(pipeline_step_run_id, str(exc))
                        errors.append(exc)

                if errors:
                    raise errors[0]

        if run_qa:
            typer.echo(f"[qa] {quarter}")
            qa_summary(quarter=quarter)

        finish_pipeline_run(pipeline_run_id, "finished")
        typer.echo(f"Pipeline finished in {format_duration(time.perf_counter() - started_at)}")
    except Exception as exc:
        finish_pipeline_run(pipeline_run_id, "failed", str(exc))
        raise


@app.command()
def backfill_all(
    max_workers: Annotated[
        int,
        typer.Option(
            help="Number of parallel quarter workers.",
        ),
    ] = 2,
    reset: Annotated[
        bool,
        typer.Option(
            "--reset/--no-reset",
            help="Drop and recreate ETL/staging/core/mart schemas before loading.",
        ),
    ] = False,
    durable: Annotated[
        bool,
        typer.Option(
            "--durable/--keep-unlogged",
            help="Convert tables from UNLOGGED to LOGGED after load.",
        ),
    ] = False,
    run_qa: Annotated[
        bool,
        typer.Option(
            "--run-qa/--no-run-qa",
            help="Run a full-database QA pass after finalization.",
        ),
    ] = False,
    work_mem: Annotated[
        str,
        typer.Option(
            "--work-mem",
            help="PostgreSQL work_mem used by load sessions.",
        ),
    ] = "128MB",
    maintenance_work_mem: Annotated[
        str,
        typer.Option(
            "--maintenance-work-mem",
            help="PostgreSQL maintenance_work_mem used during finalization.",
        ),
    ] = "512MB",
    copy_chunk_mb: Annotated[
        int,
        typer.Option(
            "--copy-chunk-mb",
            help="Approximate text chunk size sent to PostgreSQL COPY.",
        ),
    ] = 4,
):
    """Fresh full backfill: stream ASCII files directly into normalized core tables."""
    from faersdb.detect import discover_files
    from faersdb.direct_load import (
        load_all_demo,
        load_quarter_delete,
        load_quarter_links,
        purge_deleted_link_rows,
        recompute_latest_case_flags_global,
    )

    started_at = time.perf_counter()
    initialized = False
    finalized = False

    if max_workers < 1:
        raise typer.BadParameter("--max-workers must be at least 1")
    if copy_chunk_mb < 1:
        raise typer.BadParameter("--copy-chunk-mb must be at least 1")

    if not reset and core_tables_have_rows():
        typer.echo(
            "[backfill] Refusing to run against non-empty core tables. "
            "Use --reset for a fresh rebuild."
        )
        raise typer.Exit(1)

    try:
        # ---- Phase 1: reset/init ----
        if reset:
            typer.echo("[backfill] Resetting database schemas")
            drop_backfill_schemas()

        typer.echo("[backfill] Initializing database with fast_backfill profile")
        init_db(profile="fast_backfill")
        initialized = True

        # ---- Phase 2: discover quarters & files ----
        root = Path(settings.data_root)
        quarters = discover_quarters(root)
        if not quarters:
            typer.echo("[backfill] No quarter folders found. Aborting.")
            raise typer.Exit(1)

        quarter_work: list[tuple[dict, list[tuple[str, Path]]]] = []
        total_files = 0
        for q in quarters:
            files = discover_files(Path(q["folder_path"]))
            if files:
                quarter_work.append((q, files))
                total_files += len(files)

        if not quarter_work:
            typer.echo("[backfill] No loadable FAERS files found. Aborting.")
            raise typer.Exit(1)

        typer.echo(
            f"[backfill] Found {len(quarter_work)} quarters, {total_files} files"
        )
        typer.echo(
            "[backfill] Resources: "
            f"workers={max_workers}, work_mem={work_mem}, "
            f"maintenance_work_mem={maintenance_work_mem}, "
            f"copy_chunk_mb={copy_chunk_mb}"
        )

        # ---- Phase A: DEMO loading ----
        typer.echo("[backfill] Phase A: loading all DEMO files")
        phase_a_start = time.perf_counter()
        load_all_demo(
            quarter_work,
            work_mem=work_mem,
            copy_chunk_mb=copy_chunk_mb,
        )
        typer.echo(
            f"[backfill] Phase A done in "
            f"{format_duration(time.perf_counter() - phase_a_start)}"
        )

        # ---- Phase B: parallel link-table loading ----
        typer.echo(
            f"[backfill] Phase B: loading link tables "
            f"(parallel, {max_workers} workers)"
        )
        phase_b_start = time.perf_counter()
        errors: list[Exception] = []

        if max_workers == 1:
            for q_info, q_files in quarter_work:
                try:
                    load_quarter_links(
                        q_info,
                        q_files,
                        work_mem=work_mem,
                        copy_chunk_mb=copy_chunk_mb,
                    )
                except Exception as exc:
                    typer.echo(f"[backfill] ERROR in {q_info['source_quarter']}: {exc}")
                    errors.append(exc)
        else:
            with ProcessPoolExecutor(max_workers=max_workers) as executor:
                futures = {
                    executor.submit(
                        load_quarter_links,
                        q_info,
                        q_files,
                        work_mem=work_mem,
                        copy_chunk_mb=copy_chunk_mb,
                    ): q_info["source_quarter"]
                    for q_info, q_files in quarter_work
                }
                for future in as_completed(futures):
                    quarter_name = futures[future]
                    try:
                        future.result()
                    except Exception as exc:
                        typer.echo(f"[backfill] ERROR in {quarter_name}: {exc}")
                        errors.append(exc)

        if errors:
            typer.echo(f"[backfill] {len(errors)} quarter(s) failed.")
            raise errors[0]

        typer.echo(
            f"[backfill] Phase B done in "
            f"{format_duration(time.perf_counter() - phase_b_start)}"
        )

        # ---- Phase C: DELETE loading and deleted-link cleanup ----
        typer.echo("[backfill] Phase C: applying DELETE files")
        phase_c_start = time.perf_counter()
        for q_info, q_files in quarter_work:
            load_quarter_delete(
                q_info,
                q_files,
                work_mem=work_mem,
                copy_chunk_mb=copy_chunk_mb,
            )
        with get_conn() as conn:
            purge_deleted_link_rows(conn)
        typer.echo(
            f"[backfill] Phase C done in "
            f"{format_duration(time.perf_counter() - phase_c_start)}"
        )

        # ---- Phase D: global is_latest_known recomputation ----
        with get_conn() as conn:
            recompute_latest_case_flags_global(conn, work_mem=work_mem)

        # ---- Phase E: finalize (settings, integrity, indexes, analyze) ----
        typer.echo("[backfill] Finalizing: checking integrity, building indexes, running ANALYZE")
        finalize_backfill(
            durable=durable,
            run_qa=run_qa,
            maintenance_work_mem=maintenance_work_mem,
        )
        finalized = True

        typer.echo(
            f"[backfill] Complete! Total elapsed: "
            f"{format_duration(time.perf_counter() - started_at)}"
        )
    except Exception:
        if initialized and not finalized:
            restore_backfill_table_settings_safely()
        raise


if __name__ == "__main__":
    app()
