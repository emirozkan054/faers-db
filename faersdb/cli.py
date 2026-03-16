import typer
from pathlib import Path

from psycopg.types.json import Jsonb

from faersdb.config import settings
from faersdb.db import get_conn
from faersdb.manifest import discover_quarters
from faersdb.detect import discover_files
from faersdb.staging_load import (
    insert_demo_raw_rows,
    insert_drug_raw_rows,
    insert_reac_raw_rows,
    insert_outc_raw_rows,
    insert_ther_raw_rows,
    insert_indi_raw_rows,
)
from faersdb.normalize.demo import normalize_demo
from faersdb.normalize.drug import normalize_drug
from faersdb.normalize.reac import normalize_reac
from faersdb.normalize.outc import normalize_outc
from faersdb.normalize.ther import normalize_ther
from faersdb.normalize.indi import normalize_indi

app = typer.Typer()


def fetch_case_version_pk(cur, source_system: str, source_quarter: str, source_report_id: str):
    cur.execute(
        """
        select case_version_pk
        from core.case_version
        where source_system = %s
          and source_quarter = %s
          and source_report_id = %s
        """,
        (source_system, source_quarter, source_report_id),
    )
    row = cur.fetchone()
    return row[0] if row else None


@app.command()
def scan():
    quarters = discover_quarters(Path(settings.data_root))
    for q in quarters:
        typer.echo(q)


@app.command()
def init_db():
    sql_path = Path("sql/001_init.sql")
    sql_text = sql_path.read_text()
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql_text)
        conn.commit()
    typer.echo("Database initialized.")


@app.command()
def load_manifest():
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

    typer.echo(f"Manifest loaded. load_batch_id={load_batch_id}, files_inserted={inserted}")


@app.command()
def load_staging(kind: str = "DEMO", quarter: str | None = None):
    kind = kind.upper()
    loaders = {
        "DEMO": insert_demo_raw_rows,
        "DRUG": insert_drug_raw_rows,
        "REAC": insert_reac_raw_rows,
        "OUTC": insert_outc_raw_rows,
        "THER": insert_ther_raw_rows,
        "INDI": insert_indi_raw_rows,
    }
    if kind not in loaders:
        raise typer.BadParameter("Supported kinds: DEMO, DRUG, REAC, OUTC, THER, INDI")

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

    loader = loaders[kind]

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            files = cur.fetchall()

        for source_file_id, source_quarter, file_path in files:
            inserted = loader(conn, source_file_id, Path(file_path))
            typer.echo(f"{source_quarter} -> inserted {inserted} {kind} rows from {file_path}")
            total_rows += inserted
            file_count += 1

    typer.echo(f"Done. files={file_count}, rows_inserted={total_rows}")


@app.command()
def normalize_reac_cmd(quarter: str | None = None):
    select_sql = """
        select
            s.raw_record,
            f.source_quarter,
            f.source_system
        from staging.reac_raw s
        join etl.source_file f
          on f.source_file_id = s.source_file_id
    """
    params = []

    if quarter:
        select_sql += " where f.source_quarter = %s"
        params.append(quarter.lower())

    select_sql += " order by f.source_quarter, s.row_num"

    processed = 0
    skipped = 0

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(select_sql, params)
            rows = cur.fetchall()

        with conn.cursor() as cur:
            for raw_record, source_quarter, source_system in rows:
                norm = normalize_reac(raw_record, {
                    "source_quarter": source_quarter,
                    "source_system": source_system,
                })

                if not norm["source_report_id"] or not norm["reaction_pt"]:
                    skipped += 1
                    continue

                case_version_pk = fetch_case_version_pk(
                    cur,
                    norm["source_system"],
                    norm["source_quarter"],
                    norm["source_report_id"],
                )
                if not case_version_pk:
                    skipped += 1
                    continue

                cur.execute(
                    """
                    insert into core.case_reaction (
                        case_version_pk,
                        source_system,
                        source_quarter,
                        source_report_id,
                        reaction_pt,
                        outcome,
                        raw_reac
                    )
                    values (%s, %s, %s, %s, %s, %s, %s)
                    on conflict (source_system, source_quarter, source_report_id, reaction_pt) do update
                    set
                        case_version_pk = excluded.case_version_pk,
                        outcome = excluded.outcome,
                        raw_reac = excluded.raw_reac
                    """,
                    (
                        case_version_pk,
                        norm["source_system"],
                        norm["source_quarter"],
                        norm["source_report_id"],
                        norm["reaction_pt"],
                        norm["outcome"],
                        Jsonb(norm["raw_reac"]),
                    ),
                )
                processed += 1

        conn.commit()

    typer.echo(f"Normalized REAC rows. processed={processed}, skipped={skipped}")


@app.command()
def normalize_drug_cmd(quarter: str | None = None):
    select_sql = """
        select
            s.raw_record,
            f.source_quarter,
            f.source_system
        from staging.drug_raw s
        join etl.source_file f
          on f.source_file_id = s.source_file_id
    """
    params = []

    if quarter:
        select_sql += " where f.source_quarter = %s"
        params.append(quarter.lower())

    select_sql += " order by f.source_quarter, s.row_num"

    processed = 0
    skipped = 0

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(select_sql, params)
            rows = cur.fetchall()

        with conn.cursor() as cur:
            for raw_record, source_quarter, source_system in rows:
                norm = normalize_drug(raw_record, {
                    "source_quarter": source_quarter,
                    "source_system": source_system,
                })

                if not norm["source_report_id"] or not norm["drugname"]:
                    skipped += 1
                    continue

                case_version_pk = fetch_case_version_pk(
                    cur,
                    norm["source_system"],
                    norm["source_quarter"],
                    norm["source_report_id"],
                )
                if not case_version_pk:
                    skipped += 1
                    continue

                cur.execute(
                    """
                    insert into core.case_drug (
                        case_version_pk,
                        source_system,
                        source_quarter,
                        source_report_id,
                        role_cod,
                        drugname,
                        prod_ai,
                        route,
                        dose_vbm,
                        dose_amt,
                        dose_unit,
                        start_dt,
                        end_dt,
                        raw_drug
                    )
                    values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    on conflict (source_system, source_quarter, source_report_id, drugname, prod_ai, dose_vbm) do update
                    set
                        case_version_pk = excluded.case_version_pk,
                        role_cod = excluded.role_cod,
                        route = excluded.route,
                        dose_amt = excluded.dose_amt,
                        dose_unit = excluded.dose_unit,
                        start_dt = excluded.start_dt,
                        end_dt = excluded.end_dt,
                        raw_drug = excluded.raw_drug
                    """,
                    (
                        case_version_pk,
                        norm["source_system"],
                        norm["source_quarter"],
                        norm["source_report_id"],
                        norm["role_cod"],
                        norm["drugname"],
                        norm["prod_ai"],
                        norm["route"],
                        norm["dose_vbm"],
                        norm["dose_amt"],
                        norm["dose_unit"],
                        norm["start_dt"],
                        norm["end_dt"],
                        Jsonb(norm["raw_drug"]),
                    ),
                )
                processed += 1

        conn.commit()

    typer.echo(f"Normalized DRUG rows. processed={processed}, skipped={skipped}")


@app.command()
def normalize_outc_cmd(quarter: str | None = None):
    select_sql = """
        select
            s.raw_record,
            f.source_quarter,
            f.source_system
        from staging.outc_raw s
        join etl.source_file f
          on f.source_file_id = s.source_file_id
    """
    params = []

    if quarter:
        select_sql += " where f.source_quarter = %s"
        params.append(quarter.lower())

    select_sql += " order by f.source_quarter, s.row_num"

    processed = 0
    skipped = 0

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(select_sql, params)
            rows = cur.fetchall()

        with conn.cursor() as cur:
            for raw_record, source_quarter, source_system in rows:
                norm = normalize_outc(raw_record, {
                    "source_quarter": source_quarter,
                    "source_system": source_system,
                })

                if not norm["source_report_id"] or not norm["outcome"]:
                    skipped += 1
                    continue

                case_version_pk = fetch_case_version_pk(
                    cur,
                    norm["source_system"],
                    norm["source_quarter"],
                    norm["source_report_id"],
                )
                if not case_version_pk:
                    skipped += 1
                    continue

                cur.execute(
                    """
                    insert into core.case_outcome (
                        case_version_pk,
                        source_system,
                        source_quarter,
                        source_report_id,
                        outcome,
                        raw_outc
                    )
                    values (%s, %s, %s, %s, %s, %s)
                    on conflict (source_system, source_quarter, source_report_id, outcome) do update
                    set
                        case_version_pk = excluded.case_version_pk,
                        raw_outc = excluded.raw_outc
                    """,
                    (
                        case_version_pk,
                        norm["source_system"],
                        norm["source_quarter"],
                        norm["source_report_id"],
                        norm["outcome"],
                        Jsonb(norm["raw_outc"]),
                    ),
                )
                processed += 1

        conn.commit()

    typer.echo(f"Normalized OUTC rows. processed={processed}, skipped={skipped}")


@app.command()
def normalize_ther_cmd(quarter: str | None = None):
    select_sql = """
        select
            s.raw_record,
            f.source_quarter,
            f.source_system
        from staging.ther_raw s
        join etl.source_file f
          on f.source_file_id = s.source_file_id
    """
    params = []

    if quarter:
        select_sql += " where f.source_quarter = %s"
        params.append(quarter.lower())

    select_sql += " order by f.source_quarter, s.row_num"

    processed = 0
    skipped = 0

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(select_sql, params)
            rows = cur.fetchall()

        with conn.cursor() as cur:
            for raw_record, source_quarter, source_system in rows:
                norm = normalize_ther(raw_record, {
                    "source_quarter": source_quarter,
                    "source_system": source_system,
                })

                if not norm["source_report_id"]:
                    skipped += 1
                    continue

                case_version_pk = fetch_case_version_pk(
                    cur,
                    norm["source_system"],
                    norm["source_quarter"],
                    norm["source_report_id"],
                )
                if not case_version_pk:
                    skipped += 1
                    continue

                cur.execute(
                    """
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
                        raw_ther
                    )
                    values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    on conflict (source_system, source_quarter, source_report_id, drug_seq, start_dt, end_dt) do update
                    set
                        case_version_pk = excluded.case_version_pk,
                        dur = excluded.dur,
                        dur_cod = excluded.dur_cod,
                        raw_ther = excluded.raw_ther
                    """,
                    (
                        case_version_pk,
                        norm["source_system"],
                        norm["source_quarter"],
                        norm["source_report_id"],
                        norm["drug_seq"],
                        norm["start_dt"],
                        norm["end_dt"],
                        norm["dur"],
                        norm["dur_cod"],
                        Jsonb(norm["raw_ther"]),
                    ),
                )
                processed += 1

        conn.commit()

    typer.echo(f"Normalized THER rows. processed={processed}, skipped={skipped}")


@app.command()
def normalize_indi_cmd(quarter: str | None = None):
    select_sql = """
        select
            s.raw_record,
            f.source_quarter,
            f.source_system
        from staging.indi_raw s
        join etl.source_file f
          on f.source_file_id = s.source_file_id
    """
    params = []

    if quarter:
        select_sql += " where f.source_quarter = %s"
        params.append(quarter.lower())

    select_sql += " order by f.source_quarter, s.row_num"

    processed = 0
    skipped = 0

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(select_sql, params)
            rows = cur.fetchall()

        with conn.cursor() as cur:
            for raw_record, source_quarter, source_system in rows:
                norm = normalize_indi(raw_record, {
                    "source_quarter": source_quarter,
                    "source_system": source_system,
                })

                if not norm["source_report_id"] or not norm["indi_pt"]:
                    skipped += 1
                    continue

                case_version_pk = fetch_case_version_pk(
                    cur,
                    norm["source_system"],
                    norm["source_quarter"],
                    norm["source_report_id"],
                )
                if not case_version_pk:
                    skipped += 1
                    continue

                cur.execute(
                    """
                    insert into core.case_indication (
                        case_version_pk,
                        source_system,
                        source_quarter,
                        source_report_id,
                        drug_seq,
                        indi_pt,
                        raw_indi
                    )
                    values (%s, %s, %s, %s, %s, %s, %s)
                    on conflict (source_system, source_quarter, source_report_id, drug_seq, indi_pt) do update
                    set
                        case_version_pk = excluded.case_version_pk,
                        raw_indi = excluded.raw_indi
                    """,
                    (
                        case_version_pk,
                        norm["source_system"],
                        norm["source_quarter"],
                        norm["source_report_id"],
                        norm["drug_seq"],
                        norm["indi_pt"],
                        Jsonb(norm["raw_indi"]),
                    ),
                )
                processed += 1

        conn.commit()

    typer.echo(f"Normalized INDI rows. processed={processed}, skipped={skipped}")


@app.command()
def normalize_demo_cmd(quarter: str | None = None):
    select_sql = """
        select
            s.raw_record,
            f.source_quarter,
            f.source_system,
            f.schema_era
        from staging.demo_raw s
        join etl.source_file f
          on f.source_file_id = s.source_file_id
    """
    params = []

    if quarter:
        select_sql += " where f.source_quarter = %s"
        params.append(quarter.lower())

    select_sql += " order by f.source_quarter, s.row_num"

    processed = 0
    skipped = 0

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(select_sql, params)
            rows = cur.fetchall()

        with conn.cursor() as cur:
            for raw_record, source_quarter, source_system, schema_era in rows:
                meta = {
                    "source_quarter": source_quarter,
                    "source_system": source_system,
                    "schema_era": schema_era,
                }

                norm = normalize_demo(raw_record, meta)

                if not norm["source_case_id"] or not norm["source_report_id"]:
                    skipped += 1
                    continue

                canonical_case_id = f'{norm["source_system"]}:{norm["source_case_id"]}'

                cur.execute(
                    """
                    insert into core.case_master (
                        canonical_case_id,
                        source_case_id,
                        source_system,
                        first_seen_quarter,
                        latest_seen_quarter
                    )
                    values (%s, %s, %s, %s, %s)
                    on conflict (canonical_case_id) do update
                    set first_seen_quarter = least(core.case_master.first_seen_quarter, excluded.first_seen_quarter),
                        latest_seen_quarter = greatest(core.case_master.latest_seen_quarter, excluded.latest_seen_quarter)
                    returning case_pk
                    """,
                    (
                        canonical_case_id,
                        norm["source_case_id"],
                        norm["source_system"],
                        norm["source_quarter"],
                        norm["source_quarter"],
                    ),
                )
                case_pk = cur.fetchone()[0]

                cur.execute(
                    """
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
                    values (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    )
                    on conflict (source_system, source_report_id, source_quarter) do update
                    set
                        case_pk = excluded.case_pk,
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
                        raw_demo = excluded.raw_demo
                    """,
                    (
                        case_pk,
                        norm["source_quarter"],
                        norm["source_system"],
                        norm["schema_era"],
                        norm["source_report_id"],
                        norm["source_case_id"],
                        norm["case_version_num"],
                        norm["report_type"],
                        norm["initial_or_followup"],
                        norm["event_dt"],
                        norm["mfr_dt"],
                        norm["fda_dt"],
                        norm["age_value"],
                        norm["age_unit"],
                        norm["age_group"],
                        norm["sex_std"],
                        norm["weight_kg"],
                        norm["reporter_country"],
                        norm["auth_num"],
                        norm["lit_ref"],
                        Jsonb(norm["raw_demo"]),
                    ),
                )

                processed += 1

        conn.commit()

        with conn.cursor() as cur:
            cur.execute(
                """
                with ranked as (
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
                )
                update core.case_version cv
                set is_latest_known = (ranked.rn = 1)
                from ranked
                where cv.case_version_pk = ranked.case_version_pk
                """
            )
        conn.commit()

    typer.echo(f"Normalized DEMO rows. processed={processed}, skipped={skipped}")


if __name__ == "__main__":
    app()
