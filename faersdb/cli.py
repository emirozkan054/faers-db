import typer
from pathlib import Path

from psycopg.types.json import Jsonb

from faersdb.config import settings
from faersdb.db import get_conn
from faersdb.manifest import discover_quarters
from faersdb.detect import discover_files
from faersdb.staging_load import insert_demo_raw_rows
from faersdb.normalize.demo import normalize_demo

app = typer.Typer()


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
    if kind != "DEMO":
        raise typer.BadParameter("For now, only DEMO is implemented.")

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

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            files = cur.fetchall()

        for source_file_id, source_quarter, file_path in files:
            inserted = insert_demo_raw_rows(conn, source_file_id, Path(file_path))
            typer.echo(f"{source_quarter} -> inserted {inserted} DEMO rows from {file_path}")
            total_rows += inserted
            file_count += 1

    typer.echo(f"Done. files={file_count}, rows_inserted={total_rows}")


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