"""Direct-load module for fast backfill.

Bypasses the staging layer entirely: parses CSV in Python, normalizes
in-memory, and COPYs flat rows directly into core tables via temp tables.
"""

import time
from pathlib import Path

import typer

from faersdb.db import get_conn
from faersdb.normalize.demo import normalize_demo
from faersdb.normalize.drug import normalize_drug
from faersdb.normalize.reac import normalize_reac
from faersdb.normalize.outc import normalize_outc
from faersdb.normalize.ther import normalize_ther
from faersdb.normalize.indi import normalize_indi
from faersdb.normalize.rpsr import normalize_rpsr
from faersdb.staging_load import iter_delimited_records, iter_delete_records, fast_row_hash


def _fmt(seconds: float) -> str:
    if seconds < 60:
        return f"{seconds:.1f}s"
    m, s = divmod(seconds, 60)
    return f"{int(m)}m {s:.0f}s"


# ---------------------------------------------------------------------------
# Per-quarter entry points (two-phase to avoid case_master deadlocks)
# ---------------------------------------------------------------------------

def load_quarter_demo(quarter_info: dict, files: list[tuple[str, Path]]) -> dict:
    """Phase A: Load DEMO data for a single quarter.

    Inserts into case_master (upsert) and case_version.
    Must be called SEQUENTIALLY across quarters to avoid deadlocks
    on the case_master unique index.
    """
    quarter = quarter_info["source_quarter"]
    meta = {
        "source_quarter": quarter_info["source_quarter"],
        "source_system": quarter_info["source_system"],
        "schema_era": quarter_info["schema_era"],
    }

    demo_files = [p for kind, p in files if kind == "DEMO"]

    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SET synchronous_commit = off")
            cur.execute("SET work_mem = '256MB'")
        conn.commit()
        count = _load_demo(conn, meta, demo_files)
    finally:
        conn.close()

    return {"quarter": quarter, "demo": count}


def load_quarter_links(quarter_info: dict, files: list[tuple[str, Path]]) -> dict:
    """Phase B: Load link tables + DELETE for a single quarter.

    Reads from case_version (already populated by Phase A).
    Safe to run IN PARALLEL across quarters — no shared-row contention.
    """
    quarter = quarter_info["source_quarter"]
    meta = {
        "source_quarter": quarter_info["source_quarter"],
        "source_system": quarter_info["source_system"],
        "schema_era": quarter_info["schema_era"],
    }

    files_by_kind: dict[str, list[Path]] = {}
    for kind, path in files:
        if kind != "DEMO":
            files_by_kind.setdefault(kind, []).append(path)

    results: dict = {"quarter": quarter}
    t0 = time.perf_counter()

    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SET synchronous_commit = off")
            cur.execute("SET work_mem = '256MB'")
        conn.commit()

        for kind, loader in [
            ("DRUG", _load_drug),
            ("REAC", _load_reac),
            ("OUTC", _load_outc),
            ("THER", _load_ther),
            ("INDI", _load_indi),
            ("RPSR", _load_rpsr),
        ]:
            results[kind.lower()] = loader(conn, meta, files_by_kind.get(kind, []))

        results["delete"] = _load_delete(conn, meta, files_by_kind.get("DELETE", []))
    finally:
        conn.close()

    elapsed = time.perf_counter() - t0
    results["elapsed"] = elapsed
    typer.echo(f"  [{quarter}] links finished in {_fmt(elapsed)}")
    return results


# ---------------------------------------------------------------------------
# DEMO → case_master + case_version
# ---------------------------------------------------------------------------

def _load_demo(conn, meta: dict, file_paths: list[Path]) -> int:
    if not file_paths:
        return 0

    t0 = time.perf_counter()
    quarter = meta["source_quarter"]

    with conn.cursor() as cur:
        cur.execute("""
            CREATE TEMP TABLE _tmp_demo (
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
            )
        """)

        total = 0
        for fp in file_paths:
            with cur.copy("COPY _tmp_demo FROM STDIN") as copy:
                for _, raw in iter_delimited_records(fp):
                    n = normalize_demo(raw, meta)
                    if not n["source_case_id"] or not n["source_report_id"]:
                        continue
                    copy.write_row((
                        n["source_quarter"], n["source_system"], n["schema_era"],
                        n["source_case_id"], n["source_report_id"],
                        n["case_version_num"], n["report_type"],
                        n["initial_or_followup"],
                        n["event_dt"], n["mfr_dt"], n["fda_dt"],
                        n["age_value"], n["age_unit"], n["age_group"],
                        n["sex_std"], n["weight_kg"],
                        n["reporter_country"], n["auth_num"], n["lit_ref"],
                    ))
                    total += 1

        cur.execute("ANALYZE _tmp_demo")

        # Upsert case_master
        cur.execute("""
            INSERT INTO core.case_master
                (canonical_case_id, source_case_id, source_system,
                 first_seen_quarter, latest_seen_quarter)
            SELECT DISTINCT
                source_system || ':' || source_case_id,
                source_case_id, source_system,
                source_quarter, source_quarter
            FROM _tmp_demo
            ORDER BY 1
            ON CONFLICT (canonical_case_id) DO UPDATE SET
                first_seen_quarter = LEAST(
                    core.case_master.first_seen_quarter, EXCLUDED.first_seen_quarter),
                latest_seen_quarter = GREATEST(
                    core.case_master.latest_seen_quarter, EXCLUDED.latest_seen_quarter)
        """)

        # Insert case_version (raw_demo = NULL for backfill)
        cur.execute("""
            INSERT INTO core.case_version
                (case_pk, source_quarter, source_system, schema_era,
                 source_report_id, source_case_id, case_version_num,
                 report_type, initial_or_followup,
                 event_dt, mfr_dt, fda_dt,
                 age_value, age_unit, age_group, sex_std, weight_kg,
                 reporter_country, auth_num, lit_ref)
            SELECT
                cm.case_pk,
                d.source_quarter, d.source_system, d.schema_era,
                d.source_report_id, d.source_case_id, d.case_version_num,
                d.report_type, d.initial_or_followup,
                d.event_dt, d.mfr_dt, d.fda_dt,
                d.age_value, d.age_unit, d.age_group, d.sex_std, d.weight_kg,
                d.reporter_country, d.auth_num, d.lit_ref
            FROM _tmp_demo d
            JOIN core.case_master cm
              ON cm.canonical_case_id = d.source_system || ':' || d.source_case_id
            ORDER BY d.source_system, d.source_report_id, d.source_quarter
            ON CONFLICT (source_system, source_report_id, source_quarter)
            DO NOTHING
        """)

        cur.execute("DROP TABLE _tmp_demo")
    conn.commit()
    typer.echo(f"  [{quarter}] DEMO {total} rows in {_fmt(time.perf_counter() - t0)}")
    return total


# ---------------------------------------------------------------------------
# Generic link-table loader
# ---------------------------------------------------------------------------

def _load_link_table(
    conn,
    meta: dict,
    file_paths: list[Path],
    *,
    table_kind: str,
    target_table: str,
    temp_cols_sql: str,
    normalize_fn,
    row_extractor,
    insert_select_sql: str,
):
    """Generic loader for link tables (DRUG, REAC, OUTC, THER, INDI, RPSR)."""
    if not file_paths:
        return 0

    t0 = time.perf_counter()
    quarter = meta["source_quarter"]
    tmp_name = f"_tmp_{table_kind.lower()}"

    with conn.cursor() as cur:
        cur.execute(f"CREATE TEMP TABLE {tmp_name} ({temp_cols_sql})")

        total = 0
        for fp in file_paths:
            with cur.copy(f"COPY {tmp_name} FROM STDIN") as copy:
                for _, raw in iter_delimited_records(fp):
                    n = normalize_fn(raw, meta)
                    row = row_extractor(n, raw)
                    if row is not None:
                        copy.write_row(row)
                        total += 1

        cur.execute(f"ANALYZE {tmp_name}")
        cur.execute(insert_select_sql.format(tmp=tmp_name, quarter=quarter))
        cur.execute(f"DROP TABLE {tmp_name}")
    conn.commit()
    typer.echo(f"  [{quarter}] {table_kind} {total} rows in {_fmt(time.perf_counter() - t0)}")
    return total


# ---------------------------------------------------------------------------
# DRUG
# ---------------------------------------------------------------------------

def _load_drug(conn, meta: dict, file_paths: list[Path]) -> int:
    return _load_link_table(
        conn, meta, file_paths,
        table_kind="DRUG",
        target_table="core.case_drug",
        temp_cols_sql="""
            source_quarter text, source_system text, source_report_id text,
            drug_seq int, role_cod text, drugname text, prod_ai text,
            route text, dose_vbm text, dose_amt numeric, dose_unit text,
            start_dt date, end_dt date, row_hash text
        """,
        normalize_fn=normalize_drug,
        row_extractor=_drug_row,
        insert_select_sql="""
            INSERT INTO core.case_drug
                (case_version_pk, source_system, source_quarter, source_report_id,
                 drug_seq, role_cod, drugname, prod_ai, route, dose_vbm,
                 dose_amt, dose_unit, start_dt, end_dt, row_hash)
            SELECT
                cv.case_version_pk,
                t.source_system, t.source_quarter, t.source_report_id,
                t.drug_seq, t.role_cod, t.drugname, t.prod_ai, t.route,
                t.dose_vbm, t.dose_amt, t.dose_unit, t.start_dt, t.end_dt,
                t.row_hash
            FROM {tmp} t
            JOIN core.case_version cv
              ON cv.source_system = t.source_system
             AND cv.source_quarter = '{quarter}'
             AND cv.source_report_id = t.source_report_id
            ORDER BY t.source_system, t.source_quarter, t.source_report_id, t.row_hash
            ON CONFLICT (source_system, source_quarter, source_report_id, row_hash)
            DO NOTHING
        """,
    )


def _drug_row(n: dict, raw: dict):
    if not n.get("source_report_id") or not n.get("drugname"):
        return None
    return (
        n["source_quarter"], n["source_system"], n["source_report_id"],
        n["drug_seq"], n["role_cod"], n["drugname"], n["prod_ai"],
        n["route"], n["dose_vbm"], n["dose_amt"], n["dose_unit"],
        n["start_dt"], n["end_dt"], fast_row_hash(raw),
    )


# ---------------------------------------------------------------------------
# REAC
# ---------------------------------------------------------------------------

def _load_reac(conn, meta: dict, file_paths: list[Path]) -> int:
    return _load_link_table(
        conn, meta, file_paths,
        table_kind="REAC",
        target_table="core.case_reaction",
        temp_cols_sql="""
            source_quarter text, source_system text, source_report_id text,
            reaction_pt text, outcome text, row_hash text
        """,
        normalize_fn=normalize_reac,
        row_extractor=_reac_row,
        insert_select_sql="""
            INSERT INTO core.case_reaction
                (case_version_pk, source_system, source_quarter, source_report_id,
                 reaction_pt, outcome, row_hash)
            SELECT
                cv.case_version_pk,
                t.source_system, t.source_quarter, t.source_report_id,
                t.reaction_pt, t.outcome, t.row_hash
            FROM {tmp} t
            JOIN core.case_version cv
              ON cv.source_system = t.source_system
             AND cv.source_quarter = '{quarter}'
             AND cv.source_report_id = t.source_report_id
            ORDER BY t.source_system, t.source_quarter, t.source_report_id, t.row_hash
            ON CONFLICT (source_system, source_quarter, source_report_id, row_hash)
            DO NOTHING
        """,
    )


def _reac_row(n: dict, raw: dict):
    if not n.get("source_report_id") or not n.get("reaction_pt"):
        return None
    return (
        n["source_quarter"], n["source_system"], n["source_report_id"],
        n["reaction_pt"], n["outcome"], fast_row_hash(raw),
    )


# ---------------------------------------------------------------------------
# OUTC
# ---------------------------------------------------------------------------

def _load_outc(conn, meta: dict, file_paths: list[Path]) -> int:
    return _load_link_table(
        conn, meta, file_paths,
        table_kind="OUTC",
        target_table="core.case_outcome",
        temp_cols_sql="""
            source_quarter text, source_system text, source_report_id text,
            outcome text, row_hash text
        """,
        normalize_fn=normalize_outc,
        row_extractor=_outc_row,
        insert_select_sql="""
            INSERT INTO core.case_outcome
                (case_version_pk, source_system, source_quarter, source_report_id,
                 outcome, row_hash)
            SELECT
                cv.case_version_pk,
                t.source_system, t.source_quarter, t.source_report_id,
                t.outcome, t.row_hash
            FROM {tmp} t
            JOIN core.case_version cv
              ON cv.source_system = t.source_system
             AND cv.source_quarter = '{quarter}'
             AND cv.source_report_id = t.source_report_id
            ORDER BY t.source_system, t.source_quarter, t.source_report_id, t.row_hash
            ON CONFLICT (source_system, source_quarter, source_report_id, row_hash)
            DO NOTHING
        """,
    )


def _outc_row(n: dict, raw: dict):
    if not n.get("source_report_id") or not n.get("outcome"):
        return None
    return (
        n["source_quarter"], n["source_system"], n["source_report_id"],
        n["outcome"], fast_row_hash(raw),
    )


# ---------------------------------------------------------------------------
# THER
# ---------------------------------------------------------------------------

def _load_ther(conn, meta: dict, file_paths: list[Path]) -> int:
    return _load_link_table(
        conn, meta, file_paths,
        table_kind="THER",
        target_table="core.case_therapy",
        temp_cols_sql="""
            source_quarter text, source_system text, source_report_id text,
            drug_seq int, start_dt date, end_dt date,
            dur int, dur_cod text, row_hash text
        """,
        normalize_fn=normalize_ther,
        row_extractor=_ther_row,
        insert_select_sql="""
            INSERT INTO core.case_therapy
                (case_version_pk, source_system, source_quarter, source_report_id,
                 drug_seq, start_dt, end_dt, dur, dur_cod, row_hash)
            SELECT
                cv.case_version_pk,
                t.source_system, t.source_quarter, t.source_report_id,
                t.drug_seq, t.start_dt, t.end_dt, t.dur, t.dur_cod, t.row_hash
            FROM {tmp} t
            JOIN core.case_version cv
              ON cv.source_system = t.source_system
             AND cv.source_quarter = '{quarter}'
             AND cv.source_report_id = t.source_report_id
            ORDER BY t.source_system, t.source_quarter, t.source_report_id, t.row_hash
            ON CONFLICT (source_system, source_quarter, source_report_id, row_hash)
            DO NOTHING
        """,
    )


def _ther_row(n: dict, raw: dict):
    if not n.get("source_report_id"):
        return None
    return (
        n["source_quarter"], n["source_system"], n["source_report_id"],
        n["drug_seq"], n["start_dt"], n["end_dt"],
        n["dur"], n["dur_cod"], fast_row_hash(raw),
    )


# ---------------------------------------------------------------------------
# INDI
# ---------------------------------------------------------------------------

def _load_indi(conn, meta: dict, file_paths: list[Path]) -> int:
    return _load_link_table(
        conn, meta, file_paths,
        table_kind="INDI",
        target_table="core.case_indication",
        temp_cols_sql="""
            source_quarter text, source_system text, source_report_id text,
            drug_seq int, indi_pt text, row_hash text
        """,
        normalize_fn=normalize_indi,
        row_extractor=_indi_row,
        insert_select_sql="""
            INSERT INTO core.case_indication
                (case_version_pk, source_system, source_quarter, source_report_id,
                 drug_seq, indi_pt, row_hash)
            SELECT
                cv.case_version_pk,
                t.source_system, t.source_quarter, t.source_report_id,
                t.drug_seq, t.indi_pt, t.row_hash
            FROM {tmp} t
            JOIN core.case_version cv
              ON cv.source_system = t.source_system
             AND cv.source_quarter = '{quarter}'
             AND cv.source_report_id = t.source_report_id
            ORDER BY t.source_system, t.source_quarter, t.source_report_id, t.row_hash
            ON CONFLICT (source_system, source_quarter, source_report_id, row_hash)
            DO NOTHING
        """,
    )


def _indi_row(n: dict, raw: dict):
    if not n.get("source_report_id") or not n.get("indi_pt"):
        return None
    return (
        n["source_quarter"], n["source_system"], n["source_report_id"],
        n["drug_seq"], n["indi_pt"], fast_row_hash(raw),
    )


# ---------------------------------------------------------------------------
# RPSR
# ---------------------------------------------------------------------------

def _load_rpsr(conn, meta: dict, file_paths: list[Path]) -> int:
    return _load_link_table(
        conn, meta, file_paths,
        table_kind="RPSR",
        target_table="core.case_report_source",
        temp_cols_sql="""
            source_quarter text, source_system text, source_report_id text,
            reporter_type text, row_hash text
        """,
        normalize_fn=normalize_rpsr,
        row_extractor=_rpsr_row,
        insert_select_sql="""
            INSERT INTO core.case_report_source
                (case_version_pk, source_system, source_quarter, source_report_id,
                 reporter_type, row_hash)
            SELECT
                cv.case_version_pk,
                t.source_system, t.source_quarter, t.source_report_id,
                t.reporter_type, t.row_hash
            FROM {tmp} t
            JOIN core.case_version cv
              ON cv.source_system = t.source_system
             AND cv.source_quarter = '{quarter}'
             AND cv.source_report_id = t.source_report_id
            ORDER BY t.source_system, t.source_quarter, t.source_report_id, t.row_hash
            ON CONFLICT (source_system, source_quarter, source_report_id, row_hash)
            DO NOTHING
        """,
    )


def _rpsr_row(n: dict, raw: dict):
    if not n.get("source_report_id") or not n.get("reporter_type"):
        return None
    return (
        n["source_quarter"], n["source_system"], n["source_report_id"],
        n["reporter_type"], fast_row_hash(raw),
    )


# ---------------------------------------------------------------------------
# DELETE
# ---------------------------------------------------------------------------

def _load_delete(conn, meta: dict, file_paths: list[Path]) -> int:
    if not file_paths:
        return 0

    t0 = time.perf_counter()
    quarter = meta["source_quarter"]
    source_system = meta["source_system"]

    total = 0
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TEMP TABLE _tmp_delete (
                source_report_id text
            )
        """)

        for fp in file_paths:
            with cur.copy("COPY _tmp_delete FROM STDIN") as copy:
                for _, report_id in iter_delete_records(fp):
                    copy.write_row((report_id,))
                    total += 1

        cur.execute("ANALYZE _tmp_delete")

        # Mark matching case_versions as deleted (skip is_latest_known recompute)
        cur.execute("""
            UPDATE core.case_version cv
            SET is_deleted = true, is_latest_known = false
            FROM _tmp_delete d
            WHERE cv.source_system = %s
              AND cv.source_report_id = d.source_report_id
              AND cv.is_deleted = false
        """, (source_system,))

        cur.execute("DROP TABLE _tmp_delete")
    conn.commit()
    typer.echo(f"  [{quarter}] DELETE {total} rows in {_fmt(time.perf_counter() - t0)}")
    return total


# ---------------------------------------------------------------------------
# Global is_latest_known recomputation (run once after all quarters loaded)
# ---------------------------------------------------------------------------

def recompute_latest_case_flags_global(conn):
    """Recompute is_latest_known for ALL cases in a single pass.

    Much faster than per-quarter recomputation when doing a full backfill.
    """
    typer.echo("Recomputing is_latest_known flags globally...")
    t0 = time.perf_counter()

    with conn.cursor() as cur:
        cur.execute("SET synchronous_commit = off")
        cur.execute("SET work_mem = '512MB'")

        # Reset all flags
        cur.execute("UPDATE core.case_version SET is_latest_known = false")

        # Set the latest non-deleted version for each case
        cur.execute("""
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
        """)

    conn.commit()
    typer.echo(f"  is_latest_known recomputed in {_fmt(time.perf_counter() - t0)}")
