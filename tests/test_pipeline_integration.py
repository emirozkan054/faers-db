from contextlib import contextmanager
from uuid import uuid4

import pytest
from psycopg import connect, sql
from psycopg.conninfo import conninfo_to_dict, make_conninfo

from faersdb import cli
from faersdb.config import settings


def _write_text(path, content: str):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content.strip() + "\n", encoding="utf-8")


def _create_sample_quarters(data_root):
    q4 = data_root / "faers_ascii_2025q4"
    q1 = data_root / "faers_ascii_2026q1"

    _write_text(
        q4 / "ASCII" / "DEMO25Q4.txt",
        """
        primaryid$caseid$caseversion$i_f_code$event_dt$mfr_dt$fda_dt$rept_cod$auth_num$lit_ref$age$age_cod$age_grp$sex$wt$wt_cod$reporter_country
        1001$10$1$I$20250101$20250102$20250103$EXP$AUTH-10$$65$YR$E$M$80$KG$US
        2001$20$1$I$20250104$20250105$20250106$EXP$$$35$YR$A$F$60$KG$US
        """,
    )
    _write_text(
        q4 / "ASCII" / "DRUG25Q4.txt",
        """
        primaryid$caseid$drug_seq$role_cod$drugname$prod_ai$route$dose_vbm$dose_amt$dose_unit$start_dt$end_dt
        1001$10$1$PS$ASPIRIN$ASPIRIN$ORAL$TAB$10$MG$20250101$20250102
        1001$10$1$PS$ASPIRIN$ASPIRIN$IV$INF$20$MG$20250101$20250103
        2001$20$1$PS$IBUPROFEN$IBUPROFEN$ORAL$TAB$5$MG$20250104$
        2001$20$1$PS$IBUPROFEN$IBUPROFEN$ORAL$TAB$5$MG$20250104$
        """,
    )
    _write_text(
        q4 / "ASCII" / "REAC25Q4.txt",
        """
        primaryid$caseid$pt$outc_cod
        1001$10$Headache$HO
        1001$10$Nausea$HO
        2001$20$Rash$LT
        2001$20$Rash$LT
        """,
    )
    _write_text(
        q4 / "ASCII" / "OUTC25Q4.txt",
        """
        primaryid$caseid$outc_cod
        1001$10$HO
        2001$20$LT
        """,
    )
    _write_text(
        q4 / "ASCII" / "THER25Q4.txt",
        """
        primaryid$caseid$dsg_drug_seq$start_dt$end_dt$dur$dur_cod
        1001$10$1$20250101$20250102$1$DY
        2001$20$1$20250104$$2$DY
        """,
    )
    _write_text(
        q4 / "ASCII" / "INDI25Q4.txt",
        """
        primaryid$caseid$indi_drug_seq$indi_pt
        1001$10$1$Pain
        2001$20$1$Inflammation
        """,
    )
    _write_text(
        q4 / "ASCII" / "RPSR25Q4.txt",
        """
        primaryid$caseid$rpsr_cod
        1001$10$HP
        2001$20$HP
        """,
    )

    _write_text(
        q1 / "ASCII" / "DEMO26Q1.txt",
        """
        primaryid$caseid$caseversion$i_f_code$event_dt$mfr_dt$fda_dt$rept_cod$auth_num$lit_ref$age$age_cod$age_grp$sex$wt$wt_cod$reporter_country
        1002$10$2$F$20260101$20260102$20260103$EXP$AUTH-10B$$66$YR$E$M$81$KG$US
        """,
    )
    _write_text(
        q1 / "ASCII" / "DRUG26Q1.txt",
        """
        primaryid$caseid$drug_seq$role_cod$drugname$prod_ai$route$dose_vbm$dose_amt$dose_unit$start_dt$end_dt
        1002$10$1$PS$ASPIRIN$ASPIRIN$ORAL$TAB$15$MG$20260101$20260102
        """,
    )
    _write_text(
        q1 / "ASCII" / "REAC26Q1.txt",
        """
        primaryid$caseid$pt$outc_cod
        1002$10$Dizziness$HO
        """,
    )
    _write_text(
        q1 / "ASCII" / "OUTC26Q1.txt",
        """
        primaryid$caseid$outc_cod
        1002$10$HO
        """,
    )
    _write_text(
        q1 / "ASCII" / "THER26Q1.txt",
        """
        primaryid$caseid$dsg_drug_seq$start_dt$end_dt$dur$dur_cod
        1002$10$1$20260101$20260102$1$DY
        """,
    )
    _write_text(
        q1 / "ASCII" / "INDI26Q1.txt",
        """
        primaryid$caseid$indi_drug_seq$indi_pt
        1002$10$1$Pain
        """,
    )
    _write_text(
        q1 / "ASCII" / "RPSR26Q1.txt",
        """
        primaryid$caseid$rpsr_cod
        1002$10$HP
        """,
    )
    _write_text(q1 / "Deleted" / "DELETE26Q1.txt", "1002")


@pytest.fixture()
def pipeline_env_factory(tmp_path):
    data_root = tmp_path / "faers"
    _create_sample_quarters(data_root)

    try:
        conninfo = conninfo_to_dict(settings.pg_dsn)
    except Exception as exc:  # pragma: no cover - defensive skip
        pytest.skip(f"Cannot parse PG DSN for integration test: {exc}")

    db_name = f"faers_test_{uuid4().hex[:8]}"
    admin_conninfo = dict(conninfo)
    admin_conninfo["dbname"] = "postgres"
    test_conninfo = dict(conninfo)
    test_conninfo["dbname"] = db_name
    admin_dsn = make_conninfo(**admin_conninfo)
    test_dsn = make_conninfo(**test_conninfo)

    @contextmanager
    def factory():
        try:
            with connect(admin_dsn, autocommit=True) as conn:
                with conn.cursor() as cur:
                    cur.execute(sql.SQL("create database {}").format(sql.Identifier(db_name)))
        except Exception as exc:
            pytest.skip(f"PostgreSQL not available for integration test: {exc}")

        old_pg_dsn = settings.pg_dsn
        old_data_root = settings.data_root
        old_profile = settings.pipeline_profile
        settings.pg_dsn = test_dsn
        settings.data_root = str(data_root)

        try:
            yield test_dsn
        finally:
            settings.pg_dsn = old_pg_dsn
            settings.data_root = old_data_root
            settings.pipeline_profile = old_profile
            with connect(admin_dsn, autocommit=True) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "select pg_terminate_backend(pid) from pg_stat_activity where datname = %s and pid <> pg_backend_pid()",
                        (db_name,),
                    )
                    cur.execute(sql.SQL("drop database if exists {}").format(sql.Identifier(db_name)))

    return factory


def _database_snapshot():
    with connect(settings.pg_dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                select source_report_id, is_latest_known, is_deleted
                from core.case_version
                order by source_report_id
                """
            )
            case_versions = cur.fetchall()

            cur.execute(
                """
                select
                    (select count(*) from core.case_drug),
                    (select count(*) from core.case_reaction),
                    (select count(*) from core.case_outcome),
                    (select count(*) from core.case_therapy),
                    (select count(*) from core.case_indication),
                    (select count(*) from core.case_report_source)
                """
            )
            counts = cur.fetchone()

            cur.execute(
                """
                select source_report_id
                from mart.case_latest
                order by source_report_id
                """
            )
            latest = cur.fetchall()

            cur.execute(
                """
                select source_report_id, drug_seq, drugname, route, dose_amt
                from core.case_drug
                order by source_report_id, drug_seq, drugname, route, dose_amt
                """
            )
            case_drugs = cur.fetchall()

            cur.execute(
                """
                select source_report_id, reaction_pt, outcome
                from core.case_reaction
                order by source_report_id, reaction_pt, outcome
                """
            )
            reactions = cur.fetchall()

    return {
        "case_versions": case_versions,
        "counts": counts,
        "latest": latest,
        "case_drugs": case_drugs,
        "reactions": reactions,
    }


def _assert_expected_snapshot(snapshot):
    assert snapshot["case_versions"] == [
        ("1001", True, False),
        ("1002", False, True),
        ("2001", True, False),
    ]
    assert snapshot["counts"] == (3, 3, 2, 2, 2, 2)
    assert snapshot["latest"] == [("1001",), ("2001",)]
    assert snapshot["case_drugs"] == [
        ("1001", 1, "ASPIRIN", "IV", 20),
        ("1001", 1, "ASPIRIN", "ORAL", 10),
        ("2001", 1, "IBUPROFEN", "ORAL", 5),
    ]
    assert snapshot["reactions"] == [
        ("1001", "Headache", "HO"),
        ("1001", "Nausea", "HO"),
        ("2001", "Rash", "LT"),
    ]

def test_pipeline_standard_and_fast_backfill_match(pipeline_env_factory):
    snapshots = {}

    for profile in ("standard", "fast_backfill"):
        with pipeline_env_factory():
            cli.init_db(profile=profile)
            cli.load_manifest()
            cli.run_quarter("2025q4", run_qa=False, parallel_normalize=False, profile=profile)
            cli.run_quarter("2026q1", run_qa=False, parallel_normalize=False, profile=profile)
            if profile == "fast_backfill":
                cli.finalize_backfill(run_qa=False)

            snapshot = _database_snapshot()
            _assert_expected_snapshot(snapshot)
            snapshots[profile] = snapshot

    assert snapshots["standard"] == snapshots["fast_backfill"]


def test_fast_backfill_finalize_keeps_unlogged_by_default(pipeline_env_factory):
    with pipeline_env_factory():
        cli.init_db(profile="fast_backfill")
        cli.load_manifest()
        cli.run_quarter("2025q4", run_qa=False, parallel_normalize=False, profile="fast_backfill")

        with connect(settings.pg_dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    select relpersistence
                    from pg_class
                    where oid = 'core.case_drug'::regclass
                    """
                )
                assert cur.fetchone()[0] == "u"

        cli.finalize_backfill(run_qa=False)

        with connect(settings.pg_dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    select relpersistence
                    from pg_class
                    where oid = 'core.case_drug'::regclass
                    """
                )
                assert cur.fetchone()[0] == "u"


def test_fast_backfill_finalize_restores_logged_tables_when_durable(pipeline_env_factory):
    with pipeline_env_factory():
        cli.init_db(profile="fast_backfill")
        cli.load_manifest()
        cli.run_quarter("2025q4", run_qa=False, parallel_normalize=False, profile="fast_backfill")
        cli.finalize_backfill(durable=True, run_qa=False)

        with connect(settings.pg_dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    select relpersistence
                    from pg_class
                    where oid = 'core.case_drug'::regclass
                    """
                )
                assert cur.fetchone()[0] == "p"


def test_pipeline_rerun_is_idempotent(pipeline_env_factory):
    with pipeline_env_factory():
        cli.init_db(profile="standard")
        cli.load_manifest()
        cli.run_quarter("2025q4", run_qa=False, parallel_normalize=False, profile="standard")
        before = _database_snapshot()

        cli.run_quarter("2025q4", run_qa=False, parallel_normalize=False, profile="standard")
        after = _database_snapshot()

    assert before == after
