"""Tests for the ETL pipeline."""

from datetime import date

import polars as pl
import pytest

from faersdb.etl import (
    _process_demo,
    _process_drug,
    _read_ascii_file,
    build_query_tables,
    build_warehouse,
)


@pytest.fixture
def sample_demo_file(tmp_path):
    """Create a small sample DEMO file."""
    content = (
        "primaryid$caseid$caseversion$i_f_code$event_dt$mfr_dt$fda_dt$"
        "rept_cod$auth_num$mfr_num$mfr_sndr$lit_ref$age$age_cod$age_grp$"
        "sex$wt$wt_cod$reporter_country\n"
        "100001$200001$1$I$20240101$$20240115$EXP$$$$$45$YR$$M$70$KG$US\n"
        "100002$200002$1$F$20240201$$20240215$DIR$$$$$30$YR$$F$60$KG$JP\n"
        "100003$200002$2$F$20240301$$20240315$PER$$$$$30$YR$$F$60$KG$JP\n"
    )
    fp = tmp_path / "DEMO_TEST.txt"
    fp.write_text(content, encoding="utf-8")
    return fp


@pytest.fixture
def sample_drug_file(tmp_path):
    """Create a small sample DRUG file."""
    content = (
        "primaryid$drug_seq$role_cod$drugname$prod_ai$route$dose_vbm$dose_amt$dose_unit\n"
        "100001$1$PS$ASPIRIN$ACETYLSALICYLIC ACID$ORAL$$100$MG\n"
        "100001$2$SS$IBUPROFEN$IBUPROFEN$ORAL$$200$MG\n"
        "100002$1$PS$METFORMIN$METFORMIN HYDROCHLORIDE$ORAL$$500$MG\n"
    )
    fp = tmp_path / "DRUG_TEST.txt"
    fp.write_text(content, encoding="utf-8")
    return fp


def test_read_ascii_file(sample_demo_file):
    lf = _read_ascii_file(sample_demo_file)
    df = lf.collect()
    assert len(df) == 3
    assert "PRIMARYID" in df.columns
    assert "CASEID" in df.columns


def test_process_demo(sample_demo_file):
    lf = _read_ascii_file(sample_demo_file)
    meta = {"source_quarter": "2024q1", "source_system": "FAERS", "schema_era": "faers_2014q3_plus"}
    result = _process_demo(lf, meta).collect()

    assert len(result) == 3
    assert "primaryid" in result.columns
    assert "caseid" in result.columns
    assert "sex" in result.columns
    assert "wt_kg" in result.columns
    assert "source_quarter" in result.columns

    # Check sex normalization
    sexes = result["sex"].to_list()
    assert sexes == ["M", "F", "F"]

    # Check weight
    weights = result["wt_kg"].to_list()
    assert weights == [70.0, 60.0, 60.0]


def test_process_drug(sample_drug_file):
    lf = _read_ascii_file(sample_drug_file)
    meta = {"source_quarter": "2024q1", "source_system": "FAERS", "schema_era": "faers_2014q3_plus"}
    result = _process_drug(lf, meta).collect()

    assert len(result) == 3
    assert "primaryid" in result.columns
    assert "drugname" in result.columns
    assert "dose_amt" in result.columns

    drugs = result["drugname"].to_list()
    assert "ASPIRIN" in drugs
    assert "IBUPROFEN" in drugs
    assert "METFORMIN" in drugs


def test_build_warehouse_uses_shards_and_deduplicates(tmp_path):
    data_root = tmp_path / "faers"
    quarter_dir = data_root / "faers_ascii_2024q1" / "ASCII"
    warehouse_dir = tmp_path / "warehouse"
    quarter_dir.mkdir(parents=True)

    demo_content = (
        "primaryid$caseid$caseversion$i_f_code$event_dt$fda_dt$sex\n"
        "100001$200001$1$I$20240101$20240115$M\n"
        "100001$200001$1$I$20240101$20240115$M\n"
        "100002$200002$1$F$20240201$20240215$F\n"
    )
    (quarter_dir / "DEMO24Q1.txt").write_text(demo_content, encoding="utf-8")

    results = build_warehouse(
        data_root,
        warehouse_dir,
        memory_limit="256MB",
        threads=1,
    )

    assert results["DEMO"] == 2
    assert not (warehouse_dir / "_build_tmp").exists()

    demo = pl.read_parquet(warehouse_dir / "demo.parquet").sort("primaryid")
    assert demo["primaryid"].to_list() == ["100001", "100002"]
    assert "is_deleted" in demo.columns
    assert (warehouse_dir / "latest_demo.parquet").exists()
    assert (warehouse_dir / "case_summary.parquet").exists()


def test_build_query_tables_keeps_latest_non_deleted_cases(tmp_path):
    warehouse = tmp_path / "warehouse"
    warehouse.mkdir()

    pl.DataFrame(
        {
            "primaryid": ["1001", "1002", "1003", "1004"],
            "caseid": ["2001", "2001", "2002", "2003"],
            "source_quarter": ["2024q1", "2024q2", "2024q2", "2024q3"],
            "source_system": ["FAERS", "FAERS", "FAERS", "FAERS"],
            "caseversion": [1, 2, 1, 1],
            "report_type": ["EXP", "DIR", "PER", "EXP"],
            "i_f_code": ["I", "F", "I", "F"],
            "event_dt": [
                date(2024, 1, 10),
                date(2024, 2, 10),
                date(2024, 3, 10),
                date(2024, 4, 10),
            ],
            "mfr_dt": [
                date(2024, 1, 11),
                date(2024, 2, 11),
                date(2024, 3, 11),
                date(2024, 4, 11),
            ],
            "fda_dt": [
                date(2024, 1, 15),
                date(2024, 2, 15),
                date(2024, 3, 15),
                date(2024, 4, 15),
            ],
            "age": [45.0, 46.0, 60.0, 6.0],
            "age_cod": ["YR", "YR", "YR", "MON"],
            "age_grp": ["A", "A", "E", "A"],
            "sex": ["M", "M", "F", "F"],
            "wt_kg": [80.0, 82.0, 70.0, 65.0],
            "reporter_country": ["US", "US", "US", "US"],
            "auth_num": [None, None, None, None],
            "lit_ref": [None, None, None, None],
            "is_deleted": [False, False, True, False],
        }
    ).write_parquet(warehouse / "demo.parquet")

    pl.DataFrame(
        {
            "primaryid": ["1001", "1002", "1003", "1004"],
            "source_quarter": ["2024q1", "2024q2", "2024q2", "2024q3"],
            "drug_seq": [1, 1, 1, 1],
            "role_cod": ["PS", "PS", "PS", "SS"],
            "drugname": ["OLD ASPIRIN", "Aspirin", "DELETED DRUG", "IBUPROFEN"],
            "prod_ai": ["OLD", "acetylsalicylic acid", "DELETED", "IBUPROFEN"],
            "route": ["ORAL", "ORAL", "ORAL", "ORAL"],
            "dose_vbm": [None, None, None, None],
            "dose_amt": [100.0, 81.0, 500.0, 200.0],
            "dose_unit": ["MG", "MG", "MG", "MG"],
            "start_dt": [
                date(2024, 1, 1),
                date(2024, 2, 1),
                date(2024, 3, 1),
                date(2024, 4, 1),
            ],
            "end_dt": [None, None, None, None],
        }
    ).write_parquet(warehouse / "drug.parquet")

    pl.DataFrame(
        {
            "primaryid": ["1001", "1002", "1003", "1004"],
            "source_quarter": ["2024q1", "2024q2", "2024q2", "2024q3"],
            "pt": ["OLD REACTION", "Headache", "DELETED", "RASH"],
            "drug_rec_act": ["UNK", "UNK", "UNK", "UNK"],
        }
    ).write_parquet(warehouse / "reac.parquet")

    pl.DataFrame(
        {
            "primaryid": ["1002"],
            "source_quarter": ["2024q2"],
            "outc_cod": ["HO"],
        }
    ).write_parquet(warehouse / "outc.parquet")
    pl.DataFrame(
        {
            "primaryid": ["1002"],
            "source_quarter": ["2024q2"],
            "drug_seq": [1],
            "indi_pt": ["Pain"],
        }
    ).write_parquet(warehouse / "indi.parquet")
    pl.DataFrame(
        {
            "primaryid": ["1002"],
            "source_quarter": ["2024q2"],
            "drug_seq": [1],
            "start_dt": [date(2024, 2, 1)],
            "end_dt": [None],
            "dur": [5],
            "dur_cod": ["DY"],
        }
    ).write_parquet(warehouse / "ther.parquet")
    pl.DataFrame(
        {
            "primaryid": ["1002"],
            "source_quarter": ["2024q2"],
            "rpsr_cod": ["HP"],
        }
    ).write_parquet(warehouse / "rpsr.parquet")

    results = build_query_tables(warehouse, memory_limit="256MB", threads=1)

    assert results["LATEST_DEMO"] == 2
    latest_demo = pl.read_parquet(warehouse / "latest_demo.parquet").sort("primaryid")
    assert latest_demo["primaryid"].to_list() == ["1002", "1004"]
    assert latest_demo["caseversion"].to_list() == [2, 1]
    assert latest_demo["age_years"].to_list() == pytest.approx([46.0, 0.5])

    latest_drug = pl.read_parquet(warehouse / "latest_drug.parquet").sort("primaryid")
    assert latest_drug["primaryid"].to_list() == ["1002", "1004"]
    assert latest_drug["drugname_search"].to_list() == ["ASPIRIN", "IBUPROFEN"]

    summary = pl.read_parquet(warehouse / "case_summary.parquet")
    aspirin_summary = summary.filter(pl.col("case_version_pk") == "1002").row(
        0, named=True
    )
    assert aspirin_summary["canonical_case_id"] == "FAERS:2001"
    assert "drugs" not in summary.columns
    assert "reactions" not in summary.columns
