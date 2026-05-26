"""Tests for the ETL pipeline."""

import tempfile
from pathlib import Path

import polars as pl
import pytest

from faersdb.etl import _read_ascii_file, _process_demo, _process_drug


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
