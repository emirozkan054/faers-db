"""Tests for file detection logic."""

from pathlib import Path

from faersdb.detect import detect_table_kind, is_data_file, is_data_filename


def test_detect_table_kind():
    assert detect_table_kind(Path("DEMO24Q1.txt")) == "DEMO"
    assert detect_table_kind(Path("DRUG24Q1.txt")) == "DRUG"
    assert detect_table_kind(Path("REAC24Q1.txt")) == "REAC"
    assert detect_table_kind(Path("OUTC24Q1.txt")) == "OUTC"
    assert detect_table_kind(Path("THER24Q1.txt")) == "THER"
    assert detect_table_kind(Path("INDI24Q1.txt")) == "INDI"
    assert detect_table_kind(Path("RPSR24Q1.txt")) == "RPSR"
    assert detect_table_kind(Path("unknown.txt")) is None


def test_is_data_filename_accepts_valid_data_files():
    assert is_data_filename("DEMO24Q1.txt")
    assert is_data_filename("DRUG24Q1.TXT")
    assert is_data_filename("reac24q1.txt")


def test_is_data_filename_rejects_docs():
    assert not is_data_filename("README.txt")
    assert not is_data_filename("FAQ.pdf")
    assert not is_data_filename("SIZE.txt")
    assert not is_data_filename("ASC_NTS.doc")
    assert not is_data_filename("STAT_report.docx")
    assert not is_data_filename("something.xml")


def test_is_data_filename_rejects_unknown_tables():
    assert not is_data_filename("unknown.txt")
    assert not is_data_filename("notes.csv")


def test_ignores_docs():
    # Paths that don't exist on disk are rejected by is_data_file
    assert not is_data_file(Path("README.txt"))
    assert not is_data_file(Path("FAQ.pdf"))
