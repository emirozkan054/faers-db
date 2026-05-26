"""Tests for file detection logic."""

from pathlib import Path

from faersdb.detect import detect_table_kind, is_data_file


def test_detect_table_kind():
    assert detect_table_kind(Path("DEMO24Q1.txt")) == "DEMO"
    assert detect_table_kind(Path("DRUG24Q1.txt")) == "DRUG"
    assert detect_table_kind(Path("REAC24Q1.txt")) == "REAC"
    assert detect_table_kind(Path("OUTC24Q1.txt")) == "OUTC"
    assert detect_table_kind(Path("THER24Q1.txt")) == "THER"
    assert detect_table_kind(Path("INDI24Q1.txt")) == "INDI"
    assert detect_table_kind(Path("RPSR24Q1.txt")) == "RPSR"
    assert detect_table_kind(Path("unknown.txt")) is None


def test_ignores_docs():
    assert not is_data_file(Path("README.txt"))
    assert not is_data_file(Path("FAQ.pdf"))
