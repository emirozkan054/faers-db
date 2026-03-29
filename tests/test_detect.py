from pathlib import Path

from faersdb.detect import detect_table_kind, is_data_file


def test_detect_table_kind_recognizes_delete_files():
    assert detect_table_kind(Path("DELETE25Q4.txt")) == "DELETE"


def test_is_data_file_keeps_delete_text_files():
    assert is_data_file(Path("data/faers/faers_ascii_2025Q4/Deleted/DELETE25Q4.txt"))
