from pathlib import Path

from faersdb.detect import detect_table_kind, is_data_file
from faersdb.manifest import discover_quarters


def test_detect_table_kind_recognizes_delete_files():
    assert detect_table_kind(Path("DELETE25Q4.txt")) == "DELETE"


def test_is_data_file_keeps_delete_text_files():
    assert is_data_file(Path("data/faers/faers_ascii_2025Q4/Deleted/DELETE25Q4.txt"))


def test_discover_quarters_sorts_chronologically(tmp_path):
    for folder_name in [
        "faers_ascii_2025q4",
        "faers_ascii_2024Q4",
        "aers_ascii_2004q1",
        "faers_ascii_2025q1",
    ]:
        (tmp_path / folder_name).mkdir()

    quarters = discover_quarters(tmp_path)

    assert [row["source_quarter"] for row in quarters] == [
        "2004q1",
        "2024q4",
        "2025q1",
        "2025q4",
    ]
