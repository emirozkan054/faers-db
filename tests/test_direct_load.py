import pytest
from psycopg import connect

from faersdb.config import settings
from faersdb.direct_load import (
    LoadedTempTable,
    build_temp_columns,
    copy_ascii_file_to_temp,
    first_present_expr,
    row_hash_expr,
)


def test_build_temp_columns_cleans_and_deduplicates_names():
    columns = build_temp_columns(["\ufeffprimaryid", "case id", "case id", "1bad"])

    assert [column.source_name for column in columns] == [
        "PRIMARYID",
        "CASE ID",
        "CASE ID",
        "1BAD",
    ]
    assert [column.sql_name for column in columns] == [
        "primaryid",
        "case_id",
        "case_id_2",
        "c_1bad",
    ]


def test_temp_column_expressions_use_cleaned_columns():
    table = LoadedTempTable(
        "_tmp",
        build_temp_columns(["primaryid", "isr", "drugname"]),
        0,
    )

    assert first_present_expr(table, ("PRIMARYID", "REPORT_ID")) == (
        'NULLIF(BTRIM(t."primaryid"), \'\')'
    )
    assert "JSONB_BUILD_ARRAY" in row_hash_expr(table)
    assert 't."drugname"' in row_hash_expr(table)


def test_copy_ascii_file_to_temp_handles_ragged_rows_and_empty_values(tmp_path):
    sample = tmp_path / "DRUG25Q4.txt"
    sample.write_text(
        "\ufeffprimaryid$caseid$drugname$route\r\n"
        "1001$10$A\\B$\r\n"
        "1002$20$$ORAL$\r\n"
        "1003$30\r\n",
        encoding="utf-8",
    )

    try:
        conn = connect(settings.pg_dsn)
    except Exception as exc:
        pytest.skip(f"PostgreSQL not available for COPY test: {exc}")

    try:
        with conn.cursor() as cur:
            loaded = copy_ascii_file_to_temp(cur, sample, "_tmp_copy_test", copy_chunk_mb=1)
            cur.execute("select * from _tmp_copy_test order by primaryid")
            rows = cur.fetchall()
        conn.rollback()
    finally:
        conn.close()

    assert loaded.rows_loaded == 3
    assert rows == [
        ("1001", "10", "A\\B", None),
        ("1002", "20", None, "ORAL"),
        ("1003", "30", None, None),
    ]
