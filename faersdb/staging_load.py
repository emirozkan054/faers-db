import csv
import hashlib
import time
from pathlib import Path

import orjson
from psycopg.types.json import Jsonb

def clean_colname(name: str) -> str:
    return name.replace("\ufeff", "").replace("ï»¿", "").strip().upper()


def clean_value(value: str | None) -> str | None:
    if value is None:
        return None
    s = str(value).strip()
    return s if s != "" else None

def iter_delimited_records(file_path: Path, delimiter: str = "$"):
    with open(file_path, "r", encoding="utf-8-sig", errors="replace", newline="") as f:
        reader = csv.reader(f, delimiter=delimiter)

        try:
            raw_header = next(reader)
        except StopIteration:
            return

        header = [clean_colname(col) for col in raw_header]

        for row_num, row in enumerate(reader, start=1):
            if not row or all((x.strip() == "" for x in row)):
                continue

            # Common FAERS/AERS quirk: trailing delimiter adds one empty column
            if len(row) == len(header) + 1 and row[-1] == "":
                row = row[:-1]

            if len(row) < len(header):
                row = row + [""] * (len(header) - len(row))
            elif len(row) > len(header):
                # Keep only expected columns for now
                row = row[: len(header)]

            record = {
                header[i]: clean_value(row[i])
                for i in range(len(header))
            }

            yield row_num, record


def iter_delete_records(file_path: Path):
    with open(file_path, "r", encoding="utf-8-sig", errors="replace", newline="") as f:
        for row_num, line in enumerate(f, start=1):
            source_report_id = clean_value(line)
            if not source_report_id:
                continue

            yield row_num, source_report_id


def fast_row_hash(record: dict[str, str | None]) -> str:
    payload = orjson.dumps(record, option=orjson.OPT_SORT_KEYS)
    return hashlib.md5(payload).hexdigest()


def insert_raw_rows(
    conn,
    source_file_id: int,
    file_path: Path,
    table_name: str,
    append_only: bool = False,
) -> int:
    started_at = time.perf_counter()

    with conn.cursor() as cur:
        cur.execute("set local synchronous_commit = off")
        if append_only:
            total = 0
            with cur.copy(
                f"copy {table_name} (source_file_id, row_num, raw_record, row_hash) from stdin"
            ) as copy:
                for row_num, record in iter_delimited_records(file_path):
                    copy.write_row(
                        (
                            source_file_id,
                            row_num,
                            Jsonb(record),
                            fast_row_hash(record),
                        )
                    )
                    total += 1

            conn.commit()
            elapsed = time.perf_counter() - started_at
            print(f"  staged {table_name} in {elapsed:.2f}s")
            return total

        cur.execute(
            """
            create temporary table tmp_raw_load (
                source_file_id bigint,
                row_num bigint,
                raw_record jsonb
            ) on commit drop
            """
        )

        with cur.copy("copy tmp_raw_load (source_file_id, row_num, raw_record) from stdin") as copy:
            for row_num, record in iter_delimited_records(file_path):
                copy.write_row(
                    (
                        source_file_id,
                        row_num,
                        Jsonb(record),
                    )
                )

        insert_sql = f"""
            insert into {table_name} (
                source_file_id,
                row_num,
                raw_record,
                row_hash
            )
            select
                source_file_id,
                row_num,
                raw_record,
                md5(raw_record::text) as row_hash
            from tmp_raw_load
        """
        if not append_only:
            insert_sql += "\non conflict (source_file_id, row_num) do nothing"

        cur.execute(insert_sql)
        total = cur.rowcount

    conn.commit()
    elapsed = time.perf_counter() - started_at
    print(f"  staged {table_name} in {elapsed:.2f}s")
    return total


def insert_delete_raw_rows(conn, source_file_id: int, file_path: Path, append_only: bool = False) -> int:
    started_at = time.perf_counter()

    with conn.cursor() as cur:
        cur.execute("set local synchronous_commit = off")
        if append_only:
            total = 0
            with cur.copy(
                "copy staging.delete_raw (source_file_id, row_num, source_report_id, row_hash) from stdin"
            ) as copy:
                for row_num, source_report_id in iter_delete_records(file_path):
                    copy.write_row(
                        (
                            source_file_id,
                            row_num,
                            source_report_id,
                            hashlib.md5(source_report_id.encode("utf-8")).hexdigest(),
                        )
                    )
                    total += 1

            conn.commit()
            elapsed = time.perf_counter() - started_at
            print(f"  staged staging.delete_raw in {elapsed:.2f}s")
            return total

        cur.execute(
            """
            create temporary table tmp_delete_load (
                source_file_id bigint,
                row_num bigint,
                source_report_id text
            ) on commit drop
            """
        )

        with cur.copy("copy tmp_delete_load (source_file_id, row_num, source_report_id) from stdin") as copy:
            for row_num, source_report_id in iter_delete_records(file_path):
                copy.write_row(
                    (
                        source_file_id,
                        row_num,
                        source_report_id,
                    )
                )

        insert_sql = """
            insert into staging.delete_raw (
                source_file_id,
                row_num,
                source_report_id,
                row_hash
            )
            select
                source_file_id,
                row_num,
                source_report_id,
                md5(source_report_id)
            from tmp_delete_load
        """
        if not append_only:
            insert_sql += "\non conflict (source_file_id, row_num) do nothing"

        cur.execute(insert_sql)
        total = cur.rowcount

    conn.commit()
    elapsed = time.perf_counter() - started_at
    print(f"  staged staging.delete_raw in {elapsed:.2f}s")
    return total

def insert_demo_raw_rows(conn, source_file_id: int, file_path: Path, append_only: bool = False) -> int:
    return insert_raw_rows(conn, source_file_id, file_path, "staging.demo_raw", append_only=append_only)


def insert_drug_raw_rows(conn, source_file_id: int, file_path: Path, append_only: bool = False) -> int:
    return insert_raw_rows(conn, source_file_id, file_path, "staging.drug_raw", append_only=append_only)


def insert_reac_raw_rows(conn, source_file_id: int, file_path: Path, append_only: bool = False) -> int:
    return insert_raw_rows(conn, source_file_id, file_path, "staging.reac_raw", append_only=append_only)


def insert_outc_raw_rows(conn, source_file_id: int, file_path: Path, append_only: bool = False) -> int:
    return insert_raw_rows(conn, source_file_id, file_path, "staging.outc_raw", append_only=append_only)


def insert_ther_raw_rows(conn, source_file_id: int, file_path: Path, append_only: bool = False) -> int:
    return insert_raw_rows(conn, source_file_id, file_path, "staging.ther_raw", append_only=append_only)


def insert_indi_raw_rows(conn, source_file_id: int, file_path: Path, append_only: bool = False) -> int:
    return insert_raw_rows(conn, source_file_id, file_path, "staging.indi_raw", append_only=append_only)


def insert_rpsr_raw_rows(conn, source_file_id: int, file_path: Path, append_only: bool = False) -> int:
    return insert_raw_rows(conn, source_file_id, file_path, "staging.rpsr_raw", append_only=append_only)
