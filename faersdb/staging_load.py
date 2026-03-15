import csv
import hashlib
from pathlib import Path

import orjson
from psycopg.types.json import Jsonb


BATCH_SIZE = 5000


def clean_colname(name: str) -> str:
    return name.replace("\ufeff", "").replace("ï»¿", "").strip().upper()


def clean_value(value: str | None) -> str | None:
    if value is None:
        return None
    s = str(value).strip()
    return s if s != "" else None


def stable_row_hash(record: dict) -> str:
    payload = orjson.dumps(record, option=orjson.OPT_SORT_KEYS)
    return hashlib.sha256(payload).hexdigest()


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


def insert_demo_raw_rows(conn, source_file_id: int, file_path: Path) -> int:
    total = 0
    batch: list[tuple] = []

    sql = """
        insert into staging.demo_raw (
            source_file_id,
            row_num,
            raw_record,
            row_hash
        )
        values (%s, %s, %s, %s)
        on conflict (source_file_id, row_num) do nothing
    """

    with conn.cursor() as cur:
        for row_num, record in iter_delimited_records(file_path):
            batch.append(
                (
                    source_file_id,
                    row_num,
                    Jsonb(record),
                    stable_row_hash(record),
                )
            )

            if len(batch) >= BATCH_SIZE:
                cur.executemany(sql, batch)
                total += len(batch)
                batch.clear()

        if batch:
            cur.executemany(sql, batch)
            total += len(batch)

    conn.commit()
    return total