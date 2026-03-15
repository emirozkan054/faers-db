import hashlib
import polars as pl
from pathlib import Path

def hash_row(row: dict) -> str:
    payload = "|".join("" if v is None else str(v) for _, v in sorted(row.items()))
    return hashlib.sha256(payload.encode("utf-8", errors="ignore")).hexdigest()

def load_text_file(path: Path) -> pl.DataFrame:
    # Let the file itself define the header/delimiter assumptions you validate during testing.
    # Start conservative, then lock parser settings once your first quarter loads succeed.
    return pl.read_csv(
        path,
        infer_schema_length=1000,
        ignore_errors=True,
        null_values=["", "NULL", "null"],
        truncate_ragged_lines=True,
    )

def dataframe_to_raw_records(df: pl.DataFrame) -> list[dict]:
    cols = [c.upper().strip() for c in df.columns]
    df.columns = cols
    records = []
    for i, row in enumerate(df.iter_rows(named=True), start=1):
        cleaned = {
            k: (None if v == "" else v)
            for k, v in row.items()
        }
        records.append({
            "row_num": i,
            "raw_record": cleaned,
            "row_hash": hash_row(cleaned),
        })
    return records