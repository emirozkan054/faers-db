import time
import csv
from pathlib import Path
import polars as pl

def clean_colname(name: str) -> str:
    return name.replace("\ufeff", "").replace("ï»¿", "").strip().upper()

def clean_value(value: str | None) -> str | None:
    if value is None:
        return None
    s = str(value).strip()
    return s if s != "" else None

file_path = "data/faers/aers_ascii_2004q1/ascii/DEMO04Q1.TXT"

def test_csv():
    t0 = time.time()
    count = 0
    with open(file_path, "r", encoding="utf-8-sig", errors="replace", newline="") as f:
        reader = csv.reader(f, delimiter="$")
        raw_header = next(reader)
        header = [clean_colname(col) for col in raw_header]
        for row_num, row in enumerate(reader, start=1):
            if not row or all((x.strip() == "" for x in row)):
                continue
            if len(row) == len(header) + 1 and row[-1] == "":
                row = row[:-1]
            if len(row) < len(header):
                row = row + [""] * (len(header) - len(row))
            elif len(row) > len(header):
                row = row[: len(header)]
            record = {header[i]: clean_value(row[i]) for i in range(len(header))}
            count += 1
    t1 = time.time()
    print(f"CSV: {count} rows in {t1-t0:.2f}s")

def test_polars():
    t0 = time.time()
    df = pl.read_csv(
        file_path, 
        separator="$", 
        ignore_errors=True, 
        infer_schema_length=0,
        null_values=[""],
        truncate_ragged_lines=True
    )
    df = df.rename({c: clean_colname(c) for c in df.columns})
    df = df.with_columns(pl.all().str.strip_chars())
    
    count = 0
    for row in df.iter_rows(named=True):
        count += 1
    t1 = time.time()
    print(f"Polars: {count} rows in {t1-t0:.2f}s")

if __name__ == "__main__":
    if Path(file_path).exists():
        test_csv()
        test_polars()
    else:
        print("File not found")
