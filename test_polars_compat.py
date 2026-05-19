import polars as pl
from pathlib import Path
from faersdb.staging_load import iter_delimited_records as old_iter

def clean_colname(name: str) -> str:
    return name.replace("\ufeff", "").replace("ï»¿", "").strip().upper()

def new_iter_delimited_records(file_path: Path, delimiter: str = "$"):
    df = pl.read_csv(
        file_path,
        separator=delimiter,
        ignore_errors=True,
        infer_schema_length=0,
        null_values=[""],
        truncate_ragged_lines=True,
        quote_char=None,  # FAERS data usually doesn't quote fields with standard quotes properly when using $
    )
    df = df.rename({c: clean_colname(c) for c in df.columns})
    df = df.with_columns(pl.all().str.strip_chars())
    
    # Polars read_csv might produce nulls for empty fields.
    # Convert empty strings to null just in case string_chars left some
    df = df.with_columns(
        pl.when(pl.col(pl.Utf8) == "").then(None).otherwise(pl.col(pl.Utf8)).name.keep()
    )
    
    for row_num, row in enumerate(df.iter_rows(named=True), start=1):
        yield row_num, row

def test():
    file_path = Path("data/faers/aers_ascii_2004q1/ascii/DEMO04Q1.TXT")
    old = list(old_iter(file_path))
    new = list(new_iter_delimited_records(file_path))
    
    assert len(old) == len(new), f"Lengths differ: {len(old)} vs {len(new)}"
    for i in range(10):
        # old uses `clean_value` which returns None for empty strings
        assert old[i] == new[i], f"Mismatch at {i}:\nold: {old[i]}\nnew: {new[i]}"
    print("Compatibility test passed!")

if __name__ == "__main__":
    if Path("data/faers/aers_ascii_2004q1/ascii/DEMO04Q1.TXT").exists():
        test()
