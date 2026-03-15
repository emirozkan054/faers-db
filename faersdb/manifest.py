from pathlib import Path
import re

QTR_RE = re.compile(r"^(aers|faers)_ascii_(\d{4})[qQ](\d)$")

def detect_schema_era(system: str, year: int, qtr: int) -> str:
    if system == "AERS":
        return "legacy_aers"
    if (year, qtr) < (2014, 3):
        return "faers_2012q4_2014q2"
    return "faers_2014q3_plus"

def discover_quarters(root: Path) -> list[dict]:
    rows = []
    for p in sorted(root.iterdir()):
        if not p.is_dir():
            continue
        m = QTR_RE.match(p.name)
        if not m:
            continue
        system, year, qtr = m.group(1).upper(), int(m.group(2)), int(m.group(3))
        rows.append({
            "folder_name": p.name,
            "folder_path": str(p),
            "source_system": system,
            "source_year": year,
            "source_qtr": qtr,
            "source_quarter": f"{year}q{qtr}",
            "schema_era": detect_schema_era(system, year, qtr),
        })
    return rows