from pathlib import Path

TABLE_KINDS = ["DEMO", "DRUG", "REAC", "OUTC", "RPSR", "THER", "INDI"]

def detect_table_kind(path: Path) -> str | None:
    name = path.name.upper()
    for kind in TABLE_KINDS:
        if kind in name:
            return kind
    return None

def discover_files(folder: Path) -> list[tuple[str, Path]]:
    out = []
    for p in sorted(folder.iterdir()):
        if not p.is_file():
            continue
        kind = detect_table_kind(p)
        if kind:
            out.append((kind, p))
    return out