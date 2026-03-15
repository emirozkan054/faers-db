from pathlib import Path

TABLE_KINDS = ["DEMO", "DRUG", "REAC", "OUTC", "RPSR", "THER", "INDI"]


def detect_table_kind(path: Path) -> str | None:
    name = path.name.upper()
    for kind in TABLE_KINDS:
        if kind in name:
            return kind
    return None


def is_data_file(path: Path) -> bool:
    if not path.is_file():
        return False

    name = path.name.upper()

    # Ignore documentation / metadata files
    ignored_prefixes = ("README", "FAQ", "SIZE", "ASC_NTS", "STAT")
    ignored_suffixes = (".PDF", ".DOC", ".DOCX", ".XML")

    if name.startswith(ignored_prefixes):
        return False
    if name.endswith(ignored_suffixes):
        return False

    return detect_table_kind(path) is not None


def discover_files(folder: Path) -> list[tuple[str, Path]]:
    """
    Discover actual FAERS/AERS ASCII table files under a quarter folder.
    Typically they live in folder/ascii/.
    """
    out: list[tuple[str, Path]] = []

    # Prefer the ascii subdirectory if present
    ascii_dir = folder / "ascii"
    search_root = ascii_dir if ascii_dir.exists() and ascii_dir.is_dir() else folder

    for p in sorted(search_root.rglob("*")):
        if is_data_file(p):
            kind = detect_table_kind(p)
            if kind:
                out.append((kind, p))

    return out