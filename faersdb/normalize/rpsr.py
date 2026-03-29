from faersdb.normalize.demo import norm_text


def normalize_rpsr(raw: dict, meta: dict) -> dict:
    source_report_id = (
        norm_text(raw.get("PRIMARYID"))
        or norm_text(raw.get("ISR"))
        or norm_text(raw.get("REPORT_ID"))
    )

    return {
        "source_quarter": meta["source_quarter"],
        "source_system": meta["source_system"],
        "source_report_id": source_report_id,
        "reporter_type": norm_text(raw.get("RPSR_COD")) or norm_text(raw.get("REPORTER_TYPE")),
        "raw_rpsr": raw,
    }
