from faersdb.normalize.demo import norm_text


def normalize_outc(raw: dict, meta: dict) -> dict:
    source_report_id = (
        norm_text(raw.get("PRIMARYID"))
        or norm_text(raw.get("ISR"))
        or norm_text(raw.get("REPORT_ID"))
    )

    return {
        "source_quarter": meta["source_quarter"],
        "source_system": meta["source_system"],
        "source_report_id": source_report_id,
        "outcome": norm_text(raw.get("OUTC_COD")) or norm_text(raw.get("OUTCOME")),
        "raw_outc": raw,
    }
