from faersdb.normalize.demo import norm_text, to_int


def normalize_indi(raw: dict, meta: dict) -> dict:
    source_report_id = (
        norm_text(raw.get("PRIMARYID"))
        or norm_text(raw.get("ISR"))
        or norm_text(raw.get("REPORT_ID"))
    )

    return {
        "source_quarter": meta["source_quarter"],
        "source_system": meta["source_system"],
        "source_report_id": source_report_id,
        "drug_seq": to_int(raw.get("DRUG_SEQ")),
        "indi_pt": norm_text(raw.get("INDI_PT")) or norm_text(raw.get("INDICATION")),
        "raw_indi": raw,
    }
