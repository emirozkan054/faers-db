from faersdb.normalize.demo import norm_text, parse_date_yyyymmdd, to_int


def normalize_ther(raw: dict, meta: dict) -> dict:
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
        "start_dt": parse_date_yyyymmdd(raw.get("START_DT")),
        "end_dt": parse_date_yyyymmdd(raw.get("END_DT")),
        "dur": to_int(raw.get("DUR")),
        "dur_cod": norm_text(raw.get("DUR_COD")),
        "raw_ther": raw,
    }
