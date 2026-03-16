from faersdb.normalize.demo import norm_text, parse_date_yyyymmdd, to_decimal


def normalize_drug(raw: dict, meta: dict) -> dict:
    source_report_id = (
        norm_text(raw.get("PRIMARYID"))
        or norm_text(raw.get("ISR"))
        or norm_text(raw.get("REPORT_ID"))
    )

    return {
        "source_quarter": meta["source_quarter"],
        "source_system": meta["source_system"],
        "source_report_id": source_report_id,
        "role_cod": norm_text(raw.get("ROLE_COD")),
        "drugname": norm_text(raw.get("DRUGNAME")),
        "prod_ai": norm_text(raw.get("PROD_AI")),
        "route": norm_text(raw.get("ROUTE")),
        "dose_vbm": norm_text(raw.get("DOSE_VBM")),
        "dose_amt": to_decimal(raw.get("DOSE_AMT")),
        "dose_unit": norm_text(raw.get("DOSE_UNIT")),
        "start_dt": parse_date_yyyymmdd(raw.get("START_DT")),
        "end_dt": parse_date_yyyymmdd(raw.get("END_DT")),
        "raw_drug": raw,
    }
