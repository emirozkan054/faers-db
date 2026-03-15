from datetime import date
from decimal import Decimal

def norm_text(v):
    if v is None:
        return None
    s = str(v).strip()
    return s or None

def norm_sex(v):
    s = norm_text(v)
    if s in {"M", "MALE", "1"}:
        return "M"
    if s in {"F", "FEMALE", "2"}:
        return "F"
    return "UNK"

def parse_date_yyyymmdd(v):
    s = norm_text(v)
    if not s or len(s) != 8 or not s.isdigit():
        return None
    return date(int(s[:4]), int(s[4:6]), int(s[6:8]))

def normalize_demo(raw: dict, meta: dict) -> dict:
    source_case_id = norm_text(raw.get("CASE_ID")) or norm_text(raw.get("CASE"))
    source_report_id = (
        norm_text(raw.get("PRIMARYID")) or
        norm_text(raw.get("ISR")) or
        norm_text(raw.get("REPORT_ID"))
    )

    return {
        "source_quarter": meta["source_quarter"],
        "source_system": meta["source_system"],
        "schema_era": meta["schema_era"],
        "source_case_id": source_case_id,
        "source_report_id": source_report_id,
        "case_version_num": raw.get("CASEVERSION"),
        "report_type": norm_text(raw.get("TYPE")),
        "initial_or_followup": norm_text(raw.get("I_F_COD")),
        "event_dt": parse_date_yyyymmdd(raw.get("EVENT_DT")),
        "mfr_dt": parse_date_yyyymmdd(raw.get("MFR_DT")),
        "fda_dt": parse_date_yyyymmdd(raw.get("FDA_DT")),
        "age_value": raw.get("AGE"),
        "age_unit": norm_text(raw.get("AGE_COD")),
        "age_group": norm_text(raw.get("AGE_GRP")),
        "sex_std": norm_sex(raw.get("SEX") or raw.get("GNDR_COD")),
        "weight_kg": raw.get("WT"),
        "reporter_country": norm_text(raw.get("REPORTER_COUNTRY")),
        "auth_num": norm_text(raw.get("AUTH_NUM")),
        "lit_ref": norm_text(raw.get("LIT_REF")),
        "raw_demo": raw,
    }