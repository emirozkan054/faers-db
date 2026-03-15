from datetime import date
from decimal import Decimal, InvalidOperation


def norm_text(v):
    if v is None:
        return None
    s = str(v).strip()
    return s or None


def to_int(v):
    s = norm_text(v)
    if not s:
        return None
    try:
        return int(s)
    except ValueError:
        return None


def to_decimal(v):
    s = norm_text(v)
    if not s:
        return None
    try:
        return Decimal(s)
    except (InvalidOperation, ValueError):
        return None


def norm_sex(v):
    s = norm_text(v)
    if not s:
        return "UNK"

    s_up = s.upper()
    if s_up in {"M", "MALE", "1"}:
        return "M"
    if s_up in {"F", "FEMALE", "2"}:
        return "F"
    return "UNK"


def parse_date_yyyymmdd(v):
    s = norm_text(v)
    if not s or not s.isdigit():
        return None

    # For now, only keep full dates.
    # FAERS can contain partial dates like YYYY or YYYYMM.
    if len(s) != 8:
        return None

    try:
        return date(int(s[:4]), int(s[4:6]), int(s[6:8]))
    except ValueError:
        return None


def normalize_demo(raw: dict, meta: dict) -> dict:
    source_case_id = (
        norm_text(raw.get("CASE_ID"))
        or norm_text(raw.get("CASEID"))
        or norm_text(raw.get("CASE"))
    )

    source_report_id = (
        norm_text(raw.get("PRIMARYID"))
        or norm_text(raw.get("ISR"))
        or norm_text(raw.get("REPORT_ID"))
    )

    wt = to_decimal(raw.get("WT"))
    wt_cod = (norm_text(raw.get("WT_COD")) or "").upper()

    # Keep only KG as normalized weight for now.
    # If later you want, add LB/GMS conversions explicitly.
    weight_kg = wt if wt is not None and wt_cod in {"KG", ""} else None

    return {
        "source_quarter": meta["source_quarter"],
        "source_system": meta["source_system"],
        "schema_era": meta["schema_era"],
        "source_case_id": source_case_id,
        "source_report_id": source_report_id,
        "case_version_num": to_int(raw.get("CASEVERSION")),
        "report_type": norm_text(raw.get("REPT_COD")),
        "initial_or_followup": norm_text(raw.get("I_F_COD") or raw.get("I_F_CODE")),
        "event_dt": parse_date_yyyymmdd(raw.get("EVENT_DT")),
        "mfr_dt": parse_date_yyyymmdd(raw.get("MFR_DT")),
        "fda_dt": parse_date_yyyymmdd(raw.get("FDA_DT")),
        "age_value": to_decimal(raw.get("AGE")),
        "age_unit": norm_text(raw.get("AGE_COD")),
        "age_group": norm_text(raw.get("AGE_GRP")),
        "sex_std": norm_sex(raw.get("SEX") or raw.get("GNDR_COD")),
        "weight_kg": weight_kg,
        "reporter_country": norm_text(raw.get("REPORTER_COUNTRY")),
        "auth_num": norm_text(raw.get("AUTH_NUM")),
        "lit_ref": norm_text(raw.get("LIT_REF")),
        "raw_demo": raw,
    }