from faersdb.normalize.drug import normalize_drug
from faersdb.normalize.reac import normalize_reac
from faersdb.normalize.outc import normalize_outc
from faersdb.normalize.ther import normalize_ther
from faersdb.normalize.indi import normalize_indi
from faersdb.normalize.rpsr import normalize_rpsr


def test_normalize_drug_prefers_primaryid_and_maps_fields():
    raw = {
        "PRIMARYID": "123",
        "DRUGNAME": "aspirin",
        "DOSE_AMT": "10",
        "START_DT": "20240131",
    }
    out = normalize_drug(raw, {"source_quarter": "2024q1", "source_system": "FAERS"})
    assert out["source_report_id"] == "123"
    assert out["drugname"] == "aspirin"
    assert str(out["dose_amt"]) == "10"
    assert out["start_dt"].isoformat() == "2024-01-31"


def test_normalize_reac_fallbacks_to_isr_and_reac_pt():
    raw = {"ISR": "456", "REAC_PT": "NAUSEA", "OUTC_COD": "HO"}
    out = normalize_reac(raw, {"source_quarter": "2012q4", "source_system": "FAERS"})
    assert out["source_report_id"] == "456"
    assert out["reaction_pt"] == "NAUSEA"
    assert out["outcome"] == "HO"


def test_normalize_outc_picks_outc_cod_then_outcome_fallback():
    raw1 = {"ISR": "789", "OUTC_COD": "DE"}
    out1 = normalize_outc(raw1, {"source_quarter": "2014q3", "source_system": "FAERS"})
    assert out1["outcome"] == "DE"

    raw2 = {"ISR": "790", "OUTCOME": "LT"}
    out2 = normalize_outc(raw2, {"source_quarter": "2014q3", "source_system": "FAERS"})
    assert out2["outcome"] == "LT"


def test_normalize_ther_maps_duration_and_dates():
    raw = {"ISR": "800", "DRUG_SEQ": "2", "START_DT": "20231201", "END_DT": "20231220", "DUR": "19", "DUR_COD": "DY"}
    out = normalize_ther(raw, {"source_quarter": "2023q4", "source_system": "FAERS"})
    assert out["source_report_id"] == "800"
    assert out["drug_seq"] == 2
    assert out["start_dt"].isoformat() == "2023-12-01"
    assert out["dur"] == 19
    assert out["dur_cod"] == "DY"


def test_normalize_indi_maps_preferred_term_and_fallback():
    raw1 = {"ISR": "801", "DRUG_SEQ": "1", "INDI_PT": "HEADACHE"}
    out1 = normalize_indi(raw1, {"source_quarter": "2023q4", "source_system": "FAERS"})
    assert out1["indi_pt"] == "HEADACHE"

    raw2 = {"ISR": "802", "INDICATION": "MIGRAINE"}
    out2 = normalize_indi(raw2, {"source_quarter": "2023q4", "source_system": "FAERS"})
    assert out2["indi_pt"] == "MIGRAINE"


def test_normalize_rpsr_prefers_rpsr_cod_and_fallback():
    raw1 = {"ISR": "900", "RPSR_COD": "HP"}
    out1 = normalize_rpsr(raw1, {"source_quarter": "2024q1", "source_system": "FAERS"})
    assert out1["reporter_type"] == "HP"

    raw2 = {"ISR": "901", "REPORTER_TYPE": "CONSUMER"}
    out2 = normalize_rpsr(raw2, {"source_quarter": "2024q1", "source_system": "FAERS"})
    assert out2["reporter_type"] == "CONSUMER"
