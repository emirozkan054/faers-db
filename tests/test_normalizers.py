from faersdb.normalize.drug import normalize_drug
from faersdb.normalize.reac import normalize_reac
from faersdb.normalize.outc import normalize_outc


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
