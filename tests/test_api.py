"""Tests for the API endpoints using the DuckDB+Parquet backend."""
from datetime import date

import polars as pl
import pytest
from fastapi.testclient import TestClient

from faersdb.api import app
from faersdb.config import settings
from faersdb.etl import build_query_tables

client = TestClient(app)


def search_payload(**overrides):
    payload = {
        "drug_terms": [],
        "reaction_terms": [],
        "concept_mode": "any",
        "case_filters": {},
        "limit": 25,
        "offset": 0,
    }
    payload.update(overrides)
    return payload


def drug_search_payload(**drug_term):
    overrides = {}
    for key in ("limit", "offset", "concept_mode", "case_filters", "reaction_terms"):
        if key in drug_term:
            overrides[key] = drug_term.pop(key)
    return search_payload(drug_terms=[drug_term], **overrides)


@pytest.fixture(autouse=True)
def sample_warehouse(tmp_path, monkeypatch):
    warehouse = tmp_path / "warehouse"
    warehouse.mkdir()
    monkeypatch.setattr(settings, "warehouse_dir", str(warehouse))

    pl.DataFrame(
        {
            "primaryid": ["1001", "1002", "1003", "1004"],
            "caseid": ["2001", "2002", "2003", "2004"],
            "source_quarter": ["2024q1", "2024q2", "2024q2", "2024q3"],
            "source_system": ["FAERS", "FAERS", "FAERS", "FAERS"],
            "caseversion": [1, 1, 1, 1],
            "report_type": ["EXP", "DIR", "PER", "EXP"],
            "i_f_code": ["I", "F", "I", "F"],
            "event_dt": [
                date(2024, 1, 10),
                date(2024, 2, 10),
                date(2024, 3, 10),
                date(2024, 4, 10),
            ],
            "mfr_dt": [
                date(2024, 1, 11),
                date(2024, 2, 11),
                date(2024, 3, 11),
                date(2024, 4, 11),
            ],
            "fda_dt": [
                date(2024, 1, 15),
                date(2024, 2, 15),
                date(2024, 3, 15),
                date(2024, 4, 15),
            ],
            "age": [45.0, 30.0, 60.0, 52.0],
            "age_cod": ["YR", "YR", "YR", "YR"],
            "age_grp": ["A", "A", "E", "A"],
            "sex": ["M", "F", "F", "F"],
            "wt_kg": [80.0, 60.0, 70.0, 65.0],
            "reporter_country": ["US", "JP", "US", "US"],
            "auth_num": [None, None, None, None],
            "lit_ref": [None, None, None, None],
            "is_deleted": [False, False, False, False],
        }
    ).write_parquet(warehouse / "demo.parquet")

    pl.DataFrame(
        {
            "primaryid": ["1001", "1002", "1003", "1004", "1004"],
            "source_quarter": ["2024q1", "2024q2", "2024q2", "2024q3", "2024q3"],
            "drug_seq": [1, 1, 1, 1, 2],
            "role_cod": ["PS", "PS", "SS", "PS", "C"],
            "drugname": ["ASPIRIN", "ASPIRIN", "IBUPROFEN", "ASPIRIN", "CODEINE"],
            "prod_ai": [
                "ACETYLSALICYLIC ACID",
                "ASPIRIN",
                "IBUPROFEN",
                "ASPIRIN",
                "CODEINE",
            ],
            "route": ["ORAL", "ORAL", "ORAL", "ORAL", "ORAL"],
            "dose_vbm": [None, None, None, None, None],
            "dose_amt": [100.0, 81.0, 200.0, 325.0, 30.0],
            "dose_unit": ["MG", "MG", "MG", "MG", "MG"],
            "start_dt": [
                date(2024, 1, 1),
                date(2024, 2, 1),
                date(2024, 3, 1),
                date(2024, 4, 1),
                date(2024, 4, 2),
            ],
            "end_dt": [None, None, None, None, None],
        }
    ).write_parquet(warehouse / "drug.parquet")

    pl.DataFrame(
        {
            "primaryid": ["1001", "1002", "1003", "1004"],
            "source_quarter": ["2024q1", "2024q2", "2024q2", "2024q3"],
            "pt": ["HEADACHE", "NAUSEA", "DIZZINESS", "RASH"],
            "drug_rec_act": ["UNK", "UNK", "UNK", "UNK"],
        }
    ).write_parquet(warehouse / "reac.parquet")

    pl.DataFrame(
        {
            "primaryid": ["1001", "1002", "1004"],
            "source_quarter": ["2024q1", "2024q2", "2024q3"],
            "outc_cod": ["HO", "OT", "HO"],
        }
    ).write_parquet(warehouse / "outc.parquet")

    pl.DataFrame(
        {
            "primaryid": ["1001", "1002", "1004", "1004"],
            "source_quarter": ["2024q1", "2024q2", "2024q3", "2024q3"],
            "drug_seq": [1, 1, 1, 2],
            "indi_pt": ["PAIN", "FEVER", "PRODUCT USED FOR UNKNOWN INDICATION", "PAIN"],
        }
    ).write_parquet(warehouse / "indi.parquet")

    pl.DataFrame(
        {
            "primaryid": ["1001", "1002", "1004", "1004"],
            "source_quarter": ["2024q1", "2024q2", "2024q3", "2024q3"],
            "drug_seq": [1, 1, 1, 2],
            "start_dt": [
                date(2024, 1, 1),
                date(2024, 2, 1),
                date(2024, 4, 1),
                date(2024, 4, 2),
            ],
            "end_dt": [None, None, None, None],
            "dur": [10, 5, 14, 3],
            "dur_cod": ["DY", "DY", "DY", "DY"],
        }
    ).write_parquet(warehouse / "ther.parquet")

    pl.DataFrame(
        {
            "primaryid": ["1001", "1002", "1004"],
            "source_quarter": ["2024q1", "2024q2", "2024q3"],
            "rpsr_cod": ["HP", "CN", "HP"],
        }
    ).write_parquet(warehouse / "rpsr.parquet")

    build_query_tables(warehouse, memory_limit="256MB", threads=1)


def test_root():
    response = client.get("/")
    assert response.status_code == 200
    data = response.json()
    assert data["name"] == "faers-db"
    assert "/app" in data["app"]
    assert "/docs" in data["docs"]


def test_health():
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json()["status"] == "ok"


def test_app_shell():
    response = client.get("/app")
    assert response.status_code == 200
    assert "text/html" in response.headers["content-type"]


def test_filter_metadata():
    response = client.get("/filters/metadata")
    assert response.status_code == 200
    data = response.json()
    assert "quarters" in data
    assert "sex_values" in data
    assert "report_types" in data
    assert "role_codes" in data
    assert "routes" in data
    assert "dose_units" in data
    assert "reaction_outcomes" not in data
    assert "case_outcomes" in data
    assert "reporter_types" in data
    assert "dur_codes" in data
    assert isinstance(data["quarters"], list)


def test_search_cases_requires_filter():
    response = client.post("/cases/search", json=search_payload())
    assert response.status_code == 422


def test_search_cases_with_drug():
    response = client.post(
        "/cases/search",
        json=drug_search_payload(drug_name="aspirin", limit=3),
    )
    assert response.status_code == 200
    data = response.json()
    assert "total" in data
    assert "items" in data
    assert data["limit"] == 3
    assert data["offset"] == 0
    assert len(data["items"]) <= 3

    if data["items"]:
        item = data["items"][0]
        assert "case_version_pk" in item
        assert "canonical_case_id" in item
        assert "source_report_id" in item
        assert "source_quarter" in item
        assert "drugs" in item
        assert "reactions" in item
        assert "outcomes" in item
        assert "active_ingredients" in item
        assert isinstance(item["drugs"], list)
        assert isinstance(item["reactions"], list)


def test_search_cases_with_drug_and_reaction_concepts():
    response = client.post(
        "/cases/search",
        json=search_payload(
            drug_terms=[{"drug_name": "aspirin"}],
            reaction_terms=[{"reaction_pt": "headache"}],
            concept_mode="all",
            limit=3,
        ),
    )
    assert response.status_code == 200
    data = response.json()
    assert data["total"] == 1
    assert data["items"][0]["case_version_pk"] == "1001"
    assert data["items"][0]["drugs"] == ["ASPIRIN"]
    assert data["items"][0]["reactions"] == ["HEADACHE"]


def test_drug_and_indication_match_same_drug_row():
    response = client.post(
        "/cases/search",
        json=drug_search_payload(drug_name="aspirin", indication_pt="pain", limit=10),
    )
    assert response.status_code == 200
    data = response.json()

    assert data["total"] == 1
    assert data["items"][0]["case_version_pk"] == "1001"


def test_drug_attribute_stays_inside_drug_concept():
    response = client.post(
        "/cases/search",
        json=drug_search_payload(drug_name="codeine", role_cod="PS", limit=10),
    )
    assert response.status_code == 200
    assert response.json()["total"] == 0


def test_reaction_concept_matches_case_level_reaction():
    response = client.post(
        "/cases/search",
        json=search_payload(reaction_terms=[{"reaction_pt": "headache"}], limit=10),
    )
    assert response.status_code == 200
    data = response.json()

    assert data["total"] == 1
    assert data["items"][0]["case_version_pk"] == "1001"


def test_like_wildcards_are_escaped():
    response = client.post(
        "/cases/search",
        json=drug_search_payload(drug_name="ASP_RIN", limit=10),
    )
    assert response.status_code == 200
    assert response.json()["total"] == 0


def test_concept_any_mode_returns_union():
    response = client.post(
        "/cases/search",
        json=search_payload(
            drug_terms=[
                {"prod_ai": "aspirin"},
                {"drug_name": "aspirin", "indication_pt": "fever"},
            ],
            concept_mode="any",
            limit=10,
        ),
    )
    assert response.status_code == 200
    data = response.json()

    assert data["total"] == 2
    assert {item["case_version_pk"] for item in data["items"]} == {"1002", "1004"}


def test_concept_all_mode_requires_every_concept():
    response = client.post(
        "/cases/search",
        json=search_payload(
            drug_terms=[
                {"prod_ai": "aspirin"},
                {"drug_name": "aspirin", "indication_pt": "fever"},
            ],
            concept_mode="all",
            limit=10,
        ),
    )
    assert response.status_code == 200
    data = response.json()

    assert data["total"] == 1
    assert data["items"][0]["case_version_pk"] == "1002"


def test_search_cases_pagination():
    response1 = client.post(
        "/cases/search",
        json=drug_search_payload(drug_name="aspirin", limit=2, offset=0),
    )
    response2 = client.post(
        "/cases/search",
        json=drug_search_payload(drug_name="aspirin", limit=2, offset=2),
    )
    assert response1.status_code == 200
    assert response2.status_code == 200
    data1 = response1.json()
    data2 = response2.json()
    # Same total
    assert data1["total"] == data2["total"]
    # Different items (unless < 4 total results)
    if data1["total"] > 2 and data1["items"] and data2["items"]:
        assert data1["items"][0]["case_version_pk"] != data2["items"][0]["case_version_pk"]


def test_export_cases_returns_all_matches():
    response = client.post(
        "/cases/export",
        json=drug_search_payload(drug_name="aspirin", limit=1, offset=1),
    )
    assert response.status_code == 200
    data = response.json()

    assert data["total"] == 3
    assert data["limit"] == 3
    assert data["offset"] == 0
    assert [item["case_version_pk"] for item in data["items"]] == ["1004", "1002", "1001"]


def test_case_detail():
    # First get a case pk from search
    search_resp = client.post(
        "/cases/search",
        json=drug_search_payload(drug_name="aspirin", limit=1),
    )
    assert search_resp.status_code == 200
    items = search_resp.json()["items"]
    if not items:
        pytest.skip("No aspirin cases found — warehouse may be empty")

    pk = items[0]["case_version_pk"]
    response = client.get(f"/cases/{pk}")
    assert response.status_code == 200
    data = response.json()
    assert data["case_version_pk"] == pk
    assert "drugs" in data
    assert "reactions" in data
    assert isinstance(data["drugs"], list)
    assert isinstance(data["reactions"], list)
    assert all("outcome" not in reaction for reaction in data["reactions"])


def test_case_detail_not_found():
    response = client.get("/cases/99999999999")
    assert response.status_code == 404


def test_aggregate_drug_reactions_endpoint_removed():
    response = client.get("/aggregates/drug-reactions")
    assert response.status_code == 404


def test_search_with_demographic_filters():
    response = client.post(
        "/cases/search",
        json=search_payload(
            drug_terms=[{"drug_name": "aspirin"}],
            case_filters={"sex_std": "F"},
            limit=3,
        ),
    )
    assert response.status_code == 200
    data = response.json()
    assert "total" in data


def test_age_filters_use_normalized_years():
    response = client.post(
        "/cases/search",
        json=search_payload(
            drug_terms=[{"drug_name": "aspirin"}],
            case_filters={"age_min": 50},
            limit=10,
        ),
    )
    assert response.status_code == 200
    data = response.json()

    assert data["total"] == 1
    assert data["items"][0]["case_version_pk"] == "1004"


def test_search_rejects_invalid_case_and_drug_ranges():
    response = client.post(
        "/cases/search",
        json=search_payload(
            drug_terms=[{"drug_name": "aspirin", "dose_min": 100, "dose_max": 1}],
            case_filters={"age_min": 60, "age_max": 30},
        ),
    )
    assert response.status_code == 422
    detail = response.json()["detail"]
    assert any("case_filters.age_min" in item for item in detail)
    assert any("drug_terms[0].dose_min" in item for item in detail)


def test_search_with_date_filters():
    response = client.post(
        "/cases/search",
        json=search_payload(
            drug_terms=[{"drug_name": "aspirin"}],
            case_filters={
                "event_dt_from": "2024-01-01",
                "event_dt_to": "2024-12-31",
            },
            limit=3,
        ),
    )
    assert response.status_code == 200


def test_missing_query_tables_return_rebuild_message(tmp_path, monkeypatch):
    warehouse = tmp_path / "missing-derived-warehouse"
    warehouse.mkdir()
    monkeypatch.setattr(settings, "warehouse_dir", str(warehouse))

    pl.DataFrame(
        {
            "primaryid": ["1001"],
            "caseid": ["2001"],
            "source_quarter": ["2024q1"],
            "source_system": ["FAERS"],
            "caseversion": [1],
            "report_type": ["EXP"],
            "i_f_code": ["I"],
            "event_dt": [date(2024, 1, 10)],
            "mfr_dt": [date(2024, 1, 11)],
            "fda_dt": [date(2024, 1, 15)],
            "age": [45.0],
            "age_cod": ["YR"],
            "age_grp": ["A"],
            "sex": ["M"],
            "wt_kg": [80.0],
            "reporter_country": ["US"],
            "auth_num": [None],
            "lit_ref": [None],
            "is_deleted": [False],
        }
    ).write_parquet(warehouse / "demo.parquet")

    response = client.post(
        "/cases/search",
        json=drug_search_payload(drug_name="aspirin"),
    )

    assert response.status_code == 503
    assert "Query-optimized warehouse tables are missing" in response.json()["detail"]
    assert "uv run python -m faersdb build" in response.json()["detail"]
