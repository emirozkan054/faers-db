import pytest
from fastapi.testclient import TestClient

from faersdb import cli
from faersdb.api import app

pytest_plugins = ["tests.test_pipeline_integration"]


@pytest.fixture()
def api_client(pipeline_env_factory):
    with pipeline_env_factory():
        cli.init_db(profile="standard")
        cli.load_manifest()
        cli.run_quarter("2025q4", run_qa=False, parallel_normalize=False, profile="standard")
        cli.run_quarter("2026q1", run_qa=False, parallel_normalize=False, profile="standard")

        with TestClient(app) as client:
            yield client


def test_health_endpoint(api_client):
    response = api_client.get("/health")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_filter_metadata_endpoint(api_client):
    response = api_client.get("/filters/metadata")

    assert response.status_code == 200
    body = response.json()
    assert "2025q4" in body["quarters"]
    assert "EXP" in body["report_types"]
    assert "LIT" in body["report_types"]
    assert "PS" in body["role_codes"]
    assert "SS" in body["role_codes"]
    assert "MD" in body["reporter_types"]
    assert "HP" in body["reporter_types"]


def test_case_search_filters_latest_non_deleted_cases(api_client):
    response = api_client.get("/cases/search", params={"drug_name": "aspirin"})

    assert response.status_code == 200
    body = response.json()
    assert body["total"] == 1
    assert body["limit"] == 25
    assert body["offset"] == 0
    assert [item["source_report_id"] for item in body["items"]] == ["1001"]
    assert body["items"][0]["drugs"] == ["ASPIRIN"]
    assert body["items"][0]["reactions"] == ["Headache", "Nausea"]


def test_case_search_reaction_filter_and_empty_results(api_client):
    filtered = api_client.get(
        "/cases/search",
        params={"drug_name": "aspirin", "reaction_pt": "nausea"},
    )
    empty = api_client.get("/cases/search", params={"drug_name": "warfarin"})

    assert filtered.status_code == 200
    assert filtered.json()["total"] == 1
    assert [item["source_report_id"] for item in filtered.json()["items"]] == ["1001"]

    assert empty.status_code == 200
    assert empty.json()["total"] == 0
    assert empty.json()["items"] == []


def test_case_search_supports_demographic_and_case_metadata_filters(api_client):
    response = api_client.get(
        "/cases/search",
        params={
            "sex_std": "F",
            "reporter_country": "CA",
            "report_type": "LIT",
            "initial_or_followup": "F",
        },
    )

    assert response.status_code == 200
    body = response.json()
    assert body["total"] == 1
    assert [item["source_report_id"] for item in body["items"]] == ["2001"]
    assert body["items"][0]["report_type"] == "LIT"
    assert body["items"][0]["reporter_country"] == "CA"


def test_case_search_supports_drug_outcome_and_reporter_filters(api_client):
    response = api_client.get(
        "/cases/search",
        params={
            "route": "IV",
            "case_outcome": "HO",
            "reporter_type": "HP",
            "indication_pt": "Pain",
        },
    )

    assert response.status_code == 200
    body = response.json()
    assert body["total"] == 1
    assert [item["source_report_id"] for item in body["items"]] == ["1001"]
    assert "IV" in body["items"][0]["routes"]
    assert body["items"][0]["indications"] == ["Pain"]


def test_case_search_supports_ranges_and_role_filters(api_client):
    response = api_client.get(
        "/cases/search",
        params={
            "age_min": 30,
            "age_max": 40,
            "event_dt_from": "2025-01-04",
            "event_dt_to": "2025-01-05",
            "role_cod": "SS",
            "reaction_outcome": "LT",
        },
    )

    assert response.status_code == 200
    body = response.json()
    assert body["total"] == 1
    assert [item["source_report_id"] for item in body["items"]] == ["2001"]
    assert body["items"][0]["role_codes"] == ["SS"]


def test_drug_reaction_aggregates_endpoint(api_client):
    response = api_client.get("/aggregates/drug-reactions", params={"drug_name": "aspirin"})

    assert response.status_code == 200
    body = response.json()
    assert body["total"] == 2
    assert {(item["reaction_pt"], item["case_count"]) for item in body["items"]} == {
        ("Headache", 1),
        ("Nausea", 1),
    }


def test_aggregates_support_broader_filters(api_client):
    response = api_client.get(
        "/aggregates/drug-reactions",
        params={"report_type": "LIT", "sex_std": "F"},
    )

    assert response.status_code == 200
    body = response.json()
    assert body["total"] == 1
    assert body["items"] == [{"drugname": "IBUPROFEN", "reaction_pt": "Rash", "case_count": 1}]


def test_case_detail_endpoint_returns_linked_entities(api_client):
    search_response = api_client.get("/cases/search", params={"drug_name": "ibuprofen"})
    case_version_pk = search_response.json()["items"][0]["case_version_pk"]

    response = api_client.get(f"/cases/{case_version_pk}")

    assert response.status_code == 200
    body = response.json()
    assert body["source_report_id"] == "2001"
    assert body["report_type"] == "LIT"
    assert body["initial_or_followup"] == "F"
    assert body["reporter_country"] == "CA"
    assert body["outcomes"] == ["LT"]
    assert body["reporter_types"] == ["MD"]
    assert len(body["drugs"]) == 1
    assert body["drugs"][0]["drugname"] == "IBUPROFEN"
    assert body["drugs"][0]["indications"] == ["Inflammation"]
    assert body["reactions"] == [{"reaction_pt": "Rash", "outcome": "LT"}]


def test_case_detail_missing_returns_404(api_client):
    response = api_client.get("/cases/999999")

    assert response.status_code == 404
    assert response.json() == {"detail": "Case version not found"}


def test_query_param_bounds_and_missing_filters_are_enforced(api_client):
    response = api_client.get("/cases/search", params={"drug_name": "aspirin", "limit": 101})
    no_filters = api_client.get("/cases/search")

    assert response.status_code == 422
    assert no_filters.status_code == 422
