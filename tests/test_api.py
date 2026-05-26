"""Tests for the API endpoints using the DuckDB+Parquet backend."""

import pytest
from fastapi.testclient import TestClient

from faersdb.api import app

client = TestClient(app)


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
    assert "reaction_outcomes" in data
    assert "case_outcomes" in data
    assert "reporter_types" in data
    assert "dur_codes" in data
    assert isinstance(data["quarters"], list)


def test_search_cases_requires_filter():
    response = client.get("/cases/search")
    assert response.status_code == 422


def test_search_cases_with_drug():
    response = client.get("/cases/search?drug_name=aspirin&limit=3")
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
        assert isinstance(item["drugs"], list)
        assert isinstance(item["reactions"], list)


def test_search_cases_pagination():
    response1 = client.get("/cases/search?drug_name=aspirin&limit=2&offset=0")
    response2 = client.get("/cases/search?drug_name=aspirin&limit=2&offset=2")
    assert response1.status_code == 200
    assert response2.status_code == 200
    data1 = response1.json()
    data2 = response2.json()
    # Same total
    assert data1["total"] == data2["total"]
    # Different items (unless < 4 total results)
    if data1["total"] > 2 and data1["items"] and data2["items"]:
        assert data1["items"][0]["case_version_pk"] != data2["items"][0]["case_version_pk"]


def test_case_detail():
    # First get a case pk from search
    search_resp = client.get("/cases/search?drug_name=aspirin&limit=1")
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


def test_case_detail_not_found():
    response = client.get("/cases/99999999999")
    assert response.status_code == 404


def test_aggregate_drug_reactions_requires_filter():
    response = client.get("/aggregates/drug-reactions")
    assert response.status_code == 422


def test_aggregate_drug_reactions():
    response = client.get("/aggregates/drug-reactions?drug_name=aspirin&limit=5")
    assert response.status_code == 200
    data = response.json()
    assert "total" in data
    assert "items" in data
    assert len(data["items"]) <= 5

    if data["items"]:
        item = data["items"][0]
        assert "drugname" in item
        assert "reaction_pt" in item
        assert "case_count" in item
        assert item["case_count"] > 0


def test_search_with_demographic_filters():
    response = client.get("/cases/search?drug_name=aspirin&sex_std=F&limit=3")
    assert response.status_code == 200
    data = response.json()
    assert "total" in data


def test_search_with_date_filters():
    response = client.get(
        "/cases/search?drug_name=aspirin"
        "&event_dt_from=2024-01-01&event_dt_to=2024-12-31&limit=3"
    )
    assert response.status_code == 200
