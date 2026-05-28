from fastapi.testclient import TestClient

from faersdb.api import app


def test_root_metadata_includes_app_path():
    with TestClient(app) as client:
        response = client.get("/")

    assert response.status_code == 200
    assert response.json()["app"] == "/app"
    assert response.json()["filters"] == "/filters/metadata"


def test_app_shell_is_served():
    with TestClient(app) as client:
        response = client.get("/app")

    assert response.status_code == 200
    assert "FAERS Research UI" in response.text
    assert "FAERS Research Workbench" in response.text
    assert "Query Builder" in response.text
    assert "Case Inspector" in response.text
    assert "workbench-layout" in response.text
    assert '/static/app.js' in response.text
    assert "Export Cases CSV" in response.text
    assert "Export Case Report JSON" in response.text
    assert "Export Aggregates CSV" not in response.text
    assert "Export Aggregate Report JSON" not in response.text
    assert "Show Aggregates" not in response.text
    assert "Concepts" in response.text
    assert "Match concepts" in response.text
    assert "Add Drug Concept" in response.text
    assert "Add Reaction Concept" in response.text
    assert "Drug And Therapy" not in response.text
    assert "Reaction outcome" not in response.text
    assert "Case And Time" in response.text
    assert "Outcomes And Reporter" in response.text
    assert "Saved Searches" not in response.text
    assert "Save Current Search" not in response.text
    assert "Search name" not in response.text
    assert "No active cohort yet." in response.text
    assert response.headers["cache-control"] == "no-store, no-cache, must-revalidate, max-age=0"


def test_static_app_javascript_is_served():
    with TestClient(app) as client:
        response = client.get("/static/app.js")

    assert response.status_code == 200
    assert "runCaseSearch" in response.text
    assert "/cases/search" in response.text
    assert "/aggregates/drug-reactions" not in response.text
    assert "drug_terms" in response.text
    assert "reaction_terms" in response.text
    assert "concept_mode" in response.text
    assert "primary_terms" not in response.text
    assert "Active ingredients" in response.text
    assert "results-table-shell" in response.text
    assert "Drug exposure" in response.text
    assert "Case Inspector" not in response.text
    assert "reaction_outcome" not in response.text
    assert "downloadCsv" in response.text
    assert "downloadJson" in response.text
    assert "/cases/export" in response.text
    assert "Exported current case results" not in response.text
    assert "clearFilters" in response.text
    assert "/filters/metadata" in response.text
    assert "loadFilterMetadata" in response.text
    assert "localStorage" not in response.text
    assert "history.replaceState" in response.text
    assert "window.location.search" in response.text
    assert "q" in response.text
    assert "saveCurrentSearch" not in response.text
    assert "hydrateFromUrl" in response.text
    assert "await loadCaseDetail(payload.items[0].case_version_pk)" not in response.text
    assert response.headers["cache-control"] == "no-store, no-cache, must-revalidate, max-age=0"
