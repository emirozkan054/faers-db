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
    assert '/static/app.js' in response.text
    assert "Export Cases CSV" in response.text
    assert "Export Case Report JSON" in response.text
    assert "Export Aggregates CSV" in response.text
    assert "Export Aggregate Report JSON" in response.text
    assert "Case And Time" in response.text
    assert "Outcomes And Reporter" in response.text
    assert "Saved Searches" in response.text
    assert "Save Current Search" in response.text
    assert "Search name" in response.text
    assert "No active cohort yet." in response.text


def test_static_app_javascript_is_served():
    with TestClient(app) as client:
        response = client.get("/static/app.js")

    assert response.status_code == 200
    assert "runCaseSearch" in response.text
    assert "/cases/search" in response.text
    assert "/aggregates/drug-reactions" in response.text
    assert "downloadCsv" in response.text
    assert "downloadJson" in response.text
    assert "clearFilters" in response.text
    assert "/filters/metadata" in response.text
    assert "loadFilterMetadata" in response.text
    assert "localStorage" in response.text
    assert "history.replaceState" in response.text
    assert "window.location.search" in response.text
    assert "saveCurrentSearch" in response.text
    assert "hydrateFromUrl" in response.text
    assert "await loadCaseDetail(payload.items[0].case_version_pk)" not in response.text
