from fastapi.testclient import TestClient

from faersdb.api import app


def test_root_metadata_includes_app_path():
    with TestClient(app) as client:
        response = client.get("/")

    assert response.status_code == 200
    assert response.json()["app"] == "/app"


def test_app_shell_is_served():
    with TestClient(app) as client:
        response = client.get("/app")

    assert response.status_code == 200
    assert "FAERS Research UI" in response.text
    assert '/static/app.js' in response.text
    assert "Export Cases CSV" in response.text
    assert "Export Aggregates CSV" in response.text


def test_static_app_javascript_is_served():
    with TestClient(app) as client:
        response = client.get("/static/app.js")

    assert response.status_code == 200
    assert "runCaseSearch" in response.text
    assert "/cases/search" in response.text
    assert "/aggregates/drug-reactions" in response.text
    assert "downloadCsv" in response.text
    assert "clearFilters" in response.text
