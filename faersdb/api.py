from pathlib import Path
from typing import Annotated

from fastapi import Depends, FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from faersdb.api_models import (
    CaseDetailResponse,
    CaseSearchParams,
    CaseSearchResponse,
    DrugReactionAggregateParams,
    DrugReactionAggregateResponse,
    FilterMetadataResponse,
    HealthResponse,
)
from faersdb.queries import (
    QueryWarehouseError,
    aggregate_drug_reactions,
    get_case_detail,
    get_filter_metadata,
    search_cases,
)

STATIC_DIR = Path(__file__).resolve().parent / "static"

app = FastAPI(
    title="FAERS DB API",
    version="0.2.0",
    description="Read-only API for querying a local FAERS DuckDB+Parquet warehouse.",
)
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


@app.get("/", response_model=dict[str, str])
def root():
    return {
        "name": "faers-db",
        "app": "/app",
        "docs": "/docs",
        "filters": "/filters/metadata",
        "health": "/health",
    }


@app.get("/app", include_in_schema=False)
def app_shell():
    return FileResponse(STATIC_DIR / "index.html")


@app.get("/health", response_model=HealthResponse)
def health():
    return HealthResponse(status="ok")


@app.get("/filters/metadata", response_model=FilterMetadataResponse)
def filter_metadata_endpoint():
    try:
        return FilterMetadataResponse.model_validate(get_filter_metadata())
    except QueryWarehouseError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc


@app.get("/cases/search", response_model=CaseSearchResponse)
def search_cases_endpoint(params: Annotated[CaseSearchParams, Depends()]):
    errors = params.validation_errors()
    if errors:
        raise HTTPException(status_code=422, detail=errors)
    try:
        return CaseSearchResponse.model_validate(search_cases(params))
    except QueryWarehouseError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc


@app.get("/aggregates/drug-reactions", response_model=DrugReactionAggregateResponse)
def aggregate_drug_reactions_endpoint(
    params: Annotated[DrugReactionAggregateParams, Depends()]
):
    errors = params.validation_errors()
    if errors:
        raise HTTPException(status_code=422, detail=errors)
    try:
        return DrugReactionAggregateResponse.model_validate(
            aggregate_drug_reactions(params)
        )
    except QueryWarehouseError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc


@app.get("/cases/{case_version_pk}", response_model=CaseDetailResponse)
def case_detail_endpoint(case_version_pk: str):
    try:
        detail = get_case_detail(case_version_pk)
    except QueryWarehouseError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    if detail is None:
        raise HTTPException(status_code=404, detail="Case version not found")
    return CaseDetailResponse.model_validate(detail)
