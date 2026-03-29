# FAERS DB

A local-first FAERS warehouse and query API for research workflows.

## What This Repo Provides

- A PostgreSQL ETL pipeline that loads FAERS quarterly ASCII files into normalized `core` tables.
- Research-oriented marts such as `mart.case_latest` and `mart.case_drug_reaction`.
- A small read-only HTTP API for building a future UI without exposing raw SQL directly.

## Load The Database

For the fastest first-time historical load on a laptop:

```bash
uv run python -m faersdb.cli init-db --profile fast_backfill
uv run python -m faersdb.cli load-manifest
uv run python -m faersdb.cli run-quarter 2025q4 --profile fast_backfill --max-workers 4
uv run python -m faersdb.cli finalize-backfill
```

Notes:

- `fast_backfill` is optimized for initial loading speed, not crash recovery.
- `finalize-backfill` keeps tables `UNLOGGED` by default for speed.
- If you want PostgreSQL durability after the load finishes, run:

```bash
uv run python -m faersdb.cli finalize-backfill --durable
```

## Start The API And UI

Run the API locally:

```bash
uv run uvicorn faersdb.api:app --reload
```

Open the interactive docs at:

- `http://127.0.0.1:8000/docs`
- `http://127.0.0.1:8000/app` for the lightweight researcher UI

## Main API Endpoints

- `GET /health`
- `GET /cases/search`
- `GET /aggregates/drug-reactions`
- `GET /cases/{case_version_pk}`

## UI Workflow

The local UI is intentionally small and API-backed:

- Search latest non-deleted cases by drug name and optional reaction
- View aggregate drug-reaction counts
- Open a case detail panel with linked drugs, outcomes, and reactions
- Page through case results and export the current result set to CSV
- Click an aggregate reaction row to feed that reaction back into case search

The UI is served by the same FastAPI process, so there is no separate frontend build step.

### Example Queries

Search latest non-deleted cases for a drug:

```bash
curl "http://127.0.0.1:8000/cases/search?drug_name=aspirin"
```

Filter by drug and reaction:

```bash
curl "http://127.0.0.1:8000/cases/search?drug_name=aspirin&reaction_pt=headache"
```

Get aggregate counts for drug/reaction pairs:

```bash
curl "http://127.0.0.1:8000/aggregates/drug-reactions?drug_name=aspirin"
```

Fetch one case version in detail:

```bash
curl "http://127.0.0.1:8000/cases/12345"
```

## Testing

Run the full test suite:

```bash
uv run python -m pytest -q
```

Run only the API and UI tests:

```bash
uv run python -m pytest -q tests/test_api.py tests/test_ui.py
```

## What Comes Next

The next intended step is to expand this lightweight UI into a more polished researcher workflow with saved filters, clearer case summaries, and export-friendly results.

