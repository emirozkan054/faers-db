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
- `GET /filters/metadata`
- `GET /cases/search`
- `GET /aggregates/drug-reactions`
- `GET /cases/{case_version_pk}`

## Research Query Model

The query layer now supports guided faceted filtering instead of only `drug_name`, `reaction_pt`, and `quarter`.

Current filter families:

- Case and time: `quarter`, `report_type`, `initial_or_followup`, `event_dt_from/to`, `fda_dt_from/to`, `mfr_dt_from/to`
- Demographics: `sex_std`, `age_min/max`, `age_unit`, `age_group`, `weight_min/max`, `reporter_country`
- Drug: `drug_name`, `prod_ai`, `role_cod`, `route`, `dose_unit`, `dose_min/max`
- Reaction and outcomes: `reaction_pt`, `reaction_outcome`, `case_outcome`
- Therapy and indication: `indication_pt`, `therapy_start_from/to`, `therapy_end_from/to`, `dur_min/max`, `dur_cod`
- Reporter: `reporter_type`

At least one non-pagination filter is required before searching.

### Case-Level Versus Drug-Reaction-Level Filters

- `GET /cases/search` returns latest non-deleted case versions that match the selected facets.
- `GET /aggregates/drug-reactions` returns grouped drug-reaction counts after the same filter logic is applied.
- Some filters are case-level, such as `sex_std`, `reporter_country`, and `report_type`.
- Some filters are drug/reaction-level, such as `route`, `role_cod`, `reaction_pt`, and `indication_pt`.
- Aggregate counts should be interpreted as distinct latest cases, not raw joined row counts.

## UI Workflow

The local UI is intentionally small and API-backed:

- Search latest non-deleted cases across grouped faceted filters
- View aggregate drug-reaction counts under the same filter set
- Open a case detail panel with linked drugs, outcomes, and reactions
- Keep the active search synced into the browser URL for bookmarking or sharing
- Save named searches locally in the browser with no backend state
- Page through case results and export the current result set to CSV
- Export a JSON report stub that includes filters, totals, export timestamp, and current rows
- Click an aggregate reaction row to feed that reaction back into case search

The UI is served by the same FastAPI process, so there is no separate frontend build step.

### Shareable URL State

- When you run a case or aggregate search, the current filters are written into the browser query string using the same parameter names as the API.
- Opening that URL later hydrates the form from the query string and reruns the saved search mode automatically.
- The URL is local-only state. There is no server-side search history or collaboration layer.

### Saved Searches

- Saved searches live in browser `localStorage`, so they stay on the current machine and browser profile only.
- Each saved search stores a name, timestamp, search mode, and filter payload.
- Loading a saved search reapplies the filters and reruns the saved mode.
- Saving with the same name overwrites the previous saved search, which keeps rename/overwrite behavior simple for a local tool.

### Export Behavior

- CSV exports continue to produce spreadsheet-friendly result rows for cases and aggregate counts.
- JSON report exports add lightweight research context:
  - active filters
  - result totals and pagination
  - export timestamp
  - search type
  - current rows on screen
- These exports are meant to help with notes, methods writeups, and reproducibility without adding a full report generator.

### Example Queries

Search latest non-deleted cases for a drug:

```bash
curl "http://127.0.0.1:8000/cases/search?drug_name=aspirin"
```

Filter by drug and reaction:

```bash
curl "http://127.0.0.1:8000/cases/search?drug_name=aspirin&reaction_pt=headache"
```

Filter by demographics and case metadata:

```bash
curl "http://127.0.0.1:8000/cases/search?sex_std=F&reporter_country=CA&report_type=LIT"
```

Filter by drug route, indication, and case outcome:

```bash
curl "http://127.0.0.1:8000/cases/search?route=IV&indication_pt=pain&case_outcome=HO"
```

Get aggregate counts for drug/reaction pairs:

```bash
curl "http://127.0.0.1:8000/aggregates/drug-reactions?drug_name=aspirin"
```

Get guided filter metadata for selects and researcher workflows:

```bash
curl "http://127.0.0.1:8000/filters/metadata"
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

## Local-Only Limitations

- Saved searches are not shared across browsers or computers automatically.
- Exported JSON reports describe the current UI result set; they are not a frozen copy of the underlying database beyond the exported rows.
- There is still no authentication, multi-user state, or cloud sync by design.

