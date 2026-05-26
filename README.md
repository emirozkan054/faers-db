# FAERS DB

A local-first FAERS (FDA Adverse Event Reporting System) warehouse and query API designed for research workflows.

This project provides an ETL pipeline to load FAERS quarterly ASCII data into compressed Parquet files, queried in-process by DuckDB. It includes a read-only HTTP API and a lightweight web UI to browse cases and aggregate statistics.

**No external database required** — everything runs from local files.

---

## 🚀 Quick Start

### 1. Prerequisites
- **Python 3.12+**
- **uv** (for fast dependency management)

### 2. Configuration
Create a `.env` file in the root directory:

```env
DATA_ROOT=data/faers
WAREHOUSE_DIR=warehouse
```

### 3. Build the Warehouse
If you have FAERS ASCII files stored in your `DATA_ROOT`, build the Parquet warehouse:

```bash
# Build the complete warehouse from all quarters (~5-10 minutes for 88 quarters)
uv run python -m faersdb.cli build

# Or build just a single quarter (~3 seconds)
uv run python -m faersdb.cli build --quarter 2024q1
```

### 4. Start the API & UI
```bash
uv run python -m faersdb.cli serve --reload
```

Then visit:
- **UI Application**: [http://127.0.0.1:8000/app](http://127.0.0.1:8000/app)
- **Interactive API Docs**: [http://127.0.0.1:8000/docs](http://127.0.0.1:8000/docs)

---

## 💻 For Developers & Coders

### Architecture

```
FAERS ASCII files (19 GB, 88 quarters)
        │
        ▼
  ETL Pipeline (Polars)
  Read → Normalize → Deduplicate
        │
        ▼
  Parquet Warehouse (~4-8 GB)
  warehouse/demo.parquet
  warehouse/drug.parquet
  warehouse/reac.parquet
  ...
        │
        ▼
  DuckDB (in-memory, reads Parquet)
  Columnar scans, predicate pushdown
        │
        ▼
  FastAPI → Static UI
```

### Project Structure
- `faersdb/etl.py`: ETL pipeline — reads ASCII, normalizes, writes Parquet
- `faersdb/cli.py`: CLI tool (`build`, `serve`, `info`, `scan`)
- `faersdb/api.py`: FastAPI application serving the API and static UI
- `faersdb/queries.py`: DuckDB SQL query logic for the API
- `faersdb/db.py`: DuckDB connection manager with Parquet view registration
- `faersdb/config.py`: Configuration via pydantic-settings
- `faersdb/detect.py`: FAERS file detection and classification
- `faersdb/manifest.py`: Quarter folder discovery
- `faersdb/static/`: Vanilla frontend UI (HTML, JS, CSS)
- `tests/`: Test suite

### CLI Commands

```bash
# Discover available quarter folders
uv run python -m faersdb.cli scan

# Build the warehouse (all quarters)
uv run python -m faersdb.cli build

# Build a single quarter
uv run python -m faersdb.cli build --quarter 2024q1

# Show warehouse statistics
uv run python -m faersdb.cli info

# Start the API server
uv run python -m faersdb.cli serve --reload
```

### Running Tests
```bash
uv run pytest
```

---

## 📊 For Researchers & UI Users

The local UI (`http://127.0.0.1:8000/app`) provides a faceted search interface over the FAERS data.

### Features
- **Faceted Search**: Filter by demographics, drug name, reaction, indication, case outcomes, event dates, and more.
- **Aggregate Views**: See grouped drug-reaction counts across your selected filters.
- **Case Details**: Open specific cases to see all linked drugs, outcomes, and reactions.
- **Sharable Links**: Active search parameters are synced to the URL.
- **Saved Searches**: Save your frequent queries locally in your browser.
- **Export Data**: Export result tables to CSV, or export JSON reports for your methods writeups.

### Example API Usage
```bash
# Find latest cases for a drug
curl "http://127.0.0.1:8000/cases/search?drug_name=aspirin"

# Filter by drug, route, and case outcome
curl "http://127.0.0.1:8000/cases/search?drug_name=aspirin&route=ORAL&case_outcome=HO"

# Get aggregate drug-reaction counts
curl "http://127.0.0.1:8000/aggregates/drug-reactions?drug_name=aspirin"
```

---

## ⚡ Performance

| Metric | Value |
|---|---|
| Full 88-quarter build | ~5-10 minutes |
| Single quarter build | ~3 seconds |
| Warehouse on disk | ~4-8 GB (Snappy compressed Parquet) |
| Source data | ~19 GB (ASCII) |
| Query latency | Sub-second (DuckDB columnar scans) |
| Memory usage (ETL) | ~200-500 MB peak |
| Memory usage (API) | ~50-100 MB |
| External dependencies | None (no database server) |

---

## ⚠️ Limitations
- **Local-Only State**: Saved searches are stored in the browser's `localStorage` and are not synced.
- **No Authentication**: Designed for local, single-tenant use.
- **Full Rebuild**: Adding a new quarter currently requires rebuilding the entire warehouse (~5-10 min).
