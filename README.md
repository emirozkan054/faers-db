# FAERS DB

A local-first FAERS (FDA Adverse Event Reporting System) warehouse and query API designed for research workflows.

This project provides an ETL pipeline to load FAERS quarterly ASCII data into a normalized PostgreSQL database, a read-only HTTP API to query the data, and a lightweight web UI to browse cases and aggregate statistics.

---

## 🚀 Quick Start

### 1. Prerequisites
- **Python 3.12+**
- **uv** (for fast dependency management)
- **PostgreSQL** database

### 2. Configuration
Create a `.env` file in the root directory (or copy `.env.example` if available) and configure your database connection and data directory:

```env
# Example .env configuration
PG_DSN=postgresql://postgres:postgres@localhost:5432/faers
DATA_ROOT=data/faers
PIPELINE_PROFILE=standard # or fast_backfill
```

### 3. Load the Database (ETL)
If you have FAERS ASCII files stored in your `DATA_ROOT`, you can populate the database. 

**For the first-time historical load (Fast Backfill):**
Use the `backfill-all` command. This bypasses intermediate staging tables and loads all available quarters in parallel, which is **10-20x faster** than the standard pipeline.

```bash
# This single command initializes the DB, discovers files, loads all quarters, 
# recomputes case flags, builds indexes, and runs ANALYZE.
uv run python -m faersdb.cli backfill-all --max-workers 4
```
*Note: By default, this leaves tables `UNLOGGED` for speed. If you want standard PostgreSQL durability (WAL logging) after the load finishes, run it with the `--durable` flag.*

**For incremental loads (Standard Profile):**
When a new quarter is released, use the standard pipeline which safely stages and merges the new data without affecting the rest of the database:

```bash
uv run python -m faersdb.cli load-manifest
uv run python -m faersdb.cli run-quarter 2025q4 --max-workers 4
```

### 4. Start the API & UI
Run the API server locally with hot-reloading:

```bash
uv run uvicorn faersdb.api:app --reload
```

Then visit:
- **UI Application**: [http://127.0.0.1:8000/app](http://127.0.0.1:8000/app)
- **Interactive API Docs**: [http://127.0.0.1:8000/docs](http://127.0.0.1:8000/docs)

---

## 💻 For Developers & Coders

This project uses `uv` for package management and `FastAPI` for the web layer.

### Project Structure
- `faersdb/cli.py`: The ETL pipeline and CLI tool for managing the database.
- `faersdb/api.py`: FastAPI application serving the API and static UI.
- `faersdb/queries.py`: SQL query logic for the API.
- `faersdb/staging_load.py` & `faersdb/normalize/`: Data ingestion and normalization logic.
- `faersdb/static/`: Vanilla frontend UI files (HTML, JS, CSS).
- `sql/`: Raw SQL scripts, including `001_init.sql` for schema definition.
- `tests/`: Test suite.

### Database Connection
The application connects to PostgreSQL using `psycopg` (v3). Connection logic is centralized in `faersdb.db`. The connection string is retrieved from `settings.pg_dsn` (configured via `.env`).

To connect directly via the `psql` command line tool using the default configuration:
```bash
psql postgresql://postgres:postgres@localhost:5432/faers
```

To open a direct database connection in your Python code:
```python
from faersdb.db import get_conn, get_dict_conn

with get_dict_conn() as conn:
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM core.case_master LIMIT 5")
        print(cur.fetchall())
```

### Running Tests
To run the full test suite using `pytest`:

```bash
uv run pytest
```
Or for specific components:
```bash
uv run pytest tests/test_api.py tests/test_ui.py
```

---

## 📊 For Researchers & UI Users

The local UI (`http://127.0.0.1:8000/app`) provides a faceted search interface over the FAERS data.

### Features
- **Faceted Search**: Filter by demographics, drug name, reaction, indication, case outcomes, event dates, and more.
- **Aggregate Views**: See grouped drug-reaction counts across your selected filters.
- **Case Details**: Open specific cases to see all linked drugs, outcomes, and reactions.
- **Sharable Links**: Active search parameters are synced to the URL, allowing you to easily bookmark or share queries.
- **Saved Searches**: Save your frequent queries locally in your browser.
- **Export Data**: Export result tables to CSV, or export a JSON report stub containing active filters, totals, and the current result set for your methods writeups.

### Research Query Model
The underlying database supports a sophisticated query model. 
- **Case-Level Filters**: e.g., `sex_std`, `reporter_country`, `report_type`.
- **Drug/Reaction-Level Filters**: e.g., `route`, `role_cod`, `reaction_pt`, `indication_pt`.

*Note on counts: Aggregate counts shown in the API and UI represent distinct latest cases, not raw joined row counts.*

### Example API Usage
You can directly interact with the API endpoints to script your data extraction.

**Find latest cases for a drug:**
```bash
curl "http://127.0.0.1:8000/cases/search?drug_name=aspirin"
```

**Filter by drug, route, and case outcome:**
```bash
curl "http://127.0.0.1:8000/cases/search?drug_name=aspirin&route=IV&case_outcome=HO"
```

**Get aggregate drug-reaction counts:**
```bash
curl "http://127.0.0.1:8000/aggregates/drug-reactions?drug_name=aspirin"
```

---

## ⚠️ Limitations
- **Local-Only State**: Saved searches are stored in the browser's `localStorage` and are not synced. 
- **No Authentication**: The application is designed to be run locally and single-tenant. There is no built-in auth or multi-user state.
