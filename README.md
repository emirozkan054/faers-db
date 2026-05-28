# FAERS DB

A local-first FAERS (FDA Adverse Event Reporting System) warehouse and query API designed for research workflows.

This project provides an ETL pipeline to load FAERS quarterly ASCII data into compressed Parquet files, queried in-process by DuckDB. It includes a read-only HTTP API and a lightweight web UI to browse cases.

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
MEMORY_LIMIT=2GB
THREADS=4
```

### 3. Build the Warehouse
If you have FAERS ASCII files stored in your `DATA_ROOT`, build the Parquet warehouse:

```bash
# Build the complete warehouse from all quarters (~5-10 minutes for 88 quarters)
uv run python -m faersdb build

# Or build just a single quarter (~3 seconds)
uv run python -m faersdb build --quarter 2024q1

# Limit final deduplication memory on smaller machines
uv run python -m faersdb build --memory-limit 1GB --threads 2
```

### 4. Start the API & UI
```bash
uv run python -m faersdb serve --reload
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
uv run python -m faersdb scan

# Build the warehouse (all quarters)
uv run python -m faersdb build

# Build a single quarter
uv run python -m faersdb build --quarter 2024q1

# Show warehouse statistics
uv run python -m faersdb info

# Start the API server
uv run python -m faersdb serve --reload
```

### Running Tests
```bash
uv run pytest
```

---

## 📊 For Researchers & UI Users

The local UI (`http://127.0.0.1:8000/app`) provides a faceted search interface over the FAERS data.

### Features
- **Concept Search**: Filter by drug-row concepts, case-level reactions, demographics, outcomes, event dates, and more.
- **Case Details**: Open specific cases to see all linked drugs, outcomes, and reactions.
- **Sharable Links**: Active search parameters are synced to the URL.
- **Export Data**: Export all matching case results to CSV, or export JSON reports for your methods writeups.

### Example API Usage
```bash
# Find latest cases for a drug concept
curl -X POST "http://127.0.0.1:8000/cases/search" \
  -H "Content-Type: application/json" \
  -d '{"drug_terms":[{"drug_name":"aspirin"}]}'

# Filter by a drug-row concept and case outcome
curl -X POST "http://127.0.0.1:8000/cases/search" \
  -H "Content-Type: application/json" \
  -d '{"drug_terms":[{"drug_name":"aspirin","route":"ORAL"}],"case_filters":{"case_outcome":"HO"}}'

# Search with multiple concepts
curl -X POST "http://127.0.0.1:8000/cases/search" \
  -H "Content-Type: application/json" \
  -d '{"concept_mode":"any","drug_terms":[{"prod_ai":"ibuprofen"},{"drug_name":"aspirin","indication_pt":"pain"}],"reaction_terms":[{"reaction_pt":"headache"}]}'
```

---

## FAERS Field And Code Glossary

### Core fields
- `PROD_AI`: Product active ingredient in the DRUG file, when available.
- `EVENT_DT`: Date the adverse event occurred or began, when reported.
- `FDA_DT`: FDA received date for the case/version in the extract.
- `DRUG_SEQ`: Drug row identifier within a case.
- `INDI_DRUG_SEQ`: Indication-to-drug link. Indications should be joined to drugs by `PRIMARYID + DRUG_SEQ`.
- `DRUG_REC_ACT`: Drug recur action. This is reaction/event information that reappeared after rechallenge, not a general reaction outcome field, so the UI does not expose it as "reaction outcome."

### Age groups
| Code | Meaning |
|---|---|
| `N` | Neonate |
| `I` | Infant |
| `C` | Child |
| `T` | Adolescent |
| `A` | Adult |
| `E` | Elderly |

### Drug role codes
| Code | Meaning |
|---|---|
| `PS` | Primary suspect |
| `SS` | Secondary suspect |
| `C` | Concomitant |
| `I` | Interacting |
| `DN` | Drug not administered |

### Case outcome codes
| Code | Meaning |
|---|---|
| `DE` | Death |
| `LT` | Life-threatening |
| `HO` | Hospitalization |
| `DS` | Disability |
| `CA` | Congenital anomaly |
| `RI` | Required intervention |
| `OT` | Other serious outcome |

### Reporter/source codes
| Code | Meaning |
|---|---|
| `FGN` | Foreign |
| `SDY` | Study |
| `LIT` | Literature |
| `CSM` | Consumer |
| `HP` | Health professional |
| `UF` | User facility |
| `CR` | Company representative |
| `DT` | Distributor |
| `OTH` | Other |

`ROUTE` is reported route text, such as oral, intravenous, or subcutaneous. It is not a single fixed abbreviation list.

---

## ⚡ Performance

| Metric | Value |
|---|---|
| Full 88-quarter build | ~5-10 minutes |
| Single quarter build | ~3 seconds |
| Warehouse on disk | ~4-8 GB (Snappy compressed Parquet) |
| Source data | ~19 GB (ASCII) |
| Query latency | Sub-second (DuckDB columnar scans) |
| Memory usage (ETL) | Bounded by `MEMORY_LIMIT` during final deduplication |
| Memory usage (API) | ~50-100 MB |
| External dependencies | None (no database server) |

---

## ⚠️ Limitations
- **No Authentication**: Designed for local, single-tenant use.
- **Full Rebuild**: Adding a new quarter currently requires rebuilding the entire warehouse (~5-10 min).
