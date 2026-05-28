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
- **Concept Search**: Filter by drug-row concepts, case-level reactions, demographics, outcomes, reporter metadata, event dates, and more.
- **Any/All Concept Matching**: Choose whether cases must match any supplied concept or every supplied concept.
- **Case Details**: Open specific cases to see linked drugs, indications, therapy windows, reactions, outcomes, and reporter metadata.
- **Sharable Links**: Active search parameters are synced to the URL.
- **Export Data**: Export all matching case results to CSV, or export JSON reports for your methods writeups.

### API Endpoints
| Endpoint | Purpose |
|---|---|
| `GET /filters/metadata` | Distinct dropdown values for the query UI. |
| `POST /cases/search` | Paginated latest-case search. |
| `POST /cases/export` | Same filters as search, but returns every matching case and ignores pagination. |
| `GET /cases/{case_version_pk}` | Full detail for one latest case version. |

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

# Require all supplied concepts and add case-level filters
curl -X POST "http://127.0.0.1:8000/cases/search" \
  -H "Content-Type: application/json" \
  -d '{"concept_mode":"all","drug_terms":[{"prod_ai":"aspirin","role_cod":"PS","therapy_start_from":"2024-01-01"}],"reaction_terms":[{"reaction_pt":"headache"}],"case_filters":{"event_dt_from":"2024-01-01","event_dt_to":"2024-12-31","sex_std":"F","age_min":18,"case_outcome":"HO","reporter_type":"HP"},"limit":50,"offset":0}'
```

---

## FAERS Field And Code Glossary

### Query semantics
- Searches run against latest, non-deleted case versions only. The latest version is selected per `CASEID`, ordered by highest `CASEVERSION`, then the first available date in `FDA_DT`/`EVENT_DT`/`MFR_DT` priority, then source quarter and `PRIMARYID`.
- Concept filters and case filters are combined with `AND`.
- `concept_mode` controls only the concept list. Use `any` to match at least one drug or reaction concept, or `all` to require every supplied concept.
- Fields inside one `drug_terms[]` item are row-scoped to one drug exposure. If `indication_pt` or therapy filters are present, they must join to that same drug by `PRIMARYID + DRUG_SEQ`.
- `reaction_terms[]` match case-level reaction preferred terms from `REAC.PT`; they are not tied to one drug row.
- Text concept searches are case-insensitive substring matches with SQL wildcard characters escaped literally.
- Dropdown/code filters are normalized to uppercase exact matches. `quarter` is normalized to lowercase and uses values like `2024q1`.
- Date ranges are inclusive and use `YYYY-MM-DD`.
- `age_min` and `age_max` compare normalized age in years. `age_unit` still filters the original FAERS age unit code.
- `weight_min` and `weight_max` compare `WT` only when FAERS reported `WT_COD` as `KG` or left it blank.
- `/cases/search` requires at least one drug concept, reaction concept, or case filter. `limit` is 1-100 and `offset` is 0-10000.

### Request fields
| JSON field | Meaning |
|---|---|
| `drug_terms[].drug_name` | Substring search over `DRUG.DRUGNAME`. |
| `drug_terms[].prod_ai` | Substring search over `DRUG.PROD_AI`. |
| `drug_terms[].indication_pt` | Substring search over `INDI.INDI_PT`, scoped to the same drug sequence. |
| `drug_terms[].role_cod` | Exact drug role code from `DRUG.ROLE_COD`. |
| `drug_terms[].route` | Exact reported route text from `DRUG.ROUTE`, uppercased. |
| `drug_terms[].dose_unit` | Exact dose unit from `DRUG.DOSE_UNIT`, uppercased. |
| `drug_terms[].dose_min`, `drug_terms[].dose_max` | Inclusive numeric range over `DRUG.DOSE_AMT`. |
| `drug_terms[].therapy_start_from`, `drug_terms[].therapy_start_to` | Inclusive range over `THER.START_DT`, scoped to the same drug sequence. |
| `drug_terms[].therapy_end_from`, `drug_terms[].therapy_end_to` | Inclusive range over `THER.END_DT`, scoped to the same drug sequence. |
| `drug_terms[].dur_min`, `drug_terms[].dur_max` | Inclusive numeric range over `THER.DUR`. |
| `drug_terms[].dur_cod` | Exact therapy duration unit from `THER.DUR_COD`, uppercased. |
| `reaction_terms[].reaction_pt` | Substring search over `REAC.PT`. |
| `case_filters.quarter` | Source quarter, such as `2024q1`. |
| `case_filters.report_type` | Exact report type from `DEMO.REPT_COD`, uppercased. |
| `case_filters.initial_or_followup` | Exact initial/follow-up code from `DEMO.I_F_COD` or `I_F_CODE`, uppercased. |
| `case_filters.event_dt_from`, `case_filters.event_dt_to` | Inclusive range over `DEMO.EVENT_DT`. |
| `case_filters.fda_dt_from`, `case_filters.fda_dt_to` | Inclusive range over `DEMO.FDA_DT`. |
| `case_filters.mfr_dt_from`, `case_filters.mfr_dt_to` | Inclusive range over `DEMO.MFR_DT`. |
| `case_filters.sex_std` | Normalized sex code, usually `M`, `F`, or `UNK`. |
| `case_filters.age_min`, `case_filters.age_max` | Inclusive range over normalized age in years. |
| `case_filters.age_unit` | Exact original FAERS age unit code. |
| `case_filters.age_group` | Exact FAERS age group code. |
| `case_filters.weight_min`, `case_filters.weight_max` | Inclusive range over normalized kilograms. |
| `case_filters.reporter_country` | Exact reporter country code, uppercased. |
| `case_filters.case_outcome` | Exact serious outcome code from `OUTC.OUTC_COD`. |
| `case_filters.reporter_type` | Exact reporter/source code from `RPSR.RPSR_COD` or `REPORTER_TYPE`. |

### Response fields
- `case_version_pk` is the selected latest `PRIMARYID` and is used with `GET /cases/{case_version_pk}`.
- `canonical_case_id` is `source_system:CASEID`.
- Search results include compact arrays for `drugs`, `active_ingredients`, `role_codes`, `routes`, `indications`, `reactions`, `outcomes`, and `reporter_types`.
- Case detail expands linked drug rows with dose, indication, and therapy fields, plus case-level reactions, outcomes, and reporter types.

### Core source fields
- `PROD_AI`: Product active ingredient in the DRUG file, when available.
- `EVENT_DT`: Date the adverse event occurred or began, when reported.
- `FDA_DT`: FDA received date for the case/version in the extract.
- `MFR_DT`: Manufacturer received date, when reported.
- `REPT_COD`: Report type code from DEMO.
- `I_F_COD` / `I_F_CODE`: Initial or follow-up report indicator.
- `PRIMARYID`: FAERS case version/report identifier. This project exposes the selected latest `PRIMARYID` as `case_version_pk`.
- `CASEID`: Stable case identifier across versions.
- `CASEVERSION`: Version number within a case.
- `DRUG_SEQ`: Drug row identifier within a case.
- `INDI_DRUG_SEQ`: Indication-to-drug link. Indications should be joined to drugs by `PRIMARYID + DRUG_SEQ`.
- `DSG_DRUG_SEQ`: Therapy-to-drug link. Therapy rows should be joined to drugs by `PRIMARYID + DRUG_SEQ`.
- `DOSE_VBM`: Verbatim dose text.
- `DOSE_AMT`: Parsed numeric dose amount used for dose range filters.
- `DOSE_UNIT`: Dose unit text.
- `DUR` / `DUR_COD`: Therapy duration value and unit code.
- `OUTC_COD`: Serious outcome code.
- `RPSR_COD`: Reporter/source code.
- `DRUG_REC_ACT`: Drug recur action. This is reaction/event information that reappeared after rechallenge, not a general reaction outcome field, so the UI does not expose it as "reaction outcome."

### Age unit codes
| Code | Meaning |
|---|---|
| `YR` | Years |
| `DEC` | Decades |
| `MON` | Months |
| `WK` | Weeks |
| `DY` | Days |
| `HR` | Hours |
| `MIN` | Minutes |
| `SEC` | Seconds |

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

### Sex codes
| Code | Meaning |
|---|---|
| `M` | Male |
| `F` | Female |
| `UNK` | Unknown, blank, or unrecognized source value |

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
