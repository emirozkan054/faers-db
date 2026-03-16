# faers-db

A lightweight ETL + normalization pipeline for FDA FAERS/AERS quarterly ASCII files into PostgreSQL.

## What is implemented today

- Quarter discovery from folder names like `aers_ascii_2004q1` and `faers_ascii_2014q3`, with schema-era tagging (`legacy_aers`, `faers_2012q4_2014q2`, `faers_2014q3_plus`).
- File manifest ingestion into `etl.source_file`.
- Raw staging ingestion for:
  - `DEMO` -> `staging.demo_raw`
  - `DRUG` -> `staging.drug_raw`
  - `REAC` -> `staging.reac_raw`
  - `OUTC` -> `staging.outc_raw`
  - `THER` -> `staging.ther_raw`
  - `INDI` -> `staging.indi_raw`
  - `RPSR` -> `staging.rpsr_raw`
- Normalization:
  - DEMO -> `core.case_master` and `core.case_version`
  - DRUG -> `core.case_drug`
  - REAC -> `core.case_reaction`
  - OUTC -> `core.case_outcome`
  - THER -> `core.case_therapy`
  - INDI -> `core.case_indication`
  - RPSR -> `core.case_report_source`
- Clinician query starter marts:
  - `mart.case_latest`
  - `mart.case_drug_reaction` (includes indication and reporter-source terms when available)
- Latest-known version flagging (`is_latest_known`) per case.

## Why your sample outputs looked correct

For your three loaded quarters (2004q1, 2012q4, 2014q3), the behavior is consistent with current code:

- `staging.demo_raw` count by quarter should match line-level raw ingest.
- `core.case_version` should be near that count, but may be slightly lower when a raw DEMO row is missing required ids (`source_case_id` or `source_report_id`) because those rows are skipped during normalization.
- AERS rows often have null `CASEVERSION` while FAERS has more explicit follow-up/version semantics, so blank `case_version_num` in 2004 is expected.
- Canonical IDs like `AERS:<case_id>` are expected because canonicalization is source-system-prefixed.

## Important FAERS context for product design

When you build doctor-facing query features, preserve these guardrails:

- FAERS is for **signal detection**, not incidence/risk estimation.
- Reports can be duplicate, incomplete, and biased by reporting behavior.
- A single case can have multiple versions/follow-ups over time; front-end defaults should usually prefer latest known versions.
- openFDA FAERS metadata indicates quarterly updates and a lag in updates.

## Suggested next implementation steps

1. Expand ETL to remaining tables
   - Current core table coverage is DEMO/DRUG/REAC/OUTC/THER/INDI/RPSR; next can be deeper field harmonization and dedup policies.
2. Build query-first marts for clinicians
   - Example marts: `mart.case_latest`, `mart.case_drug_reaction`, `mart.disproportionality_ready`.
3. Add clinical-safe defaults
   - Default to latest case version only.
   - Expose source-system/quarter filters.
   - Add caveat banners about interpretation limits.
4. Add data quality checks per load
   - Required-id null rates.
   - Duplicate rate by `(source_system, source_report_id)`.
   - Version monotonicity per `source_case_id`.
5. Add reproducible validation SQL
   - Keep a `sql/checks/` folder with count reconciliation and null-profile checks.

## Example SQL checks to keep running

```sql
-- raw vs normalized DEMO reconciliation by quarter
select f.source_quarter,
       count(*) as raw_rows,
       count(cv.case_version_pk) as normalized_rows,
       count(*) - count(cv.case_version_pk) as dropped_rows
from staging.demo_raw s
join etl.source_file f on f.source_file_id = s.source_file_id
left join core.case_version cv
  on cv.source_system = f.source_system
 and cv.source_quarter = f.source_quarter
 and cv.source_report_id = (s.raw_record->>'PRIMARYID')
group by f.source_quarter
order by f.source_quarter;

-- rows skipped because required ids were missing
select f.source_quarter,
       sum(case when coalesce(s.raw_record->>'CASE_ID', s.raw_record->>'CASEID', s.raw_record->>'CASE') is null then 1 else 0 end) as missing_case_id,
       sum(case when coalesce(s.raw_record->>'PRIMARYID', s.raw_record->>'ISR', s.raw_record->>'REPORT_ID') is null then 1 else 0 end) as missing_report_id
from staging.demo_raw s
join etl.source_file f on f.source_file_id = s.source_file_id
group by f.source_quarter
order by f.source_quarter;
```

## CLI commands

```bash
uv run python -m faersdb.cli init-db
uv run python -m faersdb.cli load-manifest
uv run python -m faersdb.cli load-staging --kind DEMO
uv run python -m faersdb.cli normalize-demo-cmd

uv run python -m faersdb.cli load-staging --kind DRUG
uv run python -m faersdb.cli normalize-drug-cmd

uv run python -m faersdb.cli load-staging --kind REAC
uv run python -m faersdb.cli normalize-reac-cmd

uv run python -m faersdb.cli load-staging --kind OUTC
uv run python -m faersdb.cli normalize-outc-cmd

uv run python -m faersdb.cli load-staging --kind THER
uv run python -m faersdb.cli normalize-ther-cmd

uv run python -m faersdb.cli load-staging --kind INDI
uv run python -m faersdb.cli normalize-indi-cmd

uv run python -m faersdb.cli load-staging --kind RPSR
uv run python -m faersdb.cli normalize-rpsr-cmd
```
