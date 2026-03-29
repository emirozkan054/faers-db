-- Raw vs normalized reconciliation by quarter for each table kind.

with base as (
  select f.source_quarter, f.source_system, s.raw_record
  from staging.demo_raw s
  join etl.source_file f on f.source_file_id = s.source_file_id
), demo as (
  select
    b.source_quarter,
    count(*) as staging_rows,
    count(cv.case_version_pk) as core_rows
  from base b
  left join core.case_version cv
    on cv.source_system = b.source_system
   and cv.source_quarter = b.source_quarter
   and cv.source_report_id = coalesce(
      b.raw_record->>'PRIMARYID',
      b.raw_record->>'ISR',
      b.raw_record->>'REPORT_ID'
   )
  group by b.source_quarter
), drug as (
  select
    f.source_quarter,
    count(*) as staging_rows,
    count(cd.case_drug_pk) as core_rows
  from staging.drug_raw s
  join etl.source_file f on f.source_file_id = s.source_file_id
  left join core.case_drug cd
    on cd.source_system = f.source_system
   and cd.source_quarter = f.source_quarter
   and cd.source_report_id = coalesce(s.raw_record->>'PRIMARYID', s.raw_record->>'ISR', s.raw_record->>'REPORT_ID')
  group by f.source_quarter
), reac as (
  select
    f.source_quarter,
    count(*) as staging_rows,
    count(cr.case_reaction_pk) as core_rows
  from staging.reac_raw s
  join etl.source_file f on f.source_file_id = s.source_file_id
  left join core.case_reaction cr
    on cr.source_system = f.source_system
   and cr.source_quarter = f.source_quarter
   and cr.source_report_id = coalesce(s.raw_record->>'PRIMARYID', s.raw_record->>'ISR', s.raw_record->>'REPORT_ID')
  group by f.source_quarter
)
select 'DEMO' as kind, source_quarter, staging_rows, core_rows, staging_rows - core_rows as gap from demo
union all
select 'DRUG', source_quarter, staging_rows, core_rows, staging_rows - core_rows from drug
union all
select 'REAC', source_quarter, staging_rows, core_rows, staging_rows - core_rows from reac
order by source_quarter, kind;
