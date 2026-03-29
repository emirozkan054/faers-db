-- Profiles potential upsert-key collisions in staging tables by quarter.

with ther as (
  select
    f.source_quarter,
    count(*) as rows_total,
    count(distinct (
      f.source_system,
      f.source_quarter,
      coalesce(s.raw_record->>'PRIMARYID', s.raw_record->>'ISR', s.raw_record->>'REPORT_ID'),
      case when coalesce(s.raw_record->>'DRUG_SEQ','') ~ '^\d+$' then (s.raw_record->>'DRUG_SEQ')::int end,
      case when coalesce(s.raw_record->>'START_DT','') ~ '^\d{8}$' then to_date(s.raw_record->>'START_DT','YYYYMMDD') end,
      case when coalesce(s.raw_record->>'END_DT','') ~ '^\d{8}$' then to_date(s.raw_record->>'END_DT','YYYYMMDD') end
    )) as upsert_keys
  from staging.ther_raw s
  join etl.source_file f on f.source_file_id = s.source_file_id
  group by f.source_quarter
), indi as (
  select
    f.source_quarter,
    count(*) as rows_total,
    count(distinct (
      f.source_system,
      f.source_quarter,
      coalesce(s.raw_record->>'PRIMARYID', s.raw_record->>'ISR', s.raw_record->>'REPORT_ID'),
      case when coalesce(s.raw_record->>'DRUG_SEQ','') ~ '^\d+$' then (s.raw_record->>'DRUG_SEQ')::int end,
      coalesce(s.raw_record->>'INDI_PT', s.raw_record->>'INDICATION')
    )) as upsert_keys
  from staging.indi_raw s
  join etl.source_file f on f.source_file_id = s.source_file_id
  group by f.source_quarter
)
select
  'THER' as kind,
  source_quarter,
  rows_total,
  upsert_keys,
  rows_total - upsert_keys as estimated_collisions
from ther
union all
select
  'INDI',
  source_quarter,
  rows_total,
  upsert_keys,
  rows_total - upsert_keys
from indi
order by source_quarter, kind;
