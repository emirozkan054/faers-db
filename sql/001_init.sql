create schema if not exists etl;
create schema if not exists staging;
create schema if not exists core;
create schema if not exists mart;

create table if not exists etl.load_batch (
    load_batch_id bigserial primary key,
    started_at timestamptz not null default now(),
    finished_at timestamptz,
    status text not null default 'running',
    root_path text not null,
    notes text
);



create table if not exists etl.pipeline_run (
    pipeline_run_id bigserial primary key,
    quarter text not null,
    status text not null default 'running',
    started_at timestamptz not null default now(),
    finished_at timestamptz,
    notes text
);

create table if not exists etl.pipeline_step_run (
    pipeline_step_run_id bigserial primary key,
    pipeline_run_id bigint not null references etl.pipeline_run(pipeline_run_id),
    step_order int not null,
    phase text not null,
    kind text not null,
    status text not null default 'running',
    started_at timestamptz not null default now(),
    finished_at timestamptz,
    files_count int,
    rows_inserted bigint,
    processed bigint,
    skipped bigint,
    error_text text,
    unique (pipeline_run_id, step_order)
);

create table if not exists etl.source_file (
    source_file_id bigserial primary key,
    load_batch_id bigint not null references etl.load_batch(load_batch_id),
    source_quarter text not null,
    source_year int not null,
    source_qtr int not null,
    source_system text not null,
    schema_era text not null,
    folder_name text not null,
    table_kind text not null,
    file_path text not null,
    file_name text not null,
    file_size_bytes bigint,
    header_line text,
    discovered_at timestamptz not null default now(),
    unique (source_quarter, table_kind, file_path)
);

create index if not exists idx_source_file_kind_quarter
    on etl.source_file (table_kind, source_quarter);

create table if not exists staging.demo_raw (
    staging_id bigserial primary key,
    source_file_id bigint not null references etl.source_file(source_file_id),
    row_num bigint not null,
    raw_record jsonb not null,
    row_hash text not null,
    loaded_at timestamptz not null default now(),
    unique (source_file_id, row_num)
);

create index if not exists idx_demo_raw_source_file_id
    on staging.demo_raw (source_file_id);

create table if not exists staging.drug_raw (
    staging_id bigserial primary key,
    source_file_id bigint not null references etl.source_file(source_file_id),
    row_num bigint not null,
    raw_record jsonb not null,
    row_hash text not null,
    loaded_at timestamptz not null default now(),
    unique (source_file_id, row_num)
);

create index if not exists idx_drug_raw_source_file_id
    on staging.drug_raw (source_file_id);

create table if not exists staging.reac_raw (
    staging_id bigserial primary key,
    source_file_id bigint not null references etl.source_file(source_file_id),
    row_num bigint not null,
    raw_record jsonb not null,
    row_hash text not null,
    loaded_at timestamptz not null default now(),
    unique (source_file_id, row_num)
);

create index if not exists idx_reac_raw_source_file_id
    on staging.reac_raw (source_file_id);

create table if not exists staging.outc_raw (
    staging_id bigserial primary key,
    source_file_id bigint not null references etl.source_file(source_file_id),
    row_num bigint not null,
    raw_record jsonb not null,
    row_hash text not null,
    loaded_at timestamptz not null default now(),
    unique (source_file_id, row_num)
);

create index if not exists idx_outc_raw_source_file_id
    on staging.outc_raw (source_file_id);

create table if not exists staging.ther_raw (
    staging_id bigserial primary key,
    source_file_id bigint not null references etl.source_file(source_file_id),
    row_num bigint not null,
    raw_record jsonb not null,
    row_hash text not null,
    loaded_at timestamptz not null default now(),
    unique (source_file_id, row_num)
);

create index if not exists idx_ther_raw_source_file_id
    on staging.ther_raw (source_file_id);

create table if not exists staging.indi_raw (
    staging_id bigserial primary key,
    source_file_id bigint not null references etl.source_file(source_file_id),
    row_num bigint not null,
    raw_record jsonb not null,
    row_hash text not null,
    loaded_at timestamptz not null default now(),
    unique (source_file_id, row_num)
);

create index if not exists idx_indi_raw_source_file_id
    on staging.indi_raw (source_file_id);

create table if not exists staging.rpsr_raw (
    staging_id bigserial primary key,
    source_file_id bigint not null references etl.source_file(source_file_id),
    row_num bigint not null,
    raw_record jsonb not null,
    row_hash text not null,
    loaded_at timestamptz not null default now(),
    unique (source_file_id, row_num)
);

create index if not exists idx_rpsr_raw_source_file_id
    on staging.rpsr_raw (source_file_id);

create table if not exists staging.delete_raw (
    staging_id bigserial primary key,
    source_file_id bigint not null references etl.source_file(source_file_id),
    row_num bigint not null,
    source_report_id text not null,
    row_hash text not null,
    loaded_at timestamptz not null default now(),
    unique (source_file_id, row_num)
);

create index if not exists idx_delete_raw_source_file_id
    on staging.delete_raw (source_file_id);

create table if not exists core.case_master (
    case_pk bigserial primary key,
    canonical_case_id text not null unique,
    source_case_id text not null,
    source_system text not null,
    first_seen_quarter text not null,
    latest_seen_quarter text not null,
    created_at timestamptz not null default now()
);

create table if not exists core.case_version (
    case_version_pk bigserial primary key,
    case_pk bigint not null references core.case_master(case_pk),
    source_quarter text not null,
    source_system text not null,
    schema_era text not null,
    source_report_id text not null,
    source_case_id text not null,
    case_version_num integer,
    report_type text,
    initial_or_followup text,
    event_dt date,
    mfr_dt date,
    fda_dt date,
    age_value numeric,
    age_unit text,
    age_group text,
    sex_std text,
    weight_kg numeric,
    reporter_country text,
    auth_num text,
    lit_ref text,
    raw_demo jsonb,
    is_latest_known boolean not null default false,
    is_deleted boolean not null default false,
    created_at timestamptz not null default now(),
    unique (source_system, source_report_id, source_quarter)
);

create table if not exists core.case_drug (
    case_drug_pk bigserial primary key,
    case_version_pk bigint not null references core.case_version(case_version_pk),
    source_system text not null,
    source_quarter text not null,
    source_report_id text not null,
    drug_seq integer,
    role_cod text,
    drugname text,
    prod_ai text,
    route text,
    dose_vbm text,
    dose_amt numeric,
    dose_unit text,
    start_dt date,
    end_dt date,
    row_hash text not null,
    raw_drug jsonb,
    created_at timestamptz not null default now(),
    unique (source_system, source_quarter, source_report_id, row_hash)
);

create table if not exists core.case_reaction (
    case_reaction_pk bigserial primary key,
    case_version_pk bigint not null references core.case_version(case_version_pk),
    source_system text not null,
    source_quarter text not null,
    source_report_id text not null,
    reaction_pt text not null,
    outcome text,
    row_hash text not null,
    raw_reac jsonb,
    created_at timestamptz not null default now(),
    unique (source_system, source_quarter, source_report_id, row_hash)
);


create table if not exists core.case_outcome (
    case_outcome_pk bigserial primary key,
    case_version_pk bigint not null references core.case_version(case_version_pk),
    source_system text not null,
    source_quarter text not null,
    source_report_id text not null,
    outcome text not null,
    row_hash text not null,
    raw_outc jsonb,
    created_at timestamptz not null default now(),
    unique (source_system, source_quarter, source_report_id, row_hash)
);

create table if not exists core.case_therapy (
    case_therapy_pk bigserial primary key,
    case_version_pk bigint not null references core.case_version(case_version_pk),
    source_system text not null,
    source_quarter text not null,
    source_report_id text not null,
    drug_seq integer,
    start_dt date,
    end_dt date,
    dur integer,
    dur_cod text,
    row_hash text not null,
    raw_ther jsonb,
    created_at timestamptz not null default now(),
    unique (source_system, source_quarter, source_report_id, row_hash)
);

create table if not exists core.case_indication (
    case_indication_pk bigserial primary key,
    case_version_pk bigint not null references core.case_version(case_version_pk),
    source_system text not null,
    source_quarter text not null,
    source_report_id text not null,
    drug_seq integer,
    indi_pt text not null,
    row_hash text not null,
    raw_indi jsonb,
    created_at timestamptz not null default now(),
    unique (source_system, source_quarter, source_report_id, row_hash)
);

create table if not exists core.case_report_source (
    case_report_source_pk bigserial primary key,
    case_version_pk bigint not null references core.case_version(case_version_pk),
    source_system text not null,
    source_quarter text not null,
    source_report_id text not null,
    reporter_type text not null,
    row_hash text not null,
    raw_rpsr jsonb,
    created_at timestamptz not null default now(),
    unique (source_system, source_quarter, source_report_id, row_hash)
);

create index if not exists idx_staging_delete_raw_source_report_id
    on staging.delete_raw (source_report_id);

create index if not exists idx_case_version_case_pk
    on core.case_version (case_pk);

create index if not exists idx_case_version_source_report
    on core.case_version (source_system, source_quarter, source_report_id);

create index if not exists idx_case_drug_case_version_pk
    on core.case_drug (case_version_pk);

create index if not exists idx_case_reaction_case_version_pk
    on core.case_reaction (case_version_pk);

create index if not exists idx_case_outcome_case_version_pk
    on core.case_outcome (case_version_pk);

create index if not exists idx_case_therapy_case_version_pk
    on core.case_therapy (case_version_pk);

create index if not exists idx_case_indication_case_version_pk
    on core.case_indication (case_version_pk);

create index if not exists idx_case_report_source_case_version_pk
    on core.case_report_source (case_version_pk);

drop view if exists mart.case_drug_reaction;
drop view if exists mart.case_latest;

create or replace view mart.case_latest as
select
    cm.case_pk,
    cm.canonical_case_id,
    cv.case_version_pk,
    cv.source_system,
    cv.source_quarter,
    cv.source_report_id,
    cv.source_case_id,
    cv.case_version_num,
    cv.report_type,
    cv.initial_or_followup,
    cv.fda_dt,
    cv.event_dt,
    cv.mfr_dt,
    cv.sex_std,
    cv.age_value,
    cv.age_unit,
    cv.age_group,
    cv.weight_kg,
    cv.reporter_country
from core.case_master cm
join core.case_version cv
  on cv.case_pk = cm.case_pk
where cv.is_latest_known = true
  and cv.is_deleted = false;

create or replace view mart.case_drug_reaction as
select
    cv.case_version_pk,
    cm.canonical_case_id,
    cv.source_system,
    cv.source_quarter,
    cv.source_report_id,
    cv.report_type,
    cv.initial_or_followup,
    cv.event_dt,
    cv.mfr_dt,
    cv.fda_dt,
    cv.sex_std,
    cv.age_value,
    cv.age_unit,
    cv.age_group,
    cv.weight_kg,
    cv.reporter_country,
    cd.drug_seq,
    cd.role_cod,
    cd.drugname,
    cd.prod_ai,
    cd.route,
    cd.dose_vbm,
    cd.dose_amt,
    cd.dose_unit,
    cd.start_dt as drug_start_dt,
    cd.end_dt as drug_end_dt,
    ct.start_dt as therapy_start_dt,
    ct.end_dt as therapy_end_dt,
    ct.dur as therapy_dur,
    ct.dur_cod as therapy_dur_cod,
    outcomes.outcomes,
    report_sources.reporter_types,
    cr.reaction_pt,
    cr.outcome as reaction_outcome,
    ci.indi_pt as indication_pt
from core.case_version cv
join core.case_master cm
  on cm.case_pk = cv.case_pk
left join core.case_drug cd
  on cd.case_version_pk = cv.case_version_pk
left join core.case_reaction cr
  on cr.case_version_pk = cv.case_version_pk
left join core.case_indication ci
  on ci.case_version_pk = cv.case_version_pk
 and ci.drug_seq is not distinct from cd.drug_seq
left join core.case_therapy ct
  on ct.case_version_pk = cv.case_version_pk
 and ct.drug_seq is not distinct from cd.drug_seq
left join lateral (
    select array_agg(distinct outcome order by outcome) as outcomes
    from core.case_outcome co
    where co.case_version_pk = cv.case_version_pk
) outcomes on true
left join lateral (
    select array_agg(distinct reporter_type order by reporter_type) as reporter_types
    from core.case_report_source rs
    where rs.case_version_pk = cv.case_version_pk
) report_sources on true
where cv.is_latest_known = true
  and cv.is_deleted = false;
