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

create table if not exists staging.demo_raw (
    staging_id bigserial primary key,
    source_file_id bigint not null references etl.source_file(source_file_id),
    row_num bigint not null,
    raw_record jsonb not null,
    row_hash text not null,
    loaded_at timestamptz not null default now(),
    unique (source_file_id, row_num)
);

create table if not exists staging.drug_raw (
    staging_id bigserial primary key,
    source_file_id bigint not null references etl.source_file(source_file_id),
    row_num bigint not null,
    raw_record jsonb not null,
    row_hash text not null,
    loaded_at timestamptz not null default now(),
    unique (source_file_id, row_num)
);

create table if not exists staging.reac_raw (
    staging_id bigserial primary key,
    source_file_id bigint not null references etl.source_file(source_file_id),
    row_num bigint not null,
    raw_record jsonb not null,
    row_hash text not null,
    loaded_at timestamptz not null default now(),
    unique (source_file_id, row_num)
);

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
    raw_demo jsonb not null,
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
    role_cod text,
    drugname text,
    prod_ai text,
    route text,
    dose_vbm text,
    dose_amt numeric,
    dose_unit text,
    start_dt date,
    end_dt date,
    raw_drug jsonb not null,
    created_at timestamptz not null default now(),
    unique (source_system, source_quarter, source_report_id, drugname, prod_ai, dose_vbm)
);

create table if not exists core.case_reaction (
    case_reaction_pk bigserial primary key,
    case_version_pk bigint not null references core.case_version(case_version_pk),
    source_system text not null,
    source_quarter text not null,
    source_report_id text not null,
    reaction_pt text not null,
    outcome text,
    raw_reac jsonb not null,
    created_at timestamptz not null default now(),
    unique (source_system, source_quarter, source_report_id, reaction_pt)
);
