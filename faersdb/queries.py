from decimal import Decimal

from faersdb.api_models import (
    CaseSearchParams,
    DrugReactionAggregateParams,
    FilterMetadataResponse,
)
from faersdb.db import get_dict_conn


def _to_json_value(value):
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, list):
        return [_to_json_value(item) for item in value]
    if isinstance(value, dict):
        return {key: _to_json_value(item) for key, item in value.items()}
    return value


def _normalize_record(record: dict) -> dict:
    return {key: _to_json_value(value) for key, value in record.items()}


def _like_clause(value: str) -> str:
    return f"%{value.upper()}%"


def _build_filter_sql(params, alias: str = "cdr") -> tuple[str, list]:
    clauses: list[str] = []
    query_params: list = []

    if params.quarter:
        clauses.append(f"{alias}.source_quarter = %s")
        query_params.append(params.quarter)

    if params.report_type:
        clauses.append(f"upper(coalesce({alias}.report_type, '')) = %s")
        query_params.append(params.report_type)

    if params.initial_or_followup:
        clauses.append(f"upper(coalesce({alias}.initial_or_followup, '')) = %s")
        query_params.append(params.initial_or_followup)

    if params.event_dt_from:
        clauses.append(f"{alias}.event_dt >= %s")
        query_params.append(params.event_dt_from)
    if params.event_dt_to:
        clauses.append(f"{alias}.event_dt <= %s")
        query_params.append(params.event_dt_to)
    if params.fda_dt_from:
        clauses.append(f"{alias}.fda_dt >= %s")
        query_params.append(params.fda_dt_from)
    if params.fda_dt_to:
        clauses.append(f"{alias}.fda_dt <= %s")
        query_params.append(params.fda_dt_to)
    if params.mfr_dt_from:
        clauses.append(f"{alias}.mfr_dt >= %s")
        query_params.append(params.mfr_dt_from)
    if params.mfr_dt_to:
        clauses.append(f"{alias}.mfr_dt <= %s")
        query_params.append(params.mfr_dt_to)

    if params.sex_std:
        clauses.append(f"upper(coalesce({alias}.sex_std, '')) = %s")
        query_params.append(params.sex_std)
    if params.age_min is not None:
        clauses.append(f"{alias}.age_value >= %s")
        query_params.append(params.age_min)
    if params.age_max is not None:
        clauses.append(f"{alias}.age_value <= %s")
        query_params.append(params.age_max)
    if params.age_unit:
        clauses.append(f"upper(coalesce({alias}.age_unit, '')) = %s")
        query_params.append(params.age_unit)
    if params.age_group:
        clauses.append(f"upper(coalesce({alias}.age_group, '')) = %s")
        query_params.append(params.age_group)
    if params.weight_min is not None:
        clauses.append(f"{alias}.weight_kg >= %s")
        query_params.append(params.weight_min)
    if params.weight_max is not None:
        clauses.append(f"{alias}.weight_kg <= %s")
        query_params.append(params.weight_max)
    if params.reporter_country:
        clauses.append(f"upper(coalesce({alias}.reporter_country, '')) = %s")
        query_params.append(params.reporter_country)

    if params.drug_name:
        clauses.append(f"upper(coalesce({alias}.drugname, '')) like %s")
        query_params.append(_like_clause(params.drug_name))
    if params.prod_ai:
        clauses.append(f"upper(coalesce({alias}.prod_ai, '')) like %s")
        query_params.append(_like_clause(params.prod_ai))
    if params.role_cod:
        clauses.append(f"upper(coalesce({alias}.role_cod, '')) = %s")
        query_params.append(params.role_cod)
    if params.route:
        clauses.append(f"upper(coalesce({alias}.route, '')) = %s")
        query_params.append(params.route)
    if params.dose_unit:
        clauses.append(f"upper(coalesce({alias}.dose_unit, '')) = %s")
        query_params.append(params.dose_unit)
    if params.dose_min is not None:
        clauses.append(f"{alias}.dose_amt >= %s")
        query_params.append(params.dose_min)
    if params.dose_max is not None:
        clauses.append(f"{alias}.dose_amt <= %s")
        query_params.append(params.dose_max)

    if params.reaction_pt:
        clauses.append(f"upper(coalesce({alias}.reaction_pt, '')) like %s")
        query_params.append(_like_clause(params.reaction_pt))
    if params.reaction_outcome:
        clauses.append(f"upper(coalesce({alias}.reaction_outcome, '')) = %s")
        query_params.append(params.reaction_outcome)
    if params.case_outcome:
        clauses.append(
            f"""
            exists (
                select 1
                from unnest(coalesce({alias}.outcomes, '{{}}'::text[])) as outcome
                where upper(outcome) = %s
            )
            """
        )
        query_params.append(params.case_outcome)

    if params.indication_pt:
        clauses.append(f"upper(coalesce({alias}.indication_pt, '')) like %s")
        query_params.append(_like_clause(params.indication_pt))

    if params.therapy_start_from:
        clauses.append(f"{alias}.therapy_start_dt >= %s")
        query_params.append(params.therapy_start_from)
    if params.therapy_start_to:
        clauses.append(f"{alias}.therapy_start_dt <= %s")
        query_params.append(params.therapy_start_to)
    if params.therapy_end_from:
        clauses.append(f"{alias}.therapy_end_dt >= %s")
        query_params.append(params.therapy_end_from)
    if params.therapy_end_to:
        clauses.append(f"{alias}.therapy_end_dt <= %s")
        query_params.append(params.therapy_end_to)
    if params.dur_min is not None:
        clauses.append(f"{alias}.therapy_dur >= %s")
        query_params.append(params.dur_min)
    if params.dur_max is not None:
        clauses.append(f"{alias}.therapy_dur <= %s")
        query_params.append(params.dur_max)
    if params.dur_cod:
        clauses.append(f"upper(coalesce({alias}.therapy_dur_cod, '')) = %s")
        query_params.append(params.dur_cod)

    if params.reporter_type:
        clauses.append(
            f"""
            exists (
                select 1
                from unnest(coalesce({alias}.reporter_types, '{{}}'::text[])) as reporter_type
                where upper(reporter_type) = %s
            )
            """
        )
        query_params.append(params.reporter_type)

    where_clause = ""
    if clauses:
        where_clause = " where " + " and ".join(f"({clause.strip()})" for clause in clauses)

    return where_clause, query_params


def search_cases(params: CaseSearchParams) -> dict:
    where_clause, query_params = _build_filter_sql(params)

    count_sql = f"""
        with filtered_cases as (
            select distinct cdr.case_version_pk
            from mart.case_drug_reaction cdr
            {where_clause}
        )
        select count(*)::int as total
        from filtered_cases
    """
    data_sql = f"""
        with filtered_cases as (
            select distinct cdr.case_version_pk
            from mart.case_drug_reaction cdr
            {where_clause}
        ),
        paged_cases as (
            select cl.*
            from mart.case_latest cl
            join filtered_cases fc on fc.case_version_pk = cl.case_version_pk
            order by cl.source_quarter desc, cl.case_version_pk desc
            limit %s
            offset %s
        )
        select
            p.case_pk,
            p.canonical_case_id,
            p.case_version_pk,
            p.source_system,
            p.source_quarter,
            p.source_report_id,
            p.source_case_id,
            p.case_version_num,
            p.report_type,
            p.initial_or_followup,
            p.fda_dt,
            p.event_dt,
            p.mfr_dt,
            p.sex_std,
            p.age_value,
            p.age_unit,
            p.age_group,
            p.weight_kg,
            p.reporter_country,
            coalesce((
                select array_agg(distinct cd.drugname order by cd.drugname)
                from core.case_drug cd
                where cd.case_version_pk = p.case_version_pk
                  and cd.drugname is not null
            ), '{{}}'::text[]) as drugs,
            coalesce((
                select array_agg(distinct cd.prod_ai order by cd.prod_ai)
                from core.case_drug cd
                where cd.case_version_pk = p.case_version_pk
                  and cd.prod_ai is not null
            ), '{{}}'::text[]) as active_ingredients,
            coalesce((
                select array_agg(distinct cd.role_cod order by cd.role_cod)
                from core.case_drug cd
                where cd.case_version_pk = p.case_version_pk
                  and cd.role_cod is not null
            ), '{{}}'::text[]) as role_codes,
            coalesce((
                select array_agg(distinct cd.route order by cd.route)
                from core.case_drug cd
                where cd.case_version_pk = p.case_version_pk
                  and cd.route is not null
            ), '{{}}'::text[]) as routes,
            coalesce((
                select array_agg(distinct ci.indi_pt order by ci.indi_pt)
                from core.case_indication ci
                where ci.case_version_pk = p.case_version_pk
                  and ci.indi_pt is not null
            ), '{{}}'::text[]) as indications,
            coalesce((
                select array_agg(distinct cr.reaction_pt order by cr.reaction_pt)
                from core.case_reaction cr
                where cr.case_version_pk = p.case_version_pk
                  and cr.reaction_pt is not null
            ), '{{}}'::text[]) as reactions,
            coalesce((
                select array_agg(distinct co.outcome order by co.outcome)
                from core.case_outcome co
                where co.case_version_pk = p.case_version_pk
                  and co.outcome is not null
            ), '{{}}'::text[]) as outcomes,
            coalesce((
                select array_agg(distinct rs.reporter_type order by rs.reporter_type)
                from core.case_report_source rs
                where rs.case_version_pk = p.case_version_pk
                  and rs.reporter_type is not null
            ), '{{}}'::text[]) as reporter_types
        from paged_cases p
        order by p.source_quarter desc, p.case_version_pk desc
    """

    with get_dict_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(count_sql, query_params)
            total = cur.fetchone()["total"]

            cur.execute(data_sql, [*query_params, params.limit, params.offset])
            items = [_normalize_record(row) for row in cur.fetchall()]

    return {
        "total": total,
        "limit": params.limit,
        "offset": params.offset,
        "items": items,
    }


def aggregate_drug_reactions(params: DrugReactionAggregateParams) -> dict:
    where_clause, query_params = _build_filter_sql(params)

    base_cte = f"""
        with grouped as (
            select
                cdr.drugname,
                cdr.reaction_pt,
                count(distinct cdr.canonical_case_id)::int as case_count
            from mart.case_drug_reaction cdr
            {where_clause}
            {"and" if where_clause else "where"} cdr.drugname is not null
              and cdr.reaction_pt is not null
            group by cdr.drugname, cdr.reaction_pt
        )
    """
    count_sql = base_cte + "select count(*)::int as total from grouped"
    data_sql = (
        base_cte
        + """
        select drugname, reaction_pt, case_count
        from grouped
        order by case_count desc, drugname, reaction_pt
        limit %s
        offset %s
        """
    )

    with get_dict_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(count_sql, query_params)
            total = cur.fetchone()["total"]

            cur.execute(data_sql, [*query_params, params.limit, params.offset])
            items = [_normalize_record(row) for row in cur.fetchall()]

    return {
        "total": total,
        "limit": params.limit,
        "offset": params.offset,
        "items": items,
    }


def get_filter_metadata() -> dict:
    metadata_sql = """
        select
            coalesce((
                select array_agg(distinct source_quarter order by source_quarter)
                from mart.case_latest
            ), '{}'::text[]) as quarters,
            coalesce((
                select array_agg(distinct report_type order by report_type)
                from mart.case_latest
                where report_type is not null
            ), '{}'::text[]) as report_types,
            coalesce((
                select array_agg(distinct initial_or_followup order by initial_or_followup)
                from mart.case_latest
                where initial_or_followup is not null
            ), '{}'::text[]) as initial_or_followup_values,
            coalesce((
                select array_agg(distinct sex_std order by sex_std)
                from mart.case_latest
                where sex_std is not null
            ), '{}'::text[]) as sex_values,
            coalesce((
                select array_agg(distinct age_unit order by age_unit)
                from mart.case_latest
                where age_unit is not null
            ), '{}'::text[]) as age_units,
            coalesce((
                select array_agg(distinct age_group order by age_group)
                from mart.case_latest
                where age_group is not null
            ), '{}'::text[]) as age_groups,
            coalesce((
                select array_agg(distinct reporter_country order by reporter_country)
                from mart.case_latest
                where reporter_country is not null
            ), '{}'::text[]) as reporter_countries,
            coalesce((
                select array_agg(distinct role_cod order by role_cod)
                from mart.case_drug_reaction
                where role_cod is not null
            ), '{}'::text[]) as role_codes,
            coalesce((
                select array_agg(distinct route order by route)
                from mart.case_drug_reaction
                where route is not null
            ), '{}'::text[]) as routes,
            coalesce((
                select array_agg(distinct dose_unit order by dose_unit)
                from mart.case_drug_reaction
                where dose_unit is not null
            ), '{}'::text[]) as dose_units,
            coalesce((
                select array_agg(distinct reaction_outcome order by reaction_outcome)
                from mart.case_drug_reaction
                where reaction_outcome is not null
            ), '{}'::text[]) as reaction_outcomes,
            coalesce((
                select array_agg(distinct outcome order by outcome)
                from core.case_outcome co
                join mart.case_latest cl on cl.case_version_pk = co.case_version_pk
                where co.outcome is not null
            ), '{}'::text[]) as case_outcomes,
            coalesce((
                select array_agg(distinct reporter_type order by reporter_type)
                from core.case_report_source rs
                join mart.case_latest cl on cl.case_version_pk = rs.case_version_pk
                where rs.reporter_type is not null
            ), '{}'::text[]) as reporter_types,
            coalesce((
                select array_agg(distinct dur_cod order by dur_cod)
                from core.case_therapy ct
                join mart.case_latest cl on cl.case_version_pk = ct.case_version_pk
                where ct.dur_cod is not null
            ), '{}'::text[]) as dur_codes
    """

    with get_dict_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(metadata_sql)
            record = _normalize_record(cur.fetchone())

    return FilterMetadataResponse.model_validate(record).model_dump()


def get_case_detail(case_version_pk: int) -> dict | None:
    header_sql = """
        select
            cl.case_pk,
            cl.canonical_case_id,
            cl.case_version_pk,
            cl.source_system,
            cl.source_quarter,
            cl.source_report_id,
            cl.source_case_id,
            cl.case_version_num,
            cl.report_type,
            cl.initial_or_followup,
            cl.fda_dt,
            cl.event_dt,
            cl.mfr_dt,
            cl.sex_std,
            cl.age_value,
            cl.age_unit,
            cl.age_group,
            cl.weight_kg,
            cl.reporter_country,
            coalesce((
                select array_agg(distinct co.outcome order by co.outcome)
                from core.case_outcome co
                where co.case_version_pk = cl.case_version_pk
                  and co.outcome is not null
            ), '{}'::text[]) as outcomes,
            coalesce((
                select array_agg(distinct rs.reporter_type order by rs.reporter_type)
                from core.case_report_source rs
                where rs.case_version_pk = cl.case_version_pk
                  and rs.reporter_type is not null
            ), '{}'::text[]) as reporter_types
        from mart.case_latest cl
        where cl.case_version_pk = %s
    """
    drugs_sql = """
        select
            cd.drug_seq,
            cd.role_cod,
            cd.drugname,
            cd.prod_ai,
            cd.route,
            cd.dose_vbm,
            cd.dose_amt,
            cd.dose_unit,
            cd.start_dt,
            cd.end_dt,
            coalesce((
                select array_agg(distinct ci.indi_pt order by ci.indi_pt)
                from core.case_indication ci
                where ci.case_version_pk = cd.case_version_pk
                  and ci.drug_seq is not distinct from cd.drug_seq
                  and ci.indi_pt is not null
            ), '{}'::text[]) as indications,
            (
                select min(ct.start_dt)
                from core.case_therapy ct
                where ct.case_version_pk = cd.case_version_pk
                  and ct.drug_seq is not distinct from cd.drug_seq
            ) as therapy_start_dt,
            (
                select max(ct.end_dt)
                from core.case_therapy ct
                where ct.case_version_pk = cd.case_version_pk
                  and ct.drug_seq is not distinct from cd.drug_seq
            ) as therapy_end_dt
        from core.case_drug cd
        where cd.case_version_pk = %s
        order by cd.drug_seq nulls last, cd.drugname nulls last, cd.route nulls last
    """
    reactions_sql = """
        select distinct
            cr.reaction_pt,
            cr.outcome
        from core.case_reaction cr
        where cr.case_version_pk = %s
        order by cr.reaction_pt, cr.outcome
    """

    with get_dict_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(header_sql, (case_version_pk,))
            header = cur.fetchone()
            if not header:
                return None

            cur.execute(drugs_sql, (case_version_pk,))
            drugs = [_normalize_record(row) for row in cur.fetchall()]

            cur.execute(reactions_sql, (case_version_pk,))
            reactions = [_normalize_record(row) for row in cur.fetchall()]

    detail = _normalize_record(header)
    detail["drugs"] = drugs
    detail["reactions"] = reactions
    return detail
