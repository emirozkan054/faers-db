from decimal import Decimal

from faersdb.api_models import CaseSearchParams, DrugReactionAggregateParams
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


def _build_case_filters(params: CaseSearchParams) -> tuple[str, list]:
    clauses = ["upper(cdr.drugname) like %s"]
    query_params: list = [_like_clause(params.drug_name)]

    if params.reaction_pt:
        clauses.append("upper(cdr.reaction_pt) like %s")
        query_params.append(_like_clause(params.reaction_pt))

    if params.quarter:
        clauses.append("cdr.source_quarter = %s")
        query_params.append(params.quarter)

    return " where " + " and ".join(clauses), query_params


def search_cases(params: CaseSearchParams) -> dict:
    where_clause, query_params = _build_case_filters(params)

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
            p.fda_dt,
            p.event_dt,
            p.mfr_dt,
            p.sex_std,
            p.age_value,
            p.age_unit,
            coalesce((
                select array_agg(distinct cd.drugname order by cd.drugname)
                from core.case_drug cd
                where cd.case_version_pk = p.case_version_pk
                  and cd.drugname is not null
            ), '{{}}'::text[]) as drugs,
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


def _build_aggregate_filters(params: DrugReactionAggregateParams) -> tuple[str, list]:
    clauses = ["upper(cdr.drugname) like %s"]
    query_params: list = [_like_clause(params.drug_name)]

    if params.reaction_pt:
        clauses.append("upper(cdr.reaction_pt) like %s")
        query_params.append(_like_clause(params.reaction_pt))

    if params.quarter:
        clauses.append("cdr.source_quarter = %s")
        query_params.append(params.quarter)

    return " where " + " and ".join(clauses), query_params


def aggregate_drug_reactions(params: DrugReactionAggregateParams) -> dict:
    where_clause, query_params = _build_aggregate_filters(params)

    base_cte = f"""
        with grouped as (
            select
                cdr.drugname,
                cdr.reaction_pt,
                count(distinct cdr.canonical_case_id)::int as case_count
            from mart.case_drug_reaction cdr
            {where_clause}
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
            cl.fda_dt,
            cl.event_dt,
            cl.mfr_dt,
            cl.sex_std,
            cl.age_value,
            cl.age_unit,
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
