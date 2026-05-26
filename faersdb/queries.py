"""Fast DuckDB query layer over query-optimized Parquet warehouse tables."""

from __future__ import annotations

from faersdb.api_models import (
    CaseSearchParams,
    FilterMetadataResponse,
    PrimaryTerm,
)
from faersdb.db import get_conn, missing_query_tables


REBUILD_REQUIRED_MESSAGE = (
    "Query-optimized warehouse tables are missing. Run "
    "`uv run python -m faersdb build` to rebuild the warehouse."
)


class QueryWarehouseError(RuntimeError):
    """Raised when the query-optimized warehouse has not been built."""


def _ensure_query_warehouse() -> None:
    missing = missing_query_tables()
    if missing:
        raise QueryWarehouseError(
            f"{REBUILD_REQUIRED_MESSAGE} Missing: {', '.join(missing)}."
        )


def _add_clause(
    clauses: list[str],
    query_params: list,
    expr: str,
    value,
) -> None:
    query_params.append(value)
    clauses.append(expr.replace("?", f"${len(query_params)}"))


def _where(clauses: list[str]) -> str:
    if not clauses:
        return ""
    return " WHERE " + " AND ".join(f"({clause})" for clause in clauses)


def _build_case_clauses(params, query_params: list) -> list[str]:
    clauses: list[str] = []

    if params.quarter:
        _add_clause(clauses, query_params, "c.source_quarter = ?", params.quarter)
    if params.report_type:
        _add_clause(clauses, query_params, "c.report_type_search = ?", params.report_type)
    if params.initial_or_followup:
        _add_clause(
            clauses, query_params, "c.i_f_code_search = ?", params.initial_or_followup
        )
    if params.event_dt_from:
        _add_clause(clauses, query_params, "c.event_dt >= ?", params.event_dt_from)
    if params.event_dt_to:
        _add_clause(clauses, query_params, "c.event_dt <= ?", params.event_dt_to)
    if params.fda_dt_from:
        _add_clause(clauses, query_params, "c.fda_dt >= ?", params.fda_dt_from)
    if params.fda_dt_to:
        _add_clause(clauses, query_params, "c.fda_dt <= ?", params.fda_dt_to)
    if params.mfr_dt_from:
        _add_clause(clauses, query_params, "c.mfr_dt >= ?", params.mfr_dt_from)
    if params.mfr_dt_to:
        _add_clause(clauses, query_params, "c.mfr_dt <= ?", params.mfr_dt_to)

    if params.sex_std:
        _add_clause(clauses, query_params, "c.sex_search = ?", params.sex_std)
    if params.age_min is not None:
        _add_clause(clauses, query_params, "c.age >= ?", params.age_min)
    if params.age_max is not None:
        _add_clause(clauses, query_params, "c.age <= ?", params.age_max)
    if params.age_unit:
        _add_clause(clauses, query_params, "c.age_cod_search = ?", params.age_unit)
    if params.age_group:
        _add_clause(clauses, query_params, "c.age_grp_search = ?", params.age_group)
    if params.weight_min is not None:
        _add_clause(clauses, query_params, "c.wt_kg >= ?", params.weight_min)
    if params.weight_max is not None:
        _add_clause(clauses, query_params, "c.wt_kg <= ?", params.weight_max)
    if params.reporter_country:
        _add_clause(
            clauses,
            query_params,
            "c.reporter_country_search = ?",
            params.reporter_country,
        )

    return clauses


def _build_global_drug_clauses(params, alias: str, query_params: list) -> list[str]:
    clauses: list[str] = []
    if params.role_cod:
        _add_clause(clauses, query_params, f"{alias}.role_cod_search = ?", params.role_cod)
    if params.route:
        _add_clause(clauses, query_params, f"{alias}.route_search = ?", params.route)
    if params.dose_unit:
        _add_clause(clauses, query_params, f"{alias}.dose_unit_search = ?", params.dose_unit)
    if params.dose_min is not None:
        _add_clause(clauses, query_params, f"{alias}.dose_amt >= ?", params.dose_min)
    if params.dose_max is not None:
        _add_clause(clauses, query_params, f"{alias}.dose_amt <= ?", params.dose_max)
    return clauses


def _build_term_drug_clauses(
    term: PrimaryTerm, params, drug_alias: str, query_params: list
) -> list[str]:
    clauses: list[str] = []
    if term.drug_name:
        _add_clause(
            clauses,
            query_params,
            f"{drug_alias}.drugname_search LIKE ?",
            f"%{term.drug_name.upper()}%",
        )
    if term.prod_ai:
        _add_clause(
            clauses,
            query_params,
            f"{drug_alias}.prod_ai_search LIKE ?",
            f"%{term.prod_ai.upper()}%",
        )
    clauses.extend(_build_global_drug_clauses(params, drug_alias, query_params))
    return clauses


def _build_term_indication_clauses(
    term: PrimaryTerm, indication_alias: str, query_params: list
) -> list[str]:
    clauses: list[str] = []
    if term.indication_pt:
        _add_clause(
            clauses,
            query_params,
            f"{indication_alias}.indi_pt_search LIKE ?",
            f"%{term.indication_pt.upper()}%",
        )
    return clauses


def _build_reaction_clauses(term: PrimaryTerm, alias: str, query_params: list) -> list[str]:
    clauses: list[str] = []
    if term.reaction_pt:
        _add_clause(
            clauses,
            query_params,
            f"{alias}.pt_search LIKE ?",
            f"%{term.reaction_pt.upper()}%",
        )
    return clauses


def _build_outcome_clauses(params, alias: str, query_params: list) -> list[str]:
    clauses: list[str] = []
    if params.case_outcome:
        _add_clause(clauses, query_params, f"{alias}.outc_cod_search = ?", params.case_outcome)
    return clauses


def _build_therapy_clauses(params, alias: str, query_params: list) -> list[str]:
    clauses: list[str] = []
    if params.therapy_start_from:
        _add_clause(clauses, query_params, f"{alias}.start_dt >= ?", params.therapy_start_from)
    if params.therapy_start_to:
        _add_clause(clauses, query_params, f"{alias}.start_dt <= ?", params.therapy_start_to)
    if params.therapy_end_from:
        _add_clause(clauses, query_params, f"{alias}.end_dt >= ?", params.therapy_end_from)
    if params.therapy_end_to:
        _add_clause(clauses, query_params, f"{alias}.end_dt <= ?", params.therapy_end_to)
    if params.dur_min is not None:
        _add_clause(clauses, query_params, f"{alias}.dur >= ?", params.dur_min)
    if params.dur_max is not None:
        _add_clause(clauses, query_params, f"{alias}.dur <= ?", params.dur_max)
    if params.dur_cod:
        _add_clause(clauses, query_params, f"{alias}.dur_cod_search = ?", params.dur_cod)
    return clauses


def _has_global_drug_filters(params) -> bool:
    return any(
        value is not None and value != ""
        for value in (
            params.role_cod,
            params.route,
            params.dose_unit,
            params.dose_min,
            params.dose_max,
            params.therapy_start_from,
            params.therapy_start_to,
            params.therapy_end_from,
            params.therapy_end_to,
            params.dur_min,
            params.dur_max,
            params.dur_cod,
        )
    )


def _term_needs_drug_match(term: PrimaryTerm, params) -> bool:
    return any(
        value is not None and value != ""
        for value in (term.drug_name, term.prod_ai, term.indication_pt)
    ) or _has_global_drug_filters(params)


def _build_term_match_parts(params, query_params: list) -> tuple[list[str], str]:
    terms = params.primary_term_items()
    if not terms:
        if not _has_global_drug_filters(params):
            return [], ""
        terms = [PrimaryTerm()]

    ctes: list[str] = []
    term_cte_names: list[str] = []

    for index, term in enumerate(terms):
        source_ctes: list[tuple[str, str]] = []

        if _term_needs_drug_match(term, params):
            drug_cte = f"term_{index}_drug_match"
            joins = []
            clauses = _build_term_drug_clauses(term, params, "d", query_params)

            indication_clauses = _build_term_indication_clauses(term, "i", query_params)
            if indication_clauses:
                joins.append(
                    """
                    JOIN latest_indi i ON i.primaryid = d.primaryid
                        AND i.drug_seq IS NOT DISTINCT FROM d.drug_seq
                    """
                )
                clauses.extend(indication_clauses)

            therapy_clauses = _build_therapy_clauses(params, "th", query_params)
            if therapy_clauses:
                joins.append(
                    """
                    JOIN latest_ther th ON th.primaryid = d.primaryid
                        AND th.drug_seq IS NOT DISTINCT FROM d.drug_seq
                    """
                )
                clauses.extend(therapy_clauses)

            ctes.append(
                f"""
                {drug_cte} AS MATERIALIZED (
                    SELECT DISTINCT d.primaryid
                    FROM latest_drug d
                    {' '.join(joins)}
                    {_where(clauses)}
                )
                """
            )
            source_ctes.append((drug_cte, "dm"))

        reaction_clauses = _build_reaction_clauses(term, "r", query_params)
        if reaction_clauses:
            reaction_cte = f"term_{index}_reaction_match"
            ctes.append(
                f"""
                {reaction_cte} AS MATERIALIZED (
                    SELECT DISTINCT r.primaryid
                    FROM latest_reac r
                    {_where(reaction_clauses)}
                )
                """
            )
            source_ctes.append((reaction_cte, "rm"))

        if not source_ctes:
            continue

        term_cte = f"term_{index}_match"
        first_cte, first_alias = source_ctes[0]
        joins = " ".join(
            f"JOIN {cte_name} {alias} ON {alias}.primaryid = {first_alias}.primaryid"
            for cte_name, alias in source_ctes[1:]
        )
        ctes.append(
            f"""
            {term_cte} AS MATERIALIZED (
                SELECT DISTINCT {first_alias}.primaryid
                FROM {first_cte} {first_alias}
                {joins}
            )
            """
        )
        term_cte_names.append(term_cte)

    if not term_cte_names:
        return ctes, ""

    union_parts = [
        f"SELECT primaryid, {index} AS term_index FROM {cte_name}"
        for index, cte_name in enumerate(term_cte_names)
    ]
    required_count = 1 if params.primary_term_mode == "any" else len(term_cte_names)
    ctes.append(
        f"""
        primary_term_match AS MATERIALIZED (
            SELECT primaryid
            FROM ({' UNION ALL '.join(union_parts)}) term_union
            GROUP BY primaryid
            HAVING count(DISTINCT term_index) >= {required_count}
        )
        """
    )
    return ctes, "JOIN primary_term_match ptm ON ptm.primaryid = c.primaryid"


def _build_reporter_clauses(params, alias: str, query_params: list) -> list[str]:
    clauses: list[str] = []
    if params.reporter_type:
        _add_clause(clauses, query_params, f"{alias}.rpsr_cod_search = ?", params.reporter_type)
    return clauses


def _build_match_parts(params, query_params: list) -> list[str]:
    """Build materialized match CTEs and the final latest-case match CTE."""
    ctes: list[str] = []
    joins: list[str] = []

    term_ctes, term_join = _build_term_match_parts(params, query_params)
    ctes.extend(term_ctes)
    if term_join:
        joins.append(term_join)

    child_specs = [
        ("outc_match", "om", "latest_outc", "o", _build_outcome_clauses),
        ("rpsr_match", "rpm", "latest_rpsr", "rp", _build_reporter_clauses),
    ]

    for cte_name, join_alias, table_name, table_alias, builder in child_specs:
        clauses = builder(params, table_alias, query_params)
        if not clauses:
            continue
        ctes.append(
            f"""
            {cte_name} AS MATERIALIZED (
                SELECT DISTINCT {table_alias}.primaryid
                FROM {table_name} {table_alias}
                {_where(clauses)}
            )
            """
        )
        joins.append(
            f"JOIN {cte_name} {join_alias} ON {join_alias}.primaryid = c.primaryid"
        )

    case_clauses = _build_case_clauses(params, query_params)
    ctes.append(
        f"""
        matched AS MATERIALIZED (
            SELECT c.primaryid, c.source_quarter
            FROM latest_demo c
            {' '.join(joins)}
            {_where(case_clauses)}
        )
        """
    )
    return ctes


def _with_clause(parts: list[str]) -> str:
    return "WITH " + ",\n".join(parts)


def search_cases(params: CaseSearchParams) -> dict:
    """Search latest cases with compact match sets and case summaries."""
    _ensure_query_warehouse()
    query_params: list = []
    match_parts = _build_match_parts(params, query_params)

    data_sql = f"""
        {_with_clause([
            *match_parts,
            '''
            matched_with_total AS MATERIALIZED (
                SELECT
                    primaryid,
                    source_quarter,
                    count(*) OVER ()::int AS total
                FROM matched
            )
            ''',
            f'''
            paged AS MATERIALIZED (
                SELECT primaryid, source_quarter, total
                FROM matched_with_total
                ORDER BY source_quarter DESC, primaryid DESC
                LIMIT ${len(query_params) + 1}
                OFFSET ${len(query_params) + 2}
            )
            ''',
            '''
            drug_lists AS MATERIALIZED (
                SELECT
                    d.primaryid,
                    list(DISTINCT d.drugname ORDER BY d.drugname)
                        FILTER (WHERE d.drugname IS NOT NULL) AS drugs,
                    list(DISTINCT d.prod_ai ORDER BY d.prod_ai)
                        FILTER (WHERE d.prod_ai IS NOT NULL) AS active_ingredients,
                    list(DISTINCT d.role_cod ORDER BY d.role_cod)
                        FILTER (WHERE d.role_cod IS NOT NULL) AS role_codes,
                    list(DISTINCT d.route ORDER BY d.route)
                        FILTER (WHERE d.route IS NOT NULL) AS routes
                FROM latest_drug d
                JOIN paged p ON p.primaryid = d.primaryid
                GROUP BY d.primaryid
            )
            ''',
            '''
            indication_lists AS MATERIALIZED (
                SELECT
                    i.primaryid,
                    list(DISTINCT i.indi_pt ORDER BY i.indi_pt)
                        FILTER (WHERE i.indi_pt IS NOT NULL) AS indications
                FROM latest_indi i
                JOIN paged p ON p.primaryid = i.primaryid
                GROUP BY i.primaryid
            )
            ''',
            '''
            reaction_lists AS MATERIALIZED (
                SELECT
                    r.primaryid,
                    list(DISTINCT r.pt ORDER BY r.pt)
                        FILTER (WHERE r.pt IS NOT NULL) AS reactions
                FROM latest_reac r
                JOIN paged p ON p.primaryid = r.primaryid
                GROUP BY r.primaryid
            )
            ''',
            '''
            outcome_lists AS MATERIALIZED (
                SELECT
                    o.primaryid,
                    list(DISTINCT o.outc_cod ORDER BY o.outc_cod)
                        FILTER (WHERE o.outc_cod IS NOT NULL) AS outcomes
                FROM latest_outc o
                JOIN paged p ON p.primaryid = o.primaryid
                GROUP BY o.primaryid
            )
            ''',
            '''
            reporter_lists AS MATERIALIZED (
                SELECT
                    rp.primaryid,
                    list(DISTINCT rp.rpsr_cod ORDER BY rp.rpsr_cod)
                        FILTER (WHERE rp.rpsr_cod IS NOT NULL) AS reporter_types
                FROM latest_rpsr rp
                JOIN paged p ON p.primaryid = rp.primaryid
                GROUP BY rp.primaryid
            )
            '''
        ])}
        SELECT
            p.total AS _total,
            s.case_version_pk,
            s.canonical_case_id,
            s.source_case_id,
            s.source_report_id,
            s.source_quarter,
            s.source_system,
            s.case_version_num,
            s.report_type,
            s.initial_or_followup,
            s.fda_dt,
            s.event_dt,
            s.mfr_dt,
            s.sex_std,
            s.age_value,
            s.age_unit,
            s.age_group,
            s.weight_kg,
            s.reporter_country,
            coalesce(dl.drugs, CAST([] AS VARCHAR[])) AS drugs,
            coalesce(dl.active_ingredients, CAST([] AS VARCHAR[])) AS active_ingredients,
            coalesce(dl.role_codes, CAST([] AS VARCHAR[])) AS role_codes,
            coalesce(dl.routes, CAST([] AS VARCHAR[])) AS routes,
            coalesce(il.indications, CAST([] AS VARCHAR[])) AS indications,
            coalesce(rl.reactions, CAST([] AS VARCHAR[])) AS reactions,
            coalesce(ol.outcomes, CAST([] AS VARCHAR[])) AS outcomes,
            coalesce(rpl.reporter_types, CAST([] AS VARCHAR[])) AS reporter_types
        FROM paged p
        JOIN case_summary s ON s.case_version_pk = p.primaryid
        LEFT JOIN drug_lists dl ON dl.primaryid = p.primaryid
        LEFT JOIN indication_lists il ON il.primaryid = p.primaryid
        LEFT JOIN reaction_lists rl ON rl.primaryid = p.primaryid
        LEFT JOIN outcome_lists ol ON ol.primaryid = p.primaryid
        LEFT JOIN reporter_lists rpl ON rpl.primaryid = p.primaryid
        ORDER BY p.source_quarter DESC, p.primaryid DESC
    """

    conn = get_conn()
    try:
        page_params = [*query_params, params.limit, params.offset]
        rows = conn.execute(data_sql, page_params).fetchall()
        columns = [desc[0] for desc in conn.description][1:]
        if rows:
            total = rows[0][0]
            items = [_row_to_dict(columns, row[1:]) for row in rows]
        else:
            count_sql = f"""
                {_with_clause(match_parts)}
                SELECT count(*)::int AS total
                FROM matched
            """
            total = conn.execute(count_sql, query_params).fetchone()[0]
            items = []
    finally:
        conn.close()

    return {
        "total": total,
        "limit": params.limit,
        "offset": params.offset,
        "items": items,
    }


def get_filter_metadata() -> dict:
    """Return precomputed distinct values for each filter dropdown."""
    _ensure_query_warehouse()
    conn = get_conn()
    try:
        row = conn.execute("SELECT * FROM filter_metadata LIMIT 1").fetchone()
        if row is None:
            return FilterMetadataResponse(
                quarters=[],
                report_types=[],
                initial_or_followup_values=[],
                sex_values=[],
                age_units=[],
                age_groups=[],
                reporter_countries=[],
                role_codes=[],
                routes=[],
                dose_units=[],
                case_outcomes=[],
                reporter_types=[],
                dur_codes=[],
            ).model_dump()
        columns = [desc[0] for desc in conn.description]
        result = _row_to_dict(columns, row)
    finally:
        conn.close()

    return FilterMetadataResponse.model_validate(result).model_dump()


def get_case_detail(case_version_pk: int | str) -> dict | None:
    """Get full detail for one latest case by primaryid."""
    _ensure_query_warehouse()
    pid = str(case_version_pk)

    conn = get_conn()
    try:
        header_sql = """
            SELECT
                case_version_pk,
                canonical_case_id,
                source_case_id,
                source_report_id,
                source_quarter,
                source_system,
                case_version_num,
                report_type,
                initial_or_followup,
                fda_dt,
                event_dt,
                mfr_dt,
                sex_std,
                age_value,
                age_unit,
                age_group,
                weight_kg,
                reporter_country,
                coalesce((
                    SELECT list(DISTINCT o.outc_cod ORDER BY o.outc_cod)
                    FROM latest_outc o
                    WHERE o.primaryid = s.case_version_pk
                      AND o.outc_cod IS NOT NULL
                ), CAST([] AS VARCHAR[])) AS outcomes,
                coalesce((
                    SELECT list(DISTINCT rp.rpsr_cod ORDER BY rp.rpsr_cod)
                    FROM latest_rpsr rp
                    WHERE rp.primaryid = s.case_version_pk
                      AND rp.rpsr_cod IS NOT NULL
                ), CAST([] AS VARCHAR[])) AS reporter_types
            FROM case_summary s
            WHERE s.case_version_pk = $1
        """
        header_row = conn.execute(header_sql, [pid]).fetchone()
        if not header_row:
            return None
        header_cols = [desc[0] for desc in conn.description]
        header = _row_to_dict(header_cols, header_row)

        drugs_sql = """
            SELECT
                d.drug_seq,
                d.role_cod,
                d.drugname,
                d.prod_ai,
                d.route,
                d.dose_vbm,
                d.dose_amt,
                d.dose_unit,
                d.start_dt,
                d.end_dt,
                coalesce(list(DISTINCT i.indi_pt ORDER BY i.indi_pt)
                    FILTER (WHERE i.indi_pt IS NOT NULL), CAST([] AS VARCHAR[]))
                    AS indications,
                min(th.start_dt) AS therapy_start_dt,
                max(th.end_dt) AS therapy_end_dt
            FROM latest_drug d
            LEFT JOIN latest_indi i ON i.primaryid = d.primaryid
                AND i.drug_seq IS NOT DISTINCT FROM d.drug_seq
            LEFT JOIN latest_ther th ON th.primaryid = d.primaryid
                AND th.drug_seq IS NOT DISTINCT FROM d.drug_seq
            WHERE d.primaryid = $1
            GROUP BY d.drug_seq, d.role_cod, d.drugname, d.prod_ai,
                     d.route, d.dose_vbm, d.dose_amt, d.dose_unit,
                     d.start_dt, d.end_dt
            ORDER BY d.drug_seq NULLS LAST, d.drugname NULLS LAST
        """
        drug_rows = conn.execute(drugs_sql, [pid]).fetchall()
        drug_cols = [desc[0] for desc in conn.description]
        drugs = [_row_to_dict(drug_cols, row) for row in drug_rows]

        reactions_sql = """
            SELECT DISTINCT
                r.pt AS reaction_pt
            FROM latest_reac r
            WHERE r.primaryid = $1
            ORDER BY r.pt
        """
        reac_rows = conn.execute(reactions_sql, [pid]).fetchall()
        reac_cols = [desc[0] for desc in conn.description]
        reactions = [_row_to_dict(reac_cols, row) for row in reac_rows]

    finally:
        conn.close()

    header["case_pk"] = header["case_version_pk"]
    header["drugs"] = drugs
    header["reactions"] = reactions
    return header


def _row_to_dict(columns: list[str], row: tuple) -> dict:
    """Convert a DuckDB result row to a dict, handling special types."""
    result = {}
    for col, val in zip(columns, row):
        if isinstance(val, list):
            result[col] = [str(v) if v is not None else None for v in val]
        elif hasattr(val, "isoformat"):
            result[col] = val.isoformat() if val is not None else None
        else:
            result[col] = val
    return result
