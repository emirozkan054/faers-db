"""Fast DuckDB query layer over query-optimized Parquet warehouse tables."""

from __future__ import annotations

from faersdb.api_models import (
    CaseSearchRequest,
    DrugConcept,
    FilterMetadataResponse,
    ReactionConcept,
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


def _like_pattern(value: str) -> str:
    escaped = (
        value.upper()
        .replace("\\", "\\\\")
        .replace("%", "\\%")
        .replace("_", "\\_")
    )
    return f"%{escaped}%"


def _build_case_clauses(params, query_params: list) -> list[str]:
    filters = params.case_filters
    clauses: list[str] = []

    if filters.quarter:
        _add_clause(clauses, query_params, "c.source_quarter = ?", filters.quarter)
    if filters.report_type:
        _add_clause(clauses, query_params, "c.report_type_search = ?", filters.report_type)
    if filters.initial_or_followup:
        _add_clause(
            clauses, query_params, "c.i_f_code_search = ?", filters.initial_or_followup
        )
    if filters.event_dt_from:
        _add_clause(clauses, query_params, "c.event_dt >= ?", filters.event_dt_from)
    if filters.event_dt_to:
        _add_clause(clauses, query_params, "c.event_dt <= ?", filters.event_dt_to)
    if filters.fda_dt_from:
        _add_clause(clauses, query_params, "c.fda_dt >= ?", filters.fda_dt_from)
    if filters.fda_dt_to:
        _add_clause(clauses, query_params, "c.fda_dt <= ?", filters.fda_dt_to)
    if filters.mfr_dt_from:
        _add_clause(clauses, query_params, "c.mfr_dt >= ?", filters.mfr_dt_from)
    if filters.mfr_dt_to:
        _add_clause(clauses, query_params, "c.mfr_dt <= ?", filters.mfr_dt_to)

    if filters.sex_std:
        _add_clause(clauses, query_params, "c.sex_search = ?", filters.sex_std)
    if filters.age_min is not None:
        _add_clause(clauses, query_params, "c.age_years >= ?", filters.age_min)
    if filters.age_max is not None:
        _add_clause(clauses, query_params, "c.age_years <= ?", filters.age_max)
    if filters.age_unit:
        _add_clause(clauses, query_params, "c.age_cod_search = ?", filters.age_unit)
    if filters.age_group:
        _add_clause(clauses, query_params, "c.age_grp_search = ?", filters.age_group)
    if filters.weight_min is not None:
        _add_clause(clauses, query_params, "c.wt_kg >= ?", filters.weight_min)
    if filters.weight_max is not None:
        _add_clause(clauses, query_params, "c.wt_kg <= ?", filters.weight_max)
    if filters.reporter_country:
        _add_clause(
            clauses,
            query_params,
            "c.reporter_country_search = ?",
            filters.reporter_country,
        )

    return clauses


def _build_drug_concept_clauses(
    term: DrugConcept, alias: str, query_params: list
) -> list[str]:
    clauses: list[str] = []
    if term.drug_name:
        _add_clause(
            clauses,
            query_params,
            f"{alias}.drugname_search LIKE ? ESCAPE '\\'",
            _like_pattern(term.drug_name),
        )
    if term.prod_ai:
        _add_clause(
            clauses,
            query_params,
            f"{alias}.prod_ai_search LIKE ? ESCAPE '\\'",
            _like_pattern(term.prod_ai),
        )
    if term.role_cod:
        _add_clause(clauses, query_params, f"{alias}.role_cod_search = ?", term.role_cod)
    if term.route:
        _add_clause(clauses, query_params, f"{alias}.route_search = ?", term.route)
    if term.dose_unit:
        _add_clause(clauses, query_params, f"{alias}.dose_unit_search = ?", term.dose_unit)
    if term.dose_min is not None:
        _add_clause(clauses, query_params, f"{alias}.dose_amt >= ?", term.dose_min)
    if term.dose_max is not None:
        _add_clause(clauses, query_params, f"{alias}.dose_amt <= ?", term.dose_max)
    return clauses


def _build_outcome_clauses(params, alias: str, query_params: list) -> list[str]:
    filters = params.case_filters
    clauses: list[str] = []
    if filters.case_outcome:
        _add_clause(clauses, query_params, f"{alias}.outc_cod_search = ?", filters.case_outcome)
    return clauses


def _build_indication_clauses(
    term: DrugConcept, alias: str, query_params: list
) -> list[str]:
    clauses: list[str] = []
    if term.indication_pt:
        _add_clause(
            clauses,
            query_params,
            f"{alias}.indi_pt_search LIKE ? ESCAPE '\\'",
            _like_pattern(term.indication_pt),
        )
    return clauses


def _build_therapy_clauses(
    term: DrugConcept, alias: str, query_params: list
) -> list[str]:
    clauses: list[str] = []
    if term.therapy_start_from:
        _add_clause(clauses, query_params, f"{alias}.start_dt >= ?", term.therapy_start_from)
    if term.therapy_start_to:
        _add_clause(clauses, query_params, f"{alias}.start_dt <= ?", term.therapy_start_to)
    if term.therapy_end_from:
        _add_clause(clauses, query_params, f"{alias}.end_dt >= ?", term.therapy_end_from)
    if term.therapy_end_to:
        _add_clause(clauses, query_params, f"{alias}.end_dt <= ?", term.therapy_end_to)
    if term.dur_min is not None:
        _add_clause(clauses, query_params, f"{alias}.dur >= ?", term.dur_min)
    if term.dur_max is not None:
        _add_clause(clauses, query_params, f"{alias}.dur <= ?", term.dur_max)
    if term.dur_cod:
        _add_clause(clauses, query_params, f"{alias}.dur_cod_search = ?", term.dur_cod)
    return clauses


def _build_reaction_concept_clauses(
    term: ReactionConcept, alias: str, query_params: list
) -> list[str]:
    clauses: list[str] = []
    if term.reaction_pt:
        _add_clause(
            clauses,
            query_params,
            f"{alias}.pt_search LIKE ? ESCAPE '\\'",
            _like_pattern(term.reaction_pt),
        )
    return clauses


def _build_drug_concept_match(
    index: int, term: DrugConcept, query_params: list
) -> tuple[list[str], str] | None:
    ctes: list[str] = []
    joins: list[str] = []
    clauses = _build_drug_concept_clauses(term, "d", query_params)

    indication_clauses = _build_indication_clauses(term, "i", query_params)
    if indication_clauses:
        joins.append(
            """
            JOIN latest_indi i ON i.primaryid = d.primaryid
                AND i.drug_seq IS NOT DISTINCT FROM d.drug_seq
            """
        )
        clauses.extend(indication_clauses)

    therapy_clauses = _build_therapy_clauses(term, "th", query_params)
    if therapy_clauses:
        joins.append(
            """
            JOIN latest_ther th ON th.primaryid = d.primaryid
                AND th.drug_seq IS NOT DISTINCT FROM d.drug_seq
            """
        )
        clauses.extend(therapy_clauses)

    if not clauses:
        return None

    cte_name = f"drug_concept_{index}_match"
    ctes.append(
        f"""
        {cte_name} AS MATERIALIZED (
            SELECT DISTINCT d.primaryid
            FROM latest_drug d
            {' '.join(joins)}
            {_where(clauses)}
        )
        """
    )
    return ctes, cte_name


def _build_reaction_concept_match(
    index: int, term: ReactionConcept, query_params: list
) -> tuple[list[str], str] | None:
    clauses = _build_reaction_concept_clauses(term, "r", query_params)
    if not clauses:
        return None
    cte_name = f"reaction_concept_{index}_match"
    return [
        f"""
        {cte_name} AS MATERIALIZED (
            SELECT DISTINCT r.primaryid
            FROM latest_reac r
            {_where(clauses)}
        )
        """
    ], cte_name


def _build_concept_match_parts(params, query_params: list) -> tuple[list[str], str]:
    ctes: list[str] = []
    concept_cte_names: list[str] = []

    concept_index = 0
    for term in params.drug_concept_items():
        result = _build_drug_concept_match(concept_index, term, query_params)
        if result is None:
            continue
        term_ctes, cte_name = result
        ctes.extend(term_ctes)
        concept_cte_names.append(cte_name)
        concept_index += 1

    for term in params.reaction_concept_items():
        result = _build_reaction_concept_match(concept_index, term, query_params)
        if result is None:
            continue
        term_ctes, cte_name = result
        ctes.extend(term_ctes)
        concept_cte_names.append(cte_name)
        concept_index += 1

    if not concept_cte_names:
        return ctes, ""

    union_parts = [
        f"SELECT primaryid, {index} AS concept_index FROM {cte_name}"
        for index, cte_name in enumerate(concept_cte_names)
    ]
    required_count = 1 if params.concept_mode == "any" else len(concept_cte_names)
    ctes.append(
        f"""
        concept_match AS MATERIALIZED (
            SELECT primaryid
            FROM ({' UNION ALL '.join(union_parts)}) term_union
            GROUP BY primaryid
            HAVING count(DISTINCT concept_index) >= {required_count}
        )
        """
    )
    return ctes, "JOIN concept_match cm ON cm.primaryid = c.primaryid"


def _build_reporter_clauses(params, alias: str, query_params: list) -> list[str]:
    filters = params.case_filters
    clauses: list[str] = []
    if filters.reporter_type:
        _add_clause(clauses, query_params, f"{alias}.rpsr_cod_search = ?", filters.reporter_type)
    return clauses


def _build_match_parts(params, query_params: list) -> list[str]:
    """Build materialized match CTEs and the final latest-case match CTE."""
    ctes: list[str] = []
    joins: list[str] = []

    concept_ctes, concept_join = _build_concept_match_parts(params, query_params)
    ctes.extend(concept_ctes)
    if concept_join:
        joins.append(concept_join)

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


def _case_summary_result(params: CaseSearchRequest, *, paginate: bool) -> dict:
    """Search latest cases with compact match sets and case summaries."""
    _ensure_query_warehouse()
    query_params: list = []
    match_parts = _build_match_parts(params, query_params)
    page_params = [*query_params]

    if paginate:
        paged_cte = f"""
            paged AS MATERIALIZED (
                SELECT primaryid, source_quarter, total
                FROM matched_with_total
                ORDER BY source_quarter DESC, primaryid DESC
                LIMIT ${len(query_params) + 1}
                OFFSET ${len(query_params) + 2}
            )
        """
        page_params.extend([params.limit, params.offset])
    else:
        paged_cte = """
            paged AS MATERIALIZED (
                SELECT primaryid, source_quarter, total
                FROM matched_with_total
                ORDER BY source_quarter DESC, primaryid DESC
            )
        """

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
            paged_cte,
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
        "limit": params.limit if paginate else total,
        "offset": params.offset if paginate else 0,
        "items": items,
    }


def search_cases(params: CaseSearchRequest) -> dict:
    """Search latest cases with pagination for interactive browsing."""
    return _case_summary_result(params, paginate=True)


def export_cases(params: CaseSearchRequest) -> dict:
    """Return every latest case matching the supplied filters."""
    return _case_summary_result(params, paginate=False)


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
