"""Query layer: DuckDB SQL against Parquet warehouse.

All queries use the "latest case version" CTE to only return the most
recent, non-deleted version of each case — computed on-the-fly via
DuckDB window functions (sub-second even on 30M+ rows).
"""

from __future__ import annotations

from faersdb.api_models import (
    CaseSearchParams,
    DrugReactionAggregateParams,
    FilterMetadataResponse,
)
from faersdb.db import get_conn

# ─── Latest-case CTE ─────────────────────────────────────────────────────────

LATEST_CTE = """
    latest AS (
        SELECT *, ROW_NUMBER() OVER (
            PARTITION BY caseid
            ORDER BY
                caseversion DESC NULLS LAST,
                COALESCE(fda_dt, event_dt, mfr_dt) DESC NULLS LAST,
                source_quarter DESC,
                primaryid DESC
        ) AS _rn
        FROM demo
        WHERE NOT is_deleted
    ),
    cases AS (
        SELECT * FROM latest WHERE _rn = 1
    )
"""

# ─── Filter Builder ───────────────────────────────────────────────────────────


def _build_filter_sql(params) -> tuple[str, list]:
    """Build WHERE clause and parameters from research filter params.

    Uses DuckDB's $N parameter syntax via positional params list.
    """
    clauses: list[str] = []
    query_params: list = []

    def _add(expr: str, value):
        query_params.append(value)
        clauses.append(expr.replace("?", f"${len(query_params)}"))

    # ── Case / Time ──
    if params.quarter:
        _add("c.source_quarter = ?", params.quarter)
    if params.report_type:
        _add("upper(coalesce(c.report_type, '')) = ?", params.report_type)
    if params.initial_or_followup:
        _add("upper(coalesce(c.i_f_code, '')) = ?", params.initial_or_followup)
    if params.event_dt_from:
        _add("c.event_dt >= ?", params.event_dt_from)
    if params.event_dt_to:
        _add("c.event_dt <= ?", params.event_dt_to)
    if params.fda_dt_from:
        _add("c.fda_dt >= ?", params.fda_dt_from)
    if params.fda_dt_to:
        _add("c.fda_dt <= ?", params.fda_dt_to)
    if params.mfr_dt_from:
        _add("c.mfr_dt >= ?", params.mfr_dt_from)
    if params.mfr_dt_to:
        _add("c.mfr_dt <= ?", params.mfr_dt_to)

    # ── Demographics ──
    if params.sex_std:
        _add("upper(coalesce(c.sex, '')) = ?", params.sex_std)
    if params.age_min is not None:
        _add("c.age >= ?", params.age_min)
    if params.age_max is not None:
        _add("c.age <= ?", params.age_max)
    if params.age_unit:
        _add("upper(coalesce(c.age_cod, '')) = ?", params.age_unit)
    if params.age_group:
        _add("upper(coalesce(c.age_grp, '')) = ?", params.age_group)
    if params.weight_min is not None:
        _add("c.wt_kg >= ?", params.weight_min)
    if params.weight_max is not None:
        _add("c.wt_kg <= ?", params.weight_max)
    if params.reporter_country:
        _add("upper(coalesce(c.reporter_country, '')) = ?", params.reporter_country)

    # ── Drug ──
    if params.drug_name:
        _add("upper(coalesce(d.drugname, '')) LIKE ?", f"%{params.drug_name.upper()}%")
    if params.prod_ai:
        _add("upper(coalesce(d.prod_ai, '')) LIKE ?", f"%{params.prod_ai.upper()}%")
    if params.role_cod:
        _add("upper(coalesce(d.role_cod, '')) = ?", params.role_cod)
    if params.route:
        _add("upper(coalesce(d.route, '')) = ?", params.route)
    if params.dose_unit:
        _add("upper(coalesce(d.dose_unit, '')) = ?", params.dose_unit)
    if params.dose_min is not None:
        _add("d.dose_amt >= ?", params.dose_min)
    if params.dose_max is not None:
        _add("d.dose_amt <= ?", params.dose_max)

    # ── Reaction ──
    if params.reaction_pt:
        _add("upper(coalesce(r.pt, '')) LIKE ?", f"%{params.reaction_pt.upper()}%")
    if params.reaction_outcome:
        _add("upper(coalesce(r.drug_rec_act, '')) = ?", params.reaction_outcome)

    # ── Case outcome ──
    if params.case_outcome:
        _add("upper(coalesce(o.outc_cod, '')) = ?", params.case_outcome)

    # ── Indication ──
    if params.indication_pt:
        _add("upper(coalesce(i.indi_pt, '')) LIKE ?", f"%{params.indication_pt.upper()}%")

    # ── Therapy ──
    if params.therapy_start_from:
        _add("th.start_dt >= ?", params.therapy_start_from)
    if params.therapy_start_to:
        _add("th.start_dt <= ?", params.therapy_start_to)
    if params.therapy_end_from:
        _add("th.end_dt >= ?", params.therapy_end_from)
    if params.therapy_end_to:
        _add("th.end_dt <= ?", params.therapy_end_to)
    if params.dur_min is not None:
        _add("th.dur >= ?", params.dur_min)
    if params.dur_max is not None:
        _add("th.dur <= ?", params.dur_max)
    if params.dur_cod:
        _add("upper(coalesce(th.dur_cod, '')) = ?", params.dur_cod)

    # ── Reporter ──
    if params.reporter_type:
        _add("upper(coalesce(rp.rpsr_cod, '')) = ?", params.reporter_type)

    where = ""
    if clauses:
        where = " WHERE " + " AND ".join(f"({c})" for c in clauses)

    return where, query_params


def _needs_join(params, *tables: str) -> dict[str, bool]:
    """Determine which joins are needed based on active filters."""
    needs = {t: False for t in tables}

    if any(getattr(params, f, None) is not None for f in [
        "drug_name", "prod_ai", "role_cod", "route", "dose_unit", "dose_min", "dose_max"
    ]):
        needs["drug"] = True

    if any(getattr(params, f, None) is not None for f in [
        "reaction_pt", "reaction_outcome"
    ]):
        needs["reac"] = True

    if getattr(params, "case_outcome", None) is not None:
        needs["outc"] = True

    if any(getattr(params, f, None) is not None for f in [
        "indication_pt"
    ]):
        needs["indi"] = True

    if any(getattr(params, f, None) is not None for f in [
        "therapy_start_from", "therapy_start_to", "therapy_end_from",
        "therapy_end_to", "dur_min", "dur_max", "dur_cod"
    ]):
        needs["ther"] = True

    if getattr(params, "reporter_type", None) is not None:
        needs["rpsr"] = True

    return needs


def _build_filter_joins(params) -> str:
    """Build the JOIN clauses needed for the active filters."""
    needs = _needs_join(
        params, "drug", "reac", "outc", "indi", "ther", "rpsr"
    )
    joins = []
    if needs.get("drug"):
        joins.append("JOIN drug d ON d.primaryid = c.primaryid")
    if needs.get("reac"):
        joins.append("JOIN reac r ON r.primaryid = c.primaryid")
    if needs.get("outc"):
        joins.append("JOIN outc o ON o.primaryid = c.primaryid")
    if needs.get("indi"):
        joins.append("JOIN indi i ON i.primaryid = c.primaryid")
    if needs.get("ther"):
        joins.append("JOIN ther th ON th.primaryid = c.primaryid")
    if needs.get("rpsr"):
        joins.append("JOIN rpsr rp ON rp.primaryid = c.primaryid")
    return "\n    ".join(joins)


# ─── Public Query Functions ───────────────────────────────────────────────────


def search_cases(params: CaseSearchParams) -> dict:
    """Search for latest cases matching filters, with pagination."""
    where_clause, query_params = _build_filter_sql(params)
    filter_joins = _build_filter_joins(params)

    # Count distinct matching cases
    count_sql = f"""
        WITH {LATEST_CTE}
        SELECT count(DISTINCT c.primaryid)::int AS total
        FROM cases c
        {filter_joins}
        {where_clause}
    """

    # Fetch paged case data with enriched lists
    data_sql = f"""
        WITH {LATEST_CTE},
        matched AS (
            SELECT DISTINCT c.primaryid
            FROM cases c
            {filter_joins}
            {where_clause}
        ),
        paged AS (
            SELECT c.*
            FROM cases c
            JOIN matched m ON m.primaryid = c.primaryid
            ORDER BY c.source_quarter DESC, c.primaryid DESC
            LIMIT ${len(query_params) + 1}
            OFFSET ${len(query_params) + 2}
        )
        SELECT
            p.primaryid AS case_version_pk,
            p.source_system || ':' || p.caseid AS canonical_case_id,
            p.caseid AS source_case_id,
            p.primaryid AS source_report_id,
            p.source_quarter,
            p.source_system,
            p.caseversion AS case_version_num,
            p.report_type,
            p.i_f_code AS initial_or_followup,
            p.fda_dt,
            p.event_dt,
            p.mfr_dt,
            p.sex AS sex_std,
            p.age AS age_value,
            p.age_cod AS age_unit,
            p.age_grp AS age_group,
            p.wt_kg AS weight_kg,
            p.reporter_country,
            coalesce(list(DISTINCT d.drugname ORDER BY d.drugname) FILTER (WHERE d.drugname IS NOT NULL), []) AS drugs,
            coalesce(list(DISTINCT d.prod_ai ORDER BY d.prod_ai) FILTER (WHERE d.prod_ai IS NOT NULL), []) AS active_ingredients,
            coalesce(list(DISTINCT d.role_cod ORDER BY d.role_cod) FILTER (WHERE d.role_cod IS NOT NULL), []) AS role_codes,
            coalesce(list(DISTINCT d.route ORDER BY d.route) FILTER (WHERE d.route IS NOT NULL), []) AS routes,
            coalesce(list(DISTINCT i.indi_pt ORDER BY i.indi_pt) FILTER (WHERE i.indi_pt IS NOT NULL), []) AS indications,
            coalesce(list(DISTINCT r.pt ORDER BY r.pt) FILTER (WHERE r.pt IS NOT NULL), []) AS reactions,
            coalesce(list(DISTINCT o.outc_cod ORDER BY o.outc_cod) FILTER (WHERE o.outc_cod IS NOT NULL), []) AS outcomes,
            coalesce(list(DISTINCT rp.rpsr_cod ORDER BY rp.rpsr_cod) FILTER (WHERE rp.rpsr_cod IS NOT NULL), []) AS reporter_types
        FROM paged p
        LEFT JOIN drug d ON d.primaryid = p.primaryid
        LEFT JOIN reac r ON r.primaryid = p.primaryid
        LEFT JOIN outc o ON o.primaryid = p.primaryid
        LEFT JOIN indi i ON i.primaryid = p.primaryid
        LEFT JOIN rpsr rp ON rp.primaryid = p.primaryid
        GROUP BY ALL
        ORDER BY p.source_quarter DESC, p.primaryid DESC
    """

    conn = get_conn()
    try:
        total = conn.execute(count_sql, query_params).fetchone()[0]

        page_params = [*query_params, params.limit, params.offset]
        rows = conn.execute(data_sql, page_params).fetchall()
        columns = [desc[0] for desc in conn.description]
        items = [_row_to_dict(columns, row) for row in rows]
    finally:
        conn.close()

    return {
        "total": total,
        "limit": params.limit,
        "offset": params.offset,
        "items": items,
    }


def aggregate_drug_reactions(params: DrugReactionAggregateParams) -> dict:
    """Aggregate drug-reaction pair counts for matching cases."""
    where_clause, query_params = _build_filter_sql(params)
    filter_joins = _build_filter_joins(params)

    # Ensure drug + reac are joined (they're needed for the grouping)
    if "JOIN drug d" not in filter_joins:
        filter_joins = "JOIN drug d ON d.primaryid = c.primaryid\n    " + filter_joins
    if "JOIN reac r" not in filter_joins:
        filter_joins = filter_joins + "\n    JOIN reac r ON r.primaryid = c.primaryid"

    base_sql = f"""
        WITH {LATEST_CTE},
        grouped AS (
            SELECT
                d.drugname,
                r.pt AS reaction_pt,
                count(DISTINCT c.caseid)::int AS case_count
            FROM cases c
            {filter_joins}
            {where_clause}
            {"AND" if where_clause else "WHERE"} d.drugname IS NOT NULL
              AND r.pt IS NOT NULL
            GROUP BY d.drugname, r.pt
        )
    """

    count_sql = base_sql + "SELECT count(*)::int AS total FROM grouped"
    data_sql = base_sql + f"""
        SELECT drugname, reaction_pt, case_count
        FROM grouped
        ORDER BY case_count DESC, drugname, reaction_pt
        LIMIT ${len(query_params) + 1}
        OFFSET ${len(query_params) + 2}
    """

    conn = get_conn()
    try:
        total = conn.execute(count_sql, query_params).fetchone()[0]
        page_params = [*query_params, params.limit, params.offset]
        rows = conn.execute(data_sql, page_params).fetchall()
        columns = [desc[0] for desc in conn.description]
        items = [_row_to_dict(columns, row) for row in rows]
    finally:
        conn.close()

    return {
        "total": total,
        "limit": params.limit,
        "offset": params.offset,
        "items": items,
    }


def get_filter_metadata() -> dict:
    """Return distinct values for each filter dropdown."""
    conn = get_conn()
    try:
        # Check if warehouse tables exist
        tables = [r[0] for r in conn.execute("SHOW TABLES").fetchall()]
        if "demo" not in tables:
            return FilterMetadataResponse(
                quarters=[], report_types=[], initial_or_followup_values=[],
                sex_values=[], age_units=[], age_groups=[],
                reporter_countries=[], role_codes=[], routes=[],
                dose_units=[], reaction_outcomes=[], case_outcomes=[],
                reporter_types=[], dur_codes=[],
            ).model_dump()

        def _distinct(sql: str) -> list[str]:
            return [r[0] for r in conn.execute(sql).fetchall() if r[0] is not None]

        result = {
            "quarters": _distinct(
                "SELECT DISTINCT source_quarter FROM demo ORDER BY source_quarter"
            ),
            "report_types": _distinct(
                "SELECT DISTINCT report_type FROM demo WHERE report_type IS NOT NULL ORDER BY report_type"
            ),
            "initial_or_followup_values": _distinct(
                "SELECT DISTINCT i_f_code FROM demo WHERE i_f_code IS NOT NULL ORDER BY i_f_code"
            ),
            "sex_values": _distinct(
                "SELECT DISTINCT sex FROM demo WHERE sex IS NOT NULL ORDER BY sex"
            ),
            "age_units": _distinct(
                "SELECT DISTINCT age_cod FROM demo WHERE age_cod IS NOT NULL ORDER BY age_cod"
            ),
            "age_groups": _distinct(
                "SELECT DISTINCT age_grp FROM demo WHERE age_grp IS NOT NULL ORDER BY age_grp"
            ),
            "reporter_countries": _distinct(
                "SELECT DISTINCT reporter_country FROM demo WHERE reporter_country IS NOT NULL ORDER BY reporter_country"
            ),
        }

        # Drug-related
        if "drug" in tables:
            result["role_codes"] = _distinct(
                "SELECT DISTINCT role_cod FROM drug WHERE role_cod IS NOT NULL ORDER BY role_cod"
            )
            result["routes"] = _distinct(
                "SELECT DISTINCT route FROM drug WHERE route IS NOT NULL ORDER BY route"
            )
            result["dose_units"] = _distinct(
                "SELECT DISTINCT dose_unit FROM drug WHERE dose_unit IS NOT NULL ORDER BY dose_unit"
            )
        else:
            result.update(role_codes=[], routes=[], dose_units=[])

        # Reaction outcomes
        if "reac" in tables:
            result["reaction_outcomes"] = _distinct(
                "SELECT DISTINCT drug_rec_act FROM reac WHERE drug_rec_act IS NOT NULL ORDER BY drug_rec_act"
            )
        else:
            result["reaction_outcomes"] = []

        # Case outcomes
        if "outc" in tables:
            result["case_outcomes"] = _distinct(
                "SELECT DISTINCT outc_cod FROM outc WHERE outc_cod IS NOT NULL ORDER BY outc_cod"
            )
        else:
            result["case_outcomes"] = []

        # Reporter types
        if "rpsr" in tables:
            result["reporter_types"] = _distinct(
                "SELECT DISTINCT rpsr_cod FROM rpsr WHERE rpsr_cod IS NOT NULL ORDER BY rpsr_cod"
            )
        else:
            result["reporter_types"] = []

        # Duration codes
        if "ther" in tables:
            result["dur_codes"] = _distinct(
                "SELECT DISTINCT dur_cod FROM ther WHERE dur_cod IS NOT NULL ORDER BY dur_cod"
            )
        else:
            result["dur_codes"] = []

    finally:
        conn.close()

    return FilterMetadataResponse.model_validate(result).model_dump()


def get_case_detail(case_version_pk: int | str) -> dict | None:
    """Get full detail for a single case by primaryid.

    The case_version_pk is now the primaryid (string), but we accept
    both string and int to maintain backward compatibility with the API.
    """
    pid = str(case_version_pk)

    conn = get_conn()
    try:
        # Header: latest version of this case
        header_sql = f"""
            WITH {LATEST_CTE}
            SELECT
                c.primaryid AS case_version_pk,
                c.source_system || ':' || c.caseid AS canonical_case_id,
                c.caseid AS source_case_id,
                c.primaryid AS source_report_id,
                c.source_quarter,
                c.source_system,
                c.caseversion AS case_version_num,
                c.report_type,
                c.i_f_code AS initial_or_followup,
                c.fda_dt,
                c.event_dt,
                c.mfr_dt,
                c.sex AS sex_std,
                c.age AS age_value,
                c.age_cod AS age_unit,
                c.age_grp AS age_group,
                c.wt_kg AS weight_kg,
                c.reporter_country,
                coalesce(list(DISTINCT o.outc_cod ORDER BY o.outc_cod)
                    FILTER (WHERE o.outc_cod IS NOT NULL), []) AS outcomes,
                coalesce(list(DISTINCT rp.rpsr_cod ORDER BY rp.rpsr_cod)
                    FILTER (WHERE rp.rpsr_cod IS NOT NULL), []) AS reporter_types
            FROM cases c
            LEFT JOIN outc o ON o.primaryid = c.primaryid
            LEFT JOIN rpsr rp ON rp.primaryid = c.primaryid
            WHERE c.primaryid = $1
            GROUP BY ALL
        """
        header_row = conn.execute(header_sql, [pid]).fetchone()
        if not header_row:
            return None
        header_cols = [desc[0] for desc in conn.description]
        header = _row_to_dict(header_cols, header_row)

        # Drugs
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
                    FILTER (WHERE i.indi_pt IS NOT NULL), []) AS indications,
                min(th.start_dt) AS therapy_start_dt,
                max(th.end_dt) AS therapy_end_dt
            FROM drug d
            LEFT JOIN indi i ON i.primaryid = d.primaryid
                AND i.drug_seq IS NOT DISTINCT FROM d.drug_seq
            LEFT JOIN ther th ON th.primaryid = d.primaryid
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

        # Reactions
        reactions_sql = """
            SELECT DISTINCT
                r.pt AS reaction_pt,
                r.drug_rec_act AS outcome
            FROM reac r
            WHERE r.primaryid = $1
            ORDER BY r.pt, r.drug_rec_act
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


# ─── Helpers ──────────────────────────────────────────────────────────────────


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
