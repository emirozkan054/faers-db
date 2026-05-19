from datetime import date

from pydantic import BaseModel, Field, field_validator


class HealthResponse(BaseModel):
    status: str


class CaseTimeFilters(BaseModel):
    quarter: str | None = Field(default=None, pattern=r"^\d{4}q[1-4]$")
    report_type: str | None = Field(default=None, max_length=20)
    initial_or_followup: str | None = Field(default=None, max_length=20)
    event_dt_from: date | None = None
    event_dt_to: date | None = None
    fda_dt_from: date | None = None
    fda_dt_to: date | None = None
    mfr_dt_from: date | None = None
    mfr_dt_to: date | None = None


class DemographicFilters(BaseModel):
    sex_std: str | None = Field(default=None, max_length=20)
    age_min: float | None = Field(default=None, ge=0)
    age_max: float | None = Field(default=None, ge=0)
    age_unit: str | None = Field(default=None, max_length=20)
    age_group: str | None = Field(default=None, max_length=20)
    weight_min: float | None = Field(default=None, ge=0)
    weight_max: float | None = Field(default=None, ge=0)
    reporter_country: str | None = Field(default=None, max_length=20)


class DrugFilters(BaseModel):
    drug_name: str | None = Field(default=None, max_length=200)
    prod_ai: str | None = Field(default=None, max_length=200)
    role_cod: str | None = Field(default=None, max_length=20)
    route: str | None = Field(default=None, max_length=100)
    dose_unit: str | None = Field(default=None, max_length=40)
    dose_min: float | None = Field(default=None, ge=0)
    dose_max: float | None = Field(default=None, ge=0)


class ReactionOutcomeFilters(BaseModel):
    reaction_pt: str | None = Field(default=None, max_length=200)
    reaction_outcome: str | None = Field(default=None, max_length=20)
    case_outcome: str | None = Field(default=None, max_length=20)


class TherapyIndicationFilters(BaseModel):
    indication_pt: str | None = Field(default=None, max_length=200)
    therapy_start_from: date | None = None
    therapy_start_to: date | None = None
    therapy_end_from: date | None = None
    therapy_end_to: date | None = None
    dur_min: int | None = Field(default=None, ge=0)
    dur_max: int | None = Field(default=None, ge=0)
    dur_cod: str | None = Field(default=None, max_length=20)


class ReporterFilters(BaseModel):
    reporter_type: str | None = Field(default=None, max_length=50)


class ResearchFilterParams(
    CaseTimeFilters,
    DemographicFilters,
    DrugFilters,
    ReactionOutcomeFilters,
    TherapyIndicationFilters,
    ReporterFilters,
):
    limit: int = Field(default=25, ge=1, le=100)
    offset: int = Field(default=0, ge=0, le=10_000)

    @field_validator(
        "drug_name",
        "prod_ai",
        "reaction_pt",
        "indication_pt",
        mode="before",
    )
    @classmethod
    def normalize_search_text(cls, value: str | None):
        if value is None:
            return None
        cleaned = value.strip()
        return cleaned or None

    @field_validator("quarter", mode="before")
    @classmethod
    def normalize_quarter(cls, value: str | None):
        if value is None:
            return None
        cleaned = value.strip().lower()
        return cleaned or None

    @field_validator(
        "report_type",
        "initial_or_followup",
        "sex_std",
        "age_unit",
        "age_group",
        "reporter_country",
        "role_cod",
        "route",
        "dose_unit",
        "reaction_outcome",
        "case_outcome",
        "dur_cod",
        "reporter_type",
        mode="before",
    )
    @classmethod
    def normalize_enum_text(cls, value: str | None):
        if value is None:
            return None
        cleaned = value.strip().upper()
        return cleaned or None

    def validation_errors(self) -> list[str]:
        errors: list[str] = []
        range_pairs = [
            ("age_min", "age_max"),
            ("weight_min", "weight_max"),
            ("dose_min", "dose_max"),
            ("dur_min", "dur_max"),
            ("event_dt_from", "event_dt_to"),
            ("fda_dt_from", "fda_dt_to"),
            ("mfr_dt_from", "mfr_dt_to"),
            ("therapy_start_from", "therapy_start_to"),
            ("therapy_end_from", "therapy_end_to"),
        ]
        for lower_name, upper_name in range_pairs:
            lower = getattr(self, lower_name)
            upper = getattr(self, upper_name)
            if lower is not None and upper is not None and lower > upper:
                errors.append(f"{lower_name} must be less than or equal to {upper_name}")

        filter_values = [
            self.quarter,
            self.report_type,
            self.initial_or_followup,
            self.event_dt_from,
            self.event_dt_to,
            self.fda_dt_from,
            self.fda_dt_to,
            self.mfr_dt_from,
            self.mfr_dt_to,
            self.sex_std,
            self.age_min,
            self.age_max,
            self.age_unit,
            self.age_group,
            self.weight_min,
            self.weight_max,
            self.reporter_country,
            self.drug_name,
            self.prod_ai,
            self.role_cod,
            self.route,
            self.dose_unit,
            self.dose_min,
            self.dose_max,
            self.reaction_pt,
            self.reaction_outcome,
            self.case_outcome,
            self.indication_pt,
            self.therapy_start_from,
            self.therapy_start_to,
            self.therapy_end_from,
            self.therapy_end_to,
            self.dur_min,
            self.dur_max,
            self.dur_cod,
            self.reporter_type,
        ]
        if not any(value is not None and value != "" for value in filter_values):
            errors.append("Provide at least one filter before searching.")
        return errors


class CaseSearchParams(ResearchFilterParams):
    limit: int = Field(default=25, ge=1, le=100)


class DrugReactionAggregateParams(ResearchFilterParams):
    limit: int = Field(default=20, ge=1, le=100)


class CaseSummary(BaseModel):
    case_pk: int
    canonical_case_id: str
    case_version_pk: int
    source_system: str
    source_quarter: str
    source_report_id: str
    source_case_id: str
    case_version_num: int | None
    report_type: str | None
    initial_or_followup: str | None
    fda_dt: date | None
    event_dt: date | None
    mfr_dt: date | None
    sex_std: str | None
    age_value: float | None
    age_unit: str | None
    age_group: str | None
    weight_kg: float | None
    reporter_country: str | None
    drugs: list[str]
    active_ingredients: list[str]
    role_codes: list[str]
    routes: list[str]
    indications: list[str]
    reactions: list[str]
    outcomes: list[str]
    reporter_types: list[str]


class CaseSearchResponse(BaseModel):
    total: int
    limit: int
    offset: int
    items: list[CaseSummary]


class DrugReactionAggregate(BaseModel):
    drugname: str
    reaction_pt: str
    case_count: int


class FilterMetadataResponse(BaseModel):
    quarters: list[str]
    report_types: list[str]
    initial_or_followup_values: list[str]
    sex_values: list[str]
    age_units: list[str]
    age_groups: list[str]
    reporter_countries: list[str]
    role_codes: list[str]
    routes: list[str]
    dose_units: list[str]
    reaction_outcomes: list[str]
    case_outcomes: list[str]
    reporter_types: list[str]
    dur_codes: list[str]


class DrugReactionAggregateResponse(BaseModel):
    total: int
    limit: int
    offset: int
    items: list[DrugReactionAggregate]


class CaseDrugDetail(BaseModel):
    drug_seq: int | None
    role_cod: str | None
    drugname: str | None
    prod_ai: str | None
    route: str | None
    dose_vbm: str | None
    dose_amt: float | None
    dose_unit: str | None
    start_dt: date | None
    end_dt: date | None
    indications: list[str]
    therapy_start_dt: date | None
    therapy_end_dt: date | None


class CaseReactionDetail(BaseModel):
    reaction_pt: str
    outcome: str | None


class CaseDetailResponse(BaseModel):
    case_pk: int
    canonical_case_id: str
    case_version_pk: int
    source_system: str
    source_quarter: str
    source_report_id: str
    source_case_id: str
    case_version_num: int | None
    report_type: str | None
    initial_or_followup: str | None
    fda_dt: date | None
    event_dt: date | None
    mfr_dt: date | None
    sex_std: str | None
    age_value: float | None
    age_unit: str | None
    age_group: str | None
    weight_kg: float | None
    reporter_country: str | None
    outcomes: list[str]
    reporter_types: list[str]
    drugs: list[CaseDrugDetail]
    reactions: list[CaseReactionDetail]
