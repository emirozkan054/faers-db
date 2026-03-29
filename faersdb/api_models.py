from datetime import date

from pydantic import BaseModel, Field, field_validator


class HealthResponse(BaseModel):
    status: str


class CaseSearchParams(BaseModel):
    drug_name: str = Field(min_length=2, max_length=200)
    reaction_pt: str | None = Field(default=None, max_length=200)
    quarter: str | None = Field(default=None, pattern=r"^\d{4}q[1-4]$")
    limit: int = Field(default=25, ge=1, le=100)
    offset: int = Field(default=0, ge=0, le=10_000)

    @field_validator("drug_name", "reaction_pt", mode="before")
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


class DrugReactionAggregateParams(BaseModel):
    drug_name: str = Field(min_length=2, max_length=200)
    reaction_pt: str | None = Field(default=None, max_length=200)
    quarter: str | None = Field(default=None, pattern=r"^\d{4}q[1-4]$")
    limit: int = Field(default=20, ge=1, le=100)
    offset: int = Field(default=0, ge=0, le=10_000)

    @field_validator("drug_name", "reaction_pt", mode="before")
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


class CaseSummary(BaseModel):
    case_pk: int
    canonical_case_id: str
    case_version_pk: int
    source_system: str
    source_quarter: str
    source_report_id: str
    source_case_id: str
    case_version_num: int | None
    fda_dt: date | None
    event_dt: date | None
    mfr_dt: date | None
    sex_std: str | None
    age_value: float | None
    age_unit: str | None
    drugs: list[str]
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
    fda_dt: date | None
    event_dt: date | None
    mfr_dt: date | None
    sex_std: str | None
    age_value: float | None
    age_unit: str | None
    outcomes: list[str]
    reporter_types: list[str]
    drugs: list[CaseDrugDetail]
    reactions: list[CaseReactionDetail]
