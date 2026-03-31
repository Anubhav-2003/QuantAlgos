from __future__ import annotations

from datetime import date
from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, Field


EntityName = Literal["tickers_metadata", "daily_prices", "fundamentals", "company_bundle"]
FilterOperator = Literal[
    "eq",
    "ne",
    "lt",
    "lte",
    "gt",
    "gte",
    "in",
    "not_in",
    "between",
    "like",
    "ilike",
    "contains",
    "startswith",
    "endswith",
    "is_null",
    "not_null",
]
SortDirection = Literal["asc", "desc"]


class QueryFilter(BaseModel):
    field: str
    op: FilterOperator
    value: Optional[Any] = None
    values: Optional[List[Any]] = None


class SortSpec(BaseModel):
    field: str
    direction: SortDirection = "asc"


class HistoryWindow(BaseModel):
    start_date: Optional[date] = None
    end_date: Optional[date] = None
    limit: Optional[int] = Field(default=None, ge=1)


class CompanyInclude(BaseModel):
    metadata: bool = True
    latest_price: bool = True
    latest_fundamentals: bool = True
    price_history: Optional[HistoryWindow] = None
    fundamentals_history: Optional[HistoryWindow] = None


class QueryRequest(BaseModel):
    entity: EntityName
    fields: Optional[List[str]] = None
    filters: List[QueryFilter] = Field(default_factory=list)
    sort: List[SortSpec] = Field(default_factory=list)
    limit: int = Field(default=100, ge=1)
    offset: int = Field(default=0, ge=0)
    as_of: Optional[date] = None
    start_date: Optional[date] = None
    end_date: Optional[date] = None
    tickers: Optional[List[str]] = None
    include: Optional[CompanyInclude] = None


class BatchRequestItem(BaseModel):
    request_id: Optional[str] = None
    query: QueryRequest


class BatchRequest(BaseModel):
    requests: list[BatchRequestItem] = Field(default_factory=list)


class ErrorResponse(BaseModel):
    code: str
    message: str
    details: Optional[Dict[str, Any]] = None


class QueryResult(BaseModel):
    entity: EntityName
    row_count: int
    data: Any
    warnings: list[str] = Field(default_factory=list)


class BatchResultItem(BaseModel):
    request_id: Optional[str] = None
    ok: bool
    result: Optional[QueryResult] = None
    error: Optional[ErrorResponse] = None


class CapabilitiesEntity(BaseModel):
    name: str
    table: Optional[str] = None
    columns: List[str]
    default_select: List[str]
    notes: Optional[str] = None


class CapabilitiesResponse(BaseModel):
    service: str
    version: str
    host: str
    port: int
    entities: Dict[str, CapabilitiesEntity]
    operators: List[str]
    limits: Dict[str, int]


class CompanyBundle(BaseModel):
    ticker: str
    metadata: Optional[Dict[str, Any]] = None
    latest_price: Optional[Dict[str, Any]] = None
    latest_fundamentals: Optional[Dict[str, Any]] = None
    price_history: List[Dict[str, Any]] = Field(default_factory=list)
    fundamentals_history: List[Dict[str, Any]] = Field(default_factory=list)
    warnings: List[str] = Field(default_factory=list)
