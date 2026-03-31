from __future__ import annotations

from collections import defaultdict
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date
from typing import Any, Iterable, List, Optional, Tuple

from psycopg2 import sql
from psycopg2.extras import RealDictCursor
from psycopg2.pool import ThreadedConnectionPool

from src.company_data_service.config import ServiceSettings
from src.company_data_service.models import CompanyInclude, HistoryWindow, QueryFilter, QueryRequest, SortSpec


ENTITY_DEFINITIONS: dict[str, dict[str, Any]] = {
    "tickers_metadata": {
        "table": "tickers_metadata",
        "columns": ["ticker", "company_name", "sector", "exchange", "universe_type", "snapshot_date", "created_at"],
        "default_select": ["ticker", "company_name", "sector", "exchange", "universe_type", "snapshot_date"],
        "date_column": "snapshot_date",
        "default_sort": [SortSpec(field="ticker", direction="asc")],
    },
    "daily_prices": {
        "table": "daily_prices",
        "columns": ["date", "ticker", "open", "high", "low", "close", "adjusted_close", "volume"],
        "default_select": ["date", "ticker", "open", "high", "low", "close", "adjusted_close", "volume"],
        "date_column": "date",
        "default_sort": [SortSpec(field="ticker", direction="asc"), SortSpec(field="date", direction="desc")],
    },
    "fundamentals": {
        "table": "fundamentals",
        "columns": [
            "fundamental_id",
            "ticker",
            "period_end_date",
            "revenue",
            "gross_profit",
            "net_income",
            "eps_diluted",
            "total_assets",
            "total_equity",
            "long_term_debt",
            "operating_cf",
            "capex",
            "book_value_per_share",
            "free_cash_flow",
            "source",
            "created_at",
        ],
        "default_select": [
            "ticker",
            "period_end_date",
            "revenue",
            "gross_profit",
            "net_income",
            "eps_diluted",
            "total_assets",
            "total_equity",
            "long_term_debt",
            "operating_cf",
            "capex",
            "book_value_per_share",
            "free_cash_flow",
            "source",
        ],
        "date_column": "period_end_date",
        "default_sort": [SortSpec(field="ticker", direction="asc"), SortSpec(field="period_end_date", direction="desc")],
    },
}

SUPPORTED_OPERATORS = [
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


def _dedupe_preserve_order(items: Iterable[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for item in items:
        normalized = item.strip().upper()
        if normalized and normalized not in seen:
            seen.add(normalized)
            result.append(normalized)
    return result


def _build_in_clause(column: str, values: list[Any], negated: bool = False) -> tuple[sql.SQL, list[Any]]:
    if not values:
        raise ValueError(f"{column} requires at least one value")
    placeholders = sql.SQL(", ").join(sql.Placeholder() for _ in values)
    operator = sql.SQL("NOT IN") if negated else sql.SQL("IN")
    return sql.SQL("{} {} ({})").format(sql.Identifier(column), operator, placeholders), list(values)


def _build_sort_clause(entity: str, sort: list[SortSpec]) -> sql.SQL:
    definition = ENTITY_DEFINITIONS[entity]
    selected_sort = sort or definition["default_sort"]
    clauses = []
    for spec in selected_sort:
        if spec.field not in definition["columns"]:
            raise ValueError(f"Unknown sort field for {entity}: {spec.field}")
        direction = sql.SQL("ASC") if spec.direction == "asc" else sql.SQL("DESC")
        clauses.append(sql.SQL("{} {}").format(sql.Identifier(spec.field), direction))
    return sql.SQL(", ").join(clauses)


class CompanyDataStore:
    def __init__(self, settings: ServiceSettings):
        self.settings = settings
        self._pool = ThreadedConnectionPool(
            settings.pool_minconn,
            settings.pool_maxconn,
            **settings.db_config,
        )

    def close(self) -> None:
        self._pool.closeall()

    @contextmanager
    def connection(self):
        conn = self._pool.getconn()
        try:
            yield conn
        finally:
            self._pool.putconn(conn)

    def ping(self) -> dict[str, Any]:
        with self.connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("SELECT 1 AS ok")
                return dict(cursor.fetchone() or {"ok": 0})

    def snapshot_counts(self) -> dict[str, int]:
        with self.connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(
                    """
                    SELECT
                        (SELECT COUNT(*) FROM tickers_metadata) AS tickers,
                        (SELECT COUNT(*) FROM daily_prices) AS daily_prices,
                        (SELECT COUNT(*) FROM fundamentals) AS fundamentals
                    """
                )
                row = dict(cursor.fetchone() or {})
                return {
                    "tickers": int(row.get("tickers", 0)),
                    "daily_prices": int(row.get("daily_prices", 0)),
                    "fundamentals": int(row.get("fundamentals", 0)),
                }

    def capabilities(self) -> dict[str, Any]:
        entities = {}
        for entity, definition in ENTITY_DEFINITIONS.items():
            entities[entity] = {
                "name": entity,
                "table": definition["table"],
                "columns": list(definition["columns"]),
                "default_select": list(definition["default_select"]),
                "notes": None,
            }

        entities["company_bundle"] = {
            "name": "company_bundle",
            "table": None,
            "columns": ["ticker", "metadata", "latest_price", "latest_fundamentals", "price_history", "fundamentals_history", "warnings"],
            "default_select": ["ticker", "metadata", "latest_price", "latest_fundamentals"],
            "notes": "Composite response built from the three base tables.",
        }

        return {
            "service": self.settings.app_name,
            "version": self.settings.version,
            "host": self.settings.host,
            "port": self.settings.port,
            "entities": entities,
            "operators": list(SUPPORTED_OPERATORS),
            "limits": {
                "max_query_limit": self.settings.max_query_limit,
                "max_batch_items": self.settings.max_batch_items,
                "max_bundle_tickers": self.settings.max_bundle_tickers,
            },
        }

    def execute_query(self, request: QueryRequest) -> dict[str, Any]:
        if request.entity == "company_bundle":
            return self._execute_company_bundle(request)
        rows = self._fetch_rows(request)
        return {
            "entity": request.entity,
            "row_count": len(rows),
            "data": rows,
            "warnings": [],
        }

    def _validate_entity(self, entity: str) -> dict[str, Any]:
        if entity not in ENTITY_DEFINITIONS:
            raise ValueError(f"Unknown entity: {entity}")
        return ENTITY_DEFINITIONS[entity]

    def _normalize_fields(self, entity: str, fields: Optional[List[str]]) -> List[str]:
        definition = self._validate_entity(entity)
        if not fields or fields == ["*"]:
            return list(definition["default_select"])

        normalized = []
        for field in fields:
            if field not in definition["columns"]:
                raise ValueError(f"Unknown field for {entity}: {field}")
            normalized.append(field)
        return normalized

    def _build_filter_clause(self, entity: str, query: QueryRequest) -> Tuple[Optional[sql.SQL], List[Any]]:
        definition = self._validate_entity(entity)
        clauses: list[sql.SQL] = []
        params: list[Any] = []

        if query.tickers:
            tickers = _dedupe_preserve_order(query.tickers)
            clause, clause_params = _build_in_clause("ticker", tickers)
            clauses.append(clause)
            params.extend(clause_params)

        for item in query.filters:
            if item.field not in definition["columns"]:
                raise ValueError(f"Unknown field for {entity}: {item.field}")

            column = sql.Identifier(item.field)
            if item.op == "eq":
                clauses.append(sql.SQL("{} = %s").format(column))
                params.append(item.value)
            elif item.op == "ne":
                clauses.append(sql.SQL("{} <> %s").format(column))
                params.append(item.value)
            elif item.op == "lt":
                clauses.append(sql.SQL("{} < %s").format(column))
                params.append(item.value)
            elif item.op == "lte":
                clauses.append(sql.SQL("{} <= %s").format(column))
                params.append(item.value)
            elif item.op == "gt":
                clauses.append(sql.SQL("{} > %s").format(column))
                params.append(item.value)
            elif item.op == "gte":
                clauses.append(sql.SQL("{} >= %s").format(column))
                params.append(item.value)
            elif item.op == "in":
                clause, clause_params = _build_in_clause(item.field, item.values or [])
                clauses.append(clause)
                params.extend(clause_params)
            elif item.op == "not_in":
                clause, clause_params = _build_in_clause(item.field, item.values or [], negated=True)
                clauses.append(clause)
                params.extend(clause_params)
            elif item.op == "between":
                values = item.values or []
                if len(values) != 2:
                    raise ValueError(f"between operator for {item.field} requires exactly two values")
                clauses.append(sql.SQL("{} BETWEEN %s AND %s").format(column))
                params.extend(values)
            elif item.op == "like":
                clauses.append(sql.SQL("{} LIKE %s").format(column))
                params.append(item.value)
            elif item.op == "ilike":
                clauses.append(sql.SQL("{} ILIKE %s").format(column))
                params.append(item.value)
            elif item.op == "contains":
                clauses.append(sql.SQL("{} ILIKE %s").format(column))
                params.append(f"%{item.value}%")
            elif item.op == "startswith":
                clauses.append(sql.SQL("{} ILIKE %s").format(column))
                params.append(f"{item.value}%")
            elif item.op == "endswith":
                clauses.append(sql.SQL("{} ILIKE %s").format(column))
                params.append(f"%{item.value}")
            elif item.op == "is_null":
                clauses.append(sql.SQL("{} IS NULL").format(column))
            elif item.op == "not_null":
                clauses.append(sql.SQL("{} IS NOT NULL").format(column))
            else:
                raise ValueError(f"Unsupported operator: {item.op}")

        date_column = definition.get("date_column")
        if query.as_of is not None and date_column is not None:
            clauses.append(sql.SQL("{} <= %s").format(sql.Identifier(date_column)))
            params.append(query.as_of)

        if query.start_date is not None and date_column is not None:
            clauses.append(sql.SQL("{} >= %s").format(sql.Identifier(date_column)))
            params.append(query.start_date)

        if query.end_date is not None and date_column is not None:
            clauses.append(sql.SQL("{} <= %s").format(sql.Identifier(date_column)))
            params.append(query.end_date)

        if not clauses:
            return None, []

        return sql.SQL(" AND ").join(clauses), params

    def _fetch_rows(self, request: QueryRequest) -> list[dict[str, Any]]:
        definition = self._validate_entity(request.entity)
        select_fields = self._normalize_fields(request.entity, request.fields)
        where_clause, params = self._build_filter_clause(request.entity, request)
        order_clause = _build_sort_clause(request.entity, request.sort)

        query = sql.SQL("SELECT {} FROM {}").format(
            sql.SQL(", ").join(sql.Identifier(field) for field in select_fields),
            sql.Identifier(definition["table"]),
        )
        if where_clause is not None:
            query += sql.SQL(" WHERE ") + where_clause
        if order_clause:
            query += sql.SQL(" ORDER BY ") + order_clause

        limit = min(request.limit, self.settings.max_query_limit)
        query += sql.SQL(" LIMIT %s")
        params.append(limit)

        if request.offset:
            query += sql.SQL(" OFFSET %s")
            params.append(request.offset)

        with self.connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query, params)
                return [dict(row) for row in cursor.fetchall()]

    def _fetch_latest_rows(
        self,
        entity: str,
        tickers: list[str],
        date_column: str,
        as_of: Optional[date],
    ) -> dict[str, dict[str, Any]]:
        definition = self._validate_entity(entity)
        columns = list(definition["default_select"])
        query = sql.SQL("SELECT DISTINCT ON (ticker) {} FROM {} WHERE ticker = ANY(%s)").format(
            sql.SQL(", ").join(sql.Identifier(field) for field in columns),
            sql.Identifier(definition["table"]),
        )
        params: list[Any] = [tickers]
        if as_of is not None:
            query += sql.SQL(" AND {} <= %s").format(sql.Identifier(date_column))
            params.append(as_of)
        query += sql.SQL(" ORDER BY ticker, {} DESC").format(sql.Identifier(date_column))

        with self.connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(query, params)
                rows = [dict(row) for row in cursor.fetchall()]
        return {str(row["ticker"]): row for row in rows if row.get("ticker") is not None}

    def _execute_company_bundle(self, request: QueryRequest) -> dict[str, Any]:
        include = request.include or CompanyInclude()
        tickers = _dedupe_preserve_order(request.tickers or [])
        if not tickers:
            raise ValueError("company_bundle requires at least one ticker")
        if len(tickers) > self.settings.max_bundle_tickers:
            raise ValueError(f"company_bundle ticker count exceeds limit of {self.settings.max_bundle_tickers}")

        warnings: list[str] = []
        bundles: dict[str, dict[str, Any]] = {
            ticker: {"ticker": ticker, "metadata": None, "latest_price": None, "latest_fundamentals": None, "price_history": [], "fundamentals_history": [], "warnings": []}
            for ticker in tickers
        }

        metadata_rows = self._fetch_rows(
            QueryRequest(
                entity="tickers_metadata",
                fields=["ticker", "company_name", "sector", "exchange", "universe_type", "snapshot_date"],
                filters=[QueryFilter(field="ticker", op="in", values=tickers)],
                sort=[SortSpec(field="ticker", direction="asc")],
                limit=len(tickers),
            )
        )
        for row in metadata_rows:
            ticker = str(row["ticker"])
            bundles[ticker]["metadata"] = row

        missing_metadata = [ticker for ticker in tickers if bundles[ticker]["metadata"] is None]
        if missing_metadata:
            warnings.append(f"Missing metadata for: {', '.join(missing_metadata)}")
            for ticker in missing_metadata:
                bundles[ticker]["warnings"].append("metadata missing from tickers_metadata")

        if include.latest_price:
            latest_price_rows = self._fetch_latest_rows("daily_prices", tickers, "date", request.as_of)
            for ticker, row in latest_price_rows.items():
                bundles[ticker]["latest_price"] = row

        if include.latest_fundamentals:
            latest_fundamentals_rows = self._fetch_latest_rows("fundamentals", tickers, "period_end_date", request.as_of)
            for ticker, row in latest_fundamentals_rows.items():
                bundles[ticker]["latest_fundamentals"] = row

        if include.price_history is not None:
            price_rows = self._fetch_history_rows("daily_prices", tickers, include.price_history, request.as_of)
            grouped = self._group_rows_by_ticker(price_rows, include.price_history.limit)
            for ticker, rows in grouped.items():
                bundles[ticker]["price_history"] = rows

        if include.fundamentals_history is not None:
            fundamentals_rows = self._fetch_history_rows("fundamentals", tickers, include.fundamentals_history, request.as_of)
            grouped = self._group_rows_by_ticker(fundamentals_rows, include.fundamentals_history.limit)
            for ticker, rows in grouped.items():
                bundles[ticker]["fundamentals_history"] = rows

        ordered_bundles = [bundles[ticker] for ticker in tickers]
        return {
            "entity": "company_bundle",
            "row_count": len(ordered_bundles),
            "data": ordered_bundles,
            "warnings": warnings,
        }

    def _fetch_history_rows(
        self,
        entity: str,
        tickers: list[str],
        window: HistoryWindow,
        as_of: Optional[date],
    ) -> list[dict[str, Any]]:
        if window.start_date is None and window.end_date is None and window.limit is None:
            raise ValueError("History window requires a date bound or a per-ticker row limit")

        request = QueryRequest(
            entity=entity,
            filters=[QueryFilter(field="ticker", op="in", values=tickers)],
            sort=[SortSpec(field="ticker", direction="asc"), SortSpec(field=ENTITY_DEFINITIONS[entity]["date_column"], direction="asc")],
            limit=self.settings.max_query_limit,
            as_of=as_of,
            start_date=window.start_date,
            end_date=window.end_date,
        )
        rows = self._fetch_rows(request)

        if window.limit is None:
            return rows

        grouped = self._group_rows_by_ticker(rows, window.limit)
        flattened: list[dict[str, Any]] = []
        for ticker in tickers:
            flattened.extend(grouped.get(ticker, []))
        return flattened

    @staticmethod
    def _group_rows_by_ticker(rows: List[dict[str, Any]], per_ticker_limit: Optional[int]) -> dict[str, List[dict[str, Any]]]:
        grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for row in rows:
            ticker = str(row.get("ticker", "")).upper()
            if not ticker:
                continue
            grouped[ticker].append(row)

        if per_ticker_limit is not None:
            return {ticker: values[:per_ticker_limit] for ticker, values in grouped.items()}
        return dict(grouped)
