from __future__ import annotations

import logging
import time
from contextlib import asynccontextmanager
from typing import Any, Optional

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.gzip import GZipMiddleware
from psycopg2 import Error as PsycopgError

from src.company_data_service.config import get_settings
from src.company_data_service.models import BatchRequest, BatchResultItem, CapabilitiesResponse, ErrorResponse, QueryFilter, QueryRequest, QueryResult
from src.company_data_service.store import CompanyDataStore


LOGGER = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = get_settings()
    store = CompanyDataStore(settings)
    app.state.settings = settings
    app.state.store = store
    try:
        yield
    finally:
        store.close()


def create_app() -> FastAPI:
    settings = get_settings()
    app = FastAPI(
        title=settings.app_name,
        version=settings.version,
        lifespan=lifespan,
    )
    app.add_middleware(GZipMiddleware, minimum_size=1024)

    @app.middleware("http")
    async def timing_middleware(request: Request, call_next):
        start = time.perf_counter()
        response = await call_next(request)
        elapsed_ms = (time.perf_counter() - start) * 1000.0
        response.headers["X-Process-Time-ms"] = f"{elapsed_ms:.2f}"
        return response

    @app.get("/healthz")
    def healthz() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/readyz")
    def readyz(request: Request) -> dict[str, Any]:
        store: CompanyDataStore = request.app.state.store
        try:
            store.ping()
            counts = store.snapshot_counts()
        except PsycopgError as exc:
            LOGGER.exception("Readiness check failed")
            raise HTTPException(status_code=503, detail="database unavailable") from exc
        return {"status": "ready", "database": "ok", "counts": counts}

    @app.get("/v1/capabilities", response_model=CapabilitiesResponse)
    def capabilities(request: Request) -> dict[str, Any]:
        store: CompanyDataStore = request.app.state.store
        return store.capabilities()

    @app.get("/v1/tickers")
    def list_tickers(
        request: Request,
        sector: Optional[str] = None,
        exchange: Optional[str] = None,
        universe_type: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> dict[str, Any]:
        filters: list[QueryFilter] = []
        if sector:
            filters.append(QueryFilter(field="sector", op="eq", value=sector))
        if exchange:
            filters.append(QueryFilter(field="exchange", op="eq", value=exchange))
        if universe_type:
            filters.append(QueryFilter(field="universe_type", op="eq", value=universe_type))

        store: CompanyDataStore = request.app.state.store
        result = store.execute_query(
            QueryRequest(
                entity="tickers_metadata",
                fields=["ticker", "company_name", "sector", "exchange", "universe_type", "snapshot_date"],
                filters=filters,
                limit=limit,
                offset=offset,
            )
        )
        return result

    @app.get("/v1/companies/{ticker}")
    def get_company(
        ticker: str,
        request: Request,
        as_of: Optional[str] = None,
        include_price_history: bool = False,
        price_start: Optional[str] = None,
        price_end: Optional[str] = None,
        price_limit: Optional[int] = None,
        include_fundamentals_history: bool = False,
        fundamentals_start: Optional[str] = None,
        fundamentals_end: Optional[str] = None,
        fundamentals_limit: Optional[int] = None,
    ) -> dict[str, Any]:
        include = {
            "metadata": True,
            "latest_price": True,
            "latest_fundamentals": True,
        }
        if include_price_history:
            include["price_history"] = {
                "start_date": price_start,
                "end_date": price_end,
                "limit": price_limit,
            }
        if include_fundamentals_history:
            include["fundamentals_history"] = {
                "start_date": fundamentals_start,
                "end_date": fundamentals_end,
                "limit": fundamentals_limit,
            }

        query = QueryRequest(
            entity="company_bundle",
            tickers=[ticker],
            as_of=as_of,
            include=include,  # type: ignore[arg-type]
        )
        store: CompanyDataStore = request.app.state.store
        return store.execute_query(query)

    @app.post("/v1/query", response_model=QueryResult)
    def query(request: Request, payload: QueryRequest) -> dict[str, Any]:
        store: CompanyDataStore = request.app.state.store
        return store.execute_query(payload)

    @app.post("/v1/batch")
    def batch(request: Request, payload: BatchRequest) -> dict[str, Any]:
        store: CompanyDataStore = request.app.state.store
        results: list[BatchResultItem] = []
        for item in payload.requests:
            try:
                result = store.execute_query(item.query)
                results.append(BatchResultItem(request_id=item.request_id, ok=True, result=QueryResult(**result)))
            except (ValueError, HTTPException) as exc:
                results.append(
                    BatchResultItem(
                        request_id=item.request_id,
                        ok=False,
                        error=ErrorResponse(code="invalid_request", message=str(exc)),
                    )
                )
            except PsycopgError as exc:
                results.append(
                    BatchResultItem(
                        request_id=item.request_id,
                        ok=False,
                        error=ErrorResponse(code="database_error", message=str(exc)),
                    )
                )

        return {"results": [result.dict() for result in results]}

    return app


app = create_app()


def main() -> None:
    import uvicorn

    settings = get_settings()
    uvicorn.run("src.company_data_service.main:app", host=settings.host, port=settings.port, reload=False)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    main()
