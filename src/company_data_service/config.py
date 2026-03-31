from __future__ import annotations

from dataclasses import dataclass, field
from functools import lru_cache
import os

from src.init_database import build_db_config


def _read_int(name: str, default: int) -> int:
    value = os.getenv(name)
    return default if value is None or value == "" else int(value)


@dataclass(frozen=True)
class ServiceSettings:
    app_name: str = "QuantAlgos Company Data Service"
    version: str = "0.1.0"
    host: str = field(default_factory=lambda: os.getenv("COMPANY_DATA_SERVICE_HOST", "0.0.0.0"))
    port: int = field(default_factory=lambda: _read_int("COMPANY_DATA_SERVICE_PORT", 5454))
    pool_minconn: int = field(default_factory=lambda: _read_int("COMPANY_DATA_SERVICE_POOL_MINCONN", 1))
    pool_maxconn: int = field(default_factory=lambda: _read_int("COMPANY_DATA_SERVICE_POOL_MAXCONN", 10))
    max_query_limit: int = field(default_factory=lambda: _read_int("COMPANY_DATA_SERVICE_MAX_QUERY_LIMIT", 5000))
    max_batch_items: int = field(default_factory=lambda: _read_int("COMPANY_DATA_SERVICE_MAX_BATCH_ITEMS", 50))
    max_bundle_tickers: int = field(default_factory=lambda: _read_int("COMPANY_DATA_SERVICE_MAX_BUNDLE_TICKERS", 100))
    db_config: dict[str, object] = field(default_factory=build_db_config)


@lru_cache(maxsize=1)
def get_settings() -> ServiceSettings:
    return ServiceSettings()
