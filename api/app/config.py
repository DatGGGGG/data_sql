from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache


@dataclass(frozen=True)
class Settings:
    app_name: str
    app_version: str
    database_url: str
    default_limit: int
    max_limit: int
    query_timeout_ms: int
    query_default_rows: int
    query_max_rows: int


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings(
        app_name=os.getenv("API_NAME", "Strategy Data API"),
        app_version=os.getenv("API_VERSION", "0.1.0"),
        database_url=os.getenv(
            "DATABASE_URL",
            "postgresql://postgres:postgres@localhost:5433/mydb",
        ),
        default_limit=int(os.getenv("API_DEFAULT_LIMIT", "50")),
        max_limit=int(os.getenv("API_MAX_LIMIT", "500")),
        query_timeout_ms=int(os.getenv("API_QUERY_TIMEOUT_MS", "10000")),
        query_default_rows=int(os.getenv("API_QUERY_DEFAULT_ROWS", "100")),
        query_max_rows=int(os.getenv("API_QUERY_MAX_ROWS", "500")),
    )
