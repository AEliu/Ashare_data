"""Configuration loader shared across subsystems."""

from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Optional


@dataclass(slots=True)
class Settings:
    """Container for runtime configuration values."""

    database_path: Path
    qq_endpoint: str
    eastmoney_endpoint: str
    http_timeout: float
    calendar_path: Optional[Path]
    max_requests_per_second: int
    retry_attempts: int
    retry_base_delay: float


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return application wide settings sourced from environment variables."""

    database_path = Path(os.getenv("ASHARE_DATABASE", "ashare.sqlite3")).expanduser()
    calendar = os.getenv("ASHARE_CALENDAR")
    return Settings(
        database_path=database_path,
        qq_endpoint=os.getenv(
            "ASHARE_QQ_ENDPOINT",
            "https://stockapp.finance.qq.com/mstat/appStockRank/AppStockRank.php",
        ),
        eastmoney_endpoint=os.getenv(
            "ASHARE_EASTMONEY_ENDPOINT",
            "https://push2.eastmoney.com/api/qt/stock/kline/get",
        ),
        http_timeout=float(os.getenv("ASHARE_HTTP_TIMEOUT", "10")),
        calendar_path=Path(calendar).expanduser() if calendar else None,
        max_requests_per_second=int(os.getenv("ASHARE_MAX_RPS", "5")),
        retry_attempts=int(os.getenv("ASHARE_RETRY_ATTEMPTS", "3")),
        retry_base_delay=float(os.getenv("ASHARE_RETRY_BASE_DELAY", "0.5")),
    )
