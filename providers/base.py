"""Abstract provider definitions used by fetchers and schedulers.

This module introduces :class:`BaseProvider`, the contract that concrete
implementations such as :class:`~Ashare_data.providers.qq.QQProvider` and
:class:`~Ashare_data.providers.eastmoney.EastMoneyProvider` must fulfil.  The
abstraction keeps network specific logic close to the API definition while the
rest of the project can rely on a compact interface.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import date
from typing import Any, Dict, Optional

from Ashare_data.utils.config import get_settings
from Ashare_data.utils.logging import get_logger
from Ashare_data.utils.rate_limiter import AsyncRateLimiter, async_retry


@dataclass(slots=True)
class ProviderResult:
    """Normalized result returned by provider implementations."""

    symbol: str
    trade_date: date
    open: float
    high: float
    low: float
    close: float
    volume: float
    turnover: float
    raw: Dict[str, Any]


class BaseProvider(ABC):
    """Common interface for all data providers."""

    name: str = "provider"

    def __init__(self, *, priority: int = 0, rate_limiter: Optional[AsyncRateLimiter] = None) -> None:
        settings = get_settings()
        self.priority = priority
        self._logger = get_logger(f"Ashare.{self.name}")
        self._rate_limiter = rate_limiter
        self._retry_attempts = settings.retry_attempts
        self._retry_base_delay = settings.retry_base_delay

    @property
    def logger(self):
        return self._logger

    @property
    def rate_limiter(self) -> Optional[AsyncRateLimiter]:
        return self._rate_limiter

    async def _apply_rate_limit(self) -> None:
        if self._rate_limiter is None:
            return
        await self._rate_limiter.acquire()

    def _with_retry(self, func):
        return async_retry(attempts=self._retry_attempts, base_delay=self._retry_base_delay)(func)

    @abstractmethod
    async def fetch_daily(self, symbol: str, trade_date: date) -> Optional[ProviderResult]:
        """Retrieve daily bar information for ``symbol`` on ``trade_date``."""

    async def close(self) -> None:
        """Allow providers to release any network resources."""

    def __repr__(self) -> str:  # pragma: no cover - trivial
        return f"{self.__class__.__name__}(priority={self.priority})"
