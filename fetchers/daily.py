"""Aggregated daily data fetcher.

The :class:`DailyFetcher` coordinates multiple providers and picks the first
successful response for each symbol/date combination.
"""

from __future__ import annotations

from datetime import date
from typing import Iterable, List, Optional

from Ashare_data.providers.base import BaseProvider, ProviderResult
from Ashare_data.utils.logging import get_logger


class DailyFetcher:
    """Combine multiple provider instances into a single fetch API."""

    def __init__(self, providers: Iterable[BaseProvider]):
        self._providers: List[BaseProvider] = sorted(providers, key=lambda provider: provider.priority, reverse=True)
        self._logger = get_logger("Ashare.fetcher.daily")

    async def fetch(self, symbol: str, trade_date: date) -> Optional[ProviderResult]:
        """Return the first successful :class:`ProviderResult` or ``None``."""

        for provider in self._providers:
            try:
                result = await provider.fetch_daily(symbol, trade_date)
            except Exception as exc:  # pragma: no cover - network errors
                self._logger.warning("Provider %s failed for %s on %s: %s", provider.name, symbol, trade_date, exc)
                continue
            if result:
                return self._clean(result)
        return None

    def _clean(self, result: ProviderResult) -> ProviderResult:
        """Coerce numeric fields and fill basic defaults."""

        result.open = float(result.open or result.close)
        result.high = float(max(result.high, result.open, result.close))
        result.low = float(min(result.low, result.open, result.close))
        result.close = float(result.close)
        result.volume = float(result.volume or 0)
        result.turnover = float(result.turnover or 0)
        return result

    async def close(self) -> None:
        for provider in self._providers:
            await provider.close()
