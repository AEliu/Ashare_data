"""Tencent QQ market data provider."""

from __future__ import annotations

from datetime import date
from typing import Any, Dict, Optional

import httpx

from Ashare_data.providers.base import BaseProvider, ProviderResult
from Ashare_data.utils.config import get_settings
from Ashare_data.utils.rate_limiter import AsyncRateLimiter


class QQProvider(BaseProvider):
    """Fetch daily bars from the Tencent QQ quote service."""

    name = "qq"

    def __init__(self, *, priority: int = 0, client: Optional[httpx.AsyncClient] = None, rate_limiter: Optional[AsyncRateLimiter] = None) -> None:
        settings = get_settings()
        limiter = rate_limiter or AsyncRateLimiter(rate=settings.max_requests_per_second, per=1)
        super().__init__(priority=priority, rate_limiter=limiter)
        self._endpoint = settings.qq_endpoint
        self._timeout = settings.http_timeout
        self._client = client
        self._owns_client = client is None

    async def _request_daily(self, symbol: str, trade_date: date) -> Optional[Dict[str, Any]]:
        params = {
            "page": 1,
            "pageSize": 1,
            "reqDay": trade_date.strftime("%Y-%m-%d"),
            "symbol": symbol,
        }
        client = self._client or httpx.AsyncClient(timeout=self._timeout)
        close_client = client is not self._client
        try:
            response = await client.get(self._endpoint, params=params, timeout=self._timeout)
            response.raise_for_status()
            payload = response.json()
        finally:
            if close_client:
                await client.aclose()
        return payload

    def _parse_payload(self, symbol: str, trade_date: date, payload: Dict[str, Any]) -> Optional[ProviderResult]:
        data = payload.get("data") or {}
        if not data:
            return None
        # Some Tencent responses wrap the price values inside the first entry of
        # a list called "diff"; fall back to top-level keys if that is missing.
        bar: Dict[str, Any]
        if isinstance(data.get("diff"), list) and data["diff"]:
            bar = data["diff"][0]
        else:
            bar = data
        try:
            open_price = float(bar.get("open", bar.get("openPrice")))
            high_price = float(bar.get("high", bar.get("highest")))
            low_price = float(bar.get("low", bar.get("lowest")))
            close_price = float(bar.get("close", bar.get("price")))
        except (TypeError, ValueError):
            self.logger.debug("QQ provider returned incomplete data for %s", symbol)
            return None
        volume = float(bar.get("volume", bar.get("vol", 0)))
        turnover = float(bar.get("turnover", bar.get("turn", 0)))
        return ProviderResult(
            symbol=symbol,
            trade_date=trade_date,
            open=open_price,
            high=high_price,
            low=low_price,
            close=close_price,
            volume=volume,
            turnover=turnover,
            raw=payload,
        )

    async def fetch_daily(self, symbol: str, trade_date: date) -> Optional[ProviderResult]:
        await self._apply_rate_limit()
        request = self._with_retry(self._request_daily)
        payload = await request(symbol, trade_date)
        if not payload:
            return None
        return self._parse_payload(symbol, trade_date, payload)

    async def close(self) -> None:
        if self._owns_client and self._client is not None:
            await self._client.aclose()
