"""EastMoney data provider implementation."""

from __future__ import annotations

from datetime import date
from typing import Any, Dict, Optional

import httpx

from Ashare_data.providers.base import BaseProvider, ProviderResult
from Ashare_data.utils.config import get_settings
from Ashare_data.utils.rate_limiter import AsyncRateLimiter


class EastMoneyProvider(BaseProvider):
    """Fetch daily quotes using the public EastMoney API."""

    name = "eastmoney"

    def __init__(self, *, priority: int = 0, client: Optional[httpx.AsyncClient] = None, rate_limiter: Optional[AsyncRateLimiter] = None) -> None:
        settings = get_settings()
        limiter = rate_limiter or AsyncRateLimiter(rate=settings.max_requests_per_second, per=1)
        super().__init__(priority=priority, rate_limiter=limiter)
        self._endpoint = settings.eastmoney_endpoint
        self._timeout = settings.http_timeout
        self._client = client
        self._owns_client = client is None

    async def _request_daily(self, symbol: str, trade_date: date) -> Optional[Dict[str, Any]]:
        params = {
            "fields1": "f1,f2,f3,f4,f5,f6",
            "fields2": "f51,f52,f53,f54,f55,f56,f57",
            "klt": 101,
            "fqt": 1,
            "secid": symbol,
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
        klines = data.get("klines")
        if not klines:
            return None
        # ``klines`` is a list of comma separated strings: date,open,close,high,low,volume,turnover
        target = None
        date_str = trade_date.strftime("%Y-%m-%d")
        for row in klines:
            if not isinstance(row, str):
                continue
            if row.startswith(date_str):
                target = row
                break
        if target is None:
            return None
        try:
            (_date, open_price, close_price, high_price, low_price, volume, turnover) = target.split(",")[:7]
            return ProviderResult(
                symbol=symbol,
                trade_date=trade_date,
                open=float(open_price),
                high=float(high_price),
                low=float(low_price),
                close=float(close_price),
                volume=float(volume),
                turnover=float(turnover),
                raw=payload,
            )
        except (ValueError, TypeError):
            self.logger.debug("EastMoney provider returned malformed kline for %s", symbol)
            return None

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
