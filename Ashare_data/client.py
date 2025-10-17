"""HTTP client utilities for interacting with Ashare data services."""

from __future__ import annotations

import asyncio
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Dict, Mapping, Optional

import httpx

__all__ = ["AshareClient", "AshareClientError"]


class AshareClientError(RuntimeError):
    """Base exception for errors raised by :class:`AshareClient`."""


@dataclass
class AshareClient:
    """A small helper around :mod:`httpx` for the Ashare data API.

    Parameters
    ----------
    base_url:
        Base address of the Ashare compatible API.  Individual endpoints are
        appended to this base URL when making requests.
    default_params:
        Query parameters that should be included with every request.
    headers:
        Optional HTTP headers that should be sent with each request.
    timeout:
        Request timeout in seconds passed to :mod:`httpx`.
    max_retries:
        Maximum number of retry attempts when a request fails.  The total
        number of attempts is ``max_retries + 1``.
    backoff_factor:
        Multiplier used for the exponential backoff between retries.  The
        waiting time after ``n`` failures is
        ``backoff_factor * (2 ** n)`` seconds.
    rate_limit_per_second:
        Optional rate limit expressed as the maximum number of requests per
        second.  When provided, the client enforces a minimum delay between
        calls to the remote API.
    transport:
        Optional :class:`httpx.BaseTransport` instance.  This is primarily
        intended for testing where a :class:`httpx.MockTransport` can be
        supplied.
    """

    base_url: str
    default_params: Mapping[str, Any] | None = None
    headers: Mapping[str, str] | None = None
    timeout: float = 10.0
    max_retries: int = 2
    backoff_factor: float = 0.5
    rate_limit_per_second: Optional[float] = None
    transport: Optional[httpx.BaseTransport] = None
    _last_request_time_sync: float = field(default=0.0, init=False, repr=False)
    _last_request_time_async: float = field(default=0.0, init=False, repr=False)
    _sync_lock: threading.Lock = field(default_factory=threading.Lock, init=False, repr=False)
    _async_lock: asyncio.Lock | None = field(default=None, init=False, repr=False)

    def __post_init__(self) -> None:
        self.base_url = self.base_url.rstrip("/")
        if self.default_params:
            self.default_params = dict(self.default_params)
        if self.headers:
            self.headers = dict(self.headers)
        if self.max_retries < 0:
            raise ValueError("max_retries must be >= 0")
        if self.rate_limit_per_second is not None and self.rate_limit_per_second <= 0:
            raise ValueError("rate_limit_per_second must be positive when provided")

    # ------------------------------------------------------------------
    # Public synchronous API
    # ------------------------------------------------------------------
    def fetch_stock_list(self, market: str = "all", params: Optional[Mapping[str, Any]] = None) -> Any:
        """Return the available stock list.

        Parameters
        ----------
        market:
            Optional market identifier accepted by the remote API.
        params:
            Additional query string parameters to include in the request.
        """

        query: Dict[str, Any] = {"market": market}
        if params:
            query.update(params)
        return self._request_sync("GET", "/stocks", query)

    def fetch_daily_kline(
        self,
        symbol: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        params: Optional[Mapping[str, Any]] = None,
    ) -> Any:
        """Fetch the daily K-line information for ``symbol``."""

        query: Dict[str, Any] = {"symbol": symbol}
        if start_date:
            query["start_date"] = start_date
        if end_date:
            query["end_date"] = end_date
        if params:
            query.update(params)
        return self._request_sync("GET", "/daily-kline", query)

    # ------------------------------------------------------------------
    # Public asynchronous API
    # ------------------------------------------------------------------
    async def fetch_stock_list_async(
        self, market: str = "all", params: Optional[Mapping[str, Any]] = None
    ) -> Any:
        query: Dict[str, Any] = {"market": market}
        if params:
            query.update(params)
        return await self._request_async("GET", "/stocks", query)

    async def fetch_daily_kline_async(
        self,
        symbol: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        params: Optional[Mapping[str, Any]] = None,
    ) -> Any:
        query: Dict[str, Any] = {"symbol": symbol}
        if start_date:
            query["start_date"] = start_date
        if end_date:
            query["end_date"] = end_date
        if params:
            query.update(params)
        return await self._request_async("GET", "/daily-kline", query)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    @property
    def _min_interval(self) -> float:
        if not self.rate_limit_per_second:
            return 0.0
        return 1.0 / self.rate_limit_per_second

    def _merge_params(self, params: Optional[Mapping[str, Any]]) -> Dict[str, Any]:
        merged: Dict[str, Any] = {}
        if self.default_params:
            merged.update(self.default_params)
        if params:
            merged.update(params)
        return merged

    def _build_url(self, endpoint: str) -> str:
        if endpoint.startswith("http://") or endpoint.startswith("https://"):
            return endpoint
        if not endpoint.startswith("/"):
            endpoint = "/" + endpoint
        return f"{self.base_url}{endpoint}"

    def _apply_rate_limit_sync(self) -> None:
        min_interval = self._min_interval
        if not min_interval:
            return

        with self._sync_lock:
            now = time.monotonic()
            wait_for = min_interval - (now - self._last_request_time_sync)
            if wait_for > 0:
                time.sleep(wait_for)
                now = time.monotonic()
            self._last_request_time_sync = now

    async def _apply_rate_limit_async(self) -> None:
        min_interval = self._min_interval
        if not min_interval:
            return

        if self._async_lock is None:
            self._async_lock = asyncio.Lock()
        async with self._async_lock:
            now = time.monotonic()
            wait_for = min_interval - (now - self._last_request_time_async)
            if wait_for > 0:
                await asyncio.sleep(wait_for)
                now = time.monotonic()
            self._last_request_time_async = now

    def _request_sync(self, method: str, endpoint: str, params: Optional[Mapping[str, Any]]) -> Any:
        params_dict = self._merge_params(params)
        url = self._build_url(endpoint)
        last_error: Exception | None = None
        for attempt in range(self.max_retries + 1):
            if self._min_interval:
                self._apply_rate_limit_sync()
            try:
                with httpx.Client(timeout=self.timeout, headers=self.headers, transport=self.transport) as client:
                    response = client.request(method, url, params=params_dict)
                response.raise_for_status()
                return response.json()
            except (httpx.HTTPError, ValueError) as exc:
                last_error = exc
                if attempt == self.max_retries:
                    raise AshareClientError(f"{method} {url} failed after retries") from exc
                delay = self.backoff_factor * (2**attempt)
                if delay > 0:
                    time.sleep(delay)
        raise AshareClientError("Request failed") from last_error

    async def _request_async(self, method: str, endpoint: str, params: Optional[Mapping[str, Any]]) -> Any:
        params_dict = self._merge_params(params)
        url = self._build_url(endpoint)
        last_error: Exception | None = None
        for attempt in range(self.max_retries + 1):
            if self._min_interval:
                await self._apply_rate_limit_async()
            try:
                async with httpx.AsyncClient(
                    timeout=self.timeout, headers=self.headers, transport=self.transport
                ) as client:
                    response = await client.request(method, url, params=params_dict)
                response.raise_for_status()
                return response.json()
            except (httpx.HTTPError, ValueError) as exc:
                last_error = exc
                if attempt == self.max_retries:
                    raise AshareClientError(f"{method} {url} failed after retries") from exc
                delay = self.backoff_factor * (2**attempt)
                if delay > 0:
                    await asyncio.sleep(delay)
        raise AshareClientError("Request failed") from last_error

