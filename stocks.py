from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import httpx


@dataclass(slots=True)
class Stock:
    """Represents a stock in the Eastmoney listings."""

    code: str
    name: str
    market: str


_API_URL = "https://push2.eastmoney.com/api/qt/clist/get"
_PAGE_SIZE = 500
_DEFAULT_PARAMS = {
    "po": 1,
    "np": 1,
    "ut": "bd1d9ddb04089700cf9c27f6f7426281",
    "fltt": 2,
    "invt": 2,
    "fid": "f62",
    "fs": "m:1,m:0",  # mainland (m:1) and Shanghai/Shenzhen (m:0) markets
    "fields": "f12,f13,f14",
}


def _parse_stock(raw: dict[str, Any]) -> Stock | None:
    code = raw.get("f12")
    name = raw.get("f14")
    market = raw.get("f13")
    if not code or not name or market is None:
        return None
    return Stock(code=code, name=name, market=str(market))


async def list_all_stocks(client: httpx.AsyncClient | None = None) -> list[Stock]:
    """Fetch every available stock from the Eastmoney listing API.

    Parameters
    ----------
    client:
        Optional ``httpx.AsyncClient`` to reuse for the requests. When omitted the
        function manages its own client instance.

    Returns
    -------
    list[Stock]
        A list of :class:`Stock` entries.

    Raises
    ------
    httpx.HTTPStatusError
        If the remote service returns an unsuccessful response.
    ValueError
        If the payload cannot be parsed as the expected JSON structure.
    """

    close_client = False
    if client is None:
        client = httpx.AsyncClient(timeout=httpx.Timeout(10.0))
        close_client = True

    try:
        stocks: list[Stock] = []
        page = 1
        while True:
            params = {"pn": page, "pz": _PAGE_SIZE, **_DEFAULT_PARAMS}
            response = await client.get(_API_URL, params=params)
            response.raise_for_status()
            try:
                payload = response.json()
            except ValueError as exc:  # pragma: no cover - defensive
                raise ValueError("Invalid JSON payload received from Eastmoney API") from exc

            data = payload.get("data")
            if data is None:
                break

            diffs = data.get("diff")
            if not diffs:
                break

            parsed_page = [_parse_stock(item) for item in diffs]
            stocks.extend(stock for stock in parsed_page if stock is not None)

            page += 1

        return stocks
    finally:
        if close_client:
            await client.aclose()
