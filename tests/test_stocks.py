from __future__ import annotations

import pathlib
import sys
from typing import Any

import httpx
import pytest

ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT.parent) not in sys.path:
    sys.path.insert(0, str(ROOT.parent))

from Ashare_data import Stock, list_all_stocks  # noqa: E402  pylint: disable=wrong-import-position


@pytest.fixture
def anyio_backend() -> str:
    """Restrict anyio to the asyncio backend available in the test environment."""

    return "asyncio"


class MockPager:
    """Utility helper to feed sequential responses."""

    def __init__(self, pages: list[dict[str, Any]]) -> None:
        self._pages = pages

    def __call__(self, request: httpx.Request) -> httpx.Response:
        pn = int(request.url.params.get("pn", "1"))
        idx = min(pn - 1, len(self._pages) - 1)
        payload = self._pages[idx]
        return httpx.Response(200, json=payload)


@pytest.mark.anyio
async def test_list_all_stocks_paginates_until_empty() -> None:
    transport = httpx.MockTransport(
        MockPager(
            [
                {
                    "data": {
                        "diff": [
                            {"f12": "000001", "f13": 0, "f14": "Ping An Bank"},
                            {"f12": "000002", "f13": 0, "f14": "Vanke"},
                        ]
                    }
                },
                {
                    "data": {
                        "diff": [
                            {"f12": "600000", "f13": 1, "f14": "PF Bank"},
                        ]
                    }
                },
                {"data": {"diff": []}},
            ]
        )
    )

    async with httpx.AsyncClient(transport=transport) as client:
        stocks = await list_all_stocks(client=client)

    assert stocks == [
        Stock(code="000001", name="Ping An Bank", market="0"),
        Stock(code="000002", name="Vanke", market="0"),
        Stock(code="600000", name="PF Bank", market="1"),
    ]


@pytest.mark.anyio
async def test_list_all_stocks_raises_for_error_response() -> None:
    def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(500, json={"error": "oops"})

    transport = httpx.MockTransport(handler)

    async with httpx.AsyncClient(transport=transport) as client:
        with pytest.raises(httpx.HTTPStatusError):
            await list_all_stocks(client=client)
