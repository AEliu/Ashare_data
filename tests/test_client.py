import asyncio

import httpx
import pytest

from Ashare_data import AshareClient, AshareClientError


def test_fetch_stock_list_sync_merges_params_and_retries():
    call_counter = {"count": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        call_counter["count"] += 1
        assert request.url.path == "/stocks"
        if call_counter["count"] == 1:
            return httpx.Response(500, json={"error": "temporary"})
        assert request.url.params["market"] == "all"
        assert request.url.params["token"] == "secret"
        return httpx.Response(200, json={"data": ["AAA", "BBB"]})

    client = AshareClient(
        base_url="https://api.example.com",
        default_params={"token": "secret"},
        max_retries=2,
        backoff_factor=0,
        transport=httpx.MockTransport(handler),
    )

    result = client.fetch_stock_list()

    assert call_counter["count"] == 2
    assert result == {"data": ["AAA", "BBB"]}


def test_fetch_daily_kline_async_builds_expected_query_params():
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/daily-kline"
        assert request.url.params["token"] == "secret"
        assert request.url.params["symbol"] == "600000.SS"
        assert request.url.params["start_date"] == "2024-01-01"
        assert request.url.params["end_date"] == "2024-02-01"
        assert request.url.params["adjust"] == "qfq"
        payload = {
            "symbol": request.url.params["symbol"],
            "values": [
                {"date": "2024-01-02", "open": 10.0, "close": 11.0},
                {"date": "2024-01-03", "open": 11.0, "close": 11.5},
            ],
        }
        return httpx.Response(200, json=payload)

    client = AshareClient(
        base_url="https://api.example.com",
        default_params={"token": "secret"},
        transport=httpx.MockTransport(handler),
    )

    result = asyncio.run(
        client.fetch_daily_kline_async(
            "600000.SS",
            start_date="2024-01-01",
            end_date="2024-02-01",
            params={"adjust": "qfq"},
        )
    )

    assert result["symbol"] == "600000.SS"
    assert len(result["values"]) == 2


def test_fetch_daily_kline_sync_raises_after_retries():
    attempts = {"count": 0}

    def handler(_: httpx.Request) -> httpx.Response:
        attempts["count"] += 1
        return httpx.Response(502)

    client = AshareClient(
        base_url="https://api.example.com",
        max_retries=1,
        backoff_factor=0,
        transport=httpx.MockTransport(handler),
    )

    with pytest.raises(AshareClientError):
        client.fetch_daily_kline("000001.SZ")

    assert attempts["count"] == 2


def test_invalid_json_results_in_error():
    def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(200, text="not-json")

    client = AshareClient(
        base_url="https://api.example.com",
        max_retries=0,
        transport=httpx.MockTransport(handler),
    )

    with pytest.raises(AshareClientError):
        asyncio.run(client.fetch_stock_list_async())
