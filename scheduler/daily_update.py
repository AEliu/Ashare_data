"""Incremental daily scheduler."""

from __future__ import annotations

from datetime import date
from typing import Iterable, Sequence

from Ashare_data.fetchers.daily import DailyFetcher
from Ashare_data.storage.sqlite import SQLiteStorage
from Ashare_data.utils.logging import get_logger


async def run_daily_update(
    storage: SQLiteStorage,
    fetcher: DailyFetcher,
    trade_date: date,
    symbols: Sequence[str] | None = None,
) -> None:
    """Fetch and persist the latest bars for ``trade_date``."""

    logger = get_logger("Ashare.scheduler.daily")
    if symbols is None:
        symbols = await storage.list_tracked_symbols()
        logger.debug("Loaded %d tracked symbols from storage", len(symbols))
    missing_map = await _compute_missing(storage, symbols, trade_date)
    for symbol, should_fetch in missing_map.items():
        if not should_fetch:
            continue
        bar = await fetcher.fetch(symbol, trade_date)
        if bar:
            await storage.upsert_daily_bars([bar])
            logger.debug("Updated %s for %s", symbol, trade_date)
    logger.info("Daily update complete for %s", trade_date)


async def _compute_missing(storage: SQLiteStorage, symbols: Iterable[str], trade_date: date) -> dict[str, bool]:
    result: dict[str, bool] = {}
    for symbol in symbols:
        missing = await storage.missing_daily_dates(symbol, [trade_date])
        result[symbol] = bool(missing)
    return result
