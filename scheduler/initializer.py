"""Full universe bootstrap tasks."""

from __future__ import annotations

from datetime import date, datetime, timedelta
from pathlib import Path
from typing import List, Sequence

from Ashare_data.fetchers.daily import DailyFetcher
from Ashare_data.storage.sqlite import SQLiteStorage
from Ashare_data.utils.config import get_settings
from Ashare_data.utils.logging import get_logger


async def run_initial_load(
    storage: SQLiteStorage,
    fetcher: DailyFetcher,
    symbols: Sequence[str],
    start: date,
    end: date,
) -> None:
    """Fetch historic data for ``symbols`` and store it in ``storage``."""

    logger = get_logger("Ashare.scheduler.initializer")
    calendar = load_trading_calendar(start, end)
    logger.info("Starting initial load for %d symbols (%s -> %s)", len(symbols), start, end)
    for symbol in symbols:
        bars = []
        for trade_date in calendar:
            result = await fetcher.fetch(symbol, trade_date)
            if result:
                bars.append(result)
        if bars:
            await storage.upsert_daily_bars(bars)
            logger.debug("Inserted %d bars for %s", len(bars), symbol)
    logger.info("Initial load complete")


def load_trading_calendar(start: date, end: date) -> List[date]:
    """Return a list of trading dates between ``start`` and ``end``."""

    settings = get_settings()
    if settings.calendar_path and settings.calendar_path.exists():
        return _calendar_from_file(settings.calendar_path, start, end)
    return _calendar_from_weekdays(start, end)


def _calendar_from_weekdays(start: date, end: date) -> List[date]:
    cursor = start
    calendar: List[date] = []
    while cursor <= end:
        if cursor.weekday() < 5:  # Monday-Friday
            calendar.append(cursor)
        cursor += timedelta(days=1)
    return calendar


def _calendar_from_file(path: Path, start: date, end: date) -> List[date]:
    dates: List[date] = []
    with path.open("r", encoding="utf-8") as calendar_file:
        for line in calendar_file:
            line = line.strip()
            if not line:
                continue
            try:
                current = datetime.strptime(line, "%Y-%m-%d").date()
            except ValueError:
                continue
            if start <= current <= end:
                dates.append(current)
    return dates
