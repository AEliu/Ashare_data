"""SQLite based persistence layer."""

from __future__ import annotations

import asyncio
import sqlite3
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Iterable, List, Optional, Sequence

from Ashare_data.providers.base import ProviderResult
from Ashare_data.utils.config import get_settings
from Ashare_data.utils.logging import get_logger


@dataclass(slots=True)
class Security:
    """Representation of a security stored in the database."""

    symbol: str
    name: str
    asset_type: str
    listed_date: Optional[date] = None
    delisted_date: Optional[date] = None


class SQLiteStorage:
    """A small asynchronous wrapper around :mod:`sqlite3`."""

    def __init__(self, path: Optional[Path] = None) -> None:
        settings = get_settings()
        self._path = Path(path or settings.database_path)
        self._logger = get_logger("Ashare.storage.sqlite")

    async def initialize(self) -> None:
        await asyncio.to_thread(self._initialize_sync)

    def _initialize_sync(self) -> None:
        self._logger.debug("Creating SQLite schema at %s", self._path)
        conn = sqlite3.connect(self._path)
        try:
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute("PRAGMA foreign_keys=ON;")
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS securities (
                    symbol TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    asset_type TEXT NOT NULL,
                    listed_date TEXT,
                    delisted_date TEXT
                );

                CREATE TABLE IF NOT EXISTS daily_bars (
                    symbol TEXT NOT NULL,
                    trade_date TEXT NOT NULL,
                    open REAL NOT NULL,
                    high REAL NOT NULL,
                    low REAL NOT NULL,
                    close REAL NOT NULL,
                    volume REAL NOT NULL,
                    turnover REAL NOT NULL,
                    raw_payload TEXT,
                    PRIMARY KEY (symbol, trade_date)
                );

                CREATE TABLE IF NOT EXISTS adjustment_factors (
                    symbol TEXT NOT NULL,
                    trade_date TEXT NOT NULL,
                    factor REAL NOT NULL,
                    PRIMARY KEY (symbol, trade_date)
                );
                """
            )
            conn.commit()
        finally:
            conn.close()

    async def upsert_securities(self, securities: Iterable[Security]) -> None:
        records = [
            (
                security.symbol,
                security.name,
                security.asset_type,
                security.listed_date.isoformat() if security.listed_date else None,
                security.delisted_date.isoformat() if security.delisted_date else None,
            )
            for security in securities
        ]
        if not records:
            return
        await asyncio.to_thread(self._upsert_securities_sync, records)

    def _upsert_securities_sync(self, records: Sequence[tuple]) -> None:
        conn = sqlite3.connect(self._path)
        try:
            with conn:
                conn.executemany(
                    """
                    INSERT INTO securities (symbol, name, asset_type, listed_date, delisted_date)
                    VALUES (?, ?, ?, ?, ?)
                    ON CONFLICT(symbol) DO UPDATE SET
                        name=excluded.name,
                        asset_type=excluded.asset_type,
                        listed_date=excluded.listed_date,
                        delisted_date=excluded.delisted_date
                    """,
                    records,
                )
        finally:
            conn.close()

    async def upsert_daily_bars(self, bars: Iterable[ProviderResult]) -> None:
        records = [
            (
                bar.symbol,
                bar.trade_date.isoformat(),
                bar.open,
                bar.high,
                bar.low,
                bar.close,
                bar.volume,
                bar.turnover,
                json_dumps(bar.raw),
            )
            for bar in bars
        ]
        if not records:
            return
        await asyncio.to_thread(self._upsert_daily_bars_sync, records)

    def _upsert_daily_bars_sync(self, records: Sequence[tuple]) -> None:
        conn = sqlite3.connect(self._path)
        try:
            with conn:
                conn.executemany(
                    """
                    INSERT INTO daily_bars (symbol, trade_date, open, high, low, close, volume, turnover, raw_payload)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(symbol, trade_date) DO UPDATE SET
                        open=excluded.open,
                        high=excluded.high,
                        low=excluded.low,
                        close=excluded.close,
                        volume=excluded.volume,
                        turnover=excluded.turnover,
                        raw_payload=excluded.raw_payload
                    """,
                    records,
                )
        finally:
            conn.close()

    async def upsert_adjustment_factors(self, factors: Iterable[tuple[str, date, float]]) -> None:
        records = [
            (
                symbol,
                trade_date.isoformat(),
                float(value),
            )
            for symbol, trade_date, value in factors
        ]
        if not records:
            return
        await asyncio.to_thread(self._upsert_adjustment_factors_sync, records)

    def _upsert_adjustment_factors_sync(self, records: Sequence[tuple]) -> None:
        conn = sqlite3.connect(self._path)
        try:
            with conn:
                conn.executemany(
                    """
                    INSERT INTO adjustment_factors (symbol, trade_date, factor)
                    VALUES (?, ?, ?)
                    ON CONFLICT(symbol, trade_date) DO UPDATE SET
                        factor=excluded.factor
                    """,
                    records,
                )
        finally:
            conn.close()

    async def list_tracked_symbols(self) -> List[str]:
        return await asyncio.to_thread(self._list_tracked_symbols_sync)

    def _list_tracked_symbols_sync(self) -> List[str]:
        conn = sqlite3.connect(self._path)
        try:
            cursor = conn.execute("SELECT symbol FROM securities ORDER BY symbol")
            return [row[0] for row in cursor.fetchall()]
        finally:
            conn.close()

    async def missing_daily_dates(self, symbol: str, dates: Iterable[date]) -> List[date]:
        existing = await asyncio.to_thread(self._existing_dates_sync, symbol)
        missing = [trade_date for trade_date in dates if trade_date.isoformat() not in existing]
        return missing

    def _existing_dates_sync(self, symbol: str) -> set[str]:
        conn = sqlite3.connect(self._path)
        try:
            cursor = conn.execute("SELECT trade_date FROM daily_bars WHERE symbol = ?", (symbol,))
            return {row[0] for row in cursor.fetchall()}
        finally:
            conn.close()


def json_dumps(data: Any) -> str:
    import json

    return json.dumps(data, ensure_ascii=False, separators=(",", ":"))
