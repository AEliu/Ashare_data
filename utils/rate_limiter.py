"""Asynchronous rate limiting and retry utilities."""

from __future__ import annotations

import asyncio
from functools import wraps
from typing import Any, Awaitable, Callable, TypeVar

T = TypeVar("T")


class AsyncRateLimiter:
    """A token bucket style rate limiter for async contexts."""

    def __init__(self, *, rate: int, per: float) -> None:
        if rate <= 0:
            raise ValueError("rate must be positive")
        if per <= 0:
            raise ValueError("per must be positive")
        self._semaphore = asyncio.Semaphore(rate)
        self._period = per

    async def acquire(self) -> None:
        await self._semaphore.acquire()
        asyncio.create_task(self._delayed_release())

    async def _delayed_release(self) -> None:
        await asyncio.sleep(self._period)
        self._semaphore.release()


def async_retry(*, attempts: int = 3, base_delay: float = 0.5, exceptions: tuple[type[BaseException], ...] = (Exception,)) -> Callable[[Callable[..., Awaitable[T]]], Callable[..., Awaitable[T]]]:
    """Retry decorator for ``async`` callables."""

    def decorator(func: Callable[..., Awaitable[T]]) -> Callable[..., Awaitable[T]]:
        @wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> T:
            delay = base_delay
            for attempt in range(1, attempts + 1):
                try:
                    return await func(*args, **kwargs)
                except exceptions:
                    if attempt == attempts:
                        raise
                    await asyncio.sleep(delay)
                    delay *= 2
            raise RuntimeError("retry decorator failed unexpectedly")

        return wrapper

    return decorator
