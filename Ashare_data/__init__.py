"""Public interface for the :mod:`Ashare_data` package."""

from .client import AshareClient, AshareClientError

__all__ = ["AshareClient", "AshareClientError"]
