"""Consistent timeout API backed by :func:`asyncio.timeout`."""

from asyncio import timeout as _timeout


class timeout:
    """Expose ``asyncio.timeout`` with the library's existing ``expired`` property."""

    def __init__(self, delay):
        self._timeout = _timeout(delay)

    async def __aenter__(self):
        self._cm = await self._timeout.__aenter__()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return await self._timeout.__aexit__(exc_type, exc, tb)

    @property
    def expired(self):
        """Return whether the timeout expired."""
        return self._cm.expired()


__all__ = ["timeout"]
