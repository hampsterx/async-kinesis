"""
Timeout compatibility layer for Python 3.10+ support.

Provides consistent timeout API across Python versions.
"""

import sys

if sys.version_info >= (3, 11):
    from asyncio import timeout as _timeout

    # Wrapper to handle expired attribute difference between APIs
    class timeout:
        """Timeout context manager compatible with async_timeout API."""

        def __init__(self, delay):
            self._timeout = _timeout(delay)

        async def __aenter__(self):
            self._cm = await self._timeout.__aenter__()
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return await self._timeout.__aexit__(exc_type, exc, tb)

        @property
        def expired(self):
            """Check if timeout has expired - consistent with async_timeout API."""
            return self._cm.expired()

else:
    from async_timeout import timeout

__all__ = ["timeout"]
