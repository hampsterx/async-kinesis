import asyncio

import pytest

from kinesis.timeout_compat import timeout


@pytest.mark.asyncio
async def test_timeout_reports_not_expired_after_success():
    async with timeout(1) as context:
        await asyncio.sleep(0)

    assert context.expired is False


@pytest.mark.asyncio
async def test_timeout_reports_expired_after_timeout():
    context = timeout(0)

    with pytest.raises(TimeoutError):
        async with context:
            await asyncio.sleep(0)

    assert context.expired is True
