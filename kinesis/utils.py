"""
Source: https://github.com/hallazzang/asyncio-throttle

Mods:
    - add size_limit to support throttling by size
"""

import asyncio
import logging
import math
import time
from collections import deque

log = logging.getLogger(__name__)


class Throttler:
    def __init__(
        self,
        rate_limit=None,
        size_limit=None,
        period=1.0,
        retry_interval=0.05,
    ):
        self.rate_limit = rate_limit
        self.size_limit = size_limit
        self.period = period
        self.retry_interval = retry_interval

        self._task_logs = deque()

        self.size = None

    def flush(self):
        now = time.time()
        while self._task_logs:
            if now - self._task_logs[0][0] > self.period:
                self._task_logs.popleft()
            else:
                break

    def is_below_rate(self):

        if self.rate_limit:
            below_rate_requests = len(self._task_logs) < self.rate_limit

            if not below_rate_requests:
                return False

        if self.size_limit is None or not self._task_logs:
            return True

        size = sum([x[1] for x in self._task_logs])

        period = time.time() - self._task_logs[0][0]

        period_used_ratio = (self.period - period) / self.period

        remaining = self.size_limit - math.ceil(size * period_used_ratio)

        # log.debug(
        #     "rate check: size={} requested={} period={} period_used_ratio={} remaining={}".format(
        #         size, self.size, round(period, 3), round(period_used_ratio, 2), round(remaining, 2)
        #     )
        # )

        return self.size <= remaining

    async def acquire(self):

        while True:
            self.flush()
            if self.is_below_rate():
                break
            await asyncio.sleep(
                self.retry_interval,
            )

        self._task_logs.append((time.time(), self.size))

    def __call__(self, size=1):
        self.size = size
        return self

    async def __aenter__(self):
        await self.acquire()

    async def __aexit__(self, exc_type, exc, tb):
        pass
