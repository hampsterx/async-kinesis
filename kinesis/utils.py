# filtched from here: https://github.com/hallazzang/asyncio-throttle
# needed to add loop arg

import time
import asyncio
from collections import deque


class Throttler:
    def __init__(self, rate_limit, period=1.0, retry_interval=0.01, loop=None):
        self.rate_limit = rate_limit
        self.period = period
        self.loop = loop
        self.retry_interval = retry_interval

        self._task_logs = deque()

    def flush(self):
        now = time.time()
        while self._task_logs:
            if now - self._task_logs[0] > self.period:
                self._task_logs.popleft()
            else:
                break

    async def acquire(self):
        while True:
            self.flush()
            if len(self._task_logs) < self.rate_limit:
                break
            await asyncio.sleep(
                self.retry_interval,
                loop=self.loop if self.loop else asyncio.get_event_loop(),
            )

        self._task_logs.append(time.time())

    async def __aenter__(self):
        await self.acquire()

    async def __aexit__(self, exc_type, exc, tb):
        pass
