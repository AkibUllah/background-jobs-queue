from __future__ import annotations

from collections.abc import AsyncIterator

import fakeredis.aioredis
import pytest

from jobq import JobQ, ManualClock, MemoryBackend, RedisBackend, Worker
from jobq.backends.base import Backend

START = 1_750_000_000.0  # 2025-06-15T15:06:40Z


@pytest.fixture
def clock() -> ManualClock:
    return ManualClock(START)


@pytest.fixture(params=["memory", "redis"])
async def backend(request: pytest.FixtureRequest) -> AsyncIterator[Backend]:
    """Every backend test runs against both implementations."""
    if request.param == "memory":
        be: Backend = MemoryBackend()
    else:
        client = fakeredis.aioredis.FakeRedis(decode_responses=True)
        be = RedisBackend(client=client, prefix="t:")
    yield be
    await be.clear()
    await be.close()


@pytest.fixture
def app(backend: Backend, clock: ManualClock) -> JobQ:
    return JobQ(backend, clock=clock)


@pytest.fixture
def worker(app: JobQ) -> Worker:
    return Worker(app, ["high", "default", "low"], worker_id="w1", stall_timeout=60)
