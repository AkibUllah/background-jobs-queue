"""Task-level rate limits driven through JobQ and Worker on both backends."""

from __future__ import annotations

import asyncio

import pytest

from jobq import Backoff, JobQ, ManualClock, RateLimit, SystemClock, Worker
from jobq.backends.base import ClaimResult
from jobq.exceptions import JobQError
from jobq.job import JobStatus


def test_task_decorator_parses_rate_limit(app: JobQ):
    @app.task(rate_limit="10/s")
    async def send_sms() -> None:
        return None

    @app.task(rate_limit=RateLimit(5, 60.0))
    async def call_partner() -> None:
        return None

    @app.task
    async def unlimited() -> None:
        return None

    assert send_sms.rate_limit == RateLimit(10, 1.0)
    assert "rate_limit=10/s" in repr(send_sms)
    assert unlimited.rate_limit is None
    assert app.rate_limits == {
        send_sms.name: RateLimit(10, 1.0),
        call_partner.name: RateLimit(5, 60.0),
    }


def test_invalid_rate_limit_is_rejected_at_declaration(app: JobQ):
    with pytest.raises(ValueError, match="invalid rate limit"):

        @app.task(rate_limit="10 per second")
        async def bad() -> None:
            return None

    with pytest.raises(JobQError):
        app.get_task("bad")  # nothing was registered


async def test_two_workers_share_the_limit(app: JobQ, clock: ManualClock):
    ran: list[str] = []

    @app.task(rate_limit="2/s")
    async def send_sms(n: int) -> int:
        ran.append(f"sms{n}")
        return n

    for n in range(6):
        await send_sms.enqueue(n)
    w1 = Worker(app, ["default"], worker_id="w1")
    w2 = Worker(app, ["default"], worker_id="w2")

    async def round_robin() -> int:
        count = 0
        while True:
            progressed = False
            for w in (w1, w2):
                if await w.process_one() is not None:
                    count += 1
                    progressed = True
            if not progressed:
                return count

    assert await round_robin() == 2  # the burst, split between the workers
    assert w1.processed == 1 and w2.processed == 1
    clock.advance(0.25)
    assert await round_robin() == 0  # half a token
    clock.advance(0.25)
    assert await round_robin() == 1
    clock.advance(1.0)
    assert await round_robin() == 2  # capacity caps the refill at 2
    clock.advance(10)
    assert await round_robin() == 1  # only one job left
    assert len(ran) == 6


async def test_throttled_jobs_are_not_failures_or_retries(app: JobQ, clock: ManualClock):
    @app.task(rate_limit="1/m", max_retries=0)
    async def call_partner() -> str:
        return "ok"

    first = await call_partner.enqueue()
    second = await call_partner.enqueue()
    worker = Worker(app, ["default"], worker_id="w")
    done = await worker.process_one()
    assert done is not None and done.id == first.id and done.status is JobStatus.SUCCEEDED
    for _ in range(5):
        assert await worker.process_one() is None
        clock.advance(5)
    held = await app.get_job(second.id)
    assert held.status is JobStatus.QUEUED and held.attempts == 0 and held.error is None
    assert worker.failed == 0
    clock.advance(35)  # 60s after the first claim
    done = await worker.process_one()
    assert done is not None and done.id == second.id and done.status is JobStatus.SUCCEEDED
    totals = {"failed": 0, "retried": 0, "throttled": 0}
    for bucket in await app.backend.metrics(clock.now(), minutes=5):
        for key in totals:
            totals[key] += getattr(bucket, key)
    assert totals == {"failed": 0, "retried": 0, "throttled": 5}


async def test_other_tasks_keep_flowing_while_one_is_throttled(app: JobQ, clock: ManualClock):
    @app.task(rate_limit="1/s")
    async def send_sms(n: int) -> int:
        return n

    @app.task
    async def build_report(n: int) -> int:
        return n

    for n in range(3):
        await send_sms.options(priority=10).enqueue(n)
    for n in range(3):
        await build_report.enqueue(n)
    worker = Worker(app, ["default"], worker_id="w")
    tasks = [job.task for job in [await worker.process_one() for _ in range(4)] if job]
    assert tasks == [send_sms.name] + [build_report.name] * 3
    assert await worker.process_one() is None
    claimed = await worker.claim()
    assert claimed.job is None and claimed.throttled_for == pytest.approx(1.0)


async def test_retry_of_a_rate_limited_task_takes_a_new_token(app: JobQ, clock: ManualClock):
    calls = 0

    @app.task(rate_limit="1/10s", max_retries=1, backoff=Backoff(base=2, jitter=0))
    async def flaky() -> str:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise ConnectionError("partner 503")
        return "ok"

    await flaky.enqueue()
    worker = Worker(app, ["default"], worker_id="w")
    first = await worker.process_one()
    assert first is not None and first.status is JobStatus.RETRYING
    clock.advance(2)
    assert (await worker.tick())["promoted"] == 1
    assert await worker.process_one() is None  # retry is due but the bucket is not
    clock.advance(8)
    second = await worker.process_one()
    assert second is not None and second.status is JobStatus.SUCCEEDED and second.attempts == 2


async def test_run_loop_sleeps_until_the_next_token_instead_of_spinning():
    app = JobQ(clock=SystemClock())

    @app.task(rate_limit=RateLimit(1, 0.2))
    async def ping(n: int) -> int:
        return n

    for n in range(3):
        await ping.enqueue(n)
    worker = Worker(app, ["default"], poll_interval=5.0, worker_id="w", run_housekeeping=False)
    claims: list[ClaimResult] = []
    original = worker.claim

    async def counting_claim() -> ClaimResult:
        result = await original()
        claims.append(result)
        return result

    worker.claim = counting_claim  # type: ignore[method-assign]
    loop = asyncio.get_running_loop()
    started = loop.time()
    runner = asyncio.create_task(worker.run())
    for _ in range(300):
        await asyncio.sleep(0.01)
        if worker.processed == 3:
            break
    elapsed = loop.time() - started
    worker.stop()
    await asyncio.wait_for(runner, timeout=5)
    assert worker.processed == 3
    # two refills of 0.2s each, slept precisely rather than for the 5s poll interval
    assert 0.35 <= elapsed < 2.0
    throttled = [c for c in claims if c.job is None and c.throttled_for is not None]
    assert 1 <= len(throttled) <= 10  # a busy loop would make thousands of calls
