"""Rate-limited claims, run against both MemoryBackend and RedisBackend (fakeredis + Lua)."""

from __future__ import annotations

import fakeredis
import fakeredis.aioredis
import pytest

from jobq.backends.base import Backend
from jobq.backends.redis import RedisBackend
from jobq.job import Job, JobStatus
from jobq.ratelimit import RateLimit

SMS = {"sms": RateLimit(2, 1.0)}  # 2 tokens, one more every 0.5s


async def _enqueue(backend: Backend, n: int, task: str = "sms", **fields) -> list[str]:
    ids = []
    for i in range(n):
        job, _ = await backend.enqueue(Job(task=task, kwargs={"i": i}, **fields), 0.0)
        ids.append(job.id)
    return ids


async def test_empty_bucket_leaves_job_queued_and_reports_wait(backend: Backend):
    ids = await _enqueue(backend, 3)
    first = await backend.try_claim(["default"], "w", 10.0, SMS)
    second = await backend.try_claim(["default"], "w", 10.0, SMS)
    assert first.job is not None and second.job is not None
    assert first.throttled_for is None

    third = await backend.try_claim(["default"], "w", 10.1, SMS)
    assert third.job is None
    assert third.throttled_for == pytest.approx(0.4)
    held = await backend.get_job(ids[2])
    assert held is not None and held.status is JobStatus.QUEUED and held.attempts == 0
    assert (await backend.queue_stats()).queues["default"] == 1

    later = await backend.try_claim(["default"], "w", 10.5, SMS)
    assert later.job is not None and later.job.id == ids[2]


async def test_claim_without_limits_ignores_buckets(backend: Backend):
    await _enqueue(backend, 3)
    for _ in range(3):
        assert await backend.claim(["default"], "w", 1.0) is not None
    assert (await backend.try_claim(["default"], "w", 1.0)).throttled_for is None


async def test_claim_wrapper_honours_limits(backend: Backend):
    await _enqueue(backend, 3)
    assert await backend.claim(["default"], "w", 1.0, SMS) is not None
    assert await backend.claim(["default"], "w", 1.0, SMS) is not None
    assert await backend.claim(["default"], "w", 1.0, SMS) is None


async def test_lower_priority_jobs_of_other_tasks_still_run(backend: Backend):
    await _enqueue(backend, 3, priority=5)
    report, _ = await backend.enqueue(Job(task="report", priority=0), 0.0)
    limits = {"sms": RateLimit(1, 1.0)}
    got = await backend.try_claim(["default"], "w", 1.0, limits)
    assert got.job is not None and got.job.task == "sms"
    got = await backend.try_claim(["default"], "w", 1.0, limits)
    assert got.job is not None and got.job.id == report.id
    got = await backend.try_claim(["default"], "w", 1.0, limits)
    assert got.job is None and got.throttled_for == pytest.approx(1.0)


async def test_throttled_queue_falls_through_to_the_next_queue(backend: Backend):
    await _enqueue(backend, 2, queue="high")
    low, _ = await backend.enqueue(Job(task="report", queue="low"), 0.0)
    limits = {"sms": RateLimit(1, 1.0)}
    first = await backend.try_claim(["high", "low"], "w", 1.0, limits)
    second = await backend.try_claim(["high", "low"], "w", 1.0, limits)
    assert first.job is not None and first.job.queue == "high"
    assert second.job is not None and second.job.id == low.id


async def test_skipped_jobs_keep_their_place_in_the_queue(backend: Backend):
    sms = await _enqueue(backend, 3)
    other, _ = await backend.enqueue(Job(task="report"), 0.0)
    limits = {"sms": RateLimit(1, 1.0)}
    order = []
    now = 0.0
    while len(order) < 4:
        got = await backend.try_claim(["default"], "w", now, limits)
        if got.job is None:
            assert got.throttled_for is not None
            now += got.throttled_for
            continue
        order.append(got.job.id)
    assert order == [sms[0], other.id, sms[1], sms[2]]


async def test_wait_is_the_earliest_next_token_across_tasks(backend: Backend):
    await _enqueue(backend, 2, task="sms")
    await _enqueue(backend, 2, task="email")
    limits = {"sms": RateLimit(1, 1.0), "email": RateLimit(1, 0.25)}
    for _ in range(2):
        assert (await backend.try_claim(["default"], "w", 0.0, limits)).job is not None
    got = await backend.try_claim(["default"], "w", 0.0, limits)
    assert got.job is None and got.throttled_for == pytest.approx(0.25)


async def test_throttling_is_not_a_failure_or_retry(backend: Backend):
    ids = await _enqueue(backend, 2)
    limits = {"sms": RateLimit(1, 60.0)}
    assert (await backend.try_claim(["default"], "w", 60.0, limits)).job is not None
    for _ in range(3):
        got = await backend.try_claim(["default"], "w", 60.0, limits)
        assert got.job is None
    held = await backend.get_job(ids[1])
    assert held is not None and held.status is JobStatus.QUEUED
    assert held.attempts == 0 and held.error is None
    bucket = (await backend.metrics(60.0, minutes=1))[0]
    assert bucket.throttled == 3
    assert bucket.failed == 0 and bucket.retried == 0
    assert (await backend.queue_stats()).dead == 0


async def test_scan_bound_stops_looking_past_a_deep_throttled_backlog(backend: Backend):
    backend.throttle_scan = 2  # type: ignore[attr-defined]
    await _enqueue(backend, 4)
    other, _ = await backend.enqueue(Job(task="report"), 0.0)
    limits = {"sms": RateLimit(1, 1.0)}
    assert (await backend.try_claim(["default"], "w", 0.0, limits)).job is not None
    got = await backend.try_claim(["default"], "w", 0.0, limits)
    assert got.job is None and got.throttled_for == pytest.approx(1.0)
    backend.throttle_scan = 10  # type: ignore[attr-defined]
    got = await backend.try_claim(["default"], "w", 0.0, limits)
    assert got.job is not None and got.job.id == other.id


async def test_bucket_refills_but_never_beyond_its_capacity(backend: Backend):
    await _enqueue(backend, 6)
    for _ in range(2):
        assert (await backend.try_claim(["default"], "w", 0.0, SMS)).job is not None
    # an hour idle refills to 2 tokens, not 7200
    claimed = 0
    while (await backend.try_claim(["default"], "w", 3600.0, SMS)).job is not None:
        claimed += 1
    assert claimed == 2


async def test_limit_is_shared_by_two_workers(backend: Backend):
    await _enqueue(backend, 10)
    limits = {"sms": RateLimit(3, 1.0)}
    claimed: dict[str, int] = {"w1": 0, "w2": 0}
    for _ in range(4):
        for worker in ("w1", "w2"):
            if (await backend.try_claim(["default"], worker, 5.0, limits)).job is not None:
                claimed[worker] += 1
    assert sum(claimed.values()) == 3
    assert claimed["w1"] >= 1 and claimed["w2"] >= 1


async def test_two_redis_clients_share_one_bucket():
    """Two worker processes = two connections to the same Redis."""
    server = fakeredis.FakeServer()
    a = RedisBackend(client=fakeredis.aioredis.FakeRedis(server=server, decode_responses=True))
    b = RedisBackend(client=fakeredis.aioredis.FakeRedis(server=server, decode_responses=True))
    try:
        for i in range(5):
            await a.enqueue(Job(task="sms", kwargs={"i": i}), 0.0)
        limits = {"sms": RateLimit(2, 1.0)}
        assert (await a.try_claim(["default"], "a", 1.0, limits)).job is not None
        assert (await b.try_claim(["default"], "b", 1.0, limits)).job is not None
        blocked = await a.try_claim(["default"], "a", 1.0, limits)
        assert blocked.job is None and blocked.throttled_for == pytest.approx(0.5)
        assert (await b.try_claim(["default"], "b", 1.5, limits)).job is not None
        assert (await a.try_claim(["default"], "a", 1.5, limits)).job is None
    finally:
        await a.clear()
        await a.close()
        await b.close()


async def test_rate_limit_states_report_tokens_and_throttled_counts(backend: Backend):
    await _enqueue(backend, 4)
    limits = {"sms": RateLimit(2, 1.0), "email": RateLimit(10, 60.0)}
    for _ in range(2):
        assert (await backend.try_claim(["default"], "w", 100.0, limits)).job is not None
    for _ in range(3):
        assert (await backend.try_claim(["default"], "w", 100.2, limits)).job is None

    states = {s.task: s for s in await backend.rate_limit_states(limits, 100.2)}
    assert sorted(states) == ["email", "sms"]
    sms = states["sms"]
    assert sms.tokens == pytest.approx(0.4)
    assert sms.next_token_in == pytest.approx(0.3)
    assert sms.throttled == 3
    assert sms.to_dict() == {
        "task": "sms",
        "spec": "2/s",
        "limit": 2,
        "period": 1.0,
        "tokens": 0.4,
        "next_token_in": 0.3,
        "throttled": 3,
    }
    email = states["email"]  # never claimed: a full bucket and nothing throttled
    assert email.tokens == 10.0 and email.next_token_in == 0.0 and email.throttled == 0

    later = {s.task: s for s in await backend.rate_limit_states(limits, 105.0)}
    assert later["sms"].tokens == 2.0 and later["sms"].throttled == 3


async def test_rate_limit_states_with_no_limits(backend: Backend):
    assert await backend.rate_limit_states({}, 1.0) == []
