"""Behavioural tests that run against both MemoryBackend and RedisBackend (fakeredis + Lua)."""

from __future__ import annotations

import pytest

from jobq import JobQ, ManualClock
from jobq.backends.base import Backend, EnqueueOutcome
from jobq.backends.redis import RedisBackend
from jobq.job import Job, JobStatus, WorkerInfo


async def test_priority_then_fifo_ordering(backend: Backend):
    now = 1.0
    ids = []
    for prio, label in [(0, "a"), (5, "b"), (0, "c"), (5, "d"), (-1, "e")]:
        job, _ = await backend.enqueue(Job(task="t", priority=prio, kwargs={"l": label}), now)
        ids.append(job.id)
    order = []
    while (job := await backend.claim(["default"], "w", now)) is not None:
        order.append(job.kwargs["l"])
    assert order == ["b", "d", "a", "c", "e"]


async def test_queue_order_is_respected_on_claim(backend: Backend):
    await backend.enqueue(Job(task="t", queue="low", kwargs={"l": "low"}), 1.0)
    await backend.enqueue(Job(task="t", queue="high", kwargs={"l": "high"}), 1.0)
    first = await backend.claim(["high", "default", "low"], "w", 1.0)
    second = await backend.claim(["high", "default", "low"], "w", 1.0)
    assert first is not None and first.kwargs["l"] == "high"
    assert second is not None and second.kwargs["l"] == "low"
    assert await backend.claim(["high", "default", "low"], "w", 1.0) is None


async def test_claim_marks_running_and_increments_attempts(backend: Backend):
    job, _ = await backend.enqueue(Job(task="t"), 10.0)
    claimed = await backend.claim(["default"], "worker-1", 11.0)
    assert claimed is not None
    assert claimed.status is JobStatus.RUNNING
    assert claimed.attempts == 1
    assert claimed.worker_id == "worker-1"
    assert claimed.started_at == 11.0
    stored = await backend.get_job(job.id)
    assert stored is not None and stored.status is JobStatus.RUNNING


async def test_delayed_job_is_scheduled_until_promoted(backend: Backend):
    job, _ = await backend.enqueue(Job(task="t", run_at=100.0), 50.0)
    assert job.status is JobStatus.SCHEDULED
    assert await backend.claim(["default"], "w", 60.0) is None
    assert await backend.promote_scheduled(99.0) == 0
    assert await backend.promote_scheduled(100.0) == 1
    claimed = await backend.claim(["default"], "w", 100.0)
    assert claimed is not None and claimed.id == job.id


async def test_promote_respects_limit(backend: Backend):
    for _ in range(5):
        await backend.enqueue(Job(task="t", run_at=10.0), 1.0)
    assert await backend.promote_scheduled(10.0, limit=2) == 2
    assert await backend.promote_scheduled(10.0, limit=10) == 3


async def test_unique_key_dedupes_active_jobs_only(backend: Backend):
    first, outcome1 = await backend.enqueue(Job(task="t", unique_key="k1"), 1.0)
    second, outcome2 = await backend.enqueue(Job(task="t", unique_key="k1"), 2.0)
    assert outcome1 is EnqueueOutcome.ENQUEUED
    assert outcome2 is EnqueueOutcome.DEDUPED
    assert second.id == first.id
    # once the first finishes the key is free again
    claimed = await backend.claim(["default"], "w", 3.0)
    assert claimed is not None
    await backend.complete(claimed.id, {"ok": True}, 4.0)
    third, outcome3 = await backend.enqueue(Job(task="t", unique_key="k1"), 5.0)
    assert outcome3 is EnqueueOutcome.ENQUEUED and third.id != first.id


async def test_unique_key_still_held_while_running_or_retrying(backend: Backend):
    first, _ = await backend.enqueue(Job(task="t", unique_key="k"), 1.0)
    await backend.claim(["default"], "w", 2.0)
    _, outcome = await backend.enqueue(Job(task="t", unique_key="k"), 3.0)
    assert outcome is EnqueueOutcome.DEDUPED
    await backend.fail(first.id, "boom", 4.0, retry_at=50.0)
    _, outcome = await backend.enqueue(Job(task="t", unique_key="k"), 5.0)
    assert outcome is EnqueueOutcome.DEDUPED


async def test_dependencies_run_after_parents_succeed(backend: Backend):
    a, _ = await backend.enqueue(Job(task="t", kwargs={"l": "a"}), 1.0)
    b, _ = await backend.enqueue(Job(task="t", kwargs={"l": "b"}), 1.0)
    c, outcome = await backend.enqueue(
        Job(task="t", kwargs={"l": "c"}, depends_on=[a.id, b.id]), 1.0
    )
    assert outcome is EnqueueOutcome.ENQUEUED
    assert c.status is JobStatus.WAITING and c.deps_remaining == 2

    ja = await backend.claim(["default"], "w", 2.0)
    jb = await backend.claim(["default"], "w", 2.0)
    assert {ja.kwargs["l"], jb.kwargs["l"]} == {"a", "b"}  # type: ignore[union-attr]
    assert await backend.claim(["default"], "w", 2.0) is None  # c must not be claimable yet

    await backend.complete(ja.id, None, 3.0)  # type: ignore[union-attr]
    assert (await backend.get_job(c.id)).status is JobStatus.WAITING  # type: ignore[union-attr]
    await backend.complete(jb.id, None, 4.0)  # type: ignore[union-attr]
    jc = await backend.claim(["default"], "w", 5.0)
    assert jc is not None and jc.id == c.id


async def test_dependency_on_already_finished_job_is_satisfied(backend: Backend):
    a, _ = await backend.enqueue(Job(task="t"), 1.0)
    await backend.claim(["default"], "w", 1.0)
    await backend.complete(a.id, 1, 2.0)
    b, _ = await backend.enqueue(Job(task="t", depends_on=[a.id]), 3.0)
    assert b.status is JobStatus.QUEUED


async def test_dependent_with_delay_is_scheduled_after_parent_completes(backend: Backend):
    a, _ = await backend.enqueue(Job(task="t"), 1.0)
    b, _ = await backend.enqueue(Job(task="t", depends_on=[a.id], run_at=100.0), 1.0)
    await backend.claim(["default"], "w", 2.0)
    await backend.complete(a.id, None, 3.0)
    assert (await backend.get_job(b.id)).status is JobStatus.SCHEDULED  # type: ignore[union-attr]
    await backend.promote_scheduled(100.0)
    assert (await backend.get_job(b.id)).status is JobStatus.QUEUED  # type: ignore[union-attr]


async def test_failed_dependency_cancels_the_whole_chain(backend: Backend):
    a, _ = await backend.enqueue(Job(task="t", max_retries=0), 1.0)
    b, _ = await backend.enqueue(Job(task="t", depends_on=[a.id]), 1.0)
    c, _ = await backend.enqueue(Job(task="t", depends_on=[b.id]), 1.0)
    await backend.claim(["default"], "w", 2.0)
    await backend.fail(a.id, "Traceback: boom", 3.0, retry_at=None)
    sa, sb, sc = [await backend.get_job(j.id) for j in (a, b, c)]
    assert sa.status is JobStatus.FAILED  # type: ignore[union-attr]
    assert sb.status is JobStatus.CANCELLED and a.id in sb.error  # type: ignore[union-attr]
    assert sc.status is JobStatus.CANCELLED and b.id in sc.error  # type: ignore[union-attr]
    stats = await backend.queue_stats()
    assert stats.dead == 3 and stats.waiting == 0
    # enqueueing against a dead parent is rejected immediately
    d, outcome = await backend.enqueue(Job(task="t", depends_on=[a.id]), 4.0)
    assert outcome is EnqueueOutcome.CANCELLED and d.status is JobStatus.CANCELLED


async def test_fail_with_retry_moves_to_scheduled_then_back_to_queue(backend: Backend):
    job, _ = await backend.enqueue(Job(task="t"), 1.0)
    await backend.claim(["default"], "w", 2.0)
    await backend.fail(job.id, "ConnectionError", 3.0, retry_at=13.0)
    stored = await backend.get_job(job.id)
    assert stored is not None
    assert (
        stored.status is JobStatus.RETRYING
        and stored.run_at == 13.0
        and stored.error == "ConnectionError"
    )
    assert (await backend.queue_stats()).scheduled == 1
    assert await backend.claim(["default"], "w", 5.0) is None
    await backend.promote_scheduled(13.0)
    again = await backend.claim(["default"], "w", 13.0)
    assert again is not None and again.attempts == 2


async def test_dead_letter_after_final_failure_and_retry_action(backend: Backend):
    job, _ = await backend.enqueue(Job(task="t", max_retries=1), 1.0)
    await backend.claim(["default"], "w", 2.0)
    await backend.fail(job.id, "err", 3.0, retry_at=None)
    dead, total = await backend.list_jobs(status=JobStatus.FAILED)
    assert total == 1 and dead[0].id == job.id and dead[0].finished_at == 3.0
    assert (await backend.queue_stats()).dead == 1

    assert await backend.retry_job("missing", 4.0) is None
    retried = await backend.retry_job(job.id, 4.0)
    assert retried is not None and retried.status is JobStatus.QUEUED and retried.attempts == 0
    assert (await backend.queue_stats()).dead == 0
    assert await backend.retry_job(job.id, 5.0) is None  # only dead jobs can be retried


async def test_requeue_scheduled_job_runs_now(backend: Backend):
    job, _ = await backend.enqueue(Job(task="t", run_at=1000.0), 1.0)
    assert await backend.claim(["default"], "w", 2.0) is None
    requeued = await backend.requeue_job(job.id, 2.0)
    assert requeued is not None and requeued.status is JobStatus.QUEUED and requeued.run_at is None
    assert (await backend.queue_stats()).scheduled == 0
    assert await backend.requeue_job(job.id, 2.0) is None  # already queued


async def test_stalled_running_job_is_recovered(backend: Backend):
    job, _ = await backend.enqueue(Job(task="t", max_retries=3), 1.0)
    await backend.claim(["default"], "dead-worker", 10.0)
    assert await backend.recover_stalled(40.0, stall_timeout=60) == []
    await backend.heartbeat([job.id], 50.0)
    assert await backend.recover_stalled(100.0, stall_timeout=60) == []  # heartbeat at 50 is fresh
    recovered = await backend.recover_stalled(111.0, stall_timeout=60)
    assert recovered == [job.id]
    stored = await backend.get_job(job.id)
    assert stored is not None and stored.status is JobStatus.QUEUED
    assert "stalled" in (stored.error or "") and stored.worker_id is None
    claimed = await backend.claim(["default"], "w2", 112.0)
    assert claimed is not None and claimed.attempts == 2


async def test_stalled_job_out_of_retries_goes_to_dead_letter(backend: Backend):
    job, _ = await backend.enqueue(Job(task="t", max_retries=0), 1.0)
    await backend.claim(["default"], "w", 10.0)
    assert await backend.recover_stalled(200.0, stall_timeout=60) == [job.id]
    stored = await backend.get_job(job.id)
    assert stored is not None and stored.status is JobStatus.FAILED


async def test_release_puts_job_at_front_without_counting_attempt(backend: Backend):
    first, _ = await backend.enqueue(Job(task="t", kwargs={"l": "first"}), 1.0)
    await backend.enqueue(Job(task="t", kwargs={"l": "second"}), 1.0)
    claimed = await backend.claim(["default"], "w", 2.0)
    assert claimed is not None and claimed.id == first.id
    await backend.release(first.id)
    stored = await backend.get_job(first.id)
    assert stored is not None and stored.status is JobStatus.QUEUED and stored.attempts == 0
    again = await backend.claim(["default"], "w", 3.0)
    assert again is not None and again.id == first.id and again.attempts == 1


async def test_complete_stores_result_and_metrics(backend: Backend):
    job, _ = await backend.enqueue(Job(task="t"), 120.0)
    await backend.claim(["default"], "w", 121.0)
    await backend.complete(job.id, {"sent": True, "n": 3}, 125.0)
    stored = await backend.get_job(job.id)
    assert stored is not None
    assert stored.status is JobStatus.SUCCEEDED and stored.result == {"sent": True, "n": 3}
    assert stored.finished_at == 125.0 and stored.duration == 4.0
    buckets = await backend.metrics(125.0, minutes=2)
    assert [b.minute for b in buckets] == [1, 2]
    assert buckets[-1].enqueued == 1 and buckets[-1].succeeded == 1 and buckets[-1].failed == 0


async def test_list_jobs_filters_and_paginates(backend: Backend):
    for i in range(6):
        await backend.enqueue(Job(task="t", queue="high" if i % 2 else "default"), float(i))
    await backend.enqueue(Job(task="t", run_at=999.0), 10.0)
    page, total = await backend.list_jobs(status=JobStatus.QUEUED, limit=4)
    assert total == 6 and len(page) == 4
    assert [j.created_at for j in page] == [5.0, 4.0, 3.0, 2.0]  # newest first
    rest, _ = await backend.list_jobs(status=JobStatus.QUEUED, limit=4, offset=4)
    assert [j.created_at for j in rest] == [1.0, 0.0]
    high, total_high = await backend.list_jobs(status=JobStatus.QUEUED, queue="high")
    assert total_high == 3 and all(j.queue == "high" for j in high)
    everything, total_all = await backend.list_jobs(limit=100)
    assert total_all == 7 and len(everything) == 7
    scheduled, _ = await backend.list_jobs(status=JobStatus.SCHEDULED)
    assert len(scheduled) == 1 and scheduled[0].run_at == 999.0


async def test_delete_job_removes_from_queue_and_indexes(backend: Backend):
    job, _ = await backend.enqueue(Job(task="t", unique_key="u"), 1.0)
    assert await backend.delete_job(job.id) is True
    assert await backend.delete_job(job.id) is False
    assert await backend.get_job(job.id) is None
    assert await backend.claim(["default"], "w", 2.0) is None
    assert (await backend.queue_stats()).queued == 0
    _, outcome = await backend.enqueue(Job(task="t", unique_key="u"), 3.0)
    assert outcome is EnqueueOutcome.ENQUEUED  # unique slot freed


async def test_queue_stats_counts(backend: Backend):
    await backend.enqueue(Job(task="t", queue="high"), 1.0)
    await backend.enqueue(Job(task="t"), 1.0)
    await backend.enqueue(Job(task="t", run_at=50.0), 1.0)
    running, _ = await backend.enqueue(Job(task="t"), 1.0)
    await backend.claim(["default"], "w", 2.0)  # claims the oldest default job
    stats = await backend.queue_stats()
    assert stats.queues == {"high": 1, "default": 1}
    assert stats.scheduled == 1 and stats.running == 1 and stats.dead == 0
    assert stats.to_dict()["counts"]["queued"] == 2
    assert running.queue == "default"


async def test_worker_registry_expires_stale_workers(backend: Backend):
    info = WorkerInfo(
        id="w1", hostname="h", pid=1, queues=["default"], concurrency=4, started_at=1.0
    )
    await backend.register_worker(info, 10.0)
    assert [w.id for w in await backend.list_workers(20.0, stale_after=60)] == ["w1"]
    assert await backend.list_workers(100.0, stale_after=60) == []
    await backend.register_worker(info, 100.0)
    await backend.deregister_worker("w1")
    assert await backend.list_workers(100.0) == []


async def test_metrics_window_shape(backend: Backend):
    buckets = await backend.metrics(600.0, minutes=5)
    assert [b.minute for b in buckets] == [6, 7, 8, 9, 10]
    assert all(b.enqueued == 0 for b in buckets)


async def test_result_ttl_is_applied(backend: Backend, clock: ManualClock):
    job, _ = await backend.enqueue(Job(task="t", result_ttl=120), clock.now())
    await backend.claim(["default"], "w", clock.now())
    await backend.complete(job.id, "done", clock.now())
    if isinstance(backend, RedisBackend):
        ttl = await backend.redis.ttl(backend.key("job", job.id))
        assert 0 < ttl <= 120
    else:
        clock.advance(119)
        await backend.promote_scheduled(clock.now())
        assert await backend.get_job(job.id) is not None
        clock.advance(2)
        await backend.promote_scheduled(clock.now())
        assert await backend.get_job(job.id) is None


async def test_app_enqueue_wires_delay_and_dedupe(app: JobQ, clock: ManualClock):
    @app.task(unique=True, queue="high", max_retries=7, timeout=9)
    async def send(phone: str) -> str:
        return phone

    job = await send.enqueue("+8801711000000")
    assert job.queue == "high" and job.max_retries == 7 and job.timeout == 9
    assert job.unique_key is not None and job.unique_key.startswith(send.name)
    same = await send.enqueue("+8801711000000")
    other = await send.enqueue("+8801811000000")
    assert same.id == job.id and other.id != job.id

    delayed = await send.options(delay=30, priority=3, unique_key="custom").enqueue("+8801911")
    assert delayed.status is JobStatus.SCHEDULED
    assert delayed.run_at == pytest.approx(clock.now() + 30)
    assert delayed.priority == 3 and delayed.unique_key == "custom"

    with pytest.raises(ValueError):
        await send.options(priority=5000).enqueue("x")
