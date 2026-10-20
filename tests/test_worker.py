from __future__ import annotations

import asyncio
import threading

import pytest

from jobq import Backoff, JobQ, JobQError, ManualClock, Retry, Worker
from jobq.job import JobStatus


async def test_async_and_sync_tasks_run_and_store_results(app: JobQ, worker: Worker):
    @app.task
    async def add(a: int, b: int) -> int:
        return a + b

    @app.task(queue="low")
    def thread_name() -> str:
        return threading.current_thread().name

    j1 = await add.enqueue(2, 3)
    j2 = await thread_name.enqueue()
    done1 = await worker.process_one()
    done2 = await worker.process_one()
    assert done1 is not None and done1.id == j1.id and done1.result == 5
    assert done2 is not None and done2.id == j2.id
    assert done2.result.startswith("jobq")  # ran in the worker's thread pool
    assert await worker.process_one() is None
    assert worker.processed == 2


async def test_failure_retries_with_backoff_then_dead_letters(
    app: JobQ, worker: Worker, clock: ManualClock
):
    calls = 0

    @app.task(max_retries=2, backoff=Backoff(base=10, factor=2, jitter=0))
    async def flaky() -> None:
        nonlocal calls
        calls += 1
        raise ConnectionError("gateway down")

    job = await flaky.enqueue()
    first = await worker.process_one()
    assert first is not None and first.status is JobStatus.RETRYING
    assert first.run_at == pytest.approx(clock.now() + 10)
    assert "ConnectionError: gateway down" in (first.error or "")
    assert "Traceback" in (first.error or "")

    assert await worker.process_one() is None  # not due yet
    clock.advance(10)
    await worker.tick()
    second = await worker.process_one()
    assert second is not None and second.status is JobStatus.RETRYING
    assert second.run_at == pytest.approx(clock.now() + 20)  # exponential

    clock.advance(20)
    await worker.tick()
    third = await worker.process_one()
    assert third is not None and third.status is JobStatus.FAILED and third.attempts == 3
    assert calls == 3 and worker.failed == 1
    dead, _ = await app.backend.list_jobs(status=JobStatus.FAILED)
    assert [j.id for j in dead] == [job.id]


async def test_retry_exception_controls_delay(app: JobQ, worker: Worker, clock: ManualClock):
    @app.task(max_retries=5)
    async def poll_payment() -> str:
        raise Retry("still pending", delay=42)

    await poll_payment.enqueue()
    job = await worker.process_one()
    assert job is not None and job.status is JobStatus.RETRYING
    assert job.run_at == pytest.approx(clock.now() + 42)
    assert job.error == "Retry: still pending"


async def test_timeout_is_recorded_and_retried(app: JobQ, worker: Worker):
    @app.task(timeout=0.05, max_retries=0)
    async def slow() -> None:
        await asyncio.sleep(5)

    await slow.enqueue()
    job = await worker.process_one()
    assert job is not None and job.status is JobStatus.FAILED
    assert job.error is not None and job.error.startswith("JobTimeout")


async def test_unknown_task_fails_without_retry(app: JobQ, worker: Worker):
    job = await app.enqueue("not.registered", [1])
    done = await worker.process_one()
    assert done is not None and done.id == job.id and done.status is JobStatus.FAILED
    assert "TaskNotFound" in (done.error or "")


async def test_drain_runs_chain_in_dependency_order(app: JobQ, worker: Worker):
    order: list[str] = []

    @app.task
    async def step(name: str) -> str:
        order.append(name)
        return name

    extract = await step.enqueue("extract")
    transform = await step.options(depends_on=(extract.id,)).enqueue("transform")
    await step.options(depends_on=(transform.id,)).enqueue("load")
    await step.options(priority=10).enqueue("urgent")
    assert await worker.drain() == 4
    assert order == ["urgent", "extract", "transform", "load"]


async def test_result_helper_waits_and_raises(app: JobQ, worker: Worker):
    @app.task(max_retries=0)
    async def ok() -> dict:
        return {"ok": True}

    @app.task(max_retries=0)
    async def bad() -> None:
        raise ValueError("nope")

    j1 = await ok.enqueue()
    j2 = await bad.enqueue()
    await worker.drain()
    assert await app.result(j1.id) == {"ok": True}
    with pytest.raises(JobQError, match="failed"):
        await app.result(j2.id)


async def test_run_loop_processes_jobs_and_stops_gracefully(app: JobQ):
    @app.task
    async def quick(n: int) -> int:
        await asyncio.sleep(0.01)
        return n * 2

    for n in range(5):
        await quick.enqueue(n)
    w = Worker(
        app,
        ["default"],
        concurrency=3,
        poll_interval=0.01,
        heartbeat_interval=0.05,
        housekeeping_interval=0.05,
        worker_id="loop-worker",
    )
    runner = asyncio.create_task(w.run())
    for _ in range(200):
        await asyncio.sleep(0.01)
        if w.processed == 5:
            break
    workers = await app.backend.list_workers(app.clock.now())
    assert [x.id for x in workers] == ["loop-worker"]
    w.stop()
    await asyncio.wait_for(runner, timeout=5)
    assert w.processed == 5
    assert await app.backend.list_workers(app.clock.now()) == []
    succeeded, total = await app.backend.list_jobs(status=JobStatus.SUCCEEDED)
    assert total == 5 and sorted(j.result for j in succeeded) == [0, 2, 4, 6, 8]


async def test_shutdown_releases_unfinished_jobs(app: JobQ):
    started = asyncio.Event()

    @app.task
    async def forever() -> None:
        started.set()
        await asyncio.sleep(60)

    job = await forever.enqueue()
    w = Worker(app, ["default"], poll_interval=0.01, shutdown_timeout=0.05, worker_id="w")
    runner = asyncio.create_task(w.run())
    await asyncio.wait_for(started.wait(), timeout=5)
    w.stop()
    await asyncio.wait_for(runner, timeout=5)
    stored = await app.backend.get_job(job.id)
    assert stored is not None and stored.status is JobStatus.QUEUED and stored.attempts == 0


async def test_tick_recovers_stalled_jobs_from_other_workers(
    app: JobQ, worker: Worker, clock: ManualClock
):
    @app.task
    async def t() -> None:
        return None

    job = await t.enqueue()
    await app.backend.claim(["default"], "crashed-worker", clock.now())
    clock.advance(30)
    assert (await worker.tick())["recovered"] == 0
    clock.advance(31)
    assert (await worker.tick())["recovered"] == 1
    done = await worker.process_one()
    assert done is not None and done.id == job.id and done.status is JobStatus.SUCCEEDED
