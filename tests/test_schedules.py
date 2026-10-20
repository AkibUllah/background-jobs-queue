from __future__ import annotations

from datetime import datetime, timezone

import pytest

from jobq import JobQ, ManualClock, Worker
from jobq.job import JobStatus


async def test_cron_schedule_fires_once_per_due_time(app: JobQ, worker: Worker, clock: ManualClock):
    clock.set(datetime(2025, 6, 15, 10, 0, 30, tzinfo=timezone.utc).timestamp())

    @app.task
    async def report() -> str:
        return "ok"

    app.schedule("every-5", report, cron="*/5 * * * *")
    await app.sync_schedules()
    (stored,) = await app.backend.list_schedules()
    assert datetime.fromtimestamp(stored.next_run, timezone.utc).minute == 5  # type: ignore[arg-type]

    assert await app.tick_schedules() == []
    clock.advance(4 * 60 + 30)  # 10:05:00
    fired = await app.tick_schedules()
    assert len(fired) == 1 and fired[0].schedule == "every-5" and fired[0].task == report.name
    assert await app.tick_schedules() == []  # same minute, nothing new
    (stored,) = await app.backend.list_schedules()
    assert datetime.fromtimestamp(stored.next_run, timezone.utc).minute == 10  # type: ignore[arg-type]
    assert stored.last_run == pytest.approx(fired[0].created_at, abs=60)
    done = await worker.process_one()
    assert done is not None and done.result == "ok"


async def test_interval_schedule_and_catch_up_after_downtime(app: JobQ, clock: ManualClock):
    @app.task
    async def ping() -> None:
        return None

    app.schedule("ping", ping, every=30)
    await app.sync_schedules()
    clock.advance(30)
    assert len(await app.tick_schedules()) == 1
    # Worker was down for 5 minutes: exactly one job fires, and next_run moves past now.
    clock.advance(300)
    assert len(await app.tick_schedules()) == 1
    (stored,) = await app.backend.list_schedules()
    assert stored.next_run == pytest.approx(clock.now() + 30)


async def test_sync_keeps_existing_next_run_when_spec_unchanged(app: JobQ, clock: ManualClock):
    @app.task
    async def t() -> None:
        return None

    app.schedule("s", t, every=100)
    await app.sync_schedules()
    first = (await app.backend.list_schedules())[0].next_run
    clock.advance(10)
    await app.sync_schedules()  # a second worker boots
    assert (await app.backend.list_schedules())[0].next_run == first
    app.schedules["s"].every = 5  # spec changed -> recomputed
    await app.sync_schedules()
    assert (await app.backend.list_schedules())[0].next_run == pytest.approx(clock.now() + 5)


async def test_schedule_validation(app: JobQ):
    @app.task
    async def t() -> None:
        return None

    with pytest.raises(ValueError):
        app.schedule("x", t)
    with pytest.raises(ValueError):
        app.schedule("x", t, cron="* * * * *", every=5)
    with pytest.raises(ValueError):
        app.schedule("x", t, every=0)


async def test_duplicate_schedule_jobs_dedupe_across_workers(app: JobQ, clock: ManualClock):
    """Two workers racing on the same due time produce a single job."""

    @app.task
    async def t() -> None:
        return None

    app.schedule("race", t, every=10)
    await app.sync_schedules()
    clock.advance(10)
    due = await app.backend.claim_due_schedules(clock.now(), lock=60)
    assert len(due) == 1
    assert await app.backend.claim_due_schedules(clock.now(), lock=60) == []  # locked
    schedule, due_at = due[0]
    j1 = await app.enqueue(schedule.task, unique_key=f"schedule:{schedule.name}:{int(due_at)}")
    j2 = await app.enqueue(schedule.task, unique_key=f"schedule:{schedule.name}:{int(due_at)}")
    assert j1.id == j2.id and j1.status is JobStatus.QUEUED
