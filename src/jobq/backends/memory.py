"""Single-process in-memory backend.

Used for tests, benchmarks and local development without Redis. It mirrors the
Redis data model (ready queues ordered by priority then FIFO, a scheduled set,
a running set with heartbeats, dead-letter set, unique index, dependency
graph) so behaviour is identical; it is simply not shared across processes.

All methods are coroutines to match the Redis backend; they never actually
await, so the whole backend is safe on a single event loop without locks.
"""

from __future__ import annotations

import heapq
from typing import Any

from jobq.backends.base import EnqueueOutcome, minute_of
from jobq.job import (
    DEAD_STATUSES,
    Job,
    JobStatus,
    MinuteBucket,
    QueueStats,
    Schedule,
    WorkerInfo,
)

PRIORITY_SCALE = 1e10
METRICS_RETENTION_MINUTES = 180
DONE_RETENTION = 10_000


class MemoryBackend:
    def __init__(self) -> None:
        self.jobs: dict[str, Job] = {}
        self._heaps: dict[str, list[tuple[float, str]]] = {}
        self._queued: dict[str, set[str]] = {}
        self._known_queues: set[str] = set()
        self._seq = 0
        self.scheduled: dict[str, float] = {}
        self.waiting: dict[str, float] = {}
        self.running: dict[str, float] = {}
        self.done: dict[str, float] = {}
        self.dead: dict[str, float] = {}
        self.all: dict[str, float] = {}
        self.unique: dict[str, str] = {}
        self.deps: dict[str, set[str]] = {}
        self.expires: dict[str, float] = {}
        self._expiry_heap: list[tuple[float, str]] = []
        self.workers: dict[str, WorkerInfo] = {}
        self.schedules: dict[str, Schedule] = {}
        self._metrics: dict[int, MinuteBucket] = {}

    # ------------------------------------------------------------ helpers
    def _push(self, job: Job, *, front: bool = False) -> None:
        self._seq += 1
        seq = -self._seq if front else self._seq
        score = -job.priority * PRIORITY_SCALE + seq
        heapq.heappush(self._heaps.setdefault(job.queue, []), (score, job.id))
        self._queued.setdefault(job.queue, set()).add(job.id)
        self._known_queues.add(job.queue)
        job.status = JobStatus.QUEUED

    def _bump(self, now: float, field: str, amount: int = 1) -> None:
        minute = minute_of(now)
        bucket = self._metrics.setdefault(minute, MinuteBucket(minute=minute))
        setattr(bucket, field, getattr(bucket, field) + amount)
        if len(self._metrics) > METRICS_RETENTION_MINUTES:
            for old in sorted(self._metrics)[: len(self._metrics) - METRICS_RETENTION_MINUTES]:
                del self._metrics[old]

    def _schedule_expiry(self, job_id: str, at: float) -> None:
        self.expires[job_id] = at
        heapq.heappush(self._expiry_heap, (at, job_id))

    def _expire(self, now: float) -> None:
        heap = self._expiry_heap
        while heap and heap[0][0] <= now:
            at, job_id = heapq.heappop(heap)
            if self.expires.get(job_id) == at:  # ignore entries cancelled by retry/requeue
                self._forget(job_id)

    def _forget(self, job_id: str) -> None:
        job = self.jobs.pop(job_id, None)
        self.expires.pop(job_id, None)
        for index in (self.scheduled, self.waiting, self.running, self.done, self.dead, self.all):
            index.pop(job_id, None)
        if job is not None:
            self._queued.get(job.queue, set()).discard(job_id)
            if job.unique_key and self.unique.get(job.unique_key) == job_id:
                del self.unique[job.unique_key]
        self.deps.pop(job_id, None)

    def _activate(self, job: Job, now: float) -> None:
        """Place a job that has no pending dependencies into scheduled or ready."""
        if job.run_at is not None and job.run_at > now:
            job.status = JobStatus.SCHEDULED if job.attempts == 0 else JobStatus.RETRYING
            self.scheduled[job.id] = job.run_at
        else:
            self._push(job)

    def _mark_dead(self, job: Job, status: JobStatus, error: str, now: float) -> None:
        """Move a job to the dead-letter set and cancel everything that depended on it."""
        stack = [(job, status, error)]
        while stack:
            current, st, err = stack.pop()
            current.status = st
            current.error = err
            current.finished_at = now
            self.running.pop(current.id, None)
            self.waiting.pop(current.id, None)
            self.scheduled.pop(current.id, None)
            self._queued.get(current.queue, set()).discard(current.id)
            self.dead[current.id] = now
            if current.unique_key and self.unique.get(current.unique_key) == current.id:
                del self.unique[current.unique_key]
            for dep_id in self.deps.pop(current.id, set()):
                dependent = self.jobs.get(dep_id)
                if dependent is not None and dependent.status == JobStatus.WAITING:
                    stack.append(
                        (
                            dependent,
                            JobStatus.CANCELLED,
                            f"dependency {current.id} {st.value}",
                        )
                    )

    # ------------------------------------------------------------ producer
    async def enqueue(self, job: Job, now: float) -> tuple[Job, EnqueueOutcome]:
        self._expire(now)
        if job.unique_key:
            existing_id = self.unique.get(job.unique_key)
            existing = self.jobs.get(existing_id) if existing_id else None
            if existing is not None and not existing.status.terminal:
                return existing, EnqueueOutcome.DEDUPED
            self.unique[job.unique_key] = job.id
        job.created_at = now
        self.jobs[job.id] = job
        self.all[job.id] = now
        self._known_queues.add(job.queue)
        self._bump(now, "enqueued")

        pending = 0
        for dep_id in job.depends_on:
            dep = self.jobs.get(dep_id)
            if dep is None or dep.status == JobStatus.SUCCEEDED:
                continue
            if dep.status in DEAD_STATUSES:
                self._mark_dead(
                    job, JobStatus.CANCELLED, f"dependency {dep_id} {dep.status.value}", now
                )
                return job, EnqueueOutcome.CANCELLED
            pending += 1
            self.deps.setdefault(dep_id, set()).add(job.id)
        job.deps_remaining = pending
        if pending:
            job.status = JobStatus.WAITING
            self.waiting[job.id] = now
        else:
            self._activate(job, now)
        return job, EnqueueOutcome.ENQUEUED

    # ------------------------------------------------------------ worker
    async def claim(self, queues: list[str], worker_id: str, now: float) -> Job | None:
        for name in queues:
            heap = self._heaps.get(name)
            ids = self._queued.get(name, set())
            while heap:
                _, job_id = heapq.heappop(heap)
                if job_id not in ids:
                    continue  # stale heap entry (deleted or moved)
                ids.discard(job_id)
                job = self.jobs[job_id]
                job.status = JobStatus.RUNNING
                job.attempts += 1
                job.started_at = now
                job.worker_id = worker_id
                self.running[job_id] = now
                return job
        return None

    async def heartbeat(self, job_ids: list[str], now: float) -> None:
        for job_id in job_ids:
            if job_id in self.running:
                self.running[job_id] = now

    async def complete(self, job_id: str, result: Any, now: float) -> None:
        job = self.jobs.get(job_id)
        if job is None:
            return
        self.running.pop(job_id, None)
        job.status = JobStatus.SUCCEEDED
        job.result = result
        job.error = None
        job.finished_at = now
        self.done[job_id] = now
        if len(self.done) > DONE_RETENTION:
            oldest = next(iter(self.done))
            self._forget(oldest)
        if job.unique_key and self.unique.get(job.unique_key) == job_id:
            del self.unique[job.unique_key]
        self._schedule_expiry(job_id, now + job.result_ttl)
        self._bump(now, "succeeded")
        for dep_id in self.deps.pop(job_id, set()):
            dependent = self.jobs.get(dep_id)
            if dependent is None or dependent.status != JobStatus.WAITING:
                continue
            dependent.deps_remaining -= 1
            if dependent.deps_remaining <= 0:
                self.waiting.pop(dep_id, None)
                self._activate(dependent, now)
        self._expire(now)

    async def fail(self, job_id: str, error: str, now: float, retry_at: float | None) -> None:
        job = self.jobs.get(job_id)
        if job is None:
            return
        self.running.pop(job_id, None)
        job.error = error
        if retry_at is not None:
            job.status = JobStatus.RETRYING
            job.run_at = retry_at
            self.scheduled[job_id] = retry_at
            self._bump(now, "retried")
            return
        self._mark_dead(job, JobStatus.FAILED, error, now)
        self._schedule_expiry(job_id, now + job.result_ttl)
        self._bump(now, "failed")

    async def release(self, job_id: str) -> None:
        job = self.jobs.get(job_id)
        if job is None or job.status != JobStatus.RUNNING:
            return
        self.running.pop(job_id, None)
        job.attempts = max(0, job.attempts - 1)
        job.started_at = None
        job.worker_id = None
        self._push(job, front=True)

    async def promote_scheduled(self, now: float, limit: int = 1000) -> int:
        self._expire(now)
        due = sorted((ts, jid) for jid, ts in self.scheduled.items() if ts <= now)[:limit]
        for _, job_id in due:
            del self.scheduled[job_id]
            job = self.jobs.get(job_id)
            if job is not None:
                self._push(job)
        return len(due)

    async def recover_stalled(self, now: float, stall_timeout: float) -> list[str]:
        cutoff = now - stall_timeout
        recovered: list[str] = []
        for job_id, beat in list(self.running.items()):
            if beat > cutoff:
                continue
            del self.running[job_id]
            job = self.jobs.get(job_id)
            if job is None:
                continue
            error = f"stalled: no heartbeat from worker {job.worker_id} for {stall_timeout:.0f}s"
            if job.attempts <= job.max_retries:
                job.error = error
                job.worker_id = None
                job.started_at = None
                self._push(job)
                self._bump(now, "retried")
            else:
                self._mark_dead(job, JobStatus.FAILED, error, now)
                self._bump(now, "failed")
            recovered.append(job_id)
        return recovered

    # ------------------------------------------------------------ workers
    async def register_worker(self, info: WorkerInfo, now: float) -> None:
        info.heartbeat_at = now
        self.workers[info.id] = info

    async def deregister_worker(self, worker_id: str) -> None:
        self.workers.pop(worker_id, None)

    async def list_workers(self, now: float, stale_after: float = 60.0) -> list[WorkerInfo]:
        alive = []
        for worker_id, info in list(self.workers.items()):
            if now - info.heartbeat_at > stale_after:
                del self.workers[worker_id]
                continue
            alive.append(info)
        return sorted(alive, key=lambda w: w.started_at)

    # ------------------------------------------------------------ schedules
    async def save_schedule(self, schedule: Schedule) -> None:
        self.schedules[schedule.name] = schedule

    async def list_schedules(self) -> list[Schedule]:
        return sorted(self.schedules.values(), key=lambda s: s.name)

    async def delete_schedule(self, name: str) -> bool:
        return self.schedules.pop(name, None) is not None

    async def claim_due_schedules(self, now: float, lock: float) -> list[tuple[Schedule, float]]:
        due: list[tuple[Schedule, float]] = []
        for schedule in self.schedules.values():
            if schedule.enabled and schedule.next_run is not None and schedule.next_run <= now:
                due.append((schedule, schedule.next_run))
                schedule.next_run = now + lock
        return due

    async def set_schedule_next(self, name: str, next_run: float, last_run: float | None) -> None:
        schedule = self.schedules.get(name)
        if schedule is not None:
            schedule.next_run = next_run
            if last_run is not None:
                schedule.last_run = last_run

    # ------------------------------------------------------------ inspection
    async def get_job(self, job_id: str) -> Job | None:
        return self.jobs.get(job_id)

    def _index_for(self, status: JobStatus | None, queue: str | None) -> list[str]:
        if status is None:
            ids = list(self.all)
        elif status == JobStatus.QUEUED:
            if queue:
                ids = list(self._queued.get(queue, set()))
            else:
                ids = [jid for s in self._queued.values() for jid in s]
            ids.sort(key=lambda jid: self.all.get(jid, 0.0))
        elif status in (JobStatus.SCHEDULED, JobStatus.RETRYING):
            ids = sorted(self.scheduled, key=self.scheduled.get)  # type: ignore[arg-type]
        elif status == JobStatus.WAITING:
            ids = list(self.waiting)
        elif status == JobStatus.RUNNING:
            ids = list(self.running)
        elif status == JobStatus.SUCCEEDED:
            ids = list(self.done)
        else:
            ids = list(self.dead)
        return ids

    async def list_jobs(
        self,
        status: JobStatus | None = None,
        queue: str | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> tuple[list[Job], int]:
        ids = self._index_for(status, queue)
        jobs = [self.jobs[j] for j in ids if j in self.jobs]
        if status is not None:
            jobs = [j for j in jobs if j.status == status]
        if queue:
            jobs = [j for j in jobs if j.queue == queue]
        jobs.sort(key=lambda j: j.created_at, reverse=True)
        total = len(jobs)
        return jobs[offset : offset + limit], total

    async def delete_job(self, job_id: str) -> bool:
        existed = job_id in self.jobs
        self._forget(job_id)
        return existed

    async def retry_job(self, job_id: str, now: float) -> Job | None:
        job = self.jobs.get(job_id)
        if job is None or job.status not in DEAD_STATUSES:
            return None
        self.dead.pop(job_id, None)
        self.expires.pop(job_id, None)
        job.attempts = 0
        job.error = None
        job.finished_at = None
        job.started_at = None
        job.worker_id = None
        job.run_at = None
        if job.unique_key:
            self.unique[job.unique_key] = job_id
        self._push(job)
        return job

    async def requeue_job(self, job_id: str, now: float) -> Job | None:
        job = self.jobs.get(job_id)
        if job is None or job.status in (JobStatus.RUNNING, JobStatus.QUEUED):
            return None
        self.scheduled.pop(job_id, None)
        self.waiting.pop(job_id, None)
        self.dead.pop(job_id, None)
        self.done.pop(job_id, None)
        self.expires.pop(job_id, None)
        for dep_id in job.depends_on:
            self.deps.get(dep_id, set()).discard(job_id)
        job.deps_remaining = 0
        job.run_at = None
        job.error = None
        job.finished_at = None
        job.started_at = None
        job.worker_id = None
        if job.unique_key:
            self.unique[job.unique_key] = job_id
        self._push(job)
        return job

    async def queue_stats(self) -> QueueStats:
        queues = {name: len(self._queued.get(name, set())) for name in self._known_queues}
        return QueueStats(
            queues=queues,
            scheduled=len(self.scheduled),
            waiting=len(self.waiting),
            running=len(self.running),
            succeeded=len(self.done),
            dead=len(self.dead),
        )

    async def metrics(self, now: float, minutes: int = 60) -> list[MinuteBucket]:
        current = minute_of(now)
        return [
            self._metrics.get(m, MinuteBucket(minute=m))
            for m in range(current - minutes + 1, current + 1)
        ]

    # ------------------------------------------------------------ lifecycle
    async def clear(self) -> None:
        self.__init__()  # type: ignore[misc]

    async def close(self) -> None:
        return None
