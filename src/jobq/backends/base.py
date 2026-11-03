"""Backend protocol shared by the in-memory and Redis implementations.

Every method takes ``now`` explicitly where time matters, so the caller's
clock is the single source of truth and the backend never reads the wall clock.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from enum import Enum
from typing import Any, Protocol

from jobq.job import Job, JobStatus, MinuteBucket, QueueStats, Schedule, WorkerInfo
from jobq.ratelimit import RateLimit

# How many jobs of throttled tasks one claim may skip per queue while looking
# for a job it is allowed to run. Bounds the cost of a claim when a rate-limited
# task has a deep backlog in front of other work.
THROTTLE_SCAN = 1000


class EnqueueOutcome(str, Enum):
    ENQUEUED = "enqueued"  # new job stored (queued, scheduled or waiting)
    DEDUPED = "deduped"  # an active job with the same unique key already exists
    CANCELLED = "cancelled"  # a dependency had already failed


@dataclass
class ClaimResult:
    """Outcome of ``Backend.try_claim``.

    ``job`` is the claimed job, or ``None``. When nothing could be claimed
    because every candidate belongs to a rate-limited task with an empty bucket,
    ``throttled_for`` is the number of seconds until the earliest of those
    buckets has a token again; workers sleep that long instead of polling.
    """

    job: Job | None = None
    throttled_for: float | None = None


RateLimits = Mapping[str, RateLimit]


class Backend(Protocol):
    # -- producer side -----------------------------------------------------
    async def enqueue(self, job: Job, now: float) -> tuple[Job, EnqueueOutcome]: ...

    # -- worker side -------------------------------------------------------
    async def claim(
        self,
        queues: list[str],
        worker_id: str,
        now: float,
        rate_limits: RateLimits | None = None,
    ) -> Job | None: ...
    async def try_claim(
        self,
        queues: list[str],
        worker_id: str,
        now: float,
        rate_limits: RateLimits | None = None,
    ) -> ClaimResult: ...
    async def heartbeat(self, job_ids: list[str], now: float) -> None: ...
    async def complete(self, job_id: str, result: Any, now: float) -> None: ...
    async def fail(self, job_id: str, error: str, now: float, retry_at: float | None) -> None: ...
    async def release(self, job_id: str) -> None: ...
    async def promote_scheduled(self, now: float, limit: int = 1000) -> int: ...
    async def recover_stalled(self, now: float, stall_timeout: float) -> list[str]: ...

    # -- workers registry --------------------------------------------------
    async def register_worker(self, info: WorkerInfo, now: float) -> None: ...
    async def deregister_worker(self, worker_id: str) -> None: ...
    async def list_workers(self, now: float, stale_after: float = 60.0) -> list[WorkerInfo]: ...

    # -- schedules ---------------------------------------------------------
    async def save_schedule(self, schedule: Schedule) -> None: ...
    async def list_schedules(self) -> list[Schedule]: ...
    async def delete_schedule(self, name: str) -> bool: ...
    async def claim_due_schedules(
        self, now: float, lock: float
    ) -> list[tuple[Schedule, float]]: ...
    async def set_schedule_next(
        self, name: str, next_run: float, last_run: float | None
    ) -> None: ...

    # -- inspection / dashboard -------------------------------------------
    async def get_job(self, job_id: str) -> Job | None: ...
    async def list_jobs(
        self,
        status: JobStatus | None = None,
        queue: str | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> tuple[list[Job], int]: ...
    async def delete_job(self, job_id: str) -> bool: ...
    async def retry_job(self, job_id: str, now: float) -> Job | None: ...
    async def requeue_job(self, job_id: str, now: float) -> Job | None: ...
    async def queue_stats(self) -> QueueStats: ...
    async def metrics(self, now: float, minutes: int = 60) -> list[MinuteBucket]: ...

    # -- lifecycle ---------------------------------------------------------
    async def clear(self) -> None: ...
    async def close(self) -> None: ...


def minute_of(ts: float) -> int:
    return int(ts // 60)
