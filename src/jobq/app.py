"""The ``JobQ`` application object: task registry, enqueue API and schedules.

Typical usage::

    from jobq import JobQ, RedisBackend

    app = JobQ(RedisBackend("redis://localhost:6379/0"))

    @app.task(queue="high", max_retries=5, timeout=30)
    async def send_sms(phone: str, text: str) -> dict: ...

    job = await send_sms.enqueue("+8801711000000", "Your OTP is 4821")
    job = await send_sms.options(delay=60, priority=5).enqueue(...)

    app.schedule("nightly-report", generate_report, cron="30 2 * * *", tz="Asia/Dhaka")
"""

from __future__ import annotations

import asyncio
import hashlib
import inspect
import logging
from collections.abc import Callable
from dataclasses import dataclass, replace
from typing import Any

from jobq.backends.base import Backend, EnqueueOutcome
from jobq.backends.memory import MemoryBackend
from jobq.backoff import DEFAULT_BACKOFF, Backoff
from jobq.clock import Clock, SystemClock
from jobq.cron import CronSpec
from jobq.exceptions import JobNotFound, JobQError, TaskNotFound
from jobq.job import Job, JobStatus, Schedule, dumps

log = logging.getLogger("jobq")


@dataclass(frozen=True)
class EnqueueOptions:
    queue: str | None = None
    priority: int = 0
    delay: float | None = None
    run_at: float | None = None
    unique_key: str | None = None
    depends_on: tuple[str, ...] = ()
    max_retries: int | None = None
    timeout: float | None = None
    result_ttl: int | None = None


class Task:
    """A registered callable plus its execution policy."""

    def __init__(
        self,
        app: JobQ,
        fn: Callable[..., Any],
        *,
        name: str,
        queue: str,
        max_retries: int,
        timeout: float | None,
        backoff: Backoff,
        unique: bool,
        result_ttl: int | None,
    ) -> None:
        self.app = app
        self.fn = fn
        self.name = name
        self.queue = queue
        self.max_retries = max_retries
        self.timeout = timeout
        self.backoff = backoff
        self.unique = unique
        self.result_ttl = result_ttl
        self.is_async = inspect.iscoroutinefunction(fn)

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        """Call the underlying function directly (no queueing)."""
        return self.fn(*args, **kwargs)

    def __repr__(self) -> str:
        return f"<Task {self.name} queue={self.queue}>"

    def options(self, **options: Any) -> BoundTask:
        return BoundTask(self, EnqueueOptions(**options))

    async def enqueue(self, *args: Any, **kwargs: Any) -> Job:
        return await BoundTask(self, EnqueueOptions()).enqueue(*args, **kwargs)

    def unique_key_for(self, args: tuple[Any, ...], kwargs: dict[str, Any]) -> str:
        digest = hashlib.sha1(dumps([list(args), kwargs]).encode()).hexdigest()[:16]
        return f"{self.name}:{digest}"


class BoundTask:
    """A task plus enqueue options; produced by ``Task.options``."""

    def __init__(self, task: Task, options: EnqueueOptions) -> None:
        self.task = task
        self._options = options

    def options(self, **options: Any) -> BoundTask:
        return BoundTask(self.task, replace(self._options, **options))

    async def enqueue(self, *args: Any, **kwargs: Any) -> Job:
        opts = self._options
        unique_key = opts.unique_key
        if unique_key is None and self.task.unique:
            unique_key = self.task.unique_key_for(args, kwargs)
        return await self.task.app.enqueue(
            self.task.name,
            list(args),
            dict(kwargs),
            queue=opts.queue or self.task.queue,
            priority=opts.priority,
            delay=opts.delay,
            run_at=opts.run_at,
            unique_key=unique_key,
            depends_on=list(opts.depends_on),
            max_retries=self.task.max_retries if opts.max_retries is None else opts.max_retries,
            timeout=self.task.timeout if opts.timeout is None else opts.timeout,
            result_ttl=opts.result_ttl if opts.result_ttl is not None else self.task.result_ttl,
        )


class JobQ:
    def __init__(
        self,
        backend: Backend | None = None,
        *,
        clock: Clock | None = None,
        default_queue: str = "default",
        result_ttl: int = 86_400,
    ) -> None:
        self.backend: Backend = backend if backend is not None else MemoryBackend()
        self.clock: Clock = clock or SystemClock()
        self.default_queue = default_queue
        self.result_ttl = result_ttl
        self.tasks: dict[str, Task] = {}
        self.schedules: dict[str, Schedule] = {}

    # ------------------------------------------------------------ registry
    def task(
        self,
        fn: Callable[..., Any] | None = None,
        *,
        name: str | None = None,
        queue: str | None = None,
        max_retries: int = 3,
        timeout: float | None = None,
        backoff: Backoff = DEFAULT_BACKOFF,
        unique: bool = False,
        result_ttl: int | None = None,
    ) -> Any:
        def decorate(func: Callable[..., Any]) -> Task:
            task_name = name or f"{func.__module__}.{func.__qualname__}"
            if task_name in self.tasks:
                raise JobQError(f"task {task_name!r} is already registered")
            task = Task(
                self,
                func,
                name=task_name,
                queue=queue or self.default_queue,
                max_retries=max_retries,
                timeout=timeout,
                backoff=backoff,
                unique=unique,
                result_ttl=result_ttl,
            )
            self.tasks[task_name] = task
            return task

        return decorate(fn) if fn is not None else decorate

    def get_task(self, name: str) -> Task:
        try:
            return self.tasks[name]
        except KeyError:
            raise TaskNotFound(name) from None

    # ------------------------------------------------------------ enqueue
    async def enqueue(
        self,
        task: str | Task,
        args: list[Any] | None = None,
        kwargs: dict[str, Any] | None = None,
        *,
        queue: str | None = None,
        priority: int = 0,
        delay: float | None = None,
        run_at: float | None = None,
        unique_key: str | None = None,
        depends_on: list[str] | None = None,
        max_retries: int | None = None,
        timeout: float | None = None,
        result_ttl: int | None = None,
        schedule: str | None = None,
    ) -> Job:
        """Store a job. Returns the stored job (the existing one when deduplicated)."""
        task_name = task.name if isinstance(task, Task) else task
        registered = self.tasks.get(task_name)
        now = self.clock.now()
        if run_at is None and delay is not None:
            run_at = now + max(0.0, float(delay))
        if not -1000 <= priority <= 1000:
            raise ValueError("priority must be between -1000 and 1000")
        job = Job(
            task=task_name,
            queue=queue or (registered.queue if registered else self.default_queue),
            args=list(args or []),
            kwargs=dict(kwargs or {}),
            priority=int(priority),
            max_retries=(
                max_retries
                if max_retries is not None
                else (registered.max_retries if registered else 3)
            ),
            timeout=timeout
            if timeout is not None
            else (registered.timeout if registered else None),
            run_at=run_at,
            unique_key=unique_key,
            depends_on=list(depends_on or []),
            result_ttl=result_ttl if result_ttl is not None else self.result_ttl,
            schedule=schedule,
        )
        stored, outcome = await self.backend.enqueue(job, now)
        if outcome is EnqueueOutcome.DEDUPED:
            log.debug("job %s deduplicated onto %s (key=%s)", task_name, stored.id, unique_key)
        return stored

    async def get_job(self, job_id: str) -> Job:
        job = await self.backend.get_job(job_id)
        if job is None:
            raise JobNotFound(job_id)
        return job

    async def result(self, job_id: str, *, timeout: float | None = None, poll: float = 0.05) -> Any:
        """Wait for a job to finish and return its result (raises on failure)."""
        loop = asyncio.get_running_loop()
        deadline = None if timeout is None else loop.time() + timeout
        while True:
            job = await self.get_job(job_id)
            if job.status is JobStatus.SUCCEEDED:
                return job.result
            if job.status in (JobStatus.FAILED, JobStatus.CANCELLED):
                raise JobQError(f"job {job_id} {job.status.value}: {job.error}")
            if deadline is not None and loop.time() >= deadline:
                raise TimeoutError(f"job {job_id} still {job.status.value} after {timeout}s")
            await asyncio.sleep(poll)

    # ------------------------------------------------------------ schedules
    def schedule(
        self,
        name: str,
        task: str | Task,
        *,
        cron: str | None = None,
        every: float | None = None,
        args: list[Any] | None = None,
        kwargs: dict[str, Any] | None = None,
        queue: str | None = None,
        priority: int = 0,
        tz: str = "UTC",
    ) -> Schedule:
        """Register a recurring job. Synced to the backend when a worker starts."""
        if (cron is None) == (every is None):
            raise ValueError("provide exactly one of cron= or every=")
        if cron is not None:
            CronSpec.parse(cron)  # validate early
        if every is not None and every <= 0:
            raise ValueError("every= must be positive seconds")
        task_name = task.name if isinstance(task, Task) else task
        registered = self.tasks.get(task_name)
        schedule = Schedule(
            name=name,
            task=task_name,
            cron=cron,
            every=every,
            args=list(args or []),
            kwargs=dict(kwargs or {}),
            queue=queue or (registered.queue if registered else self.default_queue),
            priority=priority,
            tz=tz,
        )
        self.schedules[name] = schedule
        return schedule

    def compute_next_run(self, schedule: Schedule, after: float) -> float:
        if schedule.cron:
            return CronSpec.parse(schedule.cron).next_timestamp(after, schedule.tz)
        assert schedule.every is not None
        return after + schedule.every

    async def sync_schedules(self) -> None:
        """Push locally declared schedules to the backend, keeping existing next_run values."""
        now = self.clock.now()
        existing = {s.name: s for s in await self.backend.list_schedules()}
        for schedule in self.schedules.values():
            current = existing.get(schedule.name)
            same_spec = (
                current is not None
                and current.cron == schedule.cron
                and current.every == schedule.every
                and current.tz == schedule.tz
            )
            if same_spec and current is not None and current.next_run is not None:
                schedule.next_run = current.next_run
                schedule.last_run = current.last_run
            else:
                schedule.next_run = self.compute_next_run(schedule, now)
            await self.backend.save_schedule(schedule)

    async def tick_schedules(self, lock: float = 60.0) -> list[Job]:
        """Enqueue every schedule that is due. Safe to call from many workers."""
        now = self.clock.now()
        enqueued: list[Job] = []
        for schedule, due in await self.backend.claim_due_schedules(now, lock):
            job = await self.enqueue(
                schedule.task,
                schedule.args,
                schedule.kwargs,
                queue=schedule.queue,
                priority=schedule.priority,
                unique_key=f"schedule:{schedule.name}:{int(due)}",
                schedule=schedule.name,
            )
            enqueued.append(job)
            next_run = self.compute_next_run(schedule, max(now, due))
            await self.backend.set_schedule_next(schedule.name, next_run, due)
        return enqueued

    # ------------------------------------------------------------ lifecycle
    async def close(self) -> None:
        await self.backend.close()
