"""Job model and (de)serialisation helpers.

A ``Job`` is stored as a flat string hash in Redis (so Lua scripts can update
single fields) and as a dataclass instance in the in-memory backend. The two
``to_fields`` / ``from_fields`` helpers are the only place that knows the
on-the-wire format.
"""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class JobStatus(str, Enum):
    QUEUED = "queued"  # waiting in a ready queue
    SCHEDULED = "scheduled"  # waiting for run_at
    RETRYING = "retrying"  # failed, waiting for the backoff delay
    WAITING = "waiting"  # waiting on dependencies
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"  # retries exhausted, in the dead-letter set
    CANCELLED = "cancelled"  # a dependency failed, in the dead-letter set

    @property
    def terminal(self) -> bool:
        return self in TERMINAL_STATUSES


TERMINAL_STATUSES = frozenset({JobStatus.SUCCEEDED, JobStatus.FAILED, JobStatus.CANCELLED})
DEAD_STATUSES = frozenset({JobStatus.FAILED, JobStatus.CANCELLED})


def new_job_id() -> str:
    return uuid.uuid4().hex


def dumps(value: Any) -> str:
    """JSON encode with a tolerant fallback for non-serialisable results."""
    return json.dumps(value, default=repr, separators=(",", ":"), ensure_ascii=False)


def loads(raw: str | None, default: Any = None) -> Any:
    if raw is None or raw == "":
        return default
    try:
        return json.loads(raw)
    except ValueError:
        return raw


@dataclass
class Job:
    task: str
    id: str = field(default_factory=new_job_id)
    queue: str = "default"
    args: list[Any] = field(default_factory=list)
    kwargs: dict[str, Any] = field(default_factory=dict)
    priority: int = 0
    status: JobStatus = JobStatus.QUEUED
    attempts: int = 0
    max_retries: int = 3
    timeout: float | None = None
    run_at: float | None = None
    unique_key: str | None = None
    depends_on: list[str] = field(default_factory=list)
    deps_remaining: int = 0
    created_at: float = 0.0
    started_at: float | None = None
    finished_at: float | None = None
    worker_id: str | None = None
    result: Any = None
    error: str | None = None
    result_ttl: int = 86_400
    schedule: str | None = None

    # ------------------------------------------------------------------ wire
    def to_fields(self) -> dict[str, str]:
        """Flat string mapping suitable for ``HSET``."""
        return {
            "id": self.id,
            "task": self.task,
            "queue": self.queue,
            "args": dumps(self.args),
            "kwargs": dumps(self.kwargs),
            "priority": str(int(self.priority)),
            "status": self.status.value,
            "attempts": str(self.attempts),
            "max_retries": str(self.max_retries),
            "timeout": "" if self.timeout is None else repr(float(self.timeout)),
            "run_at": "" if self.run_at is None else repr(float(self.run_at)),
            "unique_key": self.unique_key or "",
            "depends_on": ",".join(self.depends_on),
            "deps_remaining": str(self.deps_remaining),
            "created_at": repr(float(self.created_at)),
            "started_at": "" if self.started_at is None else repr(float(self.started_at)),
            "finished_at": "" if self.finished_at is None else repr(float(self.finished_at)),
            "worker_id": self.worker_id or "",
            "result": "" if self.result is None else dumps(self.result),
            "error": self.error or "",
            "result_ttl": str(self.result_ttl),
            "schedule": self.schedule or "",
        }

    @classmethod
    def from_fields(cls, fields: dict[str, str]) -> Job:
        def opt_float(key: str) -> float | None:
            raw = fields.get(key, "")
            return float(raw) if raw not in ("", None) else None

        def opt_str(key: str) -> str | None:
            raw = fields.get(key, "")
            return raw or None

        return cls(
            id=fields["id"],
            task=fields["task"],
            queue=fields.get("queue", "default"),
            args=loads(fields.get("args"), []),
            kwargs=loads(fields.get("kwargs"), {}),
            priority=int(fields.get("priority", 0)),
            status=JobStatus(fields.get("status", "queued")),
            attempts=int(fields.get("attempts", 0)),
            max_retries=int(fields.get("max_retries", 0)),
            timeout=opt_float("timeout"),
            run_at=opt_float("run_at"),
            unique_key=opt_str("unique_key"),
            depends_on=[d for d in fields.get("depends_on", "").split(",") if d],
            deps_remaining=int(fields.get("deps_remaining", 0) or 0),
            created_at=float(fields.get("created_at", 0) or 0),
            started_at=opt_float("started_at"),
            finished_at=opt_float("finished_at"),
            worker_id=opt_str("worker_id"),
            result=loads(fields.get("result"), None),
            error=opt_str("error"),
            result_ttl=int(fields.get("result_ttl", 86_400) or 86_400),
            schedule=opt_str("schedule"),
        )

    def to_dict(self) -> dict[str, Any]:
        """JSON-friendly representation for the dashboard API."""
        return {
            "id": self.id,
            "task": self.task,
            "queue": self.queue,
            "args": self.args,
            "kwargs": self.kwargs,
            "priority": self.priority,
            "status": self.status.value,
            "attempts": self.attempts,
            "max_retries": self.max_retries,
            "timeout": self.timeout,
            "run_at": self.run_at,
            "unique_key": self.unique_key,
            "depends_on": self.depends_on,
            "deps_remaining": self.deps_remaining,
            "created_at": self.created_at,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "worker_id": self.worker_id,
            "result": self.result,
            "error": self.error,
            "result_ttl": self.result_ttl,
            "schedule": self.schedule,
        }

    @property
    def duration(self) -> float | None:
        if self.started_at is None or self.finished_at is None:
            return None
        return self.finished_at - self.started_at


@dataclass
class Schedule:
    """A recurring job definition (cron expression or fixed interval)."""

    name: str
    task: str
    cron: str | None = None
    every: float | None = None
    args: list[Any] = field(default_factory=list)
    kwargs: dict[str, Any] = field(default_factory=dict)
    queue: str = "default"
    priority: int = 0
    tz: str = "UTC"
    enabled: bool = True
    next_run: float | None = None
    last_run: float | None = None

    def to_json(self) -> str:
        return dumps(self.to_dict())

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "task": self.task,
            "cron": self.cron,
            "every": self.every,
            "args": self.args,
            "kwargs": self.kwargs,
            "queue": self.queue,
            "priority": self.priority,
            "tz": self.tz,
            "enabled": self.enabled,
            "next_run": self.next_run,
            "last_run": self.last_run,
        }

    @classmethod
    def from_json(cls, raw: str) -> Schedule:
        data = json.loads(raw)
        return cls(**data)


@dataclass
class WorkerInfo:
    id: str
    hostname: str
    pid: int
    queues: list[str]
    concurrency: int
    started_at: float
    heartbeat_at: float = 0.0
    running: int = 0
    processed: int = 0
    failed: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "hostname": self.hostname,
            "pid": self.pid,
            "queues": self.queues,
            "concurrency": self.concurrency,
            "started_at": self.started_at,
            "heartbeat_at": self.heartbeat_at,
            "running": self.running,
            "processed": self.processed,
            "failed": self.failed,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> WorkerInfo:
        return cls(**data)


@dataclass
class QueueStats:
    queues: dict[str, int]
    scheduled: int
    waiting: int
    running: int
    succeeded: int
    dead: int

    @property
    def queued(self) -> int:
        return sum(self.queues.values())

    def to_dict(self) -> dict[str, Any]:
        return {
            "queues": [
                {"name": name, "queued": size} for name, size in sorted(self.queues.items())
            ],
            "counts": {
                "queued": self.queued,
                "scheduled": self.scheduled,
                "waiting": self.waiting,
                "running": self.running,
                "succeeded": self.succeeded,
                "dead": self.dead,
            },
        }


@dataclass
class MinuteBucket:
    minute: int  # epoch minute (unix seconds // 60)
    enqueued: int = 0
    succeeded: int = 0
    failed: int = 0
    retried: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "minute": self.minute,
            "timestamp": self.minute * 60,
            "enqueued": self.enqueued,
            "succeeded": self.succeeded,
            "failed": self.failed,
            "retried": self.retried,
        }
