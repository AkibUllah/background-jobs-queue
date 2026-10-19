"""Exception hierarchy for jobq."""

from __future__ import annotations


class JobQError(Exception):
    """Base class for all jobq errors."""


class TaskNotFound(JobQError):
    """A job referenced a task name that is not registered in this process."""


class JobNotFound(JobQError):
    """No job with the given id exists (or its result has expired)."""


class InvalidCron(JobQError):
    """A cron expression could not be parsed."""


class Retry(JobQError):
    """Raise inside a task to request a retry, optionally after a specific delay."""

    def __init__(self, message: str = "retry requested", *, delay: float | None = None) -> None:
        super().__init__(message)
        self.delay = delay


class JobTimeout(JobQError):
    """A job exceeded its configured timeout."""
