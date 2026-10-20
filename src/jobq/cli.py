"""``jobq`` command line interface."""

from __future__ import annotations

import asyncio
import importlib
import json
import logging
import os
import sys
from typing import Any

import typer

from jobq import __version__
from jobq.app import JobQ
from jobq.backends.memory import MemoryBackend
from jobq.backends.redis import RedisBackend
from jobq.job import JobStatus
from jobq.settings import Settings

cli = typer.Typer(
    help="Redis-backed background jobs: run workers, inspect queues, serve the dashboard.",
    no_args_is_help=True,
)


def _settings() -> Settings:
    return Settings()


def load_app(path: str | None) -> JobQ:
    """Import ``module:attribute`` (defaults to ``JOBQ_APP``) and return the JobQ instance.

    If the object has no backend configured and ``JOBQ_REDIS_URL`` is set, the
    Redis backend is attached, so one module can serve tests (memory) and
    deployment (Redis) without code changes.
    """
    settings = _settings()
    target = path or settings.app
    if not target:
        raise typer.BadParameter("pass --app module:attr or set JOBQ_APP")
    module_name, _, attr = target.partition(":")
    sys.path.insert(0, os.getcwd())
    module = importlib.import_module(module_name)
    app = getattr(module, attr or "app")
    if not isinstance(app, JobQ):
        raise typer.BadParameter(f"{target} is not a JobQ instance")
    if settings.redis_url and isinstance(app.backend, MemoryBackend):
        app.backend = RedisBackend(settings.redis_url, prefix=settings.prefix)
    return app


def _configure_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
    )


def _run(coro: Any) -> Any:
    return asyncio.run(coro)


@cli.callback()
def main_callback() -> None:
    """jobq CLI."""


@cli.command()
def version() -> None:
    """Print the installed version."""
    typer.echo(f"jobq {__version__}")


@cli.command()
def worker(
    app: str | None = typer.Option(None, "--app", "-a", help="module:attr of the JobQ app"),
    queues: str | None = typer.Option(
        None, "--queues", "-q", help="comma separated, in priority order"
    ),
    concurrency: int | None = typer.Option(None, "--concurrency", "-c"),
    stall_timeout: float = typer.Option(60.0, help="seconds without heartbeat before recovery"),
    no_housekeeping: bool = typer.Option(False, help="disable scheduler / recovery in this worker"),
    log_level: str | None = typer.Option(None),
) -> None:
    """Run a worker process."""
    from jobq.worker import Worker

    settings = _settings()
    _configure_logging(log_level or settings.log_level)
    jq = load_app(app)
    names = [q.strip() for q in (queues or settings.queues).split(",") if q.strip()]
    w = Worker(
        jq,
        names,
        concurrency=concurrency or settings.concurrency,
        stall_timeout=stall_timeout,
        run_housekeeping=not no_housekeeping,
    )

    async def _main() -> None:
        try:
            await w.run()
        finally:
            await jq.close()

    _run(_main())


@cli.command()
def dashboard(
    app: str | None = typer.Option(None, "--app", "-a"),
    host: str | None = typer.Option(None),
    port: int | None = typer.Option(None),
    reload: bool = typer.Option(False),
    log_level: str | None = typer.Option(None),
) -> None:
    """Serve the dashboard API (and the built UI if dashboard/dist exists)."""
    import uvicorn

    from jobq.dashboard.api import create_app

    settings = _settings()
    _configure_logging(log_level or settings.log_level)
    jq = load_app(app)
    api = create_app(jq, cors_origins=settings.cors_origin_list)
    uvicorn.run(
        api,
        host=host or settings.dashboard_host,
        port=port or settings.dashboard_port,
        reload=reload,
        log_level=(log_level or settings.log_level).lower(),
    )


@cli.command()
def enqueue(
    task: str = typer.Argument(..., help="registered task name"),
    args: str = typer.Option("[]", help="JSON list of positional args"),
    kwargs: str = typer.Option("{}", help="JSON object of keyword args"),
    queue: str | None = typer.Option(None),
    priority: int = typer.Option(0),
    delay: float | None = typer.Option(None, help="seconds from now"),
    unique_key: str | None = typer.Option(None),
    app: str | None = typer.Option(None, "--app", "-a"),
) -> None:
    """Enqueue a job from the shell."""
    jq = load_app(app)

    async def _main() -> None:
        try:
            job = await jq.enqueue(
                task,
                json.loads(args),
                json.loads(kwargs),
                queue=queue,
                priority=priority,
                delay=delay,
                unique_key=unique_key,
            )
            typer.echo(json.dumps(job.to_dict(), indent=2))
        finally:
            await jq.close()

    _run(_main())


@cli.command()
def stats(app: str | None = typer.Option(None, "--app", "-a")) -> None:
    """Show queue sizes and status counts."""
    jq = load_app(app)

    async def _main() -> None:
        try:
            s = await jq.backend.queue_stats()
            workers = await jq.backend.list_workers(jq.clock.now())
            d = s.to_dict()
            typer.echo("queues:")
            for q in d["queues"] or [{"name": "(none)", "queued": 0}]:
                typer.echo(f"  {q['name']:<20} {q['queued']:>8}")
            typer.echo("status:")
            for k, v in d["counts"].items():
                typer.echo(f"  {k:<20} {v:>8}")
            typer.echo(f"workers: {len(workers)}")
        finally:
            await jq.close()

    _run(_main())


@cli.command()
def jobs(
    status: str | None = typer.Option(
        None, help="queued|scheduled|retrying|waiting|running|succeeded|failed|cancelled"
    ),
    queue: str | None = typer.Option(None),
    limit: int = typer.Option(20),
    app: str | None = typer.Option(None, "--app", "-a"),
) -> None:
    """List recent jobs."""
    jq = load_app(app)
    st = JobStatus(status) if status else None

    async def _main() -> None:
        try:
            items, total = await jq.backend.list_jobs(status=st, queue=queue, limit=limit)
            typer.echo(f"{'id':<32} {'status':<10} {'queue':<10} {'att':>3} task")
            for j in items:
                typer.echo(
                    f"{j.id:<32} {j.status.value:<10} {j.queue:<10} {j.attempts:>3} {j.task}"
                )
            typer.echo(f"({len(items)} of {total})")
        finally:
            await jq.close()

    _run(_main())


@cli.command()
def schedules(app: str | None = typer.Option(None, "--app", "-a")) -> None:
    """List recurring schedules known to the backend."""
    jq = load_app(app)

    async def _main() -> None:
        try:
            for s in await jq.backend.list_schedules():
                spec = s.cron or f"every {s.every}s"
                typer.echo(f"{s.name:<24} {spec:<20} next_run={s.next_run} task={s.task}")
        finally:
            await jq.close()

    _run(_main())


def main() -> None:
    cli()


if __name__ == "__main__":
    main()
