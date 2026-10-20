"""Dashboard API.

Read-mostly endpoints over the backend plus the three operator actions
(retry, requeue, delete). When the React UI has been built into
``dashboard/dist`` (or the directory given by ``static_dir``) it is served at
``/`` so one process can host both.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from jobq import __version__
from jobq.app import JobQ
from jobq.job import JobStatus

log = logging.getLogger("jobq.dashboard")

DEFAULT_STATIC = Path(__file__).resolve().parents[3] / "dashboard" / "dist"


class EnqueueRequest(BaseModel):
    task: str
    args: list[Any] = Field(default_factory=list)
    kwargs: dict[str, Any] = Field(default_factory=dict)
    queue: str | None = None
    priority: int = Field(default=0, ge=-1000, le=1000)
    delay: float | None = Field(default=None, ge=0)
    unique_key: str | None = None
    depends_on: list[str] = Field(default_factory=list)
    max_retries: int | None = Field(default=None, ge=0)
    timeout: float | None = Field(default=None, gt=0)


def create_app(
    app: JobQ,
    *,
    cors_origins: list[str] | None = None,
    static_dir: Path | None = DEFAULT_STATIC,
    stale_worker_after: float = 60.0,
) -> FastAPI:
    api = FastAPI(
        title="jobq dashboard",
        version=__version__,
        description="Queues, jobs, workers, schedules and throughput for a jobq deployment.",
    )
    if cors_origins:
        api.add_middleware(
            CORSMiddleware,
            allow_origins=cors_origins,
            allow_methods=["*"],
            allow_headers=["*"],
        )
    backend = app.backend

    @api.get("/api/health", tags=["system"])
    async def health() -> dict[str, Any]:
        stats = await backend.queue_stats()
        return {"status": "ok", "version": __version__, "queued": stats.queued}

    @api.get("/api/overview", tags=["queues"])
    async def overview() -> dict[str, Any]:
        now = app.clock.now()
        stats = await backend.queue_stats()
        workers = await backend.list_workers(now, stale_worker_after)
        schedules = await backend.list_schedules()
        return {
            **stats.to_dict(),
            "workers": len(workers),
            "schedules": len(schedules),
            "tasks": sorted(app.tasks),
            "now": now,
        }

    @api.get("/api/queues", tags=["queues"])
    async def queues() -> list[dict[str, Any]]:
        stats = await backend.queue_stats()
        return stats.to_dict()["queues"]

    @api.get("/api/jobs", tags=["jobs"])
    async def list_jobs(
        status: JobStatus | None = None,
        queue: str | None = None,
        limit: int = Query(default=50, ge=1, le=500),
        offset: int = Query(default=0, ge=0),
    ) -> dict[str, Any]:
        jobs, total = await backend.list_jobs(
            status=status, queue=queue, limit=limit, offset=offset
        )
        return {
            "items": [j.to_dict() for j in jobs],
            "total": total,
            "limit": limit,
            "offset": offset,
        }

    @api.post("/api/jobs", tags=["jobs"], status_code=201)
    async def enqueue(body: EnqueueRequest) -> dict[str, Any]:
        if body.task not in app.tasks:
            raise HTTPException(404, f"task {body.task!r} is not registered")
        job = await app.enqueue(
            body.task,
            body.args,
            body.kwargs,
            queue=body.queue,
            priority=body.priority,
            delay=body.delay,
            unique_key=body.unique_key,
            depends_on=body.depends_on,
            max_retries=body.max_retries,
            timeout=body.timeout,
        )
        return job.to_dict()

    @api.get("/api/jobs/{job_id}", tags=["jobs"])
    async def get_job(job_id: str) -> dict[str, Any]:
        job = await backend.get_job(job_id)
        if job is None:
            raise HTTPException(404, "job not found")
        return job.to_dict()

    @api.post("/api/jobs/{job_id}/retry", tags=["jobs"])
    async def retry_job(job_id: str) -> dict[str, Any]:
        job = await backend.retry_job(job_id, app.clock.now())
        if job is None:
            raise HTTPException(409, "only failed or cancelled jobs can be retried")
        return job.to_dict()

    @api.post("/api/jobs/{job_id}/requeue", tags=["jobs"])
    async def requeue_job(job_id: str) -> dict[str, Any]:
        job = await backend.requeue_job(job_id, app.clock.now())
        if job is None:
            raise HTTPException(409, "job is already queued or running, or does not exist")
        return job.to_dict()

    @api.delete("/api/jobs/{job_id}", tags=["jobs"], status_code=204)
    async def delete_job(job_id: str) -> None:
        if not await backend.delete_job(job_id):
            raise HTTPException(404, "job not found")

    @api.get("/api/metrics", tags=["metrics"])
    async def metrics(minutes: int = Query(default=60, ge=1, le=180)) -> dict[str, Any]:
        buckets = await backend.metrics(app.clock.now(), minutes)
        totals = {
            key: sum(getattr(b, key) for b in buckets)
            for key in ("enqueued", "succeeded", "failed", "retried")
        }
        return {"minutes": minutes, "buckets": [b.to_dict() for b in buckets], "totals": totals}

    @api.get("/api/workers", tags=["workers"])
    async def workers() -> list[dict[str, Any]]:
        found = await backend.list_workers(app.clock.now(), stale_worker_after)
        return [w.to_dict() for w in found]

    @api.get("/api/schedules", tags=["schedules"])
    async def schedules() -> list[dict[str, Any]]:
        return [s.to_dict() for s in await backend.list_schedules()]

    if static_dir is not None and (static_dir / "index.html").exists():
        api.mount("/assets", StaticFiles(directory=static_dir / "assets"), name="assets")

        @api.get("/{path:path}", include_in_schema=False)
        async def spa(path: str) -> FileResponse:
            candidate = static_dir / path
            if path and candidate.is_file():
                return FileResponse(candidate)
            return FileResponse(static_dir / "index.html")

        log.info("serving dashboard UI from %s", static_dir)

    return api
