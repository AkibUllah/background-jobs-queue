"""Asyncio worker.

One ``Worker`` claims jobs from an ordered list of queues (first queue wins),
runs up to ``concurrency`` of them at once (coroutine tasks run on the loop,
plain functions run in a thread pool), heartbeats while they run, and performs
the housekeeping every queue needs: promoting delayed jobs, firing cron
schedules and recovering jobs whose worker died.

Housekeeping is idempotent and atomic at the backend, so running many workers
against the same Redis is safe; they simply share the work.
"""

from __future__ import annotations

import asyncio
import contextlib
import functools
import logging
import os
import signal
import socket
import traceback
import uuid
from concurrent.futures import ThreadPoolExecutor
from typing import Any

from jobq.app import JobQ, Task
from jobq.exceptions import JobTimeout, Retry, TaskNotFound
from jobq.job import Job, WorkerInfo

log = logging.getLogger("jobq.worker")


class Worker:
    def __init__(
        self,
        app: JobQ,
        queues: list[str] | None = None,
        *,
        concurrency: int = 10,
        worker_id: str | None = None,
        poll_interval: float = 0.5,
        heartbeat_interval: float = 5.0,
        stall_timeout: float = 60.0,
        housekeeping_interval: float = 1.0,
        shutdown_timeout: float = 30.0,
        run_housekeeping: bool = True,
    ) -> None:
        self.app = app
        self.backend = app.backend
        self.clock = app.clock
        self.queues = queues or [app.default_queue]
        self.concurrency = max(1, concurrency)
        self.id = worker_id or f"{socket.gethostname()}:{os.getpid()}:{uuid.uuid4().hex[:6]}"
        self.poll_interval = poll_interval
        self.heartbeat_interval = heartbeat_interval
        self.stall_timeout = stall_timeout
        self.housekeeping_interval = housekeeping_interval
        self.shutdown_timeout = shutdown_timeout
        self.run_housekeeping = run_housekeeping

        self._running: dict[str, asyncio.Task[None]] = {}
        self._stop = asyncio.Event()
        self._executor = ThreadPoolExecutor(max_workers=self.concurrency, thread_name_prefix="jobq")
        self.processed = 0
        self.failed = 0
        self.started_at = 0.0

    # ------------------------------------------------------------ info
    def info(self) -> WorkerInfo:
        return WorkerInfo(
            id=self.id,
            hostname=socket.gethostname(),
            pid=os.getpid(),
            queues=list(self.queues),
            concurrency=self.concurrency,
            started_at=self.started_at,
            running=len(self._running),
            processed=self.processed,
            failed=self.failed,
        )

    @property
    def running_job_ids(self) -> list[str]:
        return list(self._running)

    # ------------------------------------------------------------ single steps (also used by tests)
    async def process_one(self) -> Job | None:
        """Claim one job and run it to completion. Returns the finished job or None."""
        job = await self.backend.claim(self.queues, self.id, self.clock.now())
        if job is None:
            return None
        await self._run_job(job)
        return await self.backend.get_job(job.id)

    async def tick(self) -> dict[str, int]:
        """One housekeeping round: promote delayed, fire schedules, recover stalled."""
        now = self.clock.now()
        promoted = await self.backend.promote_scheduled(now)
        fired = len(await self.app.tick_schedules())
        recovered = await self.backend.recover_stalled(now, self.stall_timeout)
        if recovered:
            log.warning("recovered %d stalled job(s): %s", len(recovered), ", ".join(recovered))
        return {"promoted": promoted, "fired": fired, "recovered": len(recovered)}

    async def drain(self, max_jobs: int | None = None) -> int:
        """Run tick + process until the queues are empty (handy in tests and scripts)."""
        count = 0
        while max_jobs is None or count < max_jobs:
            await self.tick()
            job = await self.process_one()
            if job is None:
                break
            count += 1
        return count

    # ------------------------------------------------------------ execution
    async def _run_job(self, job: Job) -> None:
        now = self.clock.now()
        try:
            task = self.app.get_task(job.task)
        except TaskNotFound:
            msg = f"TaskNotFound: {job.task!r} is not registered in worker {self.id}"
            log.error("job %s: %s", job.id, msg)
            await self.backend.fail(job.id, msg, now, None)
            self.failed += 1
            return

        log.info(
            "job %s start task=%s queue=%s attempt=%d", job.id, job.task, job.queue, job.attempts
        )
        try:
            result = await self._execute(task, job)
        except Retry as exc:
            await self._handle_failure(job, task, f"Retry: {exc}", forced_delay=exc.delay)
            return
        except asyncio.CancelledError:
            raise
        except BaseException as exc:  # noqa: BLE001 - any task failure must be recorded
            tb = traceback.format_exc()
            if isinstance(exc, JobTimeout):
                tb = f"JobTimeout: {exc}"
            await self._handle_failure(job, task, tb)
            return

        await self.backend.complete(job.id, result, self.clock.now())
        self.processed += 1
        log.info("job %s succeeded in %.3fs", job.id, self.clock.now() - now)

    async def _execute(self, task: Task, job: Job) -> Any:
        if task.is_async:
            coro = task.fn(*job.args, **job.kwargs)
        else:
            loop = asyncio.get_running_loop()
            call = functools.partial(task.fn, *job.args, **job.kwargs)
            coro = loop.run_in_executor(self._executor, call)
        if job.timeout is None:
            return await coro
        try:
            return await asyncio.wait_for(coro, timeout=job.timeout)
        except asyncio.TimeoutError:
            raise JobTimeout(f"exceeded {job.timeout}s") from None

    async def _handle_failure(
        self, job: Job, task: Task, error: str, *, forced_delay: float | None = None
    ) -> None:
        now = self.clock.now()
        if job.attempts <= job.max_retries:
            delay = forced_delay if forced_delay is not None else task.backoff.delay(job.attempts)
            retry_at = now + delay
            log.warning(
                "job %s failed (attempt %d/%d), retrying in %.1fs",
                job.id,
                job.attempts,
                job.max_retries + 1,
                delay,
            )
            await self.backend.fail(job.id, error, now, retry_at)
        else:
            log.error("job %s failed permanently after %d attempt(s)", job.id, job.attempts)
            await self.backend.fail(job.id, error, now, None)
            self.failed += 1

    # ------------------------------------------------------------ main loop
    async def run(self) -> None:
        """Run until ``stop()`` is called or SIGINT/SIGTERM arrives."""
        self.started_at = self.clock.now()
        self._stop.clear()
        self._install_signal_handlers()
        await self.app.sync_schedules()
        await self.backend.register_worker(self.info(), self.clock.now())
        log.info(
            "worker %s started queues=%s concurrency=%d", self.id, self.queues, self.concurrency
        )

        background = [asyncio.create_task(self._heartbeat_loop())]
        if self.run_housekeeping:
            background.append(asyncio.create_task(self._housekeeping_loop()))
        try:
            await self._consume_loop()
        finally:
            self._stop.set()
            _, pending = await asyncio.wait(background, timeout=self.shutdown_timeout)
            for t in pending:
                t.cancel()
            await asyncio.gather(*background, return_exceptions=True)
            await self._shutdown()

    def stop(self) -> None:
        self._stop.set()

    def _install_signal_handlers(self) -> None:
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            with contextlib.suppress(NotImplementedError, RuntimeError):
                loop.add_signal_handler(sig, self.stop)

    async def _consume_loop(self) -> None:
        while not self._stop.is_set():
            if len(self._running) >= self.concurrency:
                await self._wait_for_slot()
                continue
            job = await self.backend.claim(self.queues, self.id, self.clock.now())
            if job is None:
                with contextlib.suppress(asyncio.TimeoutError):
                    await asyncio.wait_for(self._stop.wait(), timeout=self.poll_interval)
                continue
            task = asyncio.create_task(self._run_job(job), name=f"jobq-{job.id}")
            self._running[job.id] = task
            task.add_done_callback(functools.partial(self._on_done, job.id))

    def _on_done(self, job_id: str, task: asyncio.Task[None]) -> None:
        self._running.pop(job_id, None)
        if not task.cancelled() and task.exception() is not None:
            log.exception(
                "unexpected error while running job %s", job_id, exc_info=task.exception()
            )

    async def _wait_for_slot(self) -> None:
        if not self._running:
            return
        await asyncio.wait(list(self._running.values()), return_when=asyncio.FIRST_COMPLETED)

    async def _sleep_unless_stopped(self, seconds: float) -> bool:
        """Sleep for ``seconds`` or until stop() is called. Returns True when stopping."""
        with contextlib.suppress(asyncio.TimeoutError):
            await asyncio.wait_for(self._stop.wait(), timeout=seconds)
        return self._stop.is_set()

    async def _heartbeat_loop(self) -> None:
        while not await self._sleep_unless_stopped(self.heartbeat_interval):
            try:
                now = self.clock.now()
                await self.backend.heartbeat(self.running_job_ids, now)
                await self.backend.register_worker(self.info(), now)
            except Exception:  # noqa: BLE001
                log.exception("heartbeat failed")

    async def _housekeeping_loop(self) -> None:
        while not self._stop.is_set():
            try:
                await self.tick()
            except Exception:  # noqa: BLE001
                log.exception("housekeeping failed")
            if await self._sleep_unless_stopped(self.housekeeping_interval):
                break

    async def _shutdown(self) -> None:
        if self._running:
            log.info(
                "waiting up to %.0fs for %d running job(s)",
                self.shutdown_timeout,
                len(self._running),
            )
            _, pending = await asyncio.wait(
                list(self._running.values()), timeout=self.shutdown_timeout
            )
            if pending:
                stuck = [jid for jid, t in list(self._running.items()) if t in pending]
                log.warning("releasing %d unfinished job(s) back to their queues", len(stuck))
                for t in pending:
                    t.cancel()
                await asyncio.gather(*pending, return_exceptions=True)
                for job_id in stuck:
                    await self.backend.release(job_id)
        await self.backend.deregister_worker(self.id)
        self._executor.shutdown(wait=False, cancel_futures=True)
        log.info("worker %s stopped (processed=%d failed=%d)", self.id, self.processed, self.failed)
