"""Throughput benchmark for jobq backends.

Measures, per backend:

* enqueue/sec  - sequential ``backend.enqueue`` calls (one producer, one connection)
* process/sec  - a worker claiming, running a no-op task and acknowledging, one job at a time
* worker/sec   - the real ``Worker.run`` loop with ``--concurrency`` tasks in flight

Backends: ``memory`` (in-process) and ``fakeredis`` (the Lua scripts executed by
fakeredis' Lua runtime, so it measures script logic, not network I/O). Pass
``--redis redis://host:6379/0`` to add a row for a real Redis server.

    python bench/bench.py --jobs 20000
"""

from __future__ import annotations

import argparse
import asyncio
import platform
import time
from collections.abc import Awaitable, Callable

from jobq import JobQ, MemoryBackend, RedisBackend, Worker
from jobq.backends.base import Backend
from jobq.job import Job


async def noop(i: int) -> int:
    return i


async def timed(label: str, fn: Callable[[], Awaitable[None]]) -> float:
    start = time.perf_counter()
    await fn()
    return time.perf_counter() - start


async def bench_backend(
    name: str, backend: Backend, jobs: int, concurrency: int
) -> dict[str, float]:
    app = JobQ(backend)
    task = app.task(noop, name="bench.noop")
    await backend.clear()

    # enqueue ------------------------------------------------------------
    latencies: list[float] = []

    async def do_enqueue() -> None:
        now = app.clock.now()
        for i in range(jobs):
            t0 = time.perf_counter()
            await backend.enqueue(Job(task=task.name, args=[i], priority=i % 3), now)
            latencies.append(time.perf_counter() - t0)

    enqueue_s = await timed("enqueue", do_enqueue)
    latencies.sort()
    p50 = latencies[len(latencies) // 2] * 1e6
    p99 = latencies[int(len(latencies) * 0.99)] * 1e6

    # process sequentially ------------------------------------------------
    worker = Worker(app, ["default"], worker_id="bench", run_housekeeping=False)

    async def do_process() -> None:
        processed = 0
        while await worker.process_one() is not None:
            processed += 1
        assert processed == jobs, processed

    process_s = await timed("process", do_process)

    # full worker loop with concurrency ------------------------------------
    now = app.clock.now()
    for i in range(jobs):
        await backend.enqueue(Job(task=task.name, args=[i]), now)
    loop_worker = Worker(
        app,
        ["default"],
        concurrency=concurrency,
        worker_id="bench-loop",
        poll_interval=0.001,
        run_housekeeping=False,
        heartbeat_interval=1.0,
    )

    async def do_loop() -> None:
        runner = asyncio.create_task(loop_worker.run())
        while loop_worker.processed < jobs:
            await asyncio.sleep(0.005)
        loop_worker.stop()
        await runner

    loop_s = await timed("worker", do_loop)
    await backend.clear()
    return {
        "enqueue_per_s": jobs / enqueue_s,
        "enqueue_p50_us": p50,
        "enqueue_p99_us": p99,
        "process_per_s": jobs / process_s,
        "worker_per_s": jobs / loop_s,
    }


def make_backends(args: argparse.Namespace) -> list[tuple[str, Backend]]:
    out: list[tuple[str, Backend]] = []
    for name in args.backends.split(","):
        name = name.strip()
        if name == "memory":
            out.append(("memory", MemoryBackend()))
        elif name == "fakeredis":
            import fakeredis.aioredis

            client = fakeredis.aioredis.FakeRedis(decode_responses=True)
            out.append(("fakeredis (Lua)", RedisBackend(client=client, prefix="bench:")))
        else:
            raise SystemExit(f"unknown backend {name!r}")
    if args.redis:
        out.append(("redis", RedisBackend(args.redis, prefix="bench:")))
    return out


async def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--jobs", type=int, default=20_000)
    parser.add_argument("--concurrency", type=int, default=20)
    parser.add_argument("--backends", default="memory,fakeredis")
    parser.add_argument("--redis", default=None, help="redis URL for an extra real-Redis row")
    args = parser.parse_args()

    print(
        f"jobq benchmark: {args.jobs} jobs, worker concurrency {args.concurrency}, "
        f"Python {platform.python_version()} on {platform.machine()} {platform.system()}\n"
    )
    print(
        "| backend | enqueue/sec | enqueue p50 | enqueue p99 "
        "| process/sec (sequential) | worker loop/sec |"
    )
    print("| --- | ---: | ---: | ---: | ---: | ---: |")
    for name, backend in make_backends(args):
        r = await bench_backend(name, backend, args.jobs, args.concurrency)
        print(
            f"| {name} | {r['enqueue_per_s']:,.0f} | {r['enqueue_p50_us']:.1f} µs | "
            f"{r['enqueue_p99_us']:.1f} µs | {r['process_per_s']:,.0f} | {r['worker_per_s']:,.0f} |"
        )
        await backend.close()


if __name__ == "__main__":
    asyncio.run(main())
