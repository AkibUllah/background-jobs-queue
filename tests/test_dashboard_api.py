from __future__ import annotations

import pytest
from httpx import ASGITransport, AsyncClient

from jobq import JobQ, ManualClock, Worker
from jobq.dashboard.api import create_app
from jobq.job import JobStatus


@pytest.fixture
async def client(app: JobQ):
    @app.task(name="send_otp", queue="high", max_retries=0)
    async def send_otp(phone: str) -> dict:
        return {"phone": phone}

    @app.task(name="explode", max_retries=0)
    async def explode() -> None:
        raise RuntimeError("kaboom")

    api = create_app(app, cors_origins=["http://localhost:5173"], static_dir=None)
    async with AsyncClient(transport=ASGITransport(app=api), base_url="http://t") as c:
        yield c


async def test_health_and_overview(client: AsyncClient, app: JobQ):
    r = await client.get("/api/health")
    assert r.status_code == 200 and r.json()["status"] == "ok"
    await app.enqueue("send_otp", ["+8801711"], queue="high")
    r = await client.get("/api/overview")
    body = r.json()
    assert body["counts"]["queued"] == 1
    assert body["queues"] == [{"name": "high", "queued": 1}]
    assert "send_otp" in body["tasks"] and body["workers"] == 0


async def test_enqueue_via_api_validates_task(client: AsyncClient):
    r = await client.post("/api/jobs", json={"task": "nope"})
    assert r.status_code == 404
    r = await client.post(
        "/api/jobs",
        json={"task": "send_otp", "args": ["+8801711000000"], "priority": 4, "delay": 60},
    )
    assert r.status_code == 201
    body = r.json()
    assert body["status"] == "scheduled" and body["priority"] == 4 and body["queue"] == "high"
    r = await client.post("/api/jobs", json={"task": "send_otp", "priority": 99999})
    assert r.status_code == 422


async def test_list_filter_and_detail(client: AsyncClient, app: JobQ, worker: Worker):
    j1 = await app.enqueue("send_otp", ["a"])
    j2 = await app.enqueue("explode")
    await worker.drain()
    r = await client.get("/api/jobs", params={"status": "succeeded"})
    assert r.json()["total"] == 1 and r.json()["items"][0]["id"] == j1.id
    r = await client.get("/api/jobs", params={"status": "failed"})
    assert [j["id"] for j in r.json()["items"]] == [j2.id]
    r = await client.get(f"/api/jobs/{j2.id}")
    assert r.status_code == 200 and "RuntimeError: kaboom" in r.json()["error"]
    assert (await client.get("/api/jobs/does-not-exist")).status_code == 404
    assert (await client.get("/api/jobs", params={"status": "bogus"})).status_code == 422


async def test_retry_requeue_delete_actions(client: AsyncClient, app: JobQ, worker: Worker):
    failed = await app.enqueue("explode")
    await worker.drain()
    assert (await client.post(f"/api/jobs/{failed.id}/retry")).json()["status"] == "queued"
    assert (await client.post(f"/api/jobs/{failed.id}/retry")).status_code == 409

    scheduled = await app.enqueue("send_otp", ["x"], delay=3600)
    assert (await client.post(f"/api/jobs/{scheduled.id}/requeue")).json()["status"] == "queued"
    assert (await client.post(f"/api/jobs/{scheduled.id}/requeue")).status_code == 409

    assert (await client.delete(f"/api/jobs/{scheduled.id}")).status_code == 204
    assert (await client.delete(f"/api/jobs/{scheduled.id}")).status_code == 404
    assert (await app.backend.queue_stats()).queued == 1


async def test_metrics_endpoint(client: AsyncClient, app: JobQ, worker: Worker, clock: ManualClock):
    await app.enqueue("send_otp", ["a"])
    await app.enqueue("explode")
    await worker.drain()
    r = await client.get("/api/metrics", params={"minutes": 3})
    body = r.json()
    assert len(body["buckets"]) == 3
    assert body["totals"] == {"enqueued": 2, "succeeded": 1, "failed": 1, "retried": 0}
    assert body["buckets"][-1]["timestamp"] == int(clock.now() // 60) * 60
    assert (await client.get("/api/metrics", params={"minutes": 0})).status_code == 422


async def test_workers_and_schedules_endpoints(client: AsyncClient, app: JobQ, worker: Worker):
    await app.backend.register_worker(worker.info(), app.clock.now())
    app.schedule("otp-smoke", "send_otp", every=20, args=["+880"])
    await app.sync_schedules()
    r = await client.get("/api/workers")
    assert [w["id"] for w in r.json()] == ["w1"]
    r = await client.get("/api/schedules")
    assert r.json()[0]["name"] == "otp-smoke" and r.json()[0]["every"] == 20
    assert r.json()[0]["next_run"] == pytest.approx(app.clock.now() + 20)


async def test_cors_headers_for_dev_ui(client: AsyncClient):
    r = await client.options(
        "/api/overview",
        headers={"Origin": "http://localhost:5173", "Access-Control-Request-Method": "GET"},
    )
    assert r.headers.get("access-control-allow-origin") == "http://localhost:5173"


async def test_openapi_lists_endpoints(client: AsyncClient):
    paths = (await client.get("/openapi.json")).json()["paths"]
    assert {"/api/jobs", "/api/jobs/{job_id}/retry", "/api/metrics", "/api/workers"} <= set(paths)


async def test_list_status_enum_values_match(client: AsyncClient):
    for status in JobStatus:
        r = await client.get("/api/jobs", params={"status": status.value})
        assert r.status_code == 200, status
