"""Demo application used by docker-compose, the benchmark smoke test and the README.

Run a worker and the dashboard against it::

    export JOBQ_REDIS_URL=redis://localhost:6379/0
    jobq worker --app examples.demo_app:app --queues high,default,low
    jobq dashboard --app examples.demo_app:app

The tasks imitate the background work of a Bangladeshi fintech/e-commerce
backend: OTP SMS, bKash payment reconciliation, nightly reports and webhooks
that sometimes fail so the retry and dead-letter paths are visible in the UI.
"""

from __future__ import annotations

import asyncio
import hashlib
import random
import time

from jobq import Backoff, JobQ, Retry

app = JobQ()


@app.task(queue="high", max_retries=5, timeout=10, backoff=Backoff(base=2, max_delay=60))
async def send_otp_sms(phone: str, code: str) -> dict:
    """Pretend to call an SMS aggregator; takes 50-150 ms."""
    await asyncio.sleep(random.uniform(0.05, 0.15))
    return {"phone": phone, "provider": "ssl-wireless", "segments": 1, "code_hash": _h(code)}


@app.task(queue="default", max_retries=3)
async def reconcile_bkash_payment(trx_id: str, amount_paisa: int) -> dict:
    """Simulated reconciliation that occasionally sees a pending transaction and asks to retry."""
    await asyncio.sleep(0.1)
    if random.random() < 0.2:
        raise Retry("bKash says Pending; check again shortly", delay=5)
    return {"trx_id": trx_id, "amount_paisa": amount_paisa, "status": "Completed"}


@app.task(queue="default", max_retries=2, backoff=Backoff(base=3, jitter=0.5))
async def deliver_webhook(url: str, event: str) -> dict:
    """Fails about 40% of the time so retries and dead letters show up in the dashboard."""
    await asyncio.sleep(0.05)
    if random.random() < 0.4:
        raise ConnectionError(f"POST {url} -> 503 Service Unavailable")
    return {"url": url, "event": event, "status": 200}


@app.task(queue="low", timeout=120)
def generate_daily_report(day: str, shop_id: int) -> dict:
    """Synchronous CPU-ish work; runs in the worker's thread pool."""
    started = time.perf_counter()
    total = 0
    for i in range(300_000):
        total = (total + i * shop_id) % 1_000_003
    return {
        "day": day,
        "shop_id": shop_id,
        "checksum": total,
        "seconds": time.perf_counter() - started,
    }


@app.task(queue="default", unique=True)
async def rebuild_search_index(shop_id: int) -> dict:
    """Unique per shop: enqueueing twice while one is pending returns the same job."""
    await asyncio.sleep(0.3)
    return {"shop_id": shop_id, "documents": random.randint(500, 5000)}


@app.task(queue="default")
async def notify_report_ready(shop_id: int) -> dict:
    return {"shop_id": shop_id, "channel": "email"}


app.schedule("otp-smoke", send_otp_sms, every=20, args=["+8801711000000", "0000"], queue="high")
app.schedule(
    "hourly-reconcile",
    reconcile_bkash_payment,
    cron="*/15 * * * *",
    args=["TRX-RECON", 0],
)
app.schedule(
    "nightly-report",
    generate_daily_report,
    cron="30 2 * * *",
    tz="Asia/Dhaka",
    args=["yesterday", 1],
    queue="low",
)
app.schedule(
    "webhook-demo", deliver_webhook, every=30, args=["https://shop.example/hooks", "order.paid"]
)


def _h(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()[:12]


async def seed(count: int = 40) -> None:
    """Enqueue a realistic burst of jobs (used by the ``producer`` compose service)."""
    phones = [f"+88017{random.randint(10000000, 99999999)}" for _ in range(count)]
    for i, phone in enumerate(phones):
        await send_otp_sms.options(priority=5 if i % 7 == 0 else 0).enqueue(
            phone, f"{random.randint(1000, 9999)}"
        )
    for _ in range(count // 2):
        await reconcile_bkash_payment.enqueue(
            f"TRX{random.randint(100000, 999999)}", random.randint(5000, 2_500_000)
        )
    for _ in range(count // 4):
        await deliver_webhook.options(delay=random.uniform(0, 30)).enqueue(
            "https://merchant.example/webhooks/orders", "order.paid"
        )
    for shop_id in (1, 2, 3):
        report = await generate_daily_report.enqueue("2026-10-20", shop_id)
        await notify_report_ready.options(depends_on=(report.id,)).enqueue(shop_id)
        await rebuild_search_index.enqueue(shop_id)
        await rebuild_search_index.enqueue(shop_id)  # deduplicated onto the first


if __name__ == "__main__":
    from jobq.backends.redis import RedisBackend
    from jobq.settings import Settings

    settings = Settings()
    if settings.redis_url:
        app.backend = RedisBackend(settings.redis_url, prefix=settings.prefix)
    asyncio.run(seed())
