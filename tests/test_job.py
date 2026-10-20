from __future__ import annotations

from jobq.job import Job, JobStatus, Schedule


def test_job_fields_roundtrip():
    job = Job(
        task="send",
        args=[1, "x", {"a": [1, 2]}],
        kwargs={"phone": "+8801711000000", "amount": 12.5},
        priority=7,
        max_retries=5,
        timeout=12.5,
        run_at=1_750_000_100.0,
        unique_key="send:abc",
        depends_on=["a1", "b2"],
        created_at=1_750_000_000.0,
        result={"ok": True},
        error=None,
    )
    fields = job.to_fields()
    assert all(isinstance(v, str) for v in fields.values())
    back = Job.from_fields(fields)
    assert back == job


def test_job_optional_fields_and_terminal_flag():
    job = Job(task="t")
    back = Job.from_fields(job.to_fields())
    assert back.timeout is None and back.run_at is None and back.depends_on == []
    assert not JobStatus.RUNNING.terminal
    assert (
        JobStatus.FAILED.terminal and JobStatus.SUCCEEDED.terminal and JobStatus.CANCELLED.terminal
    )


def test_schedule_json_roundtrip():
    s = Schedule(name="n", task="t", cron="* * * * *", args=[1], kwargs={"k": "v"}, tz="Asia/Dhaka")
    assert Schedule.from_json(s.to_json()) == s
