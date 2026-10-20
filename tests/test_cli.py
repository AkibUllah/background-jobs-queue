from __future__ import annotations

import json
import os

import pytest
from typer.testing import CliRunner

from jobq.cli import cli

runner = CliRunner()


@pytest.fixture(autouse=True)
def _env(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.delenv("JOBQ_REDIS_URL", raising=False)
    monkeypatch.setenv("JOBQ_APP", "examples.demo_app:app")
    monkeypatch.chdir(os.path.dirname(os.path.dirname(__file__)))


def test_version():
    result = runner.invoke(cli, ["version"])
    assert result.exit_code == 0 and "jobq 0.1.0" in result.output


def test_enqueue_and_stats_with_memory_backend():
    result = runner.invoke(
        cli,
        [
            "enqueue",
            "examples.demo_app.send_otp_sms",
            "--args",
            '["+8801711000000", "1234"]',
            "--priority",
            "3",
        ],
    )
    assert result.exit_code == 0, result.output
    job = json.loads(result.output)
    assert job["queue"] == "high" and job["priority"] == 3 and job["status"] == "queued"

    result = runner.invoke(cli, ["stats"])
    assert result.exit_code == 0 and "queues:" in result.output and "workers: 0" in result.output


def test_missing_app_is_a_usage_error(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.delenv("JOBQ_APP")
    result = runner.invoke(cli, ["stats"])
    assert result.exit_code != 0
    assert "JOBQ_APP" in result.output


def test_schedules_command_lists_nothing_before_a_worker_syncs():
    result = runner.invoke(cli, ["schedules"])
    assert result.exit_code == 0 and result.output.strip() == ""
