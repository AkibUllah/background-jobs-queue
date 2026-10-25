# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- API reference and benchmark screenshots in the README.
- CodeQL code scanning workflow (Python, JavaScript/TypeScript and Actions) and Dependabot updates for pip, npm (`dashboard/`), Docker and GitHub Actions.
- Roadmap tracked as GitHub issues under the v0.2.0 milestone.
- This changelog.

### Changed

- CI workflow runs with a read-only `GITHUB_TOKEN` by default.

## [0.1.0] - 2026-10-25

First public release.

### Added

- `@app.task` decorator for async and sync functions, with `options(priority=..., delay=...)` per enqueue.
- Priorities, delays and `run_at`, per-job timeouts, retries with exponential backoff and jitter, and `raise Retry(delay=...)`.
- Cron and interval schedules (timezone-aware), fired exactly once per due time across many workers.
- Unique jobs, job dependencies with cancellation cascades, dead-letter set, result storage with TTL and per-minute throughput counters.
- Heartbeats, stalled-job recovery and graceful shutdown that releases in-flight jobs to the front of their queue.
- `RedisBackend` (eleven atomic Lua scripts) and `MemoryBackend` behind one protocol.
- Typer CLI (`worker`, `dashboard`, `enqueue`, `stats`, `jobs`, `schedules`), FastAPI dashboard API and React UI.
- Throughput benchmark for both backends; 127 pytest tests across both backends and 21 vitest tests.
- Multi-stage Dockerfile, compose stack with Redis and workers, live demo kit (Swagger UI on GitHub Pages, Codespaces, Render).

### Fixed

- Heap-based result expiry and cooperative worker shutdown.

[Unreleased]: https://github.com/AkibUllah/background-jobs-queue/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/AkibUllah/background-jobs-queue/releases/tag/v0.1.0
