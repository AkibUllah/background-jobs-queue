#!/usr/bin/env bash
# Codespaces start hook: Redis + two workers + dashboard (React UI built into the image) and the
# one-shot producer service, which enqueues a burst of demo jobs on every start.
set -euo pipefail
cd "$(dirname "$0")/.."

docker compose up -d --build

echo "waiting for the dashboard on :8000 ..."
for _ in $(seq 1 90); do
  if curl -fsS http://127.0.0.1:8000/api/health >/dev/null 2>&1; then
    break
  fi
  sleep 2
done

cat <<'MSG'

Background Jobs Queue is up.
  Dashboard:  forwarded port 8000, path /        (queues, jobs, workers, throughput)
  API docs:   forwarded port 8000, path /docs
  Demo jobs:  OTP SMS, bKash reconciliation (some retry), flaky webhooks, dependent reports;
              enqueue more with: docker compose run --rm producer
MSG
