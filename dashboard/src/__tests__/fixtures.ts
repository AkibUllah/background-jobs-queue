import type { Job, Overview } from "@/lib/types";

export const NOW = 1_750_000_000;

export function makeJob(overrides: Partial<Job> = {}): Job {
  return {
    id: "b534c699390245328eacedcb40a984db",
    task: "examples.demo_app.send_otp_sms",
    queue: "high",
    args: ["+8801711000000", "4821"],
    kwargs: {},
    priority: 0,
    status: "queued",
    attempts: 0,
    max_retries: 3,
    timeout: 10,
    run_at: null,
    unique_key: null,
    depends_on: [],
    deps_remaining: 0,
    created_at: NOW - 42,
    started_at: null,
    finished_at: null,
    worker_id: null,
    result: null,
    error: null,
    result_ttl: 86400,
    schedule: null,
    ...overrides,
  };
}

export const overview: Overview = {
  queues: [
    { name: "default", queued: 12 },
    { name: "high", queued: 3 },
  ],
  counts: { queued: 15, scheduled: 4, waiting: 1, running: 2, succeeded: 1200, dead: 7 },
  workers: 2,
  schedules: 3,
  tasks: ["a", "b"],
  now: NOW,
};
