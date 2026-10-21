export type JobStatus =
  | "queued"
  | "scheduled"
  | "retrying"
  | "waiting"
  | "running"
  | "succeeded"
  | "failed"
  | "cancelled";

export const JOB_STATUSES: JobStatus[] = [
  "queued",
  "scheduled",
  "retrying",
  "waiting",
  "running",
  "succeeded",
  "failed",
  "cancelled",
];

export interface Job {
  id: string;
  task: string;
  queue: string;
  args: unknown[];
  kwargs: Record<string, unknown>;
  priority: number;
  status: JobStatus;
  attempts: number;
  max_retries: number;
  timeout: number | null;
  run_at: number | null;
  unique_key: string | null;
  depends_on: string[];
  deps_remaining: number;
  created_at: number;
  started_at: number | null;
  finished_at: number | null;
  worker_id: string | null;
  result: unknown;
  error: string | null;
  result_ttl: number;
  schedule: string | null;
}

export interface JobList {
  items: Job[];
  total: number;
  limit: number;
  offset: number;
}

export interface QueueInfo {
  name: string;
  queued: number;
}

export interface Overview {
  queues: QueueInfo[];
  counts: {
    queued: number;
    scheduled: number;
    waiting: number;
    running: number;
    succeeded: number;
    dead: number;
  };
  workers: number;
  schedules: number;
  tasks: string[];
  now: number;
}

export interface MinuteBucket {
  minute: number;
  timestamp: number;
  enqueued: number;
  succeeded: number;
  failed: number;
  retried: number;
}

export interface Metrics {
  minutes: number;
  buckets: MinuteBucket[];
  totals: { enqueued: number; succeeded: number; failed: number; retried: number };
}

export interface WorkerInfo {
  id: string;
  hostname: string;
  pid: number;
  queues: string[];
  concurrency: number;
  started_at: number;
  heartbeat_at: number;
  running: number;
  processed: number;
  failed: number;
}

export interface Schedule {
  name: string;
  task: string;
  cron: string | null;
  every: number | null;
  queue: string;
  next_run: number | null;
  last_run: number | null;
  enabled: boolean;
}
