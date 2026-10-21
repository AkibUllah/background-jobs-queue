import type { Job, JobList, JobStatus, Metrics, Overview, Schedule, WorkerInfo } from "./types";

export const API_BASE = import.meta.env.VITE_API_BASE ?? "";

export class ApiError extends Error {
  constructor(
    public status: number,
    message: string,
  ) {
    super(message);
  }
}

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const res = await fetch(`${API_BASE}${path}`, {
    headers: { "content-type": "application/json", ...(init?.headers ?? {}) },
    ...init,
  });
  if (!res.ok) {
    let detail = res.statusText;
    try {
      const body = (await res.json()) as { detail?: string };
      if (body.detail) detail = body.detail;
    } catch {
      /* non-JSON error body */
    }
    throw new ApiError(res.status, detail);
  }
  if (res.status === 204) return undefined as T;
  return (await res.json()) as T;
}

export interface JobFilters {
  status?: JobStatus | "";
  queue?: string;
  limit?: number;
  offset?: number;
}

export function jobsQueryString(filters: JobFilters): string {
  const params = new URLSearchParams();
  if (filters.status) params.set("status", filters.status);
  if (filters.queue) params.set("queue", filters.queue);
  params.set("limit", String(filters.limit ?? 50));
  params.set("offset", String(filters.offset ?? 0));
  return params.toString();
}

export const api = {
  overview: () => request<Overview>("/api/overview"),
  jobs: (filters: JobFilters) => request<JobList>(`/api/jobs?${jobsQueryString(filters)}`),
  job: (id: string) => request<Job>(`/api/jobs/${id}`),
  retry: (id: string) => request<Job>(`/api/jobs/${id}/retry`, { method: "POST" }),
  requeue: (id: string) => request<Job>(`/api/jobs/${id}/requeue`, { method: "POST" }),
  remove: (id: string) => request<void>(`/api/jobs/${id}`, { method: "DELETE" }),
  metrics: (minutes = 30) => request<Metrics>(`/api/metrics?minutes=${minutes}`),
  workers: () => request<WorkerInfo[]>("/api/workers"),
  schedules: () => request<Schedule[]>("/api/schedules"),
};
