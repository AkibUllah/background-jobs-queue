import type { Job, JobStatus } from "@/lib/types";
import { JOB_STATUSES } from "@/lib/types";
import { relativeTime, shortId, shortTaskName } from "@/lib/format";
import { StatusBadge } from "./StatusBadge";

interface Props {
  jobs: Job[];
  total: number;
  status: JobStatus | "";
  onStatusChange: (status: JobStatus | "") => void;
  offset: number;
  limit: number;
  onPage: (offset: number) => void;
  selectedId: string | null;
  onSelect: (id: string) => void;
  now: number;
  loading?: boolean;
}

export function JobsTable({
  jobs,
  total,
  status,
  onStatusChange,
  offset,
  limit,
  onPage,
  selectedId,
  onSelect,
  now,
  loading,
}: Props) {
  const from = total === 0 ? 0 : offset + 1;
  const to = Math.min(offset + limit, total);
  return (
    <div className="card p-0">
      <div className="flex flex-wrap items-center gap-2 border-b border-slate-200 p-3 dark:border-slate-800">
        <h2 className="text-sm font-semibold">Jobs</h2>
        <div className="flex flex-wrap gap-1" role="tablist" aria-label="Filter by status">
          <FilterButton active={status === ""} onClick={() => onStatusChange("")} label="all" />
          {JOB_STATUSES.map((s) => (
            <FilterButton key={s} active={status === s} onClick={() => onStatusChange(s)} label={s} />
          ))}
        </div>
        <div className="ml-auto flex items-center gap-2 text-xs text-slate-500">
          {loading && <span className="animate-pulse">refreshing…</span>}
          <span data-testid="pagination-summary">
            {from}–{to} of {total}
          </span>
          <button
            type="button"
            className="btn btn-default"
            disabled={offset === 0}
            onClick={() => onPage(Math.max(0, offset - limit))}
          >
            prev
          </button>
          <button
            type="button"
            className="btn btn-default"
            disabled={offset + limit >= total}
            onClick={() => onPage(offset + limit)}
          >
            next
          </button>
        </div>
      </div>
      <div className="overflow-x-auto">
        <table className="w-full text-sm">
          <thead className="text-left text-xs uppercase text-slate-500">
            <tr>
              <th className="px-3 py-2">id</th>
              <th>task</th>
              <th>queue</th>
              <th>status</th>
              <th className="text-right">prio</th>
              <th className="text-right">attempts</th>
              <th className="text-right">created</th>
              <th className="text-right">next / finished</th>
            </tr>
          </thead>
          <tbody>
            {jobs.length === 0 && (
              <tr>
                <td colSpan={8} className="px-3 py-6 text-center text-slate-500">
                  No jobs match this filter.
                </td>
              </tr>
            )}
            {jobs.map((job) => (
              <tr
                key={job.id}
                data-testid="job-row"
                onClick={() => onSelect(job.id)}
                className={`cursor-pointer border-t border-slate-100 hover:bg-slate-50 dark:border-slate-800 dark:hover:bg-slate-800/60 ${
                  selectedId === job.id ? "bg-sky-50 dark:bg-sky-950/40" : ""
                }`}
              >
                <td className="px-3 py-1.5 font-mono text-xs" title={job.id}>
                  {shortId(job.id)}
                </td>
                <td className="font-medium" title={job.task}>
                  {shortTaskName(job.task)}
                  {job.schedule && (
                    <span className="ml-1 rounded bg-violet-100 px-1 text-[10px] text-violet-700 dark:bg-violet-900/50 dark:text-violet-200">
                      {job.schedule}
                    </span>
                  )}
                </td>
                <td className="text-xs">{job.queue}</td>
                <td>
                  <StatusBadge status={job.status} />
                </td>
                <td className="text-right tabular-nums">{job.priority}</td>
                <td className="text-right tabular-nums">
                  {job.attempts}/{job.max_retries + 1}
                </td>
                <td className="text-right text-xs text-slate-500">{relativeTime(job.created_at, now)}</td>
                <td className="text-right text-xs text-slate-500">
                  {job.finished_at
                    ? relativeTime(job.finished_at, now)
                    : job.run_at
                      ? relativeTime(job.run_at, now)
                      : "–"}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

function FilterButton({ active, onClick, label }: { active: boolean; onClick: () => void; label: string }) {
  return (
    <button
      type="button"
      role="tab"
      aria-selected={active}
      onClick={onClick}
      className={`btn ${active ? "btn-primary" : "btn-default"}`}
    >
      {label}
    </button>
  );
}
