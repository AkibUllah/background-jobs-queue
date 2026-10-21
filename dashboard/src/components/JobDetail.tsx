import type { Job } from "@/lib/types";
import { formatDuration, formatTimestamp, prettyJson, relativeTime } from "@/lib/format";
import { StatusBadge } from "./StatusBadge";

interface Props {
  job: Job;
  now: number;
  onRetry: (id: string) => void;
  onRequeue: (id: string) => void;
  onDelete: (id: string) => void;
  onClose: () => void;
  onSelect: (id: string) => void;
  busy?: boolean;
}

export function JobDetail({ job, now, onRetry, onRequeue, onDelete, onClose, onSelect, busy }: Props) {
  const canRetry = job.status === "failed" || job.status === "cancelled";
  const canRequeue = job.status !== "running" && job.status !== "queued";
  const duration =
    job.started_at && job.finished_at ? job.finished_at - job.started_at : job.started_at ? now - job.started_at : null;

  return (
    <aside className="card space-y-4" data-testid="job-detail">
      <div className="flex items-start justify-between gap-2">
        <div>
          <div className="font-mono text-xs text-slate-500">{job.id}</div>
          <h2 className="text-base font-semibold">{job.task}</h2>
          <div className="mt-1 flex items-center gap-2">
            <StatusBadge status={job.status} />
            <span className="text-xs text-slate-500">
              queue <b>{job.queue}</b> · priority {job.priority} · attempt {job.attempts}/{job.max_retries + 1}
            </span>
          </div>
        </div>
        <button type="button" className="btn btn-default" onClick={onClose} aria-label="Close">
          ×
        </button>
      </div>

      <div className="flex flex-wrap gap-2">
        <button type="button" className="btn btn-primary" disabled={!canRetry || busy} onClick={() => onRetry(job.id)}>
          Retry
        </button>
        <button type="button" className="btn btn-default" disabled={!canRequeue || busy} onClick={() => onRequeue(job.id)}>
          Run now
        </button>
        <button type="button" className="btn btn-danger" disabled={busy} onClick={() => onDelete(job.id)}>
          Delete
        </button>
      </div>

      <dl className="grid grid-cols-2 gap-x-4 gap-y-1 text-sm">
        <Row label="created" value={`${formatTimestamp(job.created_at)} (${relativeTime(job.created_at, now)})`} />
        <Row label="started" value={formatTimestamp(job.started_at)} />
        <Row label="finished" value={formatTimestamp(job.finished_at)} />
        <Row label="duration" value={formatDuration(duration)} />
        <Row label="run at" value={job.run_at ? `${formatTimestamp(job.run_at)} (${relativeTime(job.run_at, now)})` : "–"} />
        <Row label="timeout" value={job.timeout ? formatDuration(job.timeout) : "none"} />
        <Row label="worker" value={job.worker_id ?? "–"} />
        <Row label="unique key" value={job.unique_key ?? "–"} />
        <Row label="schedule" value={job.schedule ?? "–"} />
        <Row label="result ttl" value={formatDuration(job.result_ttl)} />
      </dl>

      {job.depends_on.length > 0 && (
        <div>
          <h3 className="text-xs uppercase tracking-wide text-slate-500">
            depends on ({job.deps_remaining} pending)
          </h3>
          <ul className="mt-1 flex flex-wrap gap-1">
            {job.depends_on.map((dep) => (
              <li key={dep}>
                <button type="button" className="btn btn-default font-mono" onClick={() => onSelect(dep)}>
                  {dep.slice(0, 8)}
                </button>
              </li>
            ))}
          </ul>
        </div>
      )}

      <Section title="arguments">
        <pre className="code-block">{prettyJson({ args: job.args, kwargs: job.kwargs })}</pre>
      </Section>
      {job.result !== null && job.result !== undefined && (
        <Section title="result">
          <pre className="code-block">{prettyJson(job.result)}</pre>
        </Section>
      )}
      {job.error && (
        <Section title={job.status === "retrying" ? "last error" : "error"}>
          <pre className="code-block text-rose-700 dark:text-rose-300" data-testid="job-error">
            {job.error}
          </pre>
        </Section>
      )}
    </aside>
  );
}

function Row({ label, value }: { label: string; value: string }) {
  return (
    <>
      <dt className="text-slate-500">{label}</dt>
      <dd className="truncate font-mono text-xs" title={value}>
        {value}
      </dd>
    </>
  );
}

function Section({ title, children }: { title: string; children: React.ReactNode }) {
  return (
    <div>
      <h3 className="mb-1 text-xs uppercase tracking-wide text-slate-500">{title}</h3>
      {children}
    </div>
  );
}
