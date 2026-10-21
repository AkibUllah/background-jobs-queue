import { useEffect, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { api } from "@/lib/api";
import type { JobStatus } from "@/lib/types";
import { useNow } from "@/lib/useNow";
import { QueueCards } from "@/components/QueueCards";
import { ThroughputChart } from "@/components/ThroughputChart";
import { JobsTable } from "@/components/JobsTable";
import { JobDetail } from "@/components/JobDetail";
import { WorkersPanel } from "@/components/WorkersPanel";

const PAGE_SIZE = 25;
const REFRESH_MS = 3000;

export default function App() {
  const now = useNow();
  const queryClient = useQueryClient();
  const [status, setStatus] = useState<JobStatus | "">("");
  const [queue, setQueue] = useState("");
  const [offset, setOffset] = useState(0);
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const [live, setLive] = useState(true);
  const [dark, setDark] = useState(() => document.documentElement.classList.contains("dark"));
  const [notice, setNotice] = useState<string | null>(null);

  useEffect(() => {
    document.documentElement.classList.toggle("dark", dark);
  }, [dark]);

  const refetchInterval = live ? REFRESH_MS : false;
  const overview = useQuery({ queryKey: ["overview"], queryFn: api.overview, refetchInterval });
  const metrics = useQuery({ queryKey: ["metrics"], queryFn: () => api.metrics(30), refetchInterval });
  const workers = useQuery({ queryKey: ["workers"], queryFn: api.workers, refetchInterval });
  const schedules = useQuery({ queryKey: ["schedules"], queryFn: api.schedules, refetchInterval });
  const jobs = useQuery({
    queryKey: ["jobs", status, queue, offset],
    queryFn: () => api.jobs({ status, queue, limit: PAGE_SIZE, offset }),
    refetchInterval,
    placeholderData: (prev) => prev,
  });
  const selected = useQuery({
    queryKey: ["job", selectedId],
    queryFn: () => api.job(selectedId as string),
    enabled: selectedId !== null,
    refetchInterval,
    retry: false,
  });

  const invalidate = () => queryClient.invalidateQueries();
  const flash = (text: string) => {
    setNotice(text);
    window.setTimeout(() => setNotice(null), 2500);
  };
  const onError = (err: unknown) => flash(err instanceof Error ? err.message : "request failed");

  const retry = useMutation({ mutationFn: api.retry, onSuccess: () => { flash("Job queued for retry"); invalidate(); }, onError });
  const requeue = useMutation({ mutationFn: api.requeue, onSuccess: () => { flash("Job moved to the front of its queue"); invalidate(); }, onError });
  const remove = useMutation({
    mutationFn: api.remove,
    onSuccess: () => {
      flash("Job deleted");
      setSelectedId(null);
      invalidate();
    },
    onError,
  });
  const busy = retry.isPending || requeue.isPending || remove.isPending;

  const changeStatus = (s: JobStatus | "") => {
    setStatus(s);
    setOffset(0);
  };
  const changeQueue = (q: string) => {
    setQueue(q);
    setOffset(0);
  };

  return (
    <div className="mx-auto max-w-7xl space-y-4 p-4">
      <header className="flex flex-wrap items-center gap-3">
        <h1 className="flex items-center gap-2 text-lg font-semibold">
          <svg viewBox="0 0 32 32" className="h-6 w-6" aria-hidden="true">
            <rect width="32" height="32" rx="6" className="fill-sky-500" />
            <path d="M8 10h16M8 16h16M8 22h10" stroke="white" strokeWidth="3" strokeLinecap="round" />
          </svg>
          jobq
        </h1>
        {overview.isError && (
          <span className="rounded bg-rose-100 px-2 py-0.5 text-xs text-rose-700 dark:bg-rose-900/50 dark:text-rose-200">
            API unreachable: {(overview.error as Error).message}
          </span>
        )}
        {notice && (
          <span role="status" className="rounded bg-emerald-100 px-2 py-0.5 text-xs text-emerald-800 dark:bg-emerald-900/50 dark:text-emerald-200">
            {notice}
          </span>
        )}
        <div className="ml-auto flex items-center gap-2">
          <label className="flex items-center gap-1 text-xs text-slate-500">
            <input type="checkbox" checked={live} onChange={(e) => setLive(e.target.checked)} />
            live refresh
          </label>
          <button type="button" className="btn btn-default" onClick={() => setDark((d) => !d)}>
            {dark ? "light" : "dark"}
          </button>
        </div>
      </header>

      {overview.data && <QueueCards overview={overview.data} selectedQueue={queue} onSelectQueue={changeQueue} />}
      {metrics.data && <ThroughputChart metrics={metrics.data} />}

      <div className={`grid gap-4 ${selectedId ? "lg:grid-cols-[1fr_minmax(320px,420px)]" : ""}`}>
        <JobsTable
          jobs={jobs.data?.items ?? []}
          total={jobs.data?.total ?? 0}
          status={status}
          onStatusChange={changeStatus}
          offset={offset}
          limit={PAGE_SIZE}
          onPage={setOffset}
          selectedId={selectedId}
          onSelect={setSelectedId}
          now={now}
          loading={jobs.isFetching}
        />
        {selectedId &&
          (selected.data ? (
            <JobDetail
              job={selected.data}
              now={now}
              busy={busy}
              onRetry={(id) => retry.mutate(id)}
              onRequeue={(id) => requeue.mutate(id)}
              onDelete={(id) => remove.mutate(id)}
              onClose={() => setSelectedId(null)}
              onSelect={setSelectedId}
            />
          ) : (
            <aside className="card text-sm text-slate-500">
              {selected.isError ? "Job no longer exists (result expired or deleted)." : "Loading…"}
            </aside>
          ))}
      </div>

      <WorkersPanel workers={workers.data ?? []} schedules={schedules.data ?? []} now={now} />
    </div>
  );
}
