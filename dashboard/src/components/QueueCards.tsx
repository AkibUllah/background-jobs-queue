import type { Overview } from "@/lib/types";
import { formatNumber } from "@/lib/format";

interface Props {
  overview: Overview;
  selectedQueue: string;
  onSelectQueue: (queue: string) => void;
}

export function QueueCards({ overview, selectedQueue, onSelectQueue }: Props) {
  const { counts } = overview;
  const stats: { label: string; value: number; tone: string }[] = [
    { label: "queued", value: counts.queued, tone: "text-sky-600 dark:text-sky-400" },
    { label: "scheduled", value: counts.scheduled, tone: "text-violet-600 dark:text-violet-400" },
    { label: "waiting", value: counts.waiting, tone: "text-slate-600 dark:text-slate-300" },
    { label: "running", value: counts.running, tone: "text-emerald-600 dark:text-emerald-400" },
    { label: "succeeded", value: counts.succeeded, tone: "text-green-600 dark:text-green-400" },
    { label: "dead", value: counts.dead, tone: "text-rose-600 dark:text-rose-400" },
  ];
  return (
    <section className="space-y-3">
      <div className="grid grid-cols-2 gap-3 sm:grid-cols-3 lg:grid-cols-6">
        {stats.map((s) => (
          <div key={s.label} className="card" data-testid={`stat-${s.label}`}>
            <div className="text-xs uppercase tracking-wide text-slate-500">{s.label}</div>
            <div className={`mt-1 text-2xl font-semibold tabular-nums ${s.tone}`}>
              {formatNumber(s.value)}
            </div>
          </div>
        ))}
      </div>
      <div className="flex flex-wrap items-center gap-2">
        <span className="text-xs uppercase tracking-wide text-slate-500">Queues</span>
        <button
          type="button"
          onClick={() => onSelectQueue("")}
          className={`btn ${selectedQueue === "" ? "btn-primary" : "btn-default"}`}
        >
          all
        </button>
        {overview.queues.map((q) => (
          <button
            key={q.name}
            type="button"
            data-testid={`queue-${q.name}`}
            onClick={() => onSelectQueue(q.name)}
            className={`btn ${selectedQueue === q.name ? "btn-primary" : "btn-default"}`}
          >
            {q.name}
            <span className="rounded bg-black/10 px-1 font-mono text-[10px] dark:bg-white/10">
              {formatNumber(q.queued)}
            </span>
          </button>
        ))}
        <span className="ml-auto text-xs text-slate-500">
          {overview.workers} worker{overview.workers === 1 ? "" : "s"} · {overview.schedules}{" "}
          schedule{overview.schedules === 1 ? "" : "s"} · {overview.tasks.length} task
          {overview.tasks.length === 1 ? "" : "s"}
        </span>
      </div>
    </section>
  );
}
