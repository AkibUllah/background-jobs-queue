import type { Schedule, WorkerInfo } from "@/lib/types";
import { formatNumber, relativeTime } from "@/lib/format";

interface Props {
  workers: WorkerInfo[];
  schedules: Schedule[];
  now: number;
}

export function WorkersPanel({ workers, schedules, now }: Props) {
  return (
    <div className="grid gap-3 lg:grid-cols-2">
      <div className="card">
        <h2 className="mb-2 text-sm font-semibold">Workers</h2>
        {workers.length === 0 ? (
          <p className="text-sm text-slate-500">No workers connected. Start one with `jobq worker`.</p>
        ) : (
          <table className="w-full text-sm">
            <thead className="text-left text-xs uppercase text-slate-500">
              <tr>
                <th className="py-1">id</th>
                <th>queues</th>
                <th className="text-right">running</th>
                <th className="text-right">processed</th>
                <th className="text-right">failed</th>
                <th className="text-right">heartbeat</th>
              </tr>
            </thead>
            <tbody>
              {workers.map((w) => (
                <tr key={w.id} className="border-t border-slate-100 dark:border-slate-800">
                  <td className="py-1 font-mono text-xs">{w.id}</td>
                  <td className="text-xs">{w.queues.join(", ")}</td>
                  <td className="text-right tabular-nums">
                    {w.running}/{w.concurrency}
                  </td>
                  <td className="text-right tabular-nums">{formatNumber(w.processed)}</td>
                  <td className="text-right tabular-nums">{formatNumber(w.failed)}</td>
                  <td className="text-right text-xs text-slate-500">{relativeTime(w.heartbeat_at, now)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>
      <div className="card">
        <h2 className="mb-2 text-sm font-semibold">Schedules</h2>
        {schedules.length === 0 ? (
          <p className="text-sm text-slate-500">No recurring schedules registered.</p>
        ) : (
          <table className="w-full text-sm">
            <thead className="text-left text-xs uppercase text-slate-500">
              <tr>
                <th className="py-1">name</th>
                <th>spec</th>
                <th>queue</th>
                <th className="text-right">next run</th>
              </tr>
            </thead>
            <tbody>
              {schedules.map((s) => (
                <tr key={s.name} className="border-t border-slate-100 dark:border-slate-800">
                  <td className="py-1 font-medium">{s.name}</td>
                  <td className="font-mono text-xs">{s.cron ?? `every ${s.every}s`}</td>
                  <td className="text-xs">{s.queue}</td>
                  <td className="text-right text-xs text-slate-500">{relativeTime(s.next_run, now)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>
    </div>
  );
}
