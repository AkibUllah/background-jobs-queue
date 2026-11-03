import type { RateLimitInfo } from "@/lib/types";
import { formatDuration, formatNumber, shortTaskName } from "@/lib/format";

/** Per-task token buckets: how much quota is left right now and how often each task was held back. */
export function RateLimitsPanel({ limits }: { limits: RateLimitInfo[] }) {
  if (limits.length === 0) return null;
  return (
    <div className="card">
      <h2 className="mb-2 text-sm font-semibold">Rate limits</h2>
      <table className="w-full text-sm">
        <thead className="text-left text-xs uppercase text-slate-500">
          <tr>
            <th className="py-1">task</th>
            <th>limit</th>
            <th className="w-1/3">tokens</th>
            <th className="text-right">next token</th>
            <th className="text-right">throttled</th>
          </tr>
        </thead>
        <tbody>
          {limits.map((rl) => {
            const pct = Math.max(0, Math.min(100, (rl.tokens / rl.limit) * 100));
            const empty = rl.tokens < 1;
            return (
              <tr key={rl.task} className="border-t border-slate-100 dark:border-slate-800" data-testid={`rate-limit-${rl.task}`}>
                <td className="py-1 font-medium" title={rl.task}>
                  {shortTaskName(rl.task)}
                </td>
                <td className="font-mono text-xs">{rl.spec}</td>
                <td>
                  <div className="flex items-center gap-2">
                    <div
                      className="h-2 flex-1 rounded bg-slate-200 dark:bg-slate-700"
                      role="meter"
                      aria-label={`${rl.task} tokens`}
                      aria-valuemin={0}
                      aria-valuemax={rl.limit}
                      aria-valuenow={rl.tokens}
                    >
                      <div
                        className={`h-2 rounded ${empty ? "bg-amber-400" : "bg-sky-500"}`}
                        style={{ width: `${pct}%` }}
                      />
                    </div>
                    <span className="w-16 text-right text-xs tabular-nums">
                      {rl.tokens.toFixed(1)}/{rl.limit}
                    </span>
                  </div>
                </td>
                <td className="text-right text-xs text-slate-500">
                  {empty ? formatDuration(rl.next_token_in) : "ready"}
                </td>
                <td className="text-right tabular-nums">{formatNumber(rl.throttled)}</td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}
