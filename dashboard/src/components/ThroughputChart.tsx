import type { Metrics } from "@/lib/types";
import { formatNumber } from "@/lib/format";

/** Stacked per-minute bars (succeeded / failed / retried) drawn as inline SVG; no chart library. */
export function ThroughputChart({ metrics }: { metrics: Metrics }) {
  const buckets = metrics.buckets;
  const max = Math.max(1, ...buckets.map((b) => b.succeeded + b.failed + b.retried));
  const width = 600;
  const height = 120;
  const gap = 2;
  const barWidth = (width - gap * (buckets.length - 1)) / Math.max(1, buckets.length);
  const scale = (v: number) => (v / max) * (height - 16);

  return (
    <div className="card">
      <div className="mb-2 flex items-baseline justify-between">
        <h2 className="text-sm font-semibold">Throughput, last {metrics.minutes} min</h2>
        <div className="flex gap-3 text-xs text-slate-500">
          <Legend color="bg-green-500" label={`${formatNumber(metrics.totals.succeeded)} ok`} />
          <Legend color="bg-rose-500" label={`${formatNumber(metrics.totals.failed)} failed`} />
          <Legend color="bg-amber-400" label={`${formatNumber(metrics.totals.retried)} retried`} />
          <span>{formatNumber(metrics.totals.enqueued)} enqueued</span>
        </div>
      </div>
      <svg
        viewBox={`0 0 ${width} ${height}`}
        className="h-32 w-full"
        role="img"
        aria-label="Jobs per minute"
        data-testid="throughput-chart"
      >
        <line x1="0" y1={height - 0.5} x2={width} y2={height - 0.5} className="stroke-slate-300 dark:stroke-slate-700" />
        {buckets.map((b, i) => {
          const x = i * (barWidth + gap);
          const okH = scale(b.succeeded);
          const failH = scale(b.failed);
          const retryH = scale(b.retried);
          let y = height - 1;
          const rects = [];
          for (const [h, cls] of [
            [okH, "fill-green-500"],
            [failH, "fill-rose-500"],
            [retryH, "fill-amber-400"],
          ] as const) {
            if (h > 0) {
              y -= h;
              rects.push(<rect key={cls} x={x} y={y} width={barWidth} height={h} className={cls} />);
            }
          }
          const total = b.succeeded + b.failed + b.retried;
          return (
            <g key={b.minute}>
              <title>{`${new Date(b.timestamp * 1000).toISOString().slice(11, 16)} UTC: ${total} processed (${b.succeeded} ok, ${b.failed} failed, ${b.retried} retried), ${b.enqueued} enqueued`}</title>
              <rect x={x} y={0} width={barWidth} height={height} className="fill-transparent" />
              {rects}
            </g>
          );
        })}
      </svg>
    </div>
  );
}

function Legend({ color, label }: { color: string; label: string }) {
  return (
    <span className="inline-flex items-center gap-1">
      <span className={`inline-block h-2 w-2 rounded-sm ${color}`} /> {label}
    </span>
  );
}
