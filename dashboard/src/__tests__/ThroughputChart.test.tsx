import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import { ThroughputChart } from "@/components/ThroughputChart";
import type { Metrics } from "@/lib/types";

const metrics: Metrics = {
  minutes: 3,
  buckets: [
    { minute: 1, timestamp: 60, enqueued: 5, succeeded: 4, failed: 1, retried: 0, throttled: 0 },
    { minute: 2, timestamp: 120, enqueued: 0, succeeded: 0, failed: 0, retried: 0, throttled: 0 },
    { minute: 3, timestamp: 180, enqueued: 2, succeeded: 1, failed: 0, retried: 1, throttled: 6 },
  ],
  totals: { enqueued: 7, succeeded: 5, failed: 1, retried: 1, throttled: 6 },
};

describe("ThroughputChart", () => {
  it("draws one bar segment per non-zero series and shows totals", () => {
    render(<ThroughputChart metrics={metrics} />);
    const svg = screen.getByTestId("throughput-chart");
    expect(svg.querySelectorAll("rect.fill-green-500")).toHaveLength(2);
    expect(svg.querySelectorAll("rect.fill-rose-500")).toHaveLength(1);
    expect(svg.querySelectorAll("rect.fill-amber-400")).toHaveLength(1);
    expect(screen.getByText("5 ok")).toBeInTheDocument();
    expect(screen.getByText("7 enqueued")).toBeInTheDocument();
    expect(svg.querySelectorAll("title")[0].textContent).toContain("00:01 UTC: 5 processed");
    expect(screen.getByText("6 throttled")).toBeInTheDocument();
    expect(svg.querySelectorAll("title")[2].textContent).toContain("2 enqueued, 6 throttled");
    expect(svg.querySelectorAll("title")[0].textContent).not.toContain("throttled");
  });

  it("hides the throttled total when nothing was rate limited", () => {
    const quiet: Metrics = {
      ...metrics,
      buckets: metrics.buckets.map((b) => ({ ...b, throttled: 0 })),
      totals: { ...metrics.totals, throttled: 0 },
    };
    render(<ThroughputChart metrics={quiet} />);
    expect(screen.queryByText(/throttled/)).not.toBeInTheDocument();
  });
});
