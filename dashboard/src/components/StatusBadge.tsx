import clsx from "clsx";
import type { JobStatus } from "@/lib/types";

const STYLES: Record<JobStatus, string> = {
  queued: "bg-sky-100 text-sky-800 dark:bg-sky-900/50 dark:text-sky-200",
  scheduled: "bg-violet-100 text-violet-800 dark:bg-violet-900/50 dark:text-violet-200",
  retrying: "bg-amber-100 text-amber-800 dark:bg-amber-900/50 dark:text-amber-200",
  waiting: "bg-slate-200 text-slate-700 dark:bg-slate-700 dark:text-slate-200",
  running: "bg-emerald-100 text-emerald-800 dark:bg-emerald-900/50 dark:text-emerald-200",
  succeeded: "bg-green-100 text-green-800 dark:bg-green-900/50 dark:text-green-200",
  failed: "bg-rose-100 text-rose-800 dark:bg-rose-900/50 dark:text-rose-200",
  cancelled: "bg-slate-300 text-slate-800 dark:bg-slate-600 dark:text-slate-100",
};

export function StatusBadge({ status }: { status: JobStatus }) {
  return (
    <span
      data-testid="status-badge"
      className={clsx(
        "inline-flex items-center rounded-full px-2 py-0.5 text-xs font-medium",
        STYLES[status],
      )}
    >
      {status === "running" && (
        <span className="mr-1 inline-block h-1.5 w-1.5 animate-pulse rounded-full bg-current" />
      )}
      {status}
    </span>
  );
}
