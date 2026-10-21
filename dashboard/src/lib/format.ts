/** Formatting helpers shared by the components; pure so they are easy to test. */

export function formatDuration(seconds: number | null | undefined): string {
  if (seconds === null || seconds === undefined || Number.isNaN(seconds)) return "–";
  if (seconds < 0) seconds = 0;
  if (seconds < 1) return `${Math.round(seconds * 1000)} ms`;
  if (seconds < 60) return `${seconds.toFixed(seconds < 10 ? 2 : 1)} s`;
  const m = Math.floor(seconds / 60);
  const s = Math.round(seconds % 60);
  if (m < 60) return `${m}m ${s}s`;
  const h = Math.floor(m / 60);
  return `${h}h ${m % 60}m`;
}

/** "3 s ago", "in 2m", relative to `now` (unix seconds). */
export function relativeTime(ts: number | null | undefined, now: number): string {
  if (ts === null || ts === undefined) return "–";
  const diff = ts - now;
  const abs = Math.abs(diff);
  let text: string;
  if (abs < 1) text = "now";
  else if (abs < 60) text = `${Math.round(abs)}s`;
  else if (abs < 3600) text = `${Math.floor(abs / 60)}m ${Math.round(abs % 60)}s`;
  else if (abs < 86400) text = `${Math.floor(abs / 3600)}h ${Math.floor((abs % 3600) / 60)}m`;
  else text = `${Math.floor(abs / 86400)}d`;
  if (text === "now") return text;
  return diff < 0 ? `${text} ago` : `in ${text}`;
}

export function formatTimestamp(ts: number | null | undefined): string {
  if (ts === null || ts === undefined) return "–";
  return new Date(ts * 1000).toISOString().replace("T", " ").slice(0, 19) + "Z";
}

export function formatNumber(n: number): string {
  return new Intl.NumberFormat("en-US").format(n);
}

export function shortId(id: string, length = 8): string {
  return id.length <= length ? id : id.slice(0, length);
}

/** Last path segment of a dotted task name: "examples.demo_app.send_otp_sms" -> "send_otp_sms". */
export function shortTaskName(task: string): string {
  const idx = task.lastIndexOf(".");
  return idx === -1 ? task : task.slice(idx + 1);
}

export function prettyJson(value: unknown): string {
  if (value === null || value === undefined) return "null";
  try {
    return JSON.stringify(value, null, 2);
  } catch {
    return String(value);
  }
}
