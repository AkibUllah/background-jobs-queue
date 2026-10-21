import { describe, expect, it } from "vitest";
import {
  formatDuration,
  formatTimestamp,
  prettyJson,
  relativeTime,
  shortId,
  shortTaskName,
} from "@/lib/format";

describe("formatDuration", () => {
  it("picks sensible units", () => {
    expect(formatDuration(0.25)).toBe("250 ms");
    expect(formatDuration(4.5)).toBe("4.50 s");
    expect(formatDuration(42.25)).toBe("42.3 s");
    expect(formatDuration(125)).toBe("2m 5s");
    expect(formatDuration(3700)).toBe("1h 1m");
  });
  it("handles missing values", () => {
    expect(formatDuration(null)).toBe("–");
    expect(formatDuration(undefined)).toBe("–");
    expect(formatDuration(-3)).toBe("0 ms");
  });
});

describe("relativeTime", () => {
  const now = 1_000_000;
  it("formats past and future", () => {
    expect(relativeTime(now - 5, now)).toBe("5s ago");
    expect(relativeTime(now + 90, now)).toBe("in 1m 30s");
    expect(relativeTime(now - 7200, now)).toBe("2h 0m ago");
    expect(relativeTime(now + 3 * 86400, now)).toBe("in 3d");
    expect(relativeTime(now, now)).toBe("now");
    expect(relativeTime(null, now)).toBe("–");
  });
});

describe("misc helpers", () => {
  it("formats timestamps as UTC", () => {
    expect(formatTimestamp(0)).toBe("1970-01-01 00:00:00Z");
    expect(formatTimestamp(null)).toBe("–");
  });
  it("shortens ids and task names", () => {
    expect(shortId("b534c699390245328eacedcb40a984db")).toBe("b534c699");
    expect(shortId("abc")).toBe("abc");
    expect(shortTaskName("examples.demo_app.send_otp_sms")).toBe("send_otp_sms");
    expect(shortTaskName("plain")).toBe("plain");
  });
  it("pretty prints json", () => {
    expect(prettyJson({ a: 1 })).toBe('{\n  "a": 1\n}');
    expect(prettyJson(null)).toBe("null");
  });
});
