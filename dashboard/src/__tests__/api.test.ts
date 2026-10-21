import { afterEach, describe, expect, it, vi } from "vitest";
import { ApiError, api, jobsQueryString } from "@/lib/api";

describe("jobsQueryString", () => {
  it("omits empty filters and applies defaults", () => {
    expect(jobsQueryString({})).toBe("limit=50&offset=0");
    expect(jobsQueryString({ status: "", queue: "", limit: 25, offset: 50 })).toBe("limit=25&offset=50");
    expect(jobsQueryString({ status: "failed", queue: "high" })).toBe(
      "status=failed&queue=high&limit=50&offset=0",
    );
  });
});

describe("api client", () => {
  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it("returns parsed JSON on success", async () => {
    const fetchMock = vi.fn().mockResolvedValue(
      new Response(JSON.stringify({ items: [], total: 0, limit: 50, offset: 0 }), { status: 200 }),
    );
    vi.stubGlobal("fetch", fetchMock);
    const list = await api.jobs({ status: "queued" });
    expect(list.total).toBe(0);
    expect(fetchMock.mock.calls[0][0]).toBe("/api/jobs?status=queued&limit=50&offset=0");
  });

  it("raises ApiError with the server detail", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue(
        new Response(JSON.stringify({ detail: "only failed or cancelled jobs can be retried" }), {
          status: 409,
        }),
      ),
    );
    await expect(api.retry("abc")).rejects.toMatchObject({
      status: 409,
      message: "only failed or cancelled jobs can be retried",
    } satisfies Partial<ApiError>);
  });

  it("treats 204 as void", async () => {
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue(new Response(null, { status: 204 })));
    await expect(api.remove("abc")).resolves.toBeUndefined();
  });
});
