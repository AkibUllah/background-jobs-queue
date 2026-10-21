import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";
import { JobDetail } from "@/components/JobDetail";
import { NOW, makeJob } from "./fixtures";

function renderDetail(job = makeJob()) {
  const handlers = {
    onRetry: vi.fn(),
    onRequeue: vi.fn(),
    onDelete: vi.fn(),
    onClose: vi.fn(),
    onSelect: vi.fn(),
  };
  render(<JobDetail job={job} now={NOW} {...handlers} />);
  return handlers;
}

describe("JobDetail", () => {
  it("enables Retry only for dead-letter jobs and shows the traceback", async () => {
    const failed = makeJob({
      status: "failed",
      attempts: 4,
      started_at: NOW - 10,
      finished_at: NOW - 8,
      error: "Traceback (most recent call last):\n  ...\nConnectionError: gateway down",
    });
    const h = renderDetail(failed);
    expect(screen.getByTestId("job-error")).toHaveTextContent("ConnectionError: gateway down");
    expect(screen.getByText("2.00 s")).toBeInTheDocument();
    const retry = screen.getByRole("button", { name: "Retry" });
    expect(retry).toBeEnabled();
    await userEvent.click(retry);
    expect(h.onRetry).toHaveBeenCalledWith(failed.id);
  });

  it("disables Retry and Run now for queued jobs, Delete always works", async () => {
    const h = renderDetail(makeJob({ status: "queued" }));
    expect(screen.getByRole("button", { name: "Retry" })).toBeDisabled();
    expect(screen.getByRole("button", { name: "Run now" })).toBeDisabled();
    await userEvent.click(screen.getByRole("button", { name: "Delete" }));
    expect(h.onDelete).toHaveBeenCalled();
    await userEvent.click(screen.getByRole("button", { name: "Close" }));
    expect(h.onClose).toHaveBeenCalled();
  });

  it("lets Run now move scheduled jobs and lists dependencies", async () => {
    const dep = "d".repeat(32);
    const h = renderDetail(makeJob({ status: "waiting", depends_on: [dep], deps_remaining: 1 }));
    expect(screen.getByRole("button", { name: "Run now" })).toBeEnabled();
    await userEvent.click(screen.getByRole("button", { name: "dddddddd" }));
    expect(h.onSelect).toHaveBeenCalledWith(dep);
    expect(screen.getByText("depends on (1 pending)")).toBeInTheDocument();
  });

  it("renders arguments and result as JSON", () => {
    renderDetail(makeJob({ status: "succeeded", result: { phone: "+880", segments: 1 } }));
    expect(screen.getByText(/"segments": 1/)).toBeInTheDocument();
    expect(screen.getByText(/\+8801711000000/)).toBeInTheDocument();
  });
});
