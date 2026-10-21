import { render, screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";
import { JobsTable } from "@/components/JobsTable";
import { NOW, makeJob } from "./fixtures";

const jobs = [
  makeJob({ id: "a".repeat(32), status: "running", attempts: 1 }),
  makeJob({ id: "b".repeat(32), status: "retrying", attempts: 2, run_at: NOW + 30, error: "boom" }),
  makeJob({ id: "c".repeat(32), status: "succeeded", finished_at: NOW - 5, schedule: "otp-smoke" }),
];

function renderTable(extra: Partial<React.ComponentProps<typeof JobsTable>> = {}) {
  const props: React.ComponentProps<typeof JobsTable> = {
    jobs,
    total: 60,
    status: "",
    onStatusChange: vi.fn(),
    offset: 25,
    limit: 25,
    onPage: vi.fn(),
    selectedId: null,
    onSelect: vi.fn(),
    now: NOW,
    ...extra,
  };
  render(<JobsTable {...props} />);
  return props;
}

describe("JobsTable", () => {
  it("renders one row per job with short ids, badges and schedule tag", () => {
    renderTable();
    const rows = screen.getAllByTestId("job-row");
    expect(rows).toHaveLength(3);
    expect(within(rows[0]).getByText("aaaaaaaa")).toBeInTheDocument();
    expect(within(rows[1]).getByTestId("status-badge")).toHaveTextContent("retrying");
    expect(within(rows[1]).getByText("in 30s")).toBeInTheDocument();
    expect(within(rows[2]).getByText("otp-smoke")).toBeInTheDocument();
    expect(within(rows[0]).getByText("1/4")).toBeInTheDocument();
  });

  it("paginates and filters", async () => {
    const props = renderTable();
    expect(screen.getByTestId("pagination-summary")).toHaveTextContent("26–50 of 60");
    await userEvent.click(screen.getByRole("button", { name: "next" }));
    expect(props.onPage).toHaveBeenCalledWith(50);
    await userEvent.click(screen.getByRole("button", { name: "prev" }));
    expect(props.onPage).toHaveBeenCalledWith(0);
    await userEvent.click(screen.getByRole("tab", { name: "failed" }));
    expect(props.onStatusChange).toHaveBeenCalledWith("failed");
    await userEvent.click(screen.getAllByTestId("job-row")[1]);
    expect(props.onSelect).toHaveBeenCalledWith("b".repeat(32));
  });

  it("shows an empty state", () => {
    renderTable({ jobs: [], total: 0, offset: 0 });
    expect(screen.getByText("No jobs match this filter.")).toBeInTheDocument();
    expect(screen.getByTestId("pagination-summary")).toHaveTextContent("0–0 of 0");
  });
});
