import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import { StatusBadge } from "@/components/StatusBadge";

describe("StatusBadge", () => {
  it("renders the status text with a tone class", () => {
    render(<StatusBadge status="failed" />);
    const badge = screen.getByTestId("status-badge");
    expect(badge).toHaveTextContent("failed");
    expect(badge.className).toContain("rose");
  });
  it("shows a pulse dot only for running jobs", () => {
    const { container, rerender } = render(<StatusBadge status="running" />);
    expect(container.querySelector(".animate-pulse")).not.toBeNull();
    rerender(<StatusBadge status="queued" />);
    expect(container.querySelector(".animate-pulse")).toBeNull();
  });
});
