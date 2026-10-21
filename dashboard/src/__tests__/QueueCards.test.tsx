import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";
import { QueueCards } from "@/components/QueueCards";
import { overview } from "./fixtures";

describe("QueueCards", () => {
  it("shows counts and queue buttons", async () => {
    const onSelect = vi.fn();
    render(<QueueCards overview={overview} selectedQueue="" onSelectQueue={onSelect} />);
    expect(screen.getByTestId("stat-succeeded")).toHaveTextContent("1,200");
    expect(screen.getByTestId("stat-dead")).toHaveTextContent("7");
    expect(screen.getByText(/2 workers · 3 schedules · 2 tasks/)).toBeInTheDocument();
    await userEvent.click(screen.getByTestId("queue-high"));
    expect(onSelect).toHaveBeenCalledWith("high");
  });
});
