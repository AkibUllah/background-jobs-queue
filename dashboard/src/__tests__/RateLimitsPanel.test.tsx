import { render, screen, within } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import { RateLimitsPanel } from "@/components/RateLimitsPanel";
import type { RateLimitInfo } from "@/lib/types";

const sms: RateLimitInfo = {
  task: "examples.demo_app.send_otp_sms",
  spec: "10/s",
  limit: 10,
  period: 1,
  tokens: 0.4,
  next_token_in: 0.06,
  throttled: 1234,
};
const partner: RateLimitInfo = {
  task: "call_partner",
  spec: "600/m",
  limit: 600,
  period: 60,
  tokens: 600,
  next_token_in: 0,
  throttled: 0,
};

describe("RateLimitsPanel", () => {
  it("renders nothing when no task is rate limited", () => {
    const { container } = render(<RateLimitsPanel limits={[]} />);
    expect(container).toBeEmptyDOMElement();
  });

  it("shows tokens, time to the next token and throttled counts", () => {
    render(<RateLimitsPanel limits={[sms, partner]} />);
    const row = within(screen.getByTestId("rate-limit-examples.demo_app.send_otp_sms"));
    expect(row.getByText("send_otp_sms")).toBeInTheDocument();
    expect(row.getByText("10/s")).toBeInTheDocument();
    expect(row.getByText("0.4/10")).toBeInTheDocument();
    expect(row.getByText("60 ms")).toBeInTheDocument();
    expect(row.getByText("1,234")).toBeInTheDocument();
    expect(row.getByRole("meter").firstElementChild).toHaveClass("bg-amber-400");

    const full = within(screen.getByTestId("rate-limit-call_partner"));
    expect(full.getByText("ready")).toBeInTheDocument();
    expect(full.getByRole("meter")).toHaveAttribute("aria-valuenow", "600");
    expect(full.getByRole("meter").firstElementChild).toHaveStyle({ width: "100%" });
  });
});
