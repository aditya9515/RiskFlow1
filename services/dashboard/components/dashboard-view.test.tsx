import { render, screen, within } from "@testing-library/react";
import { describe, expect, it } from "vitest";

import { DashboardErrorState } from "./dashboard-error-state";
import { DashboardView } from "./dashboard-view";
import { dashboardFixture } from "@/test/fixture";

describe("DashboardView", () => {
  it("renders summary, explainability, versions, and recent decisions", () => {
    render(<DashboardView snapshot={dashboardFixture} />);

    expect(
      screen.getByRole("heading", { name: "Decision control room" }),
    ).toBeInTheDocument();
    expect(screen.getByText("730,000 total minor units")).toBeInTheDocument();
    expect(screen.getAllByText("xgb-synthetic-v1").length).toBeGreaterThan(0);
    expect(screen.getByText("extreme amount")).toBeInTheDocument();
    expect(screen.getByText("new device")).toBeInTheDocument();
    expect(screen.getByText("Systems nominal")).toBeInTheDocument();

    const table = screen.getByRole("table");
    expect(within(table).getByText("merchant-alpha")).toBeInTheDocument();
    expect(within(table).getByText("USD minor")).toBeInTheDocument();
    expect(within(table).getByText("BLOCK")).toBeInTheDocument();
  });

  it("shows an explicit empty state without inventing data", () => {
    render(
      <DashboardView
        snapshot={{
          ...dashboardFixture,
          recent_decisions: [],
        }}
      />,
    );

    expect(
      screen.getByRole("heading", { name: "No decisions recorded" }),
    ).toBeInTheDocument();
    expect(screen.queryByRole("table")).not.toBeInTheDocument();
  });

  it("surfaces processing and reconciliation exceptions", () => {
    render(
      <DashboardView
        snapshot={{
          ...dashboardFixture,
          processing: {
            ...dashboardFixture.processing,
            outbox_pending: 2,
            outbox_retrying: 1,
          },
          reconciliation: {
            ...dashboardFixture.reconciliation,
            exception_count: 1,
            by_code: { MISSING_DECISION: 1 },
          },
        }}
      />,
    );

    expect(screen.getByText("Attention required")).toBeInTheDocument();
    expect(screen.getByText("missing decision · 1")).toBeInTheDocument();
    const controlMetric = screen
      .getByText("Control exceptions")
      .closest("article");
    expect(controlMetric).not.toBeNull();
    expect(
      within(controlMetric as HTMLElement).getByText("3"),
    ).toBeInTheDocument();
  });
});

describe("DashboardErrorState", () => {
  it("renders a safe recoverable error", () => {
    render(
      <DashboardErrorState message="The payment API is currently unreachable." />,
    );

    expect(
      screen.getByRole("heading", { name: "Operational view unavailable" }),
    ).toBeInTheDocument();
    expect(
      screen.getByText("The payment API is currently unreachable."),
    ).toBeInTheDocument();
    expect(
      screen.getByRole("link", { name: "Retry dashboard" }),
    ).toHaveAttribute("href", "/");
  });
});
