import { describe, expect, it, vi } from "vitest";

import { DashboardLoadError, loadDashboardSnapshot } from "./dashboard";
import { dashboardFixture } from "@/test/fixture";

const environment = {
  DASHBOARD_API_URL: "http://payment-api:8080",
  DASHBOARD_API_TOKEN: "auditor-token-kept-on-the-server-only",
  DASHBOARD_RECENT_LIMIT: "12",
  DASHBOARD_FETCH_TIMEOUT_MS: "5000",
};

describe("loadDashboardSnapshot", () => {
  it("requests a no-store authenticated snapshot with the configured limit", async () => {
    const fetcher = vi.fn<typeof fetch>().mockResolvedValue(
      new Response(JSON.stringify(dashboardFixture), {
        status: 200,
        headers: { "Content-Type": "application/json" },
      }),
    );

    await expect(loadDashboardSnapshot(environment, fetcher)).resolves.toEqual(
      dashboardFixture,
    );

    expect(fetcher).toHaveBeenCalledOnce();
    const [url, options] = fetcher.mock.calls[0];
    expect(url.toString()).toBe(
      "http://payment-api:8080/v1/dashboard?recent_limit=12",
    );
    expect(options?.cache).toBe("no-store");
    expect(options?.headers).toMatchObject({
      Accept: "application/json",
      Authorization: `Bearer ${environment.DASHBOARD_API_TOKEN}`,
    });
  });

  it("rejects missing server-side credentials before making a request", async () => {
    const fetcher = vi.fn<typeof fetch>();

    await expect(
      loadDashboardSnapshot(
        { DASHBOARD_API_URL: environment.DASHBOARD_API_URL },
        fetcher,
      ),
    ).rejects.toEqual(
      expect.objectContaining({
        userMessage:
          "DASHBOARD_API_TOKEN is not configured on the dashboard server.",
      }),
    );
    expect(fetcher).not.toHaveBeenCalled();
  });

  it("maps rejected credentials to a safe operator message", async () => {
    const fetcher = vi
      .fn<typeof fetch>()
      .mockResolvedValue(new Response("secret details", { status: 401 }));

    await expect(loadDashboardSnapshot(environment, fetcher)).rejects.toEqual(
      expect.objectContaining({
        userMessage:
          "The dashboard credentials were rejected by the payment API.",
      }),
    );
  });

  it("rejects a malformed API response instead of rendering partial counts", async () => {
    const malformed = structuredClone(dashboardFixture);
    malformed.payments.total = -1;
    const fetcher = vi.fn<typeof fetch>().mockResolvedValue(
      new Response(JSON.stringify(malformed), {
        status: 200,
        headers: { "Content-Type": "application/json" },
      }),
    );

    await expect(
      loadDashboardSnapshot(environment, fetcher),
    ).rejects.toBeInstanceOf(DashboardLoadError);
  });

  it("validates bounded integer configuration", async () => {
    const fetcher = vi.fn<typeof fetch>();

    await expect(
      loadDashboardSnapshot(
        { ...environment, DASHBOARD_RECENT_LIMIT: "101" },
        fetcher,
      ),
    ).rejects.toEqual(
      expect.objectContaining({
        userMessage:
          "DASHBOARD_RECENT_LIMIT must be an integer between 1 and 100.",
      }),
    );
    expect(fetcher).not.toHaveBeenCalled();
  });
});
