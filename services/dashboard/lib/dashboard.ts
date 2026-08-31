import "server-only";

export type PaymentStatusCounts = {
  pending_risk: number;
  allowed: number;
  review: number;
  blocked: number;
  failed: number;
};

export type DashboardSnapshot = {
  generated_at: string;
  payments: {
    total: number;
    amount_minor_total: number;
    by_status: PaymentStatusCounts;
  };
  decisions: {
    total: number;
    by_outcome: {
      allow: number;
      review: number;
      block: number;
    };
    average_risk_score: number;
    latest_rule_version?: string;
    latest_model_version?: string;
    latest_decision_at?: string;
  };
  manual_review: {
    pending: number;
  };
  processing: {
    outbox_pending: number;
    outbox_retrying: number;
    outbox_dead_lettered: number;
    decision_events_rejected: number;
  };
  reconciliation: {
    grace_period: string;
    exception_count: number;
    by_code: Record<string, number>;
  };
  recent_decisions: RecentDecision[];
};

export type RecentDecision = {
  decision_id: string;
  payment_id: string;
  customer_id: string;
  merchant_id: string;
  amount_minor: number;
  currency: string;
  country: string;
  payment_status: string;
  decision: "ALLOW" | "REVIEW" | "BLOCK";
  risk_score: number;
  reason_codes: string[];
  rule_version: string;
  model_version: string;
  decision_at: string;
};

type DashboardEnvironment = {
  DASHBOARD_API_URL?: string;
  DASHBOARD_API_TOKEN?: string;
  DASHBOARD_RECENT_LIMIT?: string;
  DASHBOARD_FETCH_TIMEOUT_MS?: string;
};

export class DashboardLoadError extends Error {
  constructor(public readonly userMessage: string) {
    super(userMessage);
    this.name = "DashboardLoadError";
  }
}

export async function loadDashboardSnapshot(
  environment: DashboardEnvironment = {
    DASHBOARD_API_URL: process.env.DASHBOARD_API_URL,
    DASHBOARD_API_TOKEN: process.env.DASHBOARD_API_TOKEN,
    DASHBOARD_RECENT_LIMIT: process.env.DASHBOARD_RECENT_LIMIT,
    DASHBOARD_FETCH_TIMEOUT_MS: process.env.DASHBOARD_FETCH_TIMEOUT_MS,
  },
  fetcher: typeof fetch = fetch,
): Promise<DashboardSnapshot> {
  const config = loadDashboardConfig(environment);
  const url = new URL("/v1/dashboard", config.apiURL);
  url.searchParams.set("recent_limit", String(config.recentLimit));

  let response: Response;
  try {
    response = await fetcher(url, {
      headers: {
        Accept: "application/json",
        Authorization: `Bearer ${config.apiToken}`,
      },
      cache: "no-store",
      signal: AbortSignal.timeout(config.fetchTimeoutMS),
    });
  } catch (error) {
    if (isAbortError(error)) {
      throw new DashboardLoadError(
        "The operational snapshot timed out. Try again shortly.",
      );
    }
    throw new DashboardLoadError("The payment API is currently unreachable.");
  }

  if (!response.ok) {
    if (response.status === 401 || response.status === 403) {
      throw new DashboardLoadError(
        "The dashboard credentials were rejected by the payment API.",
      );
    }
    if (response.status === 504) {
      throw new DashboardLoadError(
        "The operational snapshot timed out. Try again shortly.",
      );
    }
    throw new DashboardLoadError(
      "Operational data is temporarily unavailable.",
    );
  }

  let body: unknown;
  try {
    body = await response.json();
  } catch {
    throw new DashboardLoadError(
      "The payment API returned an unreadable response.",
    );
  }

  if (!isDashboardSnapshot(body)) {
    throw new DashboardLoadError(
      "The payment API returned an unexpected dashboard contract.",
    );
  }
  return body;
}

function loadDashboardConfig(environment: DashboardEnvironment) {
  const rawURL = environment.DASHBOARD_API_URL?.trim();
  if (!rawURL) {
    throw new DashboardLoadError(
      "DASHBOARD_API_URL is not configured on the dashboard server.",
    );
  }

  let apiURL: URL;
  try {
    apiURL = new URL(rawURL);
  } catch {
    throw new DashboardLoadError("DASHBOARD_API_URL is not a valid URL.");
  }
  if (apiURL.protocol !== "http:" && apiURL.protocol !== "https:") {
    throw new DashboardLoadError("DASHBOARD_API_URL must use HTTP or HTTPS.");
  }

  const apiToken = environment.DASHBOARD_API_TOKEN?.trim();
  if (!apiToken) {
    throw new DashboardLoadError(
      "DASHBOARD_API_TOKEN is not configured on the dashboard server.",
    );
  }

  return {
    apiURL,
    apiToken,
    recentLimit: parseBoundedInteger(
      environment.DASHBOARD_RECENT_LIMIT,
      20,
      1,
      100,
      "DASHBOARD_RECENT_LIMIT",
    ),
    fetchTimeoutMS: parseBoundedInteger(
      environment.DASHBOARD_FETCH_TIMEOUT_MS,
      7000,
      100,
      30000,
      "DASHBOARD_FETCH_TIMEOUT_MS",
    ),
  };
}

function parseBoundedInteger(
  raw: string | undefined,
  fallback: number,
  minimum: number,
  maximum: number,
  name: string,
) {
  if (raw === undefined || raw.trim() === "") {
    return fallback;
  }
  if (!/^\d+$/.test(raw.trim())) {
    throw new DashboardLoadError(
      `${name} must be an integer between ${minimum} and ${maximum}.`,
    );
  }
  const value = Number(raw);
  if (!Number.isSafeInteger(value) || value < minimum || value > maximum) {
    throw new DashboardLoadError(
      `${name} must be an integer between ${minimum} and ${maximum}.`,
    );
  }
  return value;
}

function isAbortError(error: unknown): boolean {
  return (
    error instanceof Error &&
    (error.name === "AbortError" || error.name === "TimeoutError")
  );
}

function isDashboardSnapshot(value: unknown): value is DashboardSnapshot {
  if (!isRecord(value) || !isTimestamp(value.generated_at)) return false;
  if (
    !isRecord(value.payments) ||
    !isCount(value.payments.total) ||
    !isCount(value.payments.amount_minor_total)
  )
    return false;
  if (!isPaymentStatuses(value.payments.by_status)) return false;
  if (!isDecisionSummary(value.decisions)) return false;
  if (!isRecord(value.manual_review) || !isCount(value.manual_review.pending))
    return false;
  if (!isProcessingSummary(value.processing)) return false;
  if (!isReconciliationSummary(value.reconciliation)) return false;
  return (
    Array.isArray(value.recent_decisions) &&
    value.recent_decisions.every(isRecentDecision)
  );
}

function isPaymentStatuses(value: unknown): boolean {
  return (
    isRecord(value) &&
    isCount(value.pending_risk) &&
    isCount(value.allowed) &&
    isCount(value.review) &&
    isCount(value.blocked) &&
    isCount(value.failed)
  );
}

function isDecisionSummary(value: unknown): boolean {
  return (
    isRecord(value) &&
    isCount(value.total) &&
    isRecord(value.by_outcome) &&
    isCount(value.by_outcome.allow) &&
    isCount(value.by_outcome.review) &&
    isCount(value.by_outcome.block) &&
    isFiniteNumber(value.average_risk_score) &&
    isOptionalString(value.latest_rule_version) &&
    isOptionalString(value.latest_model_version) &&
    (value.latest_decision_at === undefined ||
      isTimestamp(value.latest_decision_at))
  );
}

function isProcessingSummary(value: unknown): boolean {
  return (
    isRecord(value) &&
    isCount(value.outbox_pending) &&
    isCount(value.outbox_retrying) &&
    isCount(value.outbox_dead_lettered) &&
    isCount(value.decision_events_rejected)
  );
}

function isReconciliationSummary(value: unknown): boolean {
  return (
    isRecord(value) &&
    typeof value.grace_period === "string" &&
    isCount(value.exception_count) &&
    isRecord(value.by_code) &&
    Object.values(value.by_code).every(isCount)
  );
}

function isRecentDecision(value: unknown): value is RecentDecision {
  return (
    isRecord(value) &&
    isNonEmptyString(value.decision_id) &&
    isNonEmptyString(value.payment_id) &&
    isNonEmptyString(value.customer_id) &&
    isNonEmptyString(value.merchant_id) &&
    isCount(value.amount_minor) &&
    isNonEmptyString(value.currency) &&
    isNonEmptyString(value.country) &&
    isNonEmptyString(value.payment_status) &&
    (value.decision === "ALLOW" ||
      value.decision === "REVIEW" ||
      value.decision === "BLOCK") &&
    isFiniteNumber(value.risk_score) &&
    value.risk_score >= 0 &&
    value.risk_score <= 100 &&
    Array.isArray(value.reason_codes) &&
    value.reason_codes.every(isNonEmptyString) &&
    isNonEmptyString(value.rule_version) &&
    isNonEmptyString(value.model_version) &&
    isTimestamp(value.decision_at)
  );
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function isFiniteNumber(value: unknown): value is number {
  return typeof value === "number" && Number.isFinite(value);
}

function isCount(value: unknown): value is number {
  return isFiniteNumber(value) && Number.isSafeInteger(value) && value >= 0;
}

function isOptionalString(value: unknown): boolean {
  return value === undefined || typeof value === "string";
}

function isNonEmptyString(value: unknown): value is string {
  return typeof value === "string" && value.length > 0;
}

function isTimestamp(value: unknown): value is string {
  return typeof value === "string" && !Number.isNaN(Date.parse(value));
}
