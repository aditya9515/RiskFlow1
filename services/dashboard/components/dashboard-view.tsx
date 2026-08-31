import type { CSSProperties, ReactNode } from "react";

import type { DashboardSnapshot, RecentDecision } from "@/lib/dashboard";

const countFormatter = new Intl.NumberFormat("en-US");
const scoreFormatter = new Intl.NumberFormat("en-US", {
  minimumFractionDigits: 1,
  maximumFractionDigits: 1,
});
const timestampFormatter = new Intl.DateTimeFormat("en-GB", {
  dateStyle: "medium",
  timeStyle: "short",
  timeZone: "UTC",
});

export function DashboardView({ snapshot }: { snapshot: DashboardSnapshot }) {
  const processingAttention =
    snapshot.processing.outbox_pending +
    snapshot.processing.outbox_dead_lettered +
    snapshot.processing.decision_events_rejected +
    snapshot.reconciliation.exception_count;
  const pipelinesNominal = processingAttention === 0;

  return (
    <main className="dashboard-shell">
      <header className="topbar">
        <div className="brand">
          <div className="brand-mark" aria-hidden="true">
            RF
          </div>
          <div>
            <p className="brand-name">RiskFlow</p>
            <p className="brand-subtitle">Payment risk operations</p>
          </div>
        </div>
        <div
          className={`system-state ${pipelinesNominal ? "nominal" : "attention"}`}
        >
          <span className="pulse-dot" aria-hidden="true" />
          {pipelinesNominal ? "Systems nominal" : "Attention required"}
        </div>
      </header>

      <div className="content">
        <section className="page-heading" aria-labelledby="dashboard-title">
          <div>
            <p className="eyebrow">Live operational snapshot</p>
            <h1 id="dashboard-title">Decision control room</h1>
            <p className="heading-copy">
              Payment flow, automated decisions, and control exceptions in one
              accountable view.
            </p>
          </div>
          <div className="snapshot-time">
            <span>Snapshot generated</span>
            <time dateTime={snapshot.generated_at}>
              {formatTimestamp(snapshot.generated_at)}
            </time>
            <small>UTC · refresh page for latest</small>
          </div>
        </section>

        <section className="metric-grid" aria-label="Operational summary">
          <MetricCard
            accent="blue"
            eyebrow="Payment volume"
            value={formatCount(snapshot.payments.total)}
            detail={`${formatCount(snapshot.payments.amount_minor_total)} total minor units`}
            icon={<CardIcon />}
          />
          <MetricCard
            accent="teal"
            eyebrow="Automated decisions"
            value={formatCount(snapshot.decisions.total)}
            detail={`${scoreFormatter.format(snapshot.decisions.average_risk_score)} average risk score`}
            icon={<SignalIcon />}
          />
          <MetricCard
            accent={snapshot.manual_review.pending > 0 ? "amber" : "teal"}
            eyebrow="Manual review queue"
            value={formatCount(snapshot.manual_review.pending)}
            detail={
              snapshot.manual_review.pending === 1
                ? "payment awaiting action"
                : "payments awaiting action"
            }
            icon={<ReviewIcon />}
          />
          <MetricCard
            accent={pipelinesNominal ? "teal" : "red"}
            eyebrow="Control exceptions"
            value={formatCount(processingAttention)}
            detail={
              pipelinesNominal
                ? "no processing or reconciliation breaks"
                : "items require investigation"
            }
            icon={<ShieldIcon />}
          />
        </section>

        <section className="dashboard-grid">
          <article className="panel decision-panel">
            <div className="panel-heading">
              <div>
                <p className="eyebrow">Decision mix</p>
                <h2>Automated outcomes</h2>
              </div>
              <span className="score-badge">
                Avg score{" "}
                {scoreFormatter.format(snapshot.decisions.average_risk_score)}
              </span>
            </div>
            <div className="outcome-list">
              <OutcomeBar
                label="Allow"
                count={snapshot.decisions.by_outcome.allow}
                total={snapshot.decisions.total}
                tone="allow"
              />
              <OutcomeBar
                label="Review"
                count={snapshot.decisions.by_outcome.review}
                total={snapshot.decisions.total}
                tone="review"
              />
              <OutcomeBar
                label="Block"
                count={snapshot.decisions.by_outcome.block}
                total={snapshot.decisions.total}
                tone="block"
              />
            </div>
            <div className="version-strip">
              <VersionItem
                label="Rules"
                value={
                  snapshot.decisions.latest_rule_version ?? "No decision yet"
                }
              />
              <VersionItem
                label="Model"
                value={
                  snapshot.decisions.latest_model_version ?? "No decision yet"
                }
              />
              <VersionItem
                label="Latest decision"
                value={
                  snapshot.decisions.latest_decision_at
                    ? formatTimestamp(snapshot.decisions.latest_decision_at)
                    : "No decision yet"
                }
              />
            </div>
          </article>

          <article className="panel controls-panel">
            <div className="panel-heading">
              <div>
                <p className="eyebrow">Reliability controls</p>
                <h2>Processing health</h2>
              </div>
              <span
                className={`health-badge ${pipelinesNominal ? "healthy" : "warning"}`}
              >
                {pipelinesNominal ? "Healthy" : "Investigate"}
              </span>
            </div>
            <dl className="control-list">
              <ControlRow
                label="Outbox awaiting publication"
                value={snapshot.processing.outbox_pending}
              />
              <ControlRow
                label="Outbox currently retrying"
                value={snapshot.processing.outbox_retrying}
              />
              <ControlRow
                label="Dead-lettered outbox events"
                value={snapshot.processing.outbox_dead_lettered}
              />
              <ControlRow
                label="Rejected decision events"
                value={snapshot.processing.decision_events_rejected}
              />
              <ControlRow
                label="Reconciliation exceptions"
                value={snapshot.reconciliation.exception_count}
              />
            </dl>
            <p className="control-footnote">
              Reconciliation grace period:{" "}
              {snapshot.reconciliation.grace_period}
            </p>
            {snapshot.reconciliation.exception_count > 0 && (
              <div
                className="exception-codes"
                aria-label="Reconciliation exceptions by code"
              >
                {Object.entries(snapshot.reconciliation.by_code).map(
                  ([code, count]) => (
                    <span key={code}>
                      {humanizeCode(code)} · {formatCount(count)}
                    </span>
                  ),
                )}
              </div>
            )}
          </article>
        </section>

        <section
          className="panel recent-panel"
          aria-labelledby="recent-decisions-title"
        >
          <div className="panel-heading recent-heading">
            <div>
              <p className="eyebrow">Traceable evidence</p>
              <h2 id="recent-decisions-title">Recent risk decisions</h2>
            </div>
            <span className="record-count">
              {formatCount(snapshot.recent_decisions.length)} records
            </span>
          </div>

          {snapshot.recent_decisions.length === 0 ? (
            <div className="empty-state">
              <SignalIcon />
              <h3>No decisions recorded</h3>
              <p>
                New risk decisions will appear here after they are persisted.
              </p>
            </div>
          ) : (
            <div className="table-scroll">
              <table>
                <thead>
                  <tr>
                    <th scope="col">Payment</th>
                    <th scope="col">Merchant / customer</th>
                    <th scope="col">Amount</th>
                    <th scope="col">Decision</th>
                    <th scope="col">Risk</th>
                    <th scope="col">Reason codes</th>
                    <th scope="col">Decided</th>
                  </tr>
                </thead>
                <tbody>
                  {snapshot.recent_decisions.map((decision) => (
                    <DecisionRow
                      key={decision.decision_id}
                      decision={decision}
                    />
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </section>

        <footer>
          <span>Operational source: PostgreSQL</span>
          <span>Amounts remain integer minor units</span>
          <span>RiskFlow · explainable decisions by design</span>
        </footer>
      </div>
    </main>
  );
}

function MetricCard({
  accent,
  eyebrow,
  value,
  detail,
  icon,
}: {
  accent: "blue" | "teal" | "amber" | "red";
  eyebrow: string;
  value: string;
  detail: string;
  icon: ReactNode;
}) {
  return (
    <article className={`metric-card accent-${accent}`}>
      <div className="metric-icon" aria-hidden="true">
        {icon}
      </div>
      <p>{eyebrow}</p>
      <strong>{value}</strong>
      <span>{detail}</span>
    </article>
  );
}

function OutcomeBar({
  label,
  count,
  total,
  tone,
}: {
  label: string;
  count: number;
  total: number;
  tone: "allow" | "review" | "block";
}) {
  const percentage = total === 0 ? 0 : (count / total) * 100;
  const displayPercentage = Math.round(percentage);
  const style = { "--share": `${percentage}%` } as CSSProperties;
  return (
    <div className="outcome-row">
      <div className="outcome-label">
        <span>{label}</span>
        <strong>
          {formatCount(count)} <small>{displayPercentage}%</small>
        </strong>
      </div>
      <div
        className="outcome-track"
        role="progressbar"
        aria-label={`${label} decisions`}
        aria-valuemin={0}
        aria-valuemax={total}
        aria-valuenow={count}
      >
        <span className={`outcome-fill ${tone}`} style={style} />
      </div>
    </div>
  );
}

function VersionItem({ label, value }: { label: string; value: string }) {
  return (
    <div>
      <span>{label}</span>
      <strong>{value}</strong>
    </div>
  );
}

function ControlRow({ label, value }: { label: string; value: number }) {
  return (
    <div>
      <dt>{label}</dt>
      <dd className={value > 0 ? "nonzero" : "zero"}>{formatCount(value)}</dd>
    </div>
  );
}

function DecisionRow({ decision }: { decision: RecentDecision }) {
  return (
    <tr>
      <td>
        <span className="mono id-value" title={decision.payment_id}>
          {shortID(decision.payment_id)}
        </span>
        <small>{decision.country}</small>
      </td>
      <td>
        <span>{decision.merchant_id}</span>
        <small>{decision.customer_id}</small>
      </td>
      <td>
        <span className="amount">{formatCount(decision.amount_minor)}</span>
        <small>{decision.currency} minor</small>
      </td>
      <td>
        <span className={`decision-pill ${decision.decision.toLowerCase()}`}>
          {decision.decision}
        </span>
        <small>{humanizeCode(decision.payment_status)}</small>
      </td>
      <td>
        <span className={`risk-score ${riskTone(decision.risk_score)}`}>
          {decision.risk_score}
        </span>
        <small>of 100</small>
      </td>
      <td>
        <div className="reason-list">
          {decision.reason_codes.map((reason) => (
            <span key={reason}>{humanizeCode(reason)}</span>
          ))}
        </div>
      </td>
      <td>
        <time dateTime={decision.decision_at}>
          {formatTimestamp(decision.decision_at)}
        </time>
        <small>{decision.model_version}</small>
      </td>
    </tr>
  );
}

function formatCount(value: number): string {
  return countFormatter.format(value);
}

function formatTimestamp(value: string): string {
  return timestampFormatter.format(new Date(value));
}

function shortID(value: string): string {
  return value.length > 12 ? `${value.slice(0, 8)}…${value.slice(-4)}` : value;
}

function humanizeCode(value: string): string {
  return value.toLowerCase().replaceAll("_", " ");
}

function riskTone(score: number): string {
  if (score >= 70) return "high";
  if (score >= 40) return "medium";
  return "low";
}

function CardIcon() {
  return (
    <svg viewBox="0 0 24 24" aria-hidden="true">
      <rect x="3" y="5" width="18" height="14" rx="3" />
      <path d="M3 10h18M7 15h4" />
    </svg>
  );
}

function SignalIcon() {
  return (
    <svg viewBox="0 0 24 24" aria-hidden="true">
      <path d="M5 18V9m7 9V5m7 13v-6" />
    </svg>
  );
}

function ReviewIcon() {
  return (
    <svg viewBox="0 0 24 24" aria-hidden="true">
      <path d="M8 3h8l1 3h2v15H5V6h2l1-3Z" />
      <path d="M9 13l2 2 4-5" />
    </svg>
  );
}

function ShieldIcon() {
  return (
    <svg viewBox="0 0 24 24" aria-hidden="true">
      <path d="M12 3 4 6v5c0 5 3.4 8.6 8 10 4.6-1.4 8-5 8-10V6l-8-3Z" />
      <path d="m9 12 2 2 4-5" />
    </svg>
  );
}
