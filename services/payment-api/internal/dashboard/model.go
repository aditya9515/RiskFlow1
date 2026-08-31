package dashboard

import "time"

// Snapshot is the bounded read model consumed by the operational dashboard.
type Snapshot struct {
	GeneratedAt     time.Time             `json:"generated_at"`
	Payments        PaymentSummary        `json:"payments"`
	Decisions       DecisionSummary       `json:"decisions"`
	ManualReview    ManualReviewSummary   `json:"manual_review"`
	Processing      ProcessingSummary     `json:"processing"`
	Reconciliation  ReconciliationSummary `json:"reconciliation"`
	RecentDecisions []RecentDecision      `json:"recent_decisions"`
}

// PaymentSummary contains additive payment totals and every valid payment state.
type PaymentSummary struct {
	Total            int64               `json:"total"`
	AmountMinorTotal int64               `json:"amount_minor_total"`
	ByStatus         PaymentStatusCounts `json:"by_status"`
}

type PaymentStatusCounts struct {
	PendingRisk int64 `json:"pending_risk"`
	Allowed     int64 `json:"allowed"`
	Review      int64 `json:"review"`
	Blocked     int64 `json:"blocked"`
	Failed      int64 `json:"failed"`
}

// DecisionSummary reports automated outcomes and the most recently used versions.
type DecisionSummary struct {
	Total              int64                 `json:"total"`
	ByOutcome          DecisionOutcomeCounts `json:"by_outcome"`
	AverageRiskScore   float64               `json:"average_risk_score"`
	LatestRuleVersion  string                `json:"latest_rule_version,omitempty"`
	LatestModelVersion string                `json:"latest_model_version,omitempty"`
	LatestDecisionAt   *time.Time            `json:"latest_decision_at,omitempty"`
}

type DecisionOutcomeCounts struct {
	Allow  int64 `json:"allow"`
	Review int64 `json:"review"`
	Block  int64 `json:"block"`
}

type ManualReviewSummary struct {
	Pending int64 `json:"pending"`
}

// ProcessingSummary distinguishes backlog, retries, and terminal failures.
type ProcessingSummary struct {
	OutboxPending          int64 `json:"outbox_pending"`
	OutboxRetrying         int64 `json:"outbox_retrying"`
	OutboxDeadLettered     int64 `json:"outbox_dead_lettered"`
	DecisionEventsRejected int64 `json:"decision_events_rejected"`
}

type ReconciliationSummary struct {
	GracePeriod    string           `json:"grace_period"`
	ExceptionCount int64            `json:"exception_count"`
	ByCode         map[string]int64 `json:"by_code"`
}

// RecentDecision contains the evidence needed for an operator to understand a decision.
type RecentDecision struct {
	DecisionID    string    `json:"decision_id"`
	PaymentID     string    `json:"payment_id"`
	CustomerID    string    `json:"customer_id"`
	MerchantID    string    `json:"merchant_id"`
	AmountMinor   int64     `json:"amount_minor"`
	Currency      string    `json:"currency"`
	Country       string    `json:"country"`
	PaymentStatus string    `json:"payment_status"`
	Decision      string    `json:"decision"`
	RiskScore     int       `json:"risk_score"`
	ReasonCodes   []string  `json:"reason_codes"`
	RuleVersion   string    `json:"rule_version"`
	ModelVersion  string    `json:"model_version"`
	DecisionAt    time.Time `json:"decision_at"`
}
