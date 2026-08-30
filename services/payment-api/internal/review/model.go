package review

import (
	"errors"
	"fmt"
	"time"
)

const (
	StatusPending  = "PENDING"
	StatusApproved = "APPROVED"
	StatusRejected = "REJECTED"

	ActionApprove = "APPROVE"
	ActionReject  = "REJECT"
)

var (
	ErrReviewIDInvalid       = errors.New("review payment ID is invalid")
	ErrReviewNotFound        = errors.New("manual review not found")
	ErrReviewAlreadyResolved = errors.New("manual review is already resolved")
	ErrReviewVersionConflict = errors.New("manual review version conflict")
	ErrReviewStateConflict   = errors.New("manual review payment state conflict")
)

// Item is one manual-review work item and its decision evidence.
type Item struct {
	PaymentID        string
	DecisionID       string
	CustomerID       string
	MerchantID       string
	AmountMinor      int64
	Currency         string
	Country          string
	PaymentStatus    string
	ReviewStatus     string
	Version          int
	RiskScore        int
	ReasonCodes      []string
	RuleVersion      string
	ModelVersion     string
	EnqueuedAt       time.Time
	ResolvedAt       *time.Time
	ReviewerID       string
	ResolutionReason string
}

// ResolveRequest is validated by Service before it reaches PostgreSQL.
type ResolveRequest struct {
	ExpectedVersion int    `json:"expected_version"`
	ReasonCode      string `json:"reason_code"`
}

// ResolveCommand contains normalized action data and generated audit identity.
type ResolveCommand struct {
	PaymentID       string
	Action          string
	ExpectedVersion int
	ReasonCode      string
	Principal       Principal
	AuditID         string
	ResolvedAt      time.Time
}

// ValidationError reports action request field failures.
type ValidationError struct {
	Fields map[string]string
}

func (e *ValidationError) Error() string {
	return fmt.Sprintf("review action validation failed for %d field(s)", len(e.Fields))
}

// ConflictError adds the current state to a typed optimistic-lock conflict.
type ConflictError struct {
	Cause          error
	CurrentStatus  string
	CurrentVersion int
}

func (e *ConflictError) Error() string {
	return fmt.Sprintf("%v: current status=%s version=%d", e.Cause, e.CurrentStatus, e.CurrentVersion)
}

func (e *ConflictError) Unwrap() error { return e.Cause }
