package payment

import (
	"encoding/json"
	"time"
)

const (
	// StatusPendingRisk is the initial state while a payment awaits risk decisioning.
	StatusPendingRisk = "PENDING_RISK"
	PaymentCreated    = "payments.created"
	SchemaVersion     = 1
)

// CreateRequest contains the client-supplied fields used to create a payment.
type CreateRequest struct {
	CustomerID  string `json:"customer_id"`
	MerchantID  string `json:"merchant_id"`
	DeviceID    string `json:"device_id"`
	AmountMinor int64  `json:"amount_minor"`
	Currency    string `json:"currency"`
	Country     string `json:"country"`
}

// Payment is the persisted payment domain object.
type Payment struct {
	ID                 string
	IdempotencyKey     string
	RequestFingerprint string
	CustomerID         string
	MerchantID         string
	DeviceID           string
	AmountMinor        int64
	Currency           string
	Country            string
	Status             string
	CreatedAt          time.Time
	UpdatedAt          time.Time
}

// OutboxEvent is written atomically with a newly created payment.
type OutboxEvent struct {
	ID            string
	AggregateID   string
	EventType     string
	SchemaVersion int
	OccurredAt    time.Time
	TraceID       string
	Payload       json.RawMessage
}

// CreateResult distinguishes a new payment from an idempotent replay.
type CreateResult struct {
	Payment  Payment
	Replayed bool
}
