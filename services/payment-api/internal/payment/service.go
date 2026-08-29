package payment

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"time"
)

// Repository persists and retrieves payments.
type Repository interface {
	Create(context.Context, Payment, OutboxEvent) (CreateResult, error)
	GetByID(context.Context, string) (Payment, error)
}

// Service implements payment validation, fingerprinting, and event creation.
type Service struct {
	repository Repository
	newID      func() (string, error)
	now        func() time.Time
}

// NewService creates a payment service with cryptographically random UUIDs and
// a UTC system clock.
func NewService(repository Repository) *Service {
	return &Service{
		repository: repository,
		newID:      newUUID,
		now:        time.Now,
	}
}

// Create validates and normalizes a request before generating its fingerprint.
func (s *Service) Create(ctx context.Context, idempotencyKey string, request CreateRequest) (CreateResult, error) {
	normalizedKey, err := normalizeIdempotencyKey(idempotencyKey)
	if err != nil {
		return CreateResult{}, err
	}

	normalizedRequest, err := NormalizeAndValidate(request)
	if err != nil {
		return CreateResult{}, err
	}

	paymentID, err := s.newID()
	if err != nil {
		return CreateResult{}, fmt.Errorf("generate payment ID: %w", err)
	}
	eventID, err := s.newID()
	if err != nil {
		return CreateResult{}, fmt.Errorf("generate event ID: %w", err)
	}
	traceID, err := s.newID()
	if err != nil {
		return CreateResult{}, fmt.Errorf("generate trace ID: %w", err)
	}

	now := s.now().UTC()
	payment := Payment{
		ID:                 paymentID,
		IdempotencyKey:     normalizedKey,
		RequestFingerprint: Fingerprint(normalizedRequest),
		CustomerID:         normalizedRequest.CustomerID,
		MerchantID:         normalizedRequest.MerchantID,
		DeviceID:           normalizedRequest.DeviceID,
		AmountMinor:        normalizedRequest.AmountMinor,
		Currency:           normalizedRequest.Currency,
		Country:            normalizedRequest.Country,
		Status:             StatusPendingRisk,
		CreatedAt:          now,
		UpdatedAt:          now,
	}

	payload, err := json.Marshal(paymentCreatedPayload{
		PaymentID:   payment.ID,
		CustomerID:  payment.CustomerID,
		MerchantID:  payment.MerchantID,
		DeviceID:    payment.DeviceID,
		AmountMinor: payment.AmountMinor,
		Currency:    payment.Currency,
		Country:     payment.Country,
		Status:      payment.Status,
		CreatedAt:   payment.CreatedAt,
	})
	if err != nil {
		return CreateResult{}, fmt.Errorf("encode payment event payload: %w", err)
	}

	event := OutboxEvent{
		ID:            eventID,
		AggregateID:   payment.ID,
		EventType:     PaymentCreated,
		SchemaVersion: SchemaVersion,
		OccurredAt:    now,
		TraceID:       traceID,
		Payload:       payload,
	}

	return s.repository.Create(ctx, payment, event)
}

// Get validates and normalizes a payment ID before loading it.
func (s *Service) Get(ctx context.Context, id string) (Payment, error) {
	normalizedID, err := NormalizeID(id)
	if err != nil {
		return Payment{}, err
	}

	return s.repository.GetByID(ctx, normalizedID)
}

type paymentCreatedPayload struct {
	PaymentID   string    `json:"payment_id"`
	CustomerID  string    `json:"customer_id"`
	MerchantID  string    `json:"merchant_id"`
	DeviceID    string    `json:"device_id"`
	AmountMinor int64     `json:"amount_minor"`
	Currency    string    `json:"currency"`
	Country     string    `json:"country"`
	Status      string    `json:"status"`
	CreatedAt   time.Time `json:"created_at"`
}

func newUUID() (string, error) {
	var bytes [16]byte
	if _, err := rand.Read(bytes[:]); err != nil {
		return "", err
	}

	bytes[6] = (bytes[6] & 0x0f) | 0x40
	bytes[8] = (bytes[8] & 0x3f) | 0x80

	return fmt.Sprintf(
		"%x-%x-%x-%x-%x",
		bytes[0:4],
		bytes[4:6],
		bytes[6:8],
		bytes[8:10],
		bytes[10:16],
	), nil
}
