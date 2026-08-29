package payment

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"
)

type capturingRepository struct {
	payment Payment
	event   OutboxEvent
	getID   string
	getErr  error
}

func (r *capturingRepository) Create(_ context.Context, payment Payment, event OutboxEvent) (CreateResult, error) {
	r.payment = payment
	r.event = event
	return CreateResult{Payment: payment}, nil
}

func (r *capturingRepository) GetByID(_ context.Context, id string) (Payment, error) {
	r.getID = id
	return r.payment, r.getErr
}

func TestServiceCreatesNormalizedPaymentAndOutboxEvent(t *testing.T) {
	t.Parallel()

	repository := &capturingRepository{}
	service := NewService(repository)
	identifiers := []string{
		"10000000-0000-4000-8000-000000000001",
		"10000000-0000-4000-8000-000000000002",
		"10000000-0000-4000-8000-000000000003",
	}
	service.newID = func() (string, error) {
		identifier := identifiers[0]
		identifiers = identifiers[1:]
		return identifier, nil
	}
	fixedTime := time.Date(2026, time.August, 29, 12, 30, 0, 0, time.FixedZone("test", 5*60*60+30*60))
	service.now = func() time.Time { return fixedTime }

	result, err := service.Create(context.Background(), " key-1 ", CreateRequest{
		CustomerID: " customer-1 ", MerchantID: "merchant-1", DeviceID: "device-1",
		AmountMinor: 2500, Currency: "usd", Country: "in",
	})
	if err != nil {
		t.Fatalf("create payment: %v", err)
	}

	if result.Payment.ID != "10000000-0000-4000-8000-000000000001" {
		t.Fatalf("payment ID = %q", result.Payment.ID)
	}
	if repository.payment.IdempotencyKey != "key-1" {
		t.Fatalf("idempotency key = %q, want key-1", repository.payment.IdempotencyKey)
	}
	if repository.payment.Currency != "USD" || repository.payment.Country != "IN" {
		t.Fatalf("payment was not normalized: %+v", repository.payment)
	}
	if repository.payment.CreatedAt.Location() != time.UTC {
		t.Fatalf("created_at location = %s, want UTC", repository.payment.CreatedAt.Location())
	}
	if repository.event.AggregateID != repository.payment.ID || repository.event.EventType != PaymentCreated {
		t.Fatalf("event does not reference payment: %+v", repository.event)
	}

	var payload paymentCreatedPayload
	if err := json.Unmarshal(repository.event.Payload, &payload); err != nil {
		t.Fatalf("decode event payload: %v", err)
	}
	if payload.PaymentID != repository.payment.ID || payload.AmountMinor != 2500 || payload.Currency != "USD" {
		t.Fatalf("unexpected event payload: %+v", payload)
	}
}

func TestServiceRejectsInvalidRequestBeforeRepository(t *testing.T) {
	t.Parallel()

	repository := &capturingRepository{}
	service := NewService(repository)

	if _, err := service.Create(context.Background(), "key-1", CreateRequest{AmountMinor: -1}); err == nil {
		t.Fatal("create returned nil error")
	}
	if repository.payment.ID != "" {
		t.Fatal("repository was called for invalid request")
	}
}

func TestServiceGetsPaymentUsingNormalizedID(t *testing.T) {
	t.Parallel()

	repository := &capturingRepository{payment: Payment{ID: "abcdef00-0000-4000-8000-00000000000a"}}
	service := NewService(repository)

	stored, err := service.Get(context.Background(), "ABCDEF00-0000-4000-8000-00000000000A")
	if err != nil {
		t.Fatalf("get payment: %v", err)
	}
	if repository.getID != "abcdef00-0000-4000-8000-00000000000a" {
		t.Fatalf("repository ID = %q", repository.getID)
	}
	if stored.ID != repository.payment.ID {
		t.Fatalf("payment ID = %q, want %q", stored.ID, repository.payment.ID)
	}
}

func TestServiceRejectsInvalidPaymentIDBeforeRepository(t *testing.T) {
	t.Parallel()

	repository := &capturingRepository{}
	service := NewService(repository)

	if _, err := service.Get(context.Background(), "not-a-uuid"); !errors.Is(err, ErrPaymentIDInvalid) {
		t.Fatalf("error = %v, want ErrPaymentIDInvalid", err)
	}
	if repository.getID != "" {
		t.Fatalf("repository called with ID %q", repository.getID)
	}
}
