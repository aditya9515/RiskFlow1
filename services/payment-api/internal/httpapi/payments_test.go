package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/aditya9515/RiskFlow1/services/payment-api/internal/payment"
)

type fakePaymentCreator struct {
	result payment.CreateResult
	err    error
	calls  int
}

func (f *fakePaymentCreator) Create(_ context.Context, _ string, _ payment.CreateRequest) (payment.CreateResult, error) {
	f.calls++
	return f.result, f.err
}

func TestCreatePaymentReturnsCreatedAndReplayStatuses(t *testing.T) {
	t.Parallel()

	stored := payment.Payment{
		ID: "10000000-0000-4000-8000-000000000001", CustomerID: "customer-1",
		MerchantID: "merchant-1", DeviceID: "device-1", AmountMinor: 1500,
		Currency: "USD", Country: "IN", Status: payment.StatusPendingRisk,
		CreatedAt: time.Date(2026, time.August, 29, 10, 0, 0, 0, time.UTC),
	}

	tests := []struct {
		name       string
		replayed   bool
		wantStatus int
	}{
		{name: "first request", replayed: false, wantStatus: http.StatusCreated},
		{name: "replay", replayed: true, wantStatus: http.StatusOK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			creator := &fakePaymentCreator{result: payment.CreateResult{Payment: stored, Replayed: tt.replayed}}
			handler := createPaymentHandler(creator, 3*time.Second, discardLogger())
			request := httptest.NewRequest(http.MethodPost, "/v1/payments", bytes.NewBufferString(validPaymentJSON))
			request.Header.Set("Idempotency-Key", "checkout-1")
			recorder := httptest.NewRecorder()
			handler.ServeHTTP(recorder, request)

			if recorder.Code != tt.wantStatus {
				t.Fatalf("status = %d, want %d", recorder.Code, tt.wantStatus)
			}
			var response paymentResponse
			if err := json.NewDecoder(recorder.Body).Decode(&response); err != nil {
				t.Fatalf("decode response: %v", err)
			}
			if response.ID != stored.ID {
				t.Fatalf("payment ID = %q, want %q", response.ID, stored.ID)
			}
		})
	}
}

func TestCreatePaymentReturnsTypedConflict(t *testing.T) {
	t.Parallel()

	creator := &fakePaymentCreator{err: payment.ErrIdempotencyConflict}
	handler := createPaymentHandler(creator, 3*time.Second, discardLogger())
	request := httptest.NewRequest(http.MethodPost, "/v1/payments", bytes.NewBufferString(validPaymentJSON))
	request.Header.Set("Idempotency-Key", "checkout-1")
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusConflict {
		t.Fatalf("status = %d, want 409", recorder.Code)
	}
	var response errorResponse
	decodeJSON(t, recorder, &response)
	if response.Error.Code != "idempotency_conflict" {
		t.Fatalf("error code = %q, want idempotency_conflict", response.Error.Code)
	}
}

func TestCreatePaymentReturnsTypedValidationError(t *testing.T) {
	t.Parallel()

	creator := &fakePaymentCreator{err: &payment.ValidationError{Fields: map[string]string{
		"amount_minor": "must be greater than zero",
	}}}
	handler := createPaymentHandler(creator, 3*time.Second, discardLogger())
	request := httptest.NewRequest(http.MethodPost, "/v1/payments", bytes.NewBufferString(validPaymentJSON))
	request.Header.Set("Idempotency-Key", "checkout-1")
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", recorder.Code)
	}
	var response errorResponse
	decodeJSON(t, recorder, &response)
	if response.Error.Code != "validation_error" || response.Error.Fields["amount_minor"] == "" {
		t.Fatalf("unexpected validation response: %+v", response)
	}
}

func TestCreatePaymentRejectsMalformedOrUnknownJSON(t *testing.T) {
	t.Parallel()

	tests := []string{
		`{"customer_id":`,
		`{"customer_id":"customer-1","unknown":true}`,
		validPaymentJSON + `{}`,
	}
	for _, body := range tests {
		creator := &fakePaymentCreator{}
		handler := createPaymentHandler(creator, 3*time.Second, discardLogger())
		request := httptest.NewRequest(http.MethodPost, "/v1/payments", bytes.NewBufferString(body))
		request.Header.Set("Idempotency-Key", "checkout-1")
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, request)

		if recorder.Code != http.StatusBadRequest {
			t.Errorf("body %q: status = %d, want 400", body, recorder.Code)
		}
		if creator.calls != 0 {
			t.Errorf("body %q: creator called %d times, want 0", body, creator.calls)
		}
	}
}

func TestCreatePaymentMapsMissingIdempotencyKey(t *testing.T) {
	t.Parallel()

	creator := &fakePaymentCreator{err: payment.ErrIdempotencyKeyRequired}
	handler := createPaymentHandler(creator, 3*time.Second, discardLogger())
	request := httptest.NewRequest(http.MethodPost, "/v1/payments", bytes.NewBufferString(validPaymentJSON))
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", recorder.Code)
	}
	var response errorResponse
	decodeJSON(t, recorder, &response)
	if !errors.Is(creator.err, payment.ErrIdempotencyKeyRequired) || response.Error.Code != "missing_idempotency_key" {
		t.Fatalf("unexpected response: %+v", response)
	}
}

const validPaymentJSON = `{
  "customer_id":"customer-1",
  "merchant_id":"merchant-1",
  "device_id":"device-1",
  "amount_minor":1500,
  "currency":"USD",
  "country":"IN"
}`
