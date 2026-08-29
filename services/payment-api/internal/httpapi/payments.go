package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/aditya9515/RiskFlow1/services/payment-api/internal/payment"
)

const maxPaymentRequestBytes = 1 << 20

// PaymentCreator is the payment application behavior required by HTTP.
type PaymentCreator interface {
	Create(context.Context, string, payment.CreateRequest) (payment.CreateResult, error)
}

type paymentResponse struct {
	ID          string    `json:"id"`
	CustomerID  string    `json:"customer_id"`
	MerchantID  string    `json:"merchant_id"`
	DeviceID    string    `json:"device_id"`
	AmountMinor int64     `json:"amount_minor"`
	Currency    string    `json:"currency"`
	Country     string    `json:"country"`
	Status      string    `json:"status"`
	CreatedAt   time.Time `json:"created_at"`
}

func createPaymentHandler(creator PaymentCreator, timeout time.Duration, logger *slog.Logger) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request payment.CreateRequest
		if err := decodePaymentRequest(w, r, &request); err != nil {
			writeJSON(w, http.StatusBadRequest, errorResponse{Error: APIError{
				Code:    "invalid_json",
				Message: err.Error(),
			}})
			return
		}

		ctx, cancel := context.WithTimeout(r.Context(), timeout)
		defer cancel()

		result, err := creator.Create(ctx, r.Header.Get("Idempotency-Key"), request)
		if err != nil {
			writeCreatePaymentError(w, r, err, logger)
			return
		}

		status := http.StatusCreated
		if result.Replayed {
			status = http.StatusOK
		}
		writeJSON(w, status, newPaymentResponse(result.Payment))
	})
}

func decodePaymentRequest(w http.ResponseWriter, r *http.Request, target *payment.CreateRequest) error {
	r.Body = http.MaxBytesReader(w, r.Body, maxPaymentRequestBytes)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()

	if err := decoder.Decode(target); err != nil {
		return fmt.Errorf("request body must contain one valid payment JSON object: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return fmt.Errorf("request body must contain exactly one JSON object")
	}

	return nil
}

func writeCreatePaymentError(w http.ResponseWriter, r *http.Request, err error, logger *slog.Logger) {
	var validationErr *payment.ValidationError
	switch {
	case errors.Is(err, payment.ErrIdempotencyKeyRequired):
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: APIError{
			Code:    "missing_idempotency_key",
			Message: "Idempotency-Key header is required",
		}})
	case errors.Is(err, payment.ErrIdempotencyKeyInvalid):
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: APIError{
			Code:    "invalid_idempotency_key",
			Message: "Idempotency-Key header must be at most 255 characters and contain no control characters",
		}})
	case errors.As(err, &validationErr):
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: APIError{
			Code:    "validation_error",
			Message: "payment request validation failed",
			Fields:  validationErr.Fields,
		}})
	case errors.Is(err, payment.ErrIdempotencyConflict):
		writeJSON(w, http.StatusConflict, errorResponse{Error: APIError{
			Code:    "idempotency_conflict",
			Message: "Idempotency-Key was already used with a different payment request",
		}})
	case errors.Is(err, context.DeadlineExceeded):
		writeJSON(w, http.StatusGatewayTimeout, errorResponse{Error: APIError{
			Code:    "request_timeout",
			Message: "payment request timed out and may be safely retried with the same Idempotency-Key",
		}})
	default:
		logger.ErrorContext(r.Context(), "create payment failed", slog.String("error", err.Error()))
		writeJSON(w, http.StatusInternalServerError, errorResponse{Error: APIError{
			Code:    "internal_error",
			Message: "payment could not be created",
		}})
	}
}

func newPaymentResponse(stored payment.Payment) paymentResponse {
	return paymentResponse{
		ID:          stored.ID,
		CustomerID:  stored.CustomerID,
		MerchantID:  stored.MerchantID,
		DeviceID:    stored.DeviceID,
		AmountMinor: stored.AmountMinor,
		Currency:    stored.Currency,
		Country:     stored.Country,
		Status:      stored.Status,
		CreatedAt:   stored.CreatedAt.UTC(),
	}
}
