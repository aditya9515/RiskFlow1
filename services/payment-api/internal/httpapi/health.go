package httpapi

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"time"
)

// Pinger is the minimum database behavior required by the readiness endpoint.
type Pinger interface {
	Ping(context.Context) error
}

type statusResponse struct {
	Status string `json:"status"`
}

type errorResponse struct {
	Error APIError `json:"error"`
}

// APIError is the stable JSON error shape returned by the HTTP API.
type APIError struct {
	Code    string            `json:"code"`
	Message string            `json:"message"`
	Fields  map[string]string `json:"fields,omitempty"`
}

// NewHandler builds the API routes with their required dependencies.
func NewHandler(
	pinger Pinger,
	paymentCreator PaymentCreator,
	readinessTimeout time.Duration,
	paymentTimeout time.Duration,
	logger *slog.Logger,
) http.Handler {
	if logger == nil {
		logger = slog.Default()
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, statusResponse{Status: "ok"})
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), readinessTimeout)
		defer cancel()

		if err := pinger.Ping(ctx); err != nil {
			logger.WarnContext(r.Context(), "readiness check failed", slog.String("error", err.Error()))
			writeJSON(w, http.StatusServiceUnavailable, errorResponse{
				Error: APIError{
					Code:    "database_unavailable",
					Message: "database unavailable",
				},
			})
			return
		}

		writeJSON(w, http.StatusOK, statusResponse{Status: "ready"})
	})
	mux.Handle("POST /v1/payments", createPaymentHandler(paymentCreator, paymentTimeout, logger))

	return mux
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}
