package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/aditya9515/RiskFlow1/services/payment-api/internal/review"
)

const maxReviewRequestBytes = 1 << 20

type ReviewService interface {
	ListPending(context.Context, int) ([]review.Item, error)
	Resolve(context.Context, string, string, review.ResolveRequest, review.Principal) (review.Item, error)
}

type ReviewAuthenticator interface {
	Authenticate(string) (review.Principal, error)
}

type reviewResponse struct {
	PaymentID        string     `json:"payment_id"`
	DecisionID       string     `json:"decision_id"`
	CustomerID       string     `json:"customer_id"`
	MerchantID       string     `json:"merchant_id"`
	AmountMinor      int64      `json:"amount_minor"`
	Currency         string     `json:"currency"`
	Country          string     `json:"country"`
	PaymentStatus    string     `json:"payment_status"`
	ReviewStatus     string     `json:"review_status"`
	Version          int        `json:"version"`
	RiskScore        int        `json:"risk_score"`
	ReasonCodes      []string   `json:"reason_codes"`
	RuleVersion      string     `json:"rule_version"`
	ModelVersion     string     `json:"model_version"`
	EnqueuedAt       time.Time  `json:"enqueued_at"`
	ResolvedAt       *time.Time `json:"resolved_at,omitempty"`
	ReviewerID       string     `json:"reviewer_id,omitempty"`
	ResolutionReason string     `json:"resolution_reason,omitempty"`
}

type reviewListResponse struct {
	Reviews []reviewResponse `json:"reviews"`
	Count   int              `json:"count"`
}

func listReviewsHandler(service ReviewService, authenticator ReviewAuthenticator, timeout time.Duration, logger *slog.Logger) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		principal, ok := authenticateReview(w, r, authenticator)
		if !ok {
			return
		}
		if principal.Role != review.RoleRiskReviewer && principal.Role != review.RoleRiskAuditor {
			writeForbidden(w)
			return
		}

		limit := 50
		if raw := r.URL.Query().Get("limit"); raw != "" {
			parsed, err := strconv.Atoi(raw)
			if err != nil {
				writeJSON(w, http.StatusBadRequest, errorResponse{Error: APIError{Code: "validation_error", Message: "review query validation failed", Fields: map[string]string{"limit": "must be an integer between 1 and 100"}}})
				return
			}
			limit = parsed
		}

		ctx, cancel := context.WithTimeout(r.Context(), timeout)
		defer cancel()
		items, err := service.ListPending(ctx, limit)
		if err != nil {
			writeReviewError(w, r, err, "list", logger)
			return
		}
		responses := make([]reviewResponse, 0, len(items))
		for _, item := range items {
			responses = append(responses, newReviewResponse(item))
		}
		writeJSON(w, http.StatusOK, reviewListResponse{Reviews: responses, Count: len(responses)})
	})
}

func resolveReviewHandler(service ReviewService, authenticator ReviewAuthenticator, timeout time.Duration, action string, logger *slog.Logger) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		principal, ok := authenticateReview(w, r, authenticator)
		if !ok {
			return
		}
		if principal.Role != review.RoleRiskReviewer {
			writeForbidden(w)
			return
		}

		var request review.ResolveRequest
		if err := decodeReviewRequest(w, r, &request); err != nil {
			writeJSON(w, http.StatusBadRequest, errorResponse{Error: APIError{Code: "invalid_json", Message: err.Error()}})
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), timeout)
		defer cancel()
		item, err := service.Resolve(ctx, r.PathValue("id"), action, request, principal)
		if err != nil {
			writeReviewError(w, r, err, "resolve", logger)
			return
		}
		writeJSON(w, http.StatusOK, newReviewResponse(item))
	})
}

func authenticateReview(w http.ResponseWriter, r *http.Request, authenticator ReviewAuthenticator) (review.Principal, bool) {
	principal, err := authenticator.Authenticate(r.Header.Get("Authorization"))
	if err != nil {
		w.Header().Set("WWW-Authenticate", "Bearer")
		writeJSON(w, http.StatusUnauthorized, errorResponse{Error: APIError{Code: "unauthorized", Message: "valid bearer credentials are required"}})
		return review.Principal{}, false
	}
	return principal, true
}

func writeForbidden(w http.ResponseWriter) {
	writeJSON(w, http.StatusForbidden, errorResponse{Error: APIError{Code: "forbidden", Message: "risk_reviewer role is required for this action"}})
}

func decodeReviewRequest(w http.ResponseWriter, r *http.Request, target *review.ResolveRequest) error {
	r.Body = http.MaxBytesReader(w, r.Body, maxReviewRequestBytes)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return fmt.Errorf("request body must contain one valid review action JSON object: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return fmt.Errorf("request body must contain exactly one JSON object")
	}
	return nil
}

func writeReviewError(w http.ResponseWriter, r *http.Request, err error, operation string, logger *slog.Logger) {
	var validationErr *review.ValidationError
	var conflictErr *review.ConflictError
	switch {
	case errors.Is(err, review.ErrReviewIDInvalid):
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: APIError{Code: "invalid_payment_id", Message: "payment ID must be a valid UUID"}})
	case errors.As(err, &validationErr):
		writeJSON(w, http.StatusBadRequest, errorResponse{Error: APIError{Code: "validation_error", Message: "review request validation failed", Fields: validationErr.Fields}})
	case errors.Is(err, review.ErrReviewNotFound):
		writeJSON(w, http.StatusNotFound, errorResponse{Error: APIError{Code: "review_not_found", Message: "manual review not found"}})
	case errors.As(err, &conflictErr) && errors.Is(err, review.ErrReviewAlreadyResolved):
		writeJSON(w, http.StatusConflict, errorResponse{Error: APIError{Code: "review_already_resolved", Message: "manual review is already resolved", Details: map[string]any{"current_status": conflictErr.CurrentStatus, "current_version": conflictErr.CurrentVersion}}})
	case errors.As(err, &conflictErr) && errors.Is(err, review.ErrReviewVersionConflict):
		writeJSON(w, http.StatusConflict, errorResponse{Error: APIError{Code: "review_version_conflict", Message: "manual review was changed by another reviewer", Details: map[string]any{"current_status": conflictErr.CurrentStatus, "current_version": conflictErr.CurrentVersion}}})
	case errors.Is(err, review.ErrReviewStateConflict):
		writeJSON(w, http.StatusConflict, errorResponse{Error: APIError{Code: "review_state_conflict", Message: "payment is not in a reviewable state"}})
	case errors.Is(err, context.DeadlineExceeded):
		writeJSON(w, http.StatusGatewayTimeout, errorResponse{Error: APIError{Code: "request_timeout", Message: "review request timed out; reload the item before retrying"}})
	default:
		logger.ErrorContext(r.Context(), "manual review request failed", slog.String("operation", operation), slog.String("error", err.Error()))
		writeJSON(w, http.StatusInternalServerError, errorResponse{Error: APIError{Code: "internal_error", Message: "manual review request failed"}})
	}
}

func newReviewResponse(item review.Item) reviewResponse {
	return reviewResponse{
		PaymentID: item.PaymentID, DecisionID: item.DecisionID, CustomerID: item.CustomerID,
		MerchantID: item.MerchantID, AmountMinor: item.AmountMinor, Currency: item.Currency,
		Country: item.Country, PaymentStatus: item.PaymentStatus, ReviewStatus: item.ReviewStatus,
		Version: item.Version, RiskScore: item.RiskScore, ReasonCodes: item.ReasonCodes,
		RuleVersion: item.RuleVersion, ModelVersion: item.ModelVersion, EnqueuedAt: item.EnqueuedAt.UTC(),
		ResolvedAt: item.ResolvedAt, ReviewerID: item.ReviewerID, ResolutionReason: item.ResolutionReason,
	}
}
