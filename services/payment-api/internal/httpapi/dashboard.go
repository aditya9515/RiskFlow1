package httpapi

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/aditya9515/RiskFlow1/services/payment-api/internal/dashboard"
	"github.com/aditya9515/RiskFlow1/services/payment-api/internal/review"
)

const (
	defaultRecentDecisionLimit = 20
	maxRecentDecisionLimit     = 100
)

type DashboardService interface {
	Load(context.Context, int) (dashboard.Snapshot, error)
}

func dashboardHandler(service DashboardService, authenticator ReviewAuthenticator, timeout time.Duration, logger *slog.Logger) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		principal, ok := authenticateReview(w, r, authenticator)
		if !ok {
			return
		}
		if principal.Role != review.RoleRiskReviewer && principal.Role != review.RoleRiskAuditor {
			writeJSON(w, http.StatusForbidden, errorResponse{Error: APIError{Code: "forbidden", Message: "risk operations role is required"}})
			return
		}

		recentLimit, err := parseRecentDecisionLimit(r)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, errorResponse{Error: APIError{
				Code: "validation_error", Message: "dashboard query validation failed",
				Fields: map[string]string{"recent_limit": err.Error()},
			}})
			return
		}

		ctx, cancel := context.WithTimeout(r.Context(), timeout)
		defer cancel()
		snapshot, err := service.Load(ctx, recentLimit)
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				writeJSON(w, http.StatusGatewayTimeout, errorResponse{Error: APIError{Code: "request_timeout", Message: "dashboard request timed out"}})
				return
			}
			logger.ErrorContext(r.Context(), "dashboard request failed", slog.String("error", err.Error()))
			writeJSON(w, http.StatusInternalServerError, errorResponse{Error: APIError{Code: "internal_error", Message: "dashboard request failed"}})
			return
		}

		w.Header().Set("Cache-Control", "no-store")
		writeJSON(w, http.StatusOK, snapshot)
	})
}

func parseRecentDecisionLimit(r *http.Request) (int, error) {
	raw := r.URL.Query().Get("recent_limit")
	if raw == "" {
		return defaultRecentDecisionLimit, nil
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value < 1 || value > maxRecentDecisionLimit {
		return 0, errors.New("must be an integer between 1 and 100")
	}
	return value, nil
}
