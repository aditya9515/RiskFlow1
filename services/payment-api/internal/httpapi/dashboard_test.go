package httpapi

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/aditya9515/RiskFlow1/services/payment-api/internal/dashboard"
	"github.com/aditya9515/RiskFlow1/services/payment-api/internal/review"
)

type fakeDashboardService struct {
	snapshot dashboard.Snapshot
	err      error
	limit    int
	calls    int
}

func (f *fakeDashboardService) Load(_ context.Context, limit int) (dashboard.Snapshot, error) {
	f.calls++
	f.limit = limit
	return f.snapshot, f.err
}

func newDashboardHTTPHandler(t *testing.T, service DashboardService) http.Handler {
	t.Helper()
	authenticator, err := review.NewTokenAuthenticator([]review.Credential{
		{ReviewerID: "reviewer-1", Role: review.RoleRiskReviewer, Token: reviewerToken},
		{ReviewerID: "auditor-1", Role: review.RoleRiskAuditor, Token: auditorToken},
	})
	if err != nil {
		t.Fatal(err)
	}
	return NewHandler(&fakePinger{}, nil, nil, service, authenticator, time.Second, 3*time.Second, 3*time.Second, 5*time.Second, discardLogger())
}

func TestDashboardAllowsAuditorAndUsesRequestedLimit(t *testing.T) {
	t.Parallel()
	service := &fakeDashboardService{snapshot: dashboard.Snapshot{
		GeneratedAt:     time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC),
		Payments:        dashboard.PaymentSummary{Total: 3, AmountMinorTotal: 6000},
		RecentDecisions: []dashboard.RecentDecision{},
	}}
	handler := newDashboardHTTPHandler(t, service)
	request := httptest.NewRequest(http.MethodGet, "/v1/dashboard?recent_limit=7", nil)
	request.Header.Set("Authorization", "Bearer "+auditorToken)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusOK || recorder.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("status/cache = %d/%q, body=%s", recorder.Code, recorder.Header().Get("Cache-Control"), recorder.Body.String())
	}
	if service.limit != 7 || service.calls != 1 {
		t.Fatalf("service limit/calls = %d/%d", service.limit, service.calls)
	}
	var body dashboard.Snapshot
	decodeJSON(t, recorder, &body)
	if body.Payments.Total != 3 || body.Payments.AmountMinorTotal != 6000 || body.RecentDecisions == nil {
		t.Fatalf("dashboard response = %+v", body)
	}
}

func TestDashboardRequiresAuthentication(t *testing.T) {
	t.Parallel()
	service := &fakeDashboardService{}
	handler := newDashboardHTTPHandler(t, service)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/v1/dashboard", nil))
	if recorder.Code != http.StatusUnauthorized || service.calls != 0 {
		t.Fatalf("status/calls = %d/%d", recorder.Code, service.calls)
	}
}

func TestDashboardRejectsInvalidLimitBeforeService(t *testing.T) {
	t.Parallel()
	service := &fakeDashboardService{}
	handler := newDashboardHTTPHandler(t, service)
	request := httptest.NewRequest(http.MethodGet, "/v1/dashboard?recent_limit=101", nil)
	request.Header.Set("Authorization", "Bearer "+reviewerToken)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusBadRequest || service.calls != 0 {
		t.Fatalf("status/calls/body = %d/%d/%s", recorder.Code, service.calls, recorder.Body.String())
	}
	var body errorResponse
	decodeJSON(t, recorder, &body)
	if body.Error.Code != "validation_error" || body.Error.Fields["recent_limit"] == "" {
		t.Fatalf("error = %+v", body.Error)
	}
}

func TestDashboardTimeoutReturnsTypedGatewayTimeout(t *testing.T) {
	t.Parallel()
	service := &fakeDashboardService{err: fmt.Errorf("load snapshot: %w", context.DeadlineExceeded)}
	handler := newDashboardHTTPHandler(t, service)
	request := httptest.NewRequest(http.MethodGet, "/v1/dashboard", nil)
	request.Header.Set("Authorization", "Bearer "+reviewerToken)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusGatewayTimeout {
		t.Fatalf("status/body = %d/%s", recorder.Code, recorder.Body.String())
	}
	var body errorResponse
	decodeJSON(t, recorder, &body)
	if body.Error.Code != "request_timeout" {
		t.Fatalf("error = %+v", body.Error)
	}
}
