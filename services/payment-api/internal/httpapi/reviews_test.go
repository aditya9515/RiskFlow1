package httpapi

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/aditya9515/RiskFlow1/services/payment-api/internal/review"
)

const (
	reviewerToken = "12345678901234567890123456789012"
	auditorToken  = "abcdefghijklmnopqrstuvwxyzABCDEF"
)

type fakeReviewService struct {
	items     []review.Item
	item      review.Item
	err       error
	principal review.Principal
	action    string
	request   review.ResolveRequest
}

func (f *fakeReviewService) ListPending(context.Context, int) ([]review.Item, error) {
	return f.items, f.err
}

func (f *fakeReviewService) Resolve(_ context.Context, _ string, action string, request review.ResolveRequest, principal review.Principal) (review.Item, error) {
	f.principal = principal
	f.action = action
	f.request = request
	return f.item, f.err
}

func newReviewHTTPHandler(t *testing.T, service ReviewService) http.Handler {
	t.Helper()
	authenticator, err := review.NewTokenAuthenticator([]review.Credential{
		{ReviewerID: "reviewer-1", Role: review.RoleRiskReviewer, Token: reviewerToken},
		{ReviewerID: "auditor-1", Role: review.RoleRiskAuditor, Token: auditorToken},
	})
	if err != nil {
		t.Fatal(err)
	}
	return NewHandler(&fakePinger{}, nil, service, authenticator, time.Second, 3*time.Second, 3*time.Second, discardLogger())
}

func TestReviewListAllowsAuditor(t *testing.T) {
	t.Parallel()
	service := &fakeReviewService{items: []review.Item{{PaymentID: "10000000-0000-4000-8000-000000000001", ReviewStatus: review.StatusPending, Version: 1}}}
	handler := newReviewHTTPHandler(t, service)
	request := httptest.NewRequest(http.MethodGet, "/v1/reviews?limit=10", nil)
	request.Header.Set("Authorization", "Bearer "+auditorToken)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status/body = %d/%s", recorder.Code, recorder.Body.String())
	}
	var body reviewListResponse
	decodeJSON(t, recorder, &body)
	if body.Count != 1 || body.Reviews[0].PaymentID != service.items[0].PaymentID {
		t.Fatalf("response = %+v", body)
	}
}

func TestReviewEndpointsEnforceAuthenticationAndRole(t *testing.T) {
	t.Parallel()
	handler := newReviewHTTPHandler(t, &fakeReviewService{})

	unauthorized := httptest.NewRecorder()
	handler.ServeHTTP(unauthorized, httptest.NewRequest(http.MethodGet, "/v1/reviews", nil))
	if unauthorized.Code != http.StatusUnauthorized || unauthorized.Header().Get("WWW-Authenticate") != "Bearer" {
		t.Fatalf("unauthorized status/header = %d/%q", unauthorized.Code, unauthorized.Header().Get("WWW-Authenticate"))
	}

	request := httptest.NewRequest(http.MethodPost, "/v1/reviews/10000000-0000-4000-8000-000000000001/approve", bytes.NewBufferString(`{"expected_version":1,"reason_code":"OK"}`))
	request.Header.Set("Authorization", "Bearer "+auditorToken)
	forbidden := httptest.NewRecorder()
	handler.ServeHTTP(forbidden, request)
	if forbidden.Code != http.StatusForbidden {
		t.Fatalf("auditor action status/body = %d/%s", forbidden.Code, forbidden.Body.String())
	}
}

func TestReviewActionUsesAuthenticatedIdentity(t *testing.T) {
	t.Parallel()
	service := &fakeReviewService{item: review.Item{PaymentID: "10000000-0000-4000-8000-000000000001", ReviewStatus: review.StatusApproved, Version: 2}}
	handler := newReviewHTTPHandler(t, service)
	request := httptest.NewRequest(http.MethodPost, "/v1/reviews/10000000-0000-4000-8000-000000000001/approve", bytes.NewBufferString(`{"expected_version":1,"reason_code":"CUSTOMER_VERIFIED"}`))
	request.Header.Set("Authorization", "Bearer "+reviewerToken)
	request.Header.Set("X-Reviewer-ID", "spoofed")
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status/body = %d/%s", recorder.Code, recorder.Body.String())
	}
	if service.principal.ReviewerID != "reviewer-1" || service.principal.Role != review.RoleRiskReviewer {
		t.Fatalf("principal = %+v", service.principal)
	}
	if service.action != review.ActionApprove || service.request.ExpectedVersion != 1 {
		t.Fatalf("action/request = %q/%+v", service.action, service.request)
	}
}

func TestReviewConflictReturnsCurrentVersion(t *testing.T) {
	t.Parallel()
	service := &fakeReviewService{err: &review.ConflictError{Cause: review.ErrReviewVersionConflict, CurrentStatus: review.StatusPending, CurrentVersion: 2}}
	handler := newReviewHTTPHandler(t, service)
	request := httptest.NewRequest(http.MethodPost, "/v1/reviews/10000000-0000-4000-8000-000000000001/reject", bytes.NewBufferString(`{"expected_version":1,"reason_code":"RISK_CONFIRMED"}`))
	request.Header.Set("Authorization", "Bearer "+reviewerToken)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusConflict {
		t.Fatalf("status/body = %d/%s", recorder.Code, recorder.Body.String())
	}
	var body errorResponse
	decodeJSON(t, recorder, &body)
	if body.Error.Code != "review_version_conflict" || body.Error.Details["current_version"] != float64(2) {
		t.Fatalf("error = %+v", body.Error)
	}
}

func TestReviewMalformedJSONIsRejectedBeforeService(t *testing.T) {
	t.Parallel()
	service := &fakeReviewService{err: errors.New("must not be called")}
	handler := newReviewHTTPHandler(t, service)
	request := httptest.NewRequest(http.MethodPost, "/v1/reviews/10000000-0000-4000-8000-000000000001/approve", bytes.NewBufferString(`{"expected_version":1,"unknown":true}`))
	request.Header.Set("Authorization", "Bearer "+reviewerToken)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("status/body = %d/%s", recorder.Code, recorder.Body.String())
	}
}
