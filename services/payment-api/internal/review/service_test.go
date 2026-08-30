package review

import (
	"context"
	"errors"
	"testing"
	"time"
)

type capturingRepository struct {
	listLimit int
	command   ResolveCommand
	item      Item
	err       error
}

func (r *capturingRepository) ListPending(_ context.Context, limit int) ([]Item, error) {
	r.listLimit = limit
	return []Item{r.item}, r.err
}

func (r *capturingRepository) Resolve(_ context.Context, command ResolveCommand) (Item, error) {
	r.command = command
	return r.item, r.err
}

func TestServiceNormalizesAndAuditsReviewCommand(t *testing.T) {
	t.Parallel()
	repository := &capturingRepository{item: Item{PaymentID: "10000000-0000-4000-8000-000000000001"}}
	service := NewService(repository)
	service.newID = func() (string, error) { return "90000000-0000-4000-8000-000000000001", nil }
	fixedTime := time.Date(2026, time.August, 30, 14, 0, 0, 0, time.FixedZone("IST", 5*60*60+30*60))
	service.now = func() time.Time { return fixedTime }

	_, err := service.Resolve(context.Background(), "10000000-0000-4000-8000-000000000001", ActionApprove, ResolveRequest{
		ExpectedVersion: 1,
		ReasonCode:      " customer_verified ",
	}, Principal{ReviewerID: "reviewer-1", Role: RoleRiskReviewer})
	if err != nil {
		t.Fatal(err)
	}
	if repository.command.ReasonCode != "CUSTOMER_VERIFIED" || repository.command.ExpectedVersion != 1 {
		t.Fatalf("command = %+v", repository.command)
	}
	if repository.command.AuditID != "90000000-0000-4000-8000-000000000001" {
		t.Fatalf("audit ID = %q", repository.command.AuditID)
	}
	if repository.command.ResolvedAt.Location() != time.UTC || !repository.command.ResolvedAt.Equal(fixedTime) {
		t.Fatalf("resolved_at = %s", repository.command.ResolvedAt)
	}
}

func TestServiceRejectsInvalidReviewBeforeRepository(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		paymentID string
		action    string
		request   ResolveRequest
		principal Principal
		want      error
	}{
		{name: "ID", paymentID: "bad", action: ActionApprove, request: ResolveRequest{ExpectedVersion: 1, ReasonCode: "OK"}, principal: Principal{ReviewerID: "r", Role: RoleRiskReviewer}, want: ErrReviewIDInvalid},
		{name: "version", paymentID: "10000000-0000-4000-8000-000000000001", action: ActionApprove, request: ResolveRequest{ReasonCode: "OK"}, principal: Principal{ReviewerID: "r", Role: RoleRiskReviewer}},
		{name: "reason", paymentID: "10000000-0000-4000-8000-000000000001", action: ActionApprove, request: ResolveRequest{ExpectedVersion: 1, ReasonCode: "not valid!"}, principal: Principal{ReviewerID: "r", Role: RoleRiskReviewer}},
		{name: "role", paymentID: "10000000-0000-4000-8000-000000000001", action: ActionApprove, request: ResolveRequest{ExpectedVersion: 1, ReasonCode: "OK"}, principal: Principal{ReviewerID: "a", Role: RoleRiskAuditor}},
		{name: "action", paymentID: "10000000-0000-4000-8000-000000000001", action: "DELETE", request: ResolveRequest{ExpectedVersion: 1, ReasonCode: "OK"}, principal: Principal{ReviewerID: "r", Role: RoleRiskReviewer}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			repository := &capturingRepository{}
			service := NewService(repository)
			_, err := service.Resolve(context.Background(), test.paymentID, test.action, test.request, test.principal)
			if test.want != nil {
				if !errors.Is(err, test.want) {
					t.Fatalf("error = %v, want %v", err, test.want)
				}
			} else {
				var validationError *ValidationError
				if !errors.As(err, &validationError) {
					t.Fatalf("error = %v, want ValidationError", err)
				}
			}
			if repository.command.PaymentID != "" {
				t.Fatal("repository was called")
			}
		})
	}
}

func TestServiceValidatesListLimit(t *testing.T) {
	t.Parallel()
	repository := &capturingRepository{}
	service := NewService(repository)
	if _, err := service.ListPending(context.Background(), 0); err == nil {
		t.Fatal("zero limit accepted")
	}
	if _, err := service.ListPending(context.Background(), 100); err != nil || repository.listLimit != 100 {
		t.Fatalf("valid limit/error = %d/%v", repository.listLimit, err)
	}
}
