package dashboard

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/aditya9515/RiskFlow1/services/payment-api/internal/decision"
)

type fakeRepository struct {
	snapshot Snapshot
	err      error
	limit    int
}

func (f *fakeRepository) Load(_ context.Context, limit int) (Snapshot, error) {
	f.limit = limit
	return f.snapshot, f.err
}

type fakeReconciler struct {
	report decision.ReconciliationCounts
	err    error
}

func (f *fakeReconciler) Count(context.Context) (decision.ReconciliationCounts, error) {
	return f.report, f.err
}

func TestServiceCombinesSnapshotWithReconciliationCounts(t *testing.T) {
	t.Parallel()
	generatedAt := time.Date(2026, 8, 30, 12, 0, 0, 0, time.FixedZone("test", 3600))
	repository := &fakeRepository{snapshot: Snapshot{Payments: PaymentSummary{Total: 4}}}
	reconciler := &fakeReconciler{report: decision.ReconciliationCounts{
		GeneratedAt: generatedAt, GracePeriod: "30s", ExceptionCount: 3,
		ByCode: map[string]int64{
			"MISSING_DECISION": 1, "DUPLICATE_DELIVERY": 2,
		},
	}}
	service, err := NewService(repository, reconciler)
	if err != nil {
		t.Fatal(err)
	}

	snapshot, err := service.Load(context.Background(), 7)
	if err != nil {
		t.Fatalf("load dashboard snapshot: %v", err)
	}
	if repository.limit != 7 || snapshot.Payments.Total != 4 {
		t.Fatalf("repository result/limit = %+v/%d", snapshot.Payments, repository.limit)
	}
	if !snapshot.GeneratedAt.Equal(generatedAt.UTC()) || snapshot.Reconciliation.GracePeriod != "30s" {
		t.Fatalf("reconciliation metadata = %+v", snapshot.Reconciliation)
	}
	if snapshot.Reconciliation.ByCode["DUPLICATE_DELIVERY"] != 2 || snapshot.Reconciliation.ByCode["MISSING_DECISION"] != 1 {
		t.Fatalf("reconciliation counts = %+v", snapshot.Reconciliation.ByCode)
	}
	if snapshot.RecentDecisions == nil {
		t.Fatal("recent decisions must encode as an empty array, not null")
	}
}

func TestServiceReturnsTypedSourceErrors(t *testing.T) {
	t.Parallel()
	sourceErr := errors.New("database unavailable")
	service, err := NewService(&fakeRepository{err: sourceErr}, &fakeReconciler{})
	if err != nil {
		t.Fatal(err)
	}
	_, err = service.Load(context.Background(), 20)
	if !errors.Is(err, sourceErr) {
		t.Fatalf("error = %v, want wrapped source error", err)
	}
}

func TestNewServiceValidatesDependencies(t *testing.T) {
	t.Parallel()
	if _, err := NewService(nil, &fakeReconciler{}); err == nil {
		t.Fatal("nil repository accepted")
	}
	if _, err := NewService(&fakeRepository{}, nil); err == nil {
		t.Fatal("nil reconciler accepted")
	}
}
