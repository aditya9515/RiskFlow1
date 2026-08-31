package dashboard

import (
	"context"
	"fmt"

	"github.com/aditya9515/RiskFlow1/services/payment-api/internal/decision"
)

// Repository loads the PostgreSQL-backed portion of a dashboard snapshot.
type Repository interface {
	Load(context.Context, int) (Snapshot, error)
}

// ReconciliationRunner exposes the existing read-only control check.
type ReconciliationRunner interface {
	Count(context.Context) (decision.ReconciliationCounts, error)
}

// Service combines operational totals with the independent reconciliation control.
type Service struct {
	repository Repository
	reconciler ReconciliationRunner
}

func NewService(repository Repository, reconciler ReconciliationRunner) (*Service, error) {
	if repository == nil {
		return nil, fmt.Errorf("dashboard repository is required")
	}
	if reconciler == nil {
		return nil, fmt.Errorf("dashboard reconciler is required")
	}
	return &Service{repository: repository, reconciler: reconciler}, nil
}

// Load returns a bounded, read-only operational view.
func (s *Service) Load(ctx context.Context, recentLimit int) (Snapshot, error) {
	snapshot, err := s.repository.Load(ctx, recentLimit)
	if err != nil {
		return Snapshot{}, fmt.Errorf("load dashboard data: %w", err)
	}
	report, err := s.reconciler.Count(ctx)
	if err != nil {
		return Snapshot{}, fmt.Errorf("run dashboard reconciliation: %w", err)
	}

	snapshot.GeneratedAt = report.GeneratedAt.UTC()
	snapshot.Reconciliation = ReconciliationSummary{
		GracePeriod: report.GracePeriod, ExceptionCount: report.ExceptionCount, ByCode: report.ByCode,
	}
	if snapshot.RecentDecisions == nil {
		snapshot.RecentDecisions = make([]RecentDecision, 0)
	}
	return snapshot, nil
}
