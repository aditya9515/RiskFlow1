package review

import (
	"context"
	"crypto/rand"
	"fmt"
	"strings"
	"time"

	"github.com/aditya9515/RiskFlow1/services/payment-api/internal/payment"
)

// Repository is the persistence boundary for manual-review use cases.
type Repository interface {
	ListPending(context.Context, int) ([]Item, error)
	Resolve(context.Context, ResolveCommand) (Item, error)
}

// Service validates review commands and generates immutable audit identities.
type Service struct {
	repository Repository
	newID      func() (string, error)
	now        func() time.Time
}

func NewService(repository Repository) *Service {
	return &Service{repository: repository, newID: newUUID, now: time.Now}
}

func (s *Service) ListPending(ctx context.Context, limit int) ([]Item, error) {
	if limit < 1 || limit > 100 {
		return nil, &ValidationError{Fields: map[string]string{"limit": "must be between 1 and 100"}}
	}
	return s.repository.ListPending(ctx, limit)
}

func (s *Service) Resolve(
	ctx context.Context,
	paymentID string,
	action string,
	request ResolveRequest,
	principal Principal,
) (Item, error) {
	normalizedID, err := payment.NormalizeID(paymentID)
	if err != nil {
		return Item{}, ErrReviewIDInvalid
	}

	reasonCode := strings.ToUpper(strings.TrimSpace(request.ReasonCode))
	fields := make(map[string]string)
	if request.ExpectedVersion <= 0 {
		fields["expected_version"] = "must be greater than zero"
	}
	if !validReasonCode(reasonCode) {
		fields["reason_code"] = "must contain 1-100 uppercase letters, digits, or underscores"
	}
	if principal.ReviewerID == "" || principal.Role != RoleRiskReviewer {
		fields["principal"] = "must be an authenticated risk reviewer"
	}
	if action != ActionApprove && action != ActionReject {
		fields["action"] = "must be APPROVE or REJECT"
	}
	if len(fields) > 0 {
		return Item{}, &ValidationError{Fields: fields}
	}

	auditID, err := s.newID()
	if err != nil {
		return Item{}, fmt.Errorf("generate manual review audit ID: %w", err)
	}
	return s.repository.Resolve(ctx, ResolveCommand{
		PaymentID:       normalizedID,
		Action:          action,
		ExpectedVersion: request.ExpectedVersion,
		ReasonCode:      reasonCode,
		Principal:       principal,
		AuditID:         auditID,
		ResolvedAt:      s.now().UTC(),
	})
}

func validReasonCode(value string) bool {
	if value == "" || len(value) > 100 {
		return false
	}
	for _, character := range value {
		if (character >= 'A' && character <= 'Z') || (character >= '0' && character <= '9') || character == '_' {
			continue
		}
		return false
	}
	return true
}

func newUUID() (string, error) {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", err
	}
	value[6] = (value[6] & 0x0f) | 0x40
	value[8] = (value[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", value[0:4], value[4:6], value[6:8], value[8:10], value[10:16]), nil
}
