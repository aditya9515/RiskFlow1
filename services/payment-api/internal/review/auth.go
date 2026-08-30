package review

import (
	"crypto/sha256"
	"crypto/subtle"
	"errors"
	"fmt"
	"strings"
	"unicode"
)

const (
	RoleRiskReviewer = "risk_reviewer"
	RoleRiskAuditor  = "risk_auditor"
)

var ErrUnauthorized = errors.New("review authorization failed")

// Credential binds one secret bearer token to a non-spoofable identity and role.
type Credential struct {
	ReviewerID string
	Role       string
	Token      string
}

// Principal is the authenticated caller used in audit records.
type Principal struct {
	ReviewerID string
	Role       string
}

// Authenticator validates an HTTP Authorization value.
type Authenticator interface {
	Authenticate(string) (Principal, error)
}

type tokenCredential struct {
	principal Principal
	digest    [sha256.Size]byte
}

// TokenAuthenticator stores only token hashes after startup and compares them
// in constant time.
type TokenAuthenticator struct {
	credentials []tokenCredential
}

func NewTokenAuthenticator(credentials []Credential) (*TokenAuthenticator, error) {
	if len(credentials) == 0 {
		return nil, errors.New("at least one review credential is required")
	}

	stored := make([]tokenCredential, 0, len(credentials))
	identities := make(map[string]struct{}, len(credentials))
	digests := make(map[[sha256.Size]byte]struct{}, len(credentials))
	for _, credential := range credentials {
		reviewerID := strings.TrimSpace(credential.ReviewerID)
		role := strings.TrimSpace(credential.Role)
		if reviewerID == "" || len(reviewerID) > 255 || containsControl(reviewerID) {
			return nil, errors.New("reviewer IDs must contain 1-255 characters without controls")
		}
		if role != RoleRiskReviewer && role != RoleRiskAuditor {
			return nil, fmt.Errorf("reviewer %s has unsupported role %q", reviewerID, role)
		}
		if len(credential.Token) < 32 || len(credential.Token) > 512 || containsWhitespace(credential.Token) || containsControl(credential.Token) {
			return nil, fmt.Errorf("reviewer %s token must contain 32-512 characters without whitespace or controls", reviewerID)
		}
		if _, duplicate := identities[reviewerID]; duplicate {
			return nil, fmt.Errorf("duplicate reviewer ID %q", reviewerID)
		}
		digest := sha256.Sum256([]byte(credential.Token))
		if _, duplicate := digests[digest]; duplicate {
			return nil, errors.New("duplicate review token")
		}
		identities[reviewerID] = struct{}{}
		digests[digest] = struct{}{}
		stored = append(stored, tokenCredential{
			principal: Principal{ReviewerID: reviewerID, Role: role},
			digest:    digest,
		})
	}
	return &TokenAuthenticator{credentials: stored}, nil
}

func containsWhitespace(value string) bool {
	for _, character := range value {
		if unicode.IsSpace(character) {
			return true
		}
	}
	return false
}

func (a *TokenAuthenticator) Authenticate(authorization string) (Principal, error) {
	parts := strings.Fields(authorization)
	if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") {
		return Principal{}, ErrUnauthorized
	}
	presented := sha256.Sum256([]byte(parts[1]))
	match := -1
	for index := range a.credentials {
		if subtle.ConstantTimeCompare(presented[:], a.credentials[index].digest[:]) == 1 {
			match = index
		}
	}
	if match < 0 {
		return Principal{}, ErrUnauthorized
	}
	return a.credentials[match].principal, nil
}

func containsControl(value string) bool {
	for _, character := range value {
		if character < 0x20 || character == 0x7f {
			return true
		}
	}
	return false
}
