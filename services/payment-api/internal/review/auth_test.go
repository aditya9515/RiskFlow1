package review

import (
	"errors"
	"testing"
)

func TestTokenAuthenticatorMapsTokensToFixedPrincipals(t *testing.T) {
	t.Parallel()
	authenticator, err := NewTokenAuthenticator([]Credential{
		{ReviewerID: "reviewer-1", Role: RoleRiskReviewer, Token: "12345678901234567890123456789012"},
		{ReviewerID: "auditor-1", Role: RoleRiskAuditor, Token: "abcdefghijklmnopqrstuvwxyzABCDEF"},
	})
	if err != nil {
		t.Fatal(err)
	}

	principal, err := authenticator.Authenticate("Bearer 12345678901234567890123456789012")
	if err != nil {
		t.Fatal(err)
	}
	if principal.ReviewerID != "reviewer-1" || principal.Role != RoleRiskReviewer {
		t.Fatalf("principal = %+v", principal)
	}
	principal, err = authenticator.Authenticate("bearer abcdefghijklmnopqrstuvwxyzABCDEF")
	if err != nil || principal.ReviewerID != "auditor-1" || principal.Role != RoleRiskAuditor {
		t.Fatalf("auditor principal/error = %+v/%v", principal, err)
	}
}

func TestTokenAuthenticatorRejectsInvalidAuthorization(t *testing.T) {
	t.Parallel()
	authenticator, err := NewTokenAuthenticator([]Credential{{ReviewerID: "reviewer-1", Role: RoleRiskReviewer, Token: "12345678901234567890123456789012"}})
	if err != nil {
		t.Fatal(err)
	}
	for _, authorization := range []string{"", "Basic abc", "Bearer", "Bearer wrong", "Bearer token extra"} {
		if _, err := authenticator.Authenticate(authorization); !errors.Is(err, ErrUnauthorized) {
			t.Errorf("authorization %q error = %v, want ErrUnauthorized", authorization, err)
		}
	}
}

func TestTokenAuthenticatorRejectsUnsafeCredentialSets(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		credentials []Credential
	}{
		{name: "empty"},
		{name: "short token", credentials: []Credential{{ReviewerID: "reviewer", Role: RoleRiskReviewer, Token: "short"}}},
		{name: "token whitespace", credentials: []Credential{{ReviewerID: "reviewer", Role: RoleRiskReviewer, Token: "1234567890123456 7890123456789012"}}},
		{name: "unsupported role", credentials: []Credential{{ReviewerID: "reviewer", Role: "admin", Token: "12345678901234567890123456789012"}}},
		{name: "duplicate identity", credentials: []Credential{{ReviewerID: "same", Role: RoleRiskReviewer, Token: "12345678901234567890123456789012"}, {ReviewerID: "same", Role: RoleRiskAuditor, Token: "abcdefghijklmnopqrstuvwxyzABCDEF"}}},
		{name: "duplicate token", credentials: []Credential{{ReviewerID: "one", Role: RoleRiskReviewer, Token: "12345678901234567890123456789012"}, {ReviewerID: "two", Role: RoleRiskAuditor, Token: "12345678901234567890123456789012"}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if _, err := NewTokenAuthenticator(test.credentials); err == nil {
				t.Fatal("NewTokenAuthenticator returned nil error")
			}
		})
	}
}
