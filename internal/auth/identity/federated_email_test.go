package identity

import (
	"errors"
	"testing"
)

var replacementEmails = []string{
	"e�@example.com",
	"�@example.com",
	"e@exa�mple.com",
	"Ev�l.User@Example.com",
}

// A federated address that holds U+FFFD is not the provider's address: a lone
// surrogate escape decodes to it, and a real one is no mailbox of ours.
func TestNormalizeFederatedEmailRefusesTheReplacementCharacter(t *testing.T) {
	for _, raw := range replacementEmails {
		if got, err := NormalizeFederatedEmail(raw); !errors.Is(err, ErrInvalidEmail) || got != "" {
			t.Errorf("NormalizeFederatedEmail(%q) = %q, %v; want ErrInvalidEmail", raw, got, err)
		}
	}
}

func TestNormalizeFederatedEmailKeepsTheLoginKeyOfOtherAddresses(t *testing.T) {
	for _, raw := range []string{"User@Example.COM", "name <a.b@example.com>", "é名@example.com", " spaced@example.com "} {
		want, wantErr := NormalizeEmail(raw)
		got, err := NormalizeFederatedEmail(raw)
		if got != want || (err == nil) != (wantErr == nil) {
			t.Errorf("NormalizeFederatedEmail(%q) = %q, %v; NormalizeEmail = %q, %v", raw, got, err, want, wantErr)
		}
	}
}

// NormalizeEmail keeps go-api's login key rule for local sign-in: it is not
// the federated guard.
func TestNormalizeEmailStillAcceptsTheReplacementCharacterInTheLocalPart(t *testing.T) {
	for _, raw := range []string{"e�@example.com", "�@example.com"} {
		if got, err := NormalizeEmail(raw); err != nil || got != raw {
			t.Errorf("NormalizeEmail(%q) = %q, %v; want it unchanged", raw, got, err)
		}
	}
}
