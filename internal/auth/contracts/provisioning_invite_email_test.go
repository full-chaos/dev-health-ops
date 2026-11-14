package contracts_test

import (
	"encoding/json"
	"os"
	"regexp"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/identity"
)

// An external test package: identity reaches contracts through requestid, so an internal test of contracts that
// imports identity is an import cycle.

func inviteEmailSchemaPattern(t *testing.T) string {
	t.Helper()
	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	root, err := contracts.RepoRoot(wd)
	if err != nil {
		t.Fatalf("locating repo root: %v", err)
	}
	raw, err := os.ReadFile(contracts.SchemaPath(root, "invite-create-request.v1"))
	if err != nil {
		t.Fatal(err)
	}
	var schema struct {
		Properties struct {
			Email struct {
				Pattern string `json:"pattern"`
			} `json:"email"`
		} `json:"properties"`
	}
	if err := json.Unmarshal(raw, &schema); err != nil {
		t.Fatal(err)
	}
	if schema.Properties.Email.Pattern == "" {
		t.Fatal("invite-create-request.v1 has no properties.email.pattern")
	}
	return schema.Properties.Email.Pattern
}

// The invite address pattern accepts only addresses the login-key normalizer
// keeps unchanged, and refuses what it cannot normalize.
func TestInviteEmailPatternIsASubsetOfTheNormalizerFixedPoints(t *testing.T) {
	pattern := regexp.MustCompile(inviteEmailSchemaPattern(t))
	accepted := []string{
		"a@example.com", "new.member@example.com", "first.last+tag@sub.example.org", "a_b-c%d@ex-ample.io",
		"x@a.b.c.dev", "user123@mail.example.net",
	}
	for _, address := range accepted {
		if !pattern.MatchString(address) {
			t.Errorf("pattern refuses %q", address)
			continue
		}
		if got, err := identity.NormalizeEmail(address); err != nil || got != address {
			t.Errorf("pattern accepts %q but NormalizeEmail gives %q, %v", address, got, err)
		}
	}
	refused := []string{
		"a@.", "a@", "@example.com", "a@example", "a@example.1", "a..b@example.com", ".a@example.com", "a.@example.com",
		"New@example.com", "\u00c9@example.com", "a b@example.com", "a@-example.com", "a@example-.com", "a@@example.com",
		"a@example.com\n", "<a@example.com>", "a@exa mple.com",
	}
	for _, address := range refused {
		if pattern.MatchString(address) {
			t.Errorf("pattern accepts %q", address)
		}
	}
}
