package sso

import (
	"context"
	"log/slog"
	"strings"
	"testing"

	"github.com/google/uuid"
)

// CHAOS-11619: the one point every SSO and OAuth provisioning goes through
// refuses a non-local account with a nil, empty, blank or ValidSubject-invalid
// provider id before it opens a transaction (the pool is nil: any database
// work panics).
func TestCreateAccountRefusesAnInvalidProviderIDBeforeTheDatabase(t *testing.T) {
	h := handlers{Deps{Logger: slog.New(slog.DiscardHandler)}}
	row := &providerRow{OrgID: uuid.NewString()}
	ptr := func(s string) *string { return &s }
	for _, provider := range []string{"oidc", "saml", "github", "gitlab", "google"} {
		for name, id := range map[string]*string{"nil": nil, "empty": ptr(""), "blank": ptr("  "), "edge whitespace": ptr(" MARKER77"), "control": ptr("7\x01MARKER77")} {
			t.Run(provider+"/"+name, func(t *testing.T) {
				var err error
				func() {
					defer func() {
						if recovered := recover(); recovered != nil {
							t.Fatalf("went on to the database with an invalid provider id: %v", recovered)
						}
					}()
					_, _, err = h.createAccountAndMembership(context.Background(), row,
						ssoNewAccount{email: "a@example.test", authProvider: provider, externalID: id})
				}()
				if err == nil {
					t.Fatal("no refusal")
				}
				if strings.Contains(err.Error(), "MARKER77") {
					t.Fatalf("the refusal carries the id: %v", err)
				}
			})
		}
	}
}

// A valid subject is not refused: the call goes on to the database (control).
func TestCreateAccountGoesOnWithAValidProviderID(t *testing.T) {
	h := handlers{Deps{Logger: slog.New(slog.DiscardHandler)}}
	row := &providerRow{OrgID: uuid.NewString()}
	id := "12345"
	for _, provider := range []string{"oidc", "saml", "github"} {
		reached := false
		func() {
			defer func() { reached = recover() != nil }()
			_, _, _ = h.createAccountAndMembership(context.Background(), row,
				ssoNewAccount{email: "a@example.test", authProvider: provider, externalID: &id})
		}()
		if !reached {
			t.Errorf("%s: refused before the database", provider)
		}
	}
}
