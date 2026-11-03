package sso

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/oauthprovider"
)

// The OAuth profile fetch applies the provider email rule before the verified-email check: a lone surrogate (which
// the profile reader turns into "unverified") and every other refused form answer invalid_provider_email, confirmed
// or not, never the no-verified-email refusal.
func TestFetchOAuthUserInfoRefusesAnUnusableEmailBeforeTheVerifiedCheck(t *testing.T) {
	for name, email := range map[string]string{
		"lone surrogate":   subjectMarker + `\ud800@example.test`,
		"nul":              subjectMarker + `\u0000@example.test`,
		"control":          subjectMarker + `\u0001@example.test`,
		"c1":               subjectMarker + `\u0085x@example.test`,
		"replacement char": subjectMarker + `�@example.test`,
	} {
		for _, confirmed := range []string{`"2026-01-01T00:00:00Z"`, `null`} {
			t.Run(name+"/confirmed="+confirmed, func(t *testing.T) {
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
					w.Header().Set("Content-Type", "application/json")
					_, _ = w.Write([]byte(`{"id": 7, "email": "` + email + `", "username": "u", "name": "U", "confirmed_at": ` + confirmed + `}`))
				}))
				defer server.Close()
				client := &oauthprovider.Client{HTTP: server.Client(), Endpoints: oauthprovider.Endpoints{GitLabBase: server.URL}}
				_, err := fetchOAuthUserInfo(context.Background(), client, oauthprovider.GitLab, "test-access-token")
				if !errors.Is(err, errInvalidProviderEmail) {
					t.Fatalf("err = %v, want errInvalidProviderEmail", err)
				}
			})
		}
	}
}

// The allowlist check runs after the subject and email rules and before any statement: an unusable address whose
// domain is not on the list is the email rule's refusal; a usable one on another domain is refused with a typed error
// whose text names no domain.
func TestProvisionOrGetUserChecksTheAllowlistAfterTheEmailRule(t *testing.T) {
	domains := `["allowed.example"]`
	for _, protocol := range []string{"oidc", "saml"} {
		t.Run(protocol, func(t *testing.T) {
			row := &providerRow{OrgID: uuid.NewString(), Protocol: protocol, AutoProvision: true, AllowedDomains: &domains}
			h, dials, logs := subjectGuardHandlers(t)
			_, _, err := h.provisionOrGetUser(context.Background(), row, subjectMarker+"\u0085x@other.example", "A", uuid.New(), "12345", nil)
			assertEmailRefusedBeforeAnyStatement(t, err, dials.Load(), logs.String())

			h, dials, _ = subjectGuardHandlers(t)
			_, _, err = h.provisionOrGetUser(context.Background(), row, "person@other.example", "A", uuid.New(), "12345", nil)
			var denied *domainNotAllowedError
			if !errors.As(err, &denied) || denied.domain != "other.example" || strings.Contains(err.Error(), "other.example") {
				t.Fatalf("err = %v, want a domainNotAllowedError whose text names no domain", err)
			}
			if dials.Load() != 0 {
				t.Fatal("a statement ran before the domain refusal")
			}

			h, dials, _ = subjectGuardHandlers(t)
			_, _, err = h.provisionOrGetUser(context.Background(), row, "no-at-sign", "A", uuid.New(), "12345", nil)
			if !errors.Is(err, errProviderEmailMalformed) || dials.Load() != 0 {
				t.Fatalf("err = %v with %d statements, want errProviderEmailMalformed before any statement", err, dials.Load())
			}
		})
	}
}
