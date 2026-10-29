package sso

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

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
