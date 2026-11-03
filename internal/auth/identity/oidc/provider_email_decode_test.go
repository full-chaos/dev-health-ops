package oidc

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

// rawGitLab is a self-hosted GitLab whose user endpoint writes emailJSON (the raw JSON value of the email member).
func rawGitLab(t *testing.T, emailJSON string, confirmed bool) *httptest.Server {
	t.Helper()
	confirmedAt := "null"
	if confirmed {
		confirmedAt = `"2026-01-01T00:00:00Z"`
	}
	mux := http.NewServeMux()
	write := func(w http.ResponseWriter, body string) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(body))
	}
	mux.HandleFunc("POST /oauth/token", func(w http.ResponseWriter, _ *http.Request) {
		write(w, `{"access_token": "at-1", "token_type": "bearer"}`)
	})
	mux.HandleFunc("GET /oauth/token/info", func(w http.ResponseWriter, _ *http.Request) {
		write(w, `{"resource_owner_id": 7, "application": {"uid": "client-1"}}`)
	})
	mux.HandleFunc("GET /api/v4/user", func(w http.ResponseWriter, _ *http.Request) {
		write(w, `{"id": 7, "username": "u", "name": "U", "email": `+emailJSON+`, "confirmed_at": `+confirmedAt+`}`)
	})
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	return server
}

// The OAuth callback answers a provider email as the raw profile bytes carry it: a lone surrogate escape and a
// literal U+FFFD are both refused by the email rule (400 invalid_provider_email, verified or not); a profile body
// that is not UTF-8 is a provider failure (400 oauth_userinfo_fetch_failed), as go-api's decoder refuses it. No
// identity store call runs for any of them, linked or not.
func TestOAuthCallbackAnswersTheRawProviderEmail(t *testing.T) {
	const fetchFailed = `{"detail":{"message":"OAuth authentication failed","reason":"oauth_userinfo_fetch_failed"}}`
	for name, tc := range map[string]struct {
		emailJSON string
		want      refusal
	}{
		"lone surrogate escape": {`"e\ud800@example.test"`, refusal{status: http.StatusBadRequest, reason: ReasonMalformed, detail: `{"detail":` + invalidProviderEmailDetail + `}`}},
		"literal U+FFFD":        {"\"e�@example.test\"", refusal{status: http.StatusBadRequest, reason: ReasonMalformed, detail: `{"detail":` + invalidProviderEmailDetail + `}`}},
		"body not UTF-8":        {"\"e\xff@example.test\"", refusal{status: http.StatusBadRequest, reason: ReasonDependency, detail: fetchFailed}},
	} {
		for _, confirmed := range []bool{true, false} {
			label := name
			if !confirmed {
				label += "/unverified"
			}
			t.Run(label, func(t *testing.T) {
				linked, linkedIDs := oauthCallbackRunOn(t, rawGitLab(t, tc.emailJSON, confirmed), true)
				unknown, unknownIDs := oauthCallbackRunOn(t, rawGitLab(t, tc.emailJSON, confirmed), false)
				if linked != unknown || linked != tc.want {
					t.Fatalf("linked %+v, unknown %+v, want %+v", linked, unknown, tc.want)
				}
				if linkedIDs.calls+unknownIDs.calls != 0 {
					t.Fatalf("%d identity store calls ran", linkedIDs.calls+unknownIDs.calls)
				}
			})
		}
	}
}
