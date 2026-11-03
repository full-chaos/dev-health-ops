package oidc

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

// rawGitLabUser is a self-hosted GitLab whose user endpoint writes userJSON as it is.
func rawGitLabUser(t *testing.T, userJSON string) *httptest.Server {
	t.Helper()
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
	mux.HandleFunc("GET /api/v4/user", func(w http.ResponseWriter, _ *http.Request) { write(w, userJSON) })
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	return server
}

// The OAuth callback answers the provider email member as the social client reads it: no "email" member (or one under
// "Email" only) is the client's refusal of the token, and a member that is not a JSON string is a provider failure;
// both are 400 oauth_userinfo_fetch_failed, linked or not, with no identity store call.
func TestOAuthCallbackAnswersTheEmailMember(t *testing.T) {
	const fetchFailed = `{"detail":{"message":"OAuth authentication failed","reason":"oauth_userinfo_fetch_failed"}}`
	const confirmed = `"confirmed_at": "2026-01-01T00:00:00Z"`
	for name, tc := range map[string]struct {
		user string
		want refusal
	}{
		"no email member":       {`{"id": 7, "username": "u", "name": "U", ` + confirmed + `}`, refusal{http.StatusBadRequest, ReasonInvalidCredentials, fetchFailed}},
		"email under Email":     {`{"id": 7, "username": "u", "name": "U", "Email": "u@example.test", ` + confirmed + `}`, refusal{http.StatusBadRequest, ReasonInvalidCredentials, fetchFailed}},
		"email a number":        {`{"id": 7, "username": "u", "name": "U", "email": 7, ` + confirmed + `}`, refusal{http.StatusBadRequest, ReasonDependency, fetchFailed}},
		"email null, confirmed": {`{"id": 7, "username": "u", "name": "U", "email": null, ` + confirmed + `}`, refusal{http.StatusBadRequest, ReasonDependency, fetchFailed}},
	} {
		t.Run(name, func(t *testing.T) {
			linked, linkedIDs := oauthCallbackRunOn(t, rawGitLabUser(t, tc.user), true)
			unknown, unknownIDs := oauthCallbackRunOn(t, rawGitLabUser(t, tc.user), false)
			if linked != unknown || linked != tc.want {
				t.Fatalf("linked %+v, unknown %+v, want %+v", linked, unknown, tc.want)
			}
			if linkedIDs.calls+unknownIDs.calls != 0 {
				t.Fatalf("%d identity store calls ran", linkedIDs.calls+unknownIDs.calls)
			}
		})
	}
}
