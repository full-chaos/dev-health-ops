package social

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
)

// ProfileRequest marks exactly go-api's OAuthUserInfoError of a profile request (oauthprovider get): the request
// for the profile go-api also fetches failed in transport or answered a non-2xx status. A failed token info request
// (go-api makes none), a token of another client, and a profile body go-api cannot decode (its bare 500) are not.
func TestProfileRequestMarksOnlyGoAPIsProfileRequestFailures(t *testing.T) {
	for name, tc := range map[string]struct {
		infoStatus, userStatus int
		owner, userBody        string
		want                   bool
	}{
		"profile 502":           {userStatus: http.StatusBadGateway, want: true},
		"profile 401":           {userStatus: http.StatusUnauthorized, want: true},
		"profile redirect":      {userStatus: http.StatusFound, want: true},
		"token info 502":        {infoStatus: http.StatusBadGateway},
		"token info 401":        {infoStatus: http.StatusUnauthorized},
		"token of another app":  {owner: "other-client"},
		"profile body not JSON": {userBody: "<html>"},
	} {
		t.Run(name, func(t *testing.T) {
			mux := http.NewServeMux()
			reply := func(w http.ResponseWriter, status int, body any) {
				w.Header().Set("Content-Type", "application/json")
				if status != 0 {
					w.WriteHeader(status)
				}
				_ = json.NewEncoder(w).Encode(body)
			}
			owner := tc.owner
			if owner == "" {
				owner = "client-1"
			}
			mux.HandleFunc("GET /oauth/token/info", func(w http.ResponseWriter, _ *http.Request) {
				reply(w, tc.infoStatus, map[string]any{"resource_owner_id": 7, "application": map[string]any{"uid": owner}})
			})
			mux.HandleFunc("GET /api/v4/user", func(w http.ResponseWriter, _ *http.Request) {
				if tc.userBody != "" {
					_, _ = w.Write([]byte(tc.userBody))
					return
				}
				if tc.userStatus == http.StatusFound {
					w.Header().Set("Location", "/elsewhere")
				}
				reply(w, tc.userStatus, map[string]any{"id": 7, "email": "u@example.test"})
			})
			server := httptest.NewServer(mux)
			defer server.Close()
			c := &ProviderClient{HTTP: server.Client(), Endpoints: Endpoints{GitLabBase: server.URL},
				Clients: map[Provider]ClientCredentials{GitLab: NewClientCredentials("client-1", "secret-1")}}
			_, err := c.Fetch(context.Background(), GitLab, contracts.NewSecretString("at-1"))
			var perr *ProviderError
			if !errors.As(err, &perr) {
				t.Fatalf("Fetch error %v, want a *ProviderError", err)
			}
			if perr.ProfileRequest != tc.want {
				t.Fatalf("ProfileRequest = %v (%+v), want %v", perr.ProfileRequest, perr, tc.want)
			}
		})
	}
}
