package sso

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/api/oauthprovider"
)

// TestFetchOAuthUserInfoGoogleUsernameStaysEmpty is a white-box unit test
// of r1's P1 finding (CHAOS-6986): Google's fetch_user_info never sets
// Username at all (nil), and router.py:1229 passes user_info.username
// straight into User(username=...) with no str() coercion -- Python
// stores SQL NULL. An earlier version of fetchOAuthUserInfo called
// oauthprovider.PyStr(info.Username) unconditionally, which turns nil
// into the literal string "None" (pyRepr's own `case nil: return
// "None"`) -- not empty, so it would have stored "None" as EVERY
// auto-provisioned Google user's username, colliding on
// users.username's unique index from the second such user onward.
//
// This is a white-box test (package sso, not sso_test) because there is
// no way to redirect Google's userinfo fetch through a fake server via
// the real HTTP handler chain: oauthDefaultEndpoints hardcodes
// accounts.google.com/oauth2.googleapis.com for every Google provider,
// unlike GitLab's admin-configurable base_url (oauth_integration_test.go's
// own doc comment explains this same constraint for the full round-trip
// tests). Constructing an *oauthprovider.Client with Endpoints.GoogleUser
// pointed at a fake server, then calling fetchOAuthUserInfo directly, is
// the real production code path minus only the token-exchange HTTP hop.
func TestFetchOAuthUserInfoGoogleUsernameStaysEmpty(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		// A real Google userinfo response: no "username" key at all --
		// GoogleOAuthProvider.fetch_user_info never reads or sets one.
		_ = json.NewEncoder(w).Encode(map[string]any{
			"id": "112233445566778899", "email": "googler@example.test", "name": "A Googler", "picture": "https://example.test/pic.jpg",
			"verified_email": true, // D2745 P1-1: fetchOAuthUserInfo now refuses an unverified email.
		})
	}))
	defer server.Close()

	client := &oauthprovider.Client{
		HTTP:      server.Client(),
		Endpoints: oauthprovider.Endpoints{GoogleUser: server.URL},
	}
	claims, err := fetchOAuthUserInfo(context.Background(), client, oauthprovider.Google, "test-access-token")
	if err != nil {
		t.Fatalf("fetchOAuthUserInfo: %v", err)
	}
	if claims.username != "" {
		t.Fatalf("username = %q, want \"\" (a nil Python value must never become the literal string %q)", claims.username, "None")
	}
	if claims.fullName != "A Googler" {
		t.Fatalf("fullName = %q, want %q", claims.fullName, "A Googler")
	}
	if claims.avatarURL != "https://example.test/pic.jpg" {
		t.Fatalf("avatarURL = %q, want the Google 'picture' field (r1 P2: this was never populated before)", claims.avatarURL)
	}
	if claims.externalID != "112233445566778899" {
		t.Fatalf("externalID = %q, want the Google numeric id stringified", claims.externalID)
	}
}
