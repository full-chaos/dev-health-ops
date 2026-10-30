package providerfoundation

import (
	"errors"
	"testing"
)

// PagerDutyStoredRevokeToken must reject what OAuthTokens.model_validate_json
// rejects (Python then skips the revocation) and pick refresh_token or
// access_token otherwise.
func TestPagerDutyRevokeTokenValidatesTheOAuthTokensModel(t *testing.T) {
	cases := []struct {
		name, payload, want string
		wantErr             bool
	}{
		{"access only", `{"access_token":"a","expires_at":"2099-01-01T00:00:00Z"}`, "a", false},
		{"refresh wins", `{"access_token":"a","refresh_token":"r","expires_at":"2099-01-01T00:00:00Z","granted_scopes":["x"]}`, "r", false},
		{"null refresh", `{"access_token":"a","refresh_token":null,"expires_at":"2099-01-01T00:00:00.123456+00:00","granted_scopes":[]}`, "a", false},
		{"empty refresh falls back", `{"access_token":"a","refresh_token":"","expires_at":"2099-01-01T00:00:00Z"}`, "a", false},
		{"epoch expiry", `{"access_token":"a","expires_at":4070908800}`, "a", false},
		{"empty object", `{}`, "", true},
		{"missing expires_at", `{"access_token":"a"}`, "", true},
		{"missing access_token", `{"expires_at":"2099-01-01T00:00:00Z"}`, "", true},
		{"null access_token", `{"access_token":null,"expires_at":"2099-01-01T00:00:00Z"}`, "", true},
		{"access_token wrong type", `{"access_token":5,"expires_at":"2099-01-01T00:00:00Z"}`, "", true},
		{"refresh_token wrong type", `{"access_token":"a","refresh_token":5,"expires_at":"2099-01-01T00:00:00Z"}`, "", true},
		{"expires_at not a date", `{"access_token":"a","expires_at":"soon"}`, "", true},
		{"expires_at null", `{"access_token":"a","expires_at":null}`, "", true},
		{"granted_scopes not a list", `{"access_token":"a","expires_at":"2099-01-01T00:00:00Z","granted_scopes":"x"}`, "", true},
		{"granted_scopes null", `{"access_token":"a","expires_at":"2099-01-01T00:00:00Z","granted_scopes":null}`, "", true},
		{"extra field", `{"access_token":"a","expires_at":"2099-01-01T00:00:00Z","x":1}`, "", true},
		{"not an object", `[]`, "", true},
		{"null", `null`, "", true},
		{"not json", `nope`, "", true},
	}
	for _, tc := range cases {
		got, err := PagerDutyStoredRevokeToken([]byte(tc.payload))
		if (err != nil) != tc.wantErr || got != tc.want {
			t.Errorf("%s: got %q, err %v; want %q, error %v", tc.name, got, err, tc.want, tc.wantErr)
		}
	}
}

// A revocation row is decoded by its key version: v1 is the token itself; oauth-v1 is a stored credential's
// OAuthTokens JSON (refresh_token, else access_token); any other version is refused and nothing is sent.
func TestPagerDutyRevocationTokenDecodesByKeyVersion(t *testing.T) {
	stored := `{"access_token":"acc-1","refresh_token":"ref-1","expires_at":"2026-01-01T00:00:00Z"}`
	accessOnly := `{"access_token":"acc-2","expires_at":"2026-01-01T00:00:00Z"}`
	for _, tc := range []struct {
		version, plaintext, want string
		err                      error
	}{
		{RevocationKeyToken, "raw-token", "raw-token", nil},
		{RevocationKeyToken, stored, stored, nil},
		{RevocationKeyOAuth, stored, "ref-1", nil},
		{RevocationKeyOAuth, accessOnly, "acc-2", nil},
		{"v2", "raw-token", "", ErrRevocationKeyVersion},
		{"", "raw-token", "", ErrRevocationKeyVersion},
	} {
		got, err := PagerDutyRevocationToken(tc.version, []byte(tc.plaintext))
		if got != tc.want || (tc.err != nil && !errors.Is(err, tc.err)) || (tc.err == nil && err != nil) {
			t.Errorf("version %q: got %q, %v; want %q, %v", tc.version, got, err, tc.want, tc.err)
		}
	}
	if got, err := PagerDutyRevocationToken(RevocationKeyOAuth, []byte("raw-token")); err == nil || got != "" {
		t.Errorf("oauth-v1 over a raw token: got %q, %v; want a refusal", got, err)
	}
	if RevocationKeyToken != "v1" || RevocationKeyOAuth != "oauth-v1" {
		t.Error("the key version strings are stored in rows: they never change")
	}
}
