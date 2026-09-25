package admin

import "testing"

// pagerDutyRevokeToken must reject what OAuthTokens.model_validate_json
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
		got, err := pagerDutyRevokeToken([]byte(tc.payload))
		if (err != nil) != tc.wantErr || got != tc.want {
			t.Errorf("%s: got %q, err %v; want %q, error %v", tc.name, got, err, tc.want, tc.wantErr)
		}
	}
}
