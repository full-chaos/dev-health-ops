package authapi

import (
	"net/http"
	"strings"
	"testing"
)

// auth-service reads its bodies with pybody.Read, not the go-api boundary
// (pybody.ReadChecked): a lone surrogate in an unbounded field is still kept here.
// The answers below are the ones main gave before CHAOS-12282 (harm cells).
func TestAuthServiceKeepsItsAnswersForALoneSurrogate(t *testing.T) {
	cases := []struct {
		name, path, body string
		status           int
		contains         string
	}{
		{"refresh_token", PatternCompatRefresh, `{"refresh_token":"\ud800"}`, http.StatusUnauthorized, "Invalid or expired refresh token"},
		{"logout refresh_token", PatternCompatLogout, `{"refresh_token":"a\udc00b"}`, http.StatusOK, "Logout successful"},
		{"login org_id", PatternCompatLogin, `{"email":"alice@example.com","password":"correct horse","org_id":"\ud800"}`, http.StatusOK, "access_token"},
		{"login password (bounded)", PatternCompatLogin, `{"email":"alice@example.com","password":"\ud800"}`, http.StatusUnprocessableEntity, `"string_unicode"`},
		{"social provider (pattern)", PatternCompatSocialLogin, `{"provider":"\ud800","provider_access_token":"x"}`, http.StatusUnprocessableEntity, `"string_pattern_mismatch"`},
	}
	for _, c := range cases {
		f, _ := newSocialFixture(t)
		r := f.do(http.MethodPost, c.path, c.body, map[string]string{"Content-Type": "application/json"})
		if r.Code != c.status || !strings.Contains(r.Body.String(), c.contains) {
			t.Errorf("%s: %d %.200s, want %d containing %q", c.name, r.Code, r.Body.String(), c.status, c.contains)
		}
	}
}
