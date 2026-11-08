package authapi

import (
	"net/http"
	"strings"
	"testing"
)

// CHAOS-13154 D1: without AUTH_TOKEN_ISSUER_URL auth-service has no access-token verifier, so validate and switch-org
// are not mounted, each with one log line naming the variable; with it both mount (TestValidateCompatC8...).
func TestCompatValidateAndSwitchOrgNotMountedWithoutTheIssuer(t *testing.T) {
	v := newValidateFixture(t, false)
	v.logs.Reset()
	v.build()
	if r := v.post(v.server, tokenBody("any"), nil); r.Code != http.StatusNotFound {
		t.Errorf("validate without the issuer: %d %s, want 404 (not mounted)", r.Code, r.Body)
	}
	if got := v.logs.String(); strings.Count(got, "auth validate route not mounted") != 1 || !strings.Contains(got, "AUTH_TOKEN_ISSUER_URL") {
		t.Errorf("validate not-mounted line: %s", got)
	}
	hs256, err := NewIssuer(testSecret, testIssuer, testAudience)
	if err != nil {
		t.Fatal(err)
	}
	s := newSwitchFixtureWith(t, hs256)
	s.logs.Reset()
	s.build()
	if r := s.post(`{"org_id":"`+s.orgB.String()+`"}`, map[string]string{"Authorization": "Bearer any"}); r.Code != http.StatusNotFound {
		t.Errorf("switch-org without the issuer: %d %s, want 404 (not mounted)", r.Code, r.Body)
	}
	if got := s.logs.String(); strings.Count(got, "auth switch-org route not mounted") != 1 || !strings.Contains(got, "AUTH_TOKEN_ISSUER_URL") {
		t.Errorf("switch-org not-mounted line: %s", got)
	}
}
