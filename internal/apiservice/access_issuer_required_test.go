package apiservice

import (
	"strings"
	"testing"
)

// CHAOS-13154 D1: with a pool (protected routes) go-api has no access-token verifier but the issuer's, so a start
// without AUTH_ACCESS_TOKEN_ISSUER is refused with one error naming it. With no pool there is no protected route and
// go-api starts with no verifier.
func TestBuildAccessVerify_IssuerRequiredOnlyWithAPool(t *testing.T) {
	protected, err := newProtection(apiTestConfig(), nil, false, quietLogger())
	if err != nil {
		t.Fatal(err)
	}
	for name, lookup := range map[string]func(string) (string, bool){
		"unset": func(string) (string, bool) { return "", false },
		"blank": func(string) (string, bool) { return "  ", true },
	} {
		if _, _, err := buildAccessVerify(lookup, protected.auth, quietLogger()); err == nil || !strings.Contains(err.Error(), envAccessTokenIssuer) {
			t.Errorf("pool, issuer %s: err=%v, want a start error naming %s", name, err, envAccessTokenIssuer)
		}
		if validator, keys, err := buildAccessVerify(lookup, nil, quietLogger()); err != nil || validator != nil || keys != nil {
			t.Errorf("no pool, issuer %s: validator %v keys %v err %v, want a start with no verifier", name, validator != nil, keys != nil, err)
		}
	}
}
