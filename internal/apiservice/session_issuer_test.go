package apiservice

import (
	"crypto/rand"
	"encoding/base64"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/api/sessionissue"
)

func issueEnv(values map[string]string) func(string) (string, bool) {
	return func(key string) (string, bool) {
		value, ok := values[key]
		return value, ok
	}
}

// testIssueToken is an auth-session-issuer credential built at run time.
func testIssueToken(t *testing.T) string {
	t.Helper()
	random := make([]byte, 32)
	if _, err := rand.Read(random); err != nil {
		t.Fatal(err)
	}
	return "svc_authsess_" + base64.RawURLEncoding.EncodeToString(random)
}

func TestSessionIssuerFollowsTheSessionsFlag(t *testing.T) {
	token := testIssueToken(t)
	on, err := ParseAuthProxyConfig(issueEnv(map[string]string{EnvAuthProxyURL: "http://auth-service:8095", "AUTH_PROXY_SESSIONS": "true"}))
	if err != nil {
		t.Fatal(err)
	}
	off, err := ParseAuthProxyConfig(issueEnv(map[string]string{EnvAuthProxyURL: "http://auth-service:8095"}))
	if err != nil {
		t.Fatal(err)
	}

	if issuer, err := sessionIssuer(off, issueEnv(map[string]string{sessionissue.EnvToken: token})); issuer != nil || err != nil {
		t.Fatalf("flag off: %v %v, want go-api to mint", issuer, err)
	}
	issuer, err := sessionIssuer(on, issueEnv(map[string]string{sessionissue.EnvToken: " " + token + "\n"}))
	if err != nil || issuer == nil {
		t.Fatalf("flag on with a credential: %v %v", issuer, err)
	}
	for name, env := range map[string]map[string]string{
		"no credential":  {},
		"empty":          {sessionissue.EnvToken: ""},
		"acr credential": {sessionissue.EnvToken: "svc_acr_" + token[len("svc_authsess_"):]},
	} {
		issuer, err := sessionIssuer(on, issueEnv(env))
		if err == nil || issuer != nil {
			t.Errorf("flag on, %s: %v %v, want a start-up error", name, issuer, err)
			continue
		}
		if !strings.Contains(err.Error(), "AUTH_PROXY_SESSIONS") || strings.Contains(err.Error(), token[len("svc_authsess_"):]) {
			t.Errorf("flag on, %s: error %q must name the flag and never the credential", name, err)
		}
	}
}
