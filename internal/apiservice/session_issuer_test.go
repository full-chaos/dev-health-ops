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

// The session issuer follows the sessions flag: off builds none (go-api mints), on builds the workload client's issuer,
// and on with no workload credential is a start-up error naming the flag. AUTH_SESSION_ISSUE_TOKEN is never read.
func TestSessionIssuerFollowsTheSessionsFlag(t *testing.T) {
	on, err := ParseAuthProxyConfig(issueEnv(map[string]string{EnvAuthProxyURL: "http://auth-service:8095", "AUTH_PROXY_SESSIONS": "true"}))
	if err != nil {
		t.Fatal(err)
	}
	off, err := ParseAuthProxyConfig(issueEnv(map[string]string{EnvAuthProxyURL: "http://auth-service:8095"}))
	if err != nil {
		t.Fatal(err)
	}
	if issuer, err := sessionIssuer(off, sessionCredential{}); issuer != nil || err != nil {
		t.Fatalf("flag off: %v %v, want go-api to mint", issuer, err)
	}
	if issuer, err := sessionIssuer(on, sessionCredential{}); err == nil || issuer != nil || !strings.Contains(err.Error(), "AUTH_PROXY_SESSIONS") {
		t.Fatalf("flag on without a workload credential: %v %v, want a start-up error naming the flag", issuer, err)
	}
	_, session, err := newCredentials(on, issueEnv(workloadEnv(t, map[string]string{sessionissue.EnvToken: testIssueToken(t)})))
	if err != nil {
		t.Fatal(err)
	}
	if issuer, err := sessionIssuer(on, session); err != nil || issuer == nil {
		t.Fatalf("flag on with the workload credential: %v %v", issuer, err)
	}
}
