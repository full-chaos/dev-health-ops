package authruntime

import (
	"encoding/base64"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/auth/authapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/authconfig"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
)

// testAccessSecret and testSuccessorKey are made up for the tests here; they
// sign nothing outside a test process.
var (
	testAccessSecret = strings.Repeat("session-route-test-key-", 2)
	testSuccessorKey = base64.RawURLEncoding.EncodeToString([]byte("session-route-successor-key-0032"))
	testLockoutKey   = base64.RawURLEncoding.EncodeToString([]byte("session-route-lockout-key-000032"))
	testValkeyURI    = "redis://auth-ratelimit:test-only-password@127.0.0.1:1/1"
)

// sessionEnvironment adds the session secrets to env, which mounts the
// session routes. The loopback peer of a test server is the one trusted proxy.
func sessionEnvironment(env map[string]string) map[string]string {
	env[authconfig.EnvAccessTokenSecret] = testAccessSecret
	env[authconfig.EnvSessionSuccessorKey] = testSuccessorKey
	env[authconfig.EnvTrustedProxies] = "127.0.0.1"
	env[authconfig.EnvValkeyURI] = testValkeyURI
	return env
}

// compatEnvironment is sessionEnvironment with the login route mounted too.
func compatEnvironment(env map[string]string) map[string]string {
	env = sessionEnvironment(env)
	env[authconfig.EnvLoginLockoutKey] = testLockoutKey
	return env
}

// mustLimits is the in-process limits a test mounts the limited routes with.
func mustLimits(t *testing.T) *authapi.Limits {
	t.Helper()
	limits, err := authapi.NewLimits(httpapi.NewMemoryCounters(nil), testAccessSecret)
	if err != nil {
		t.Fatal(err)
	}
	return limits
}
