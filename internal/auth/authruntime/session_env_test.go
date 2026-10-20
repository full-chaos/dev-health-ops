package authruntime

import (
	"encoding/base64"
	"strings"

	"github.com/full-chaos/dev-health-ops/internal/auth/authconfig"
)

// testAccessSecret and testSuccessorKey are made up for the tests here; they
// sign nothing outside a test process.
var (
	testAccessSecret = strings.Repeat("session-route-test-key-", 2)
	testSuccessorKey = base64.RawURLEncoding.EncodeToString([]byte("session-route-successor-key-0032"))
	testLockoutKey   = base64.RawURLEncoding.EncodeToString([]byte("session-route-lockout-key-000032"))
)

// sessionEnvironment adds the session secrets and the login lockout key to
// env, which mounts every route the service serves. The loopback peer of a
// test server is the one trusted proxy.
func sessionEnvironment(env map[string]string) map[string]string {
	env[authconfig.EnvAccessTokenSecret] = testAccessSecret
	env[authconfig.EnvSessionSuccessorKey] = testSuccessorKey
	env[authconfig.EnvLoginLockoutKey] = testLockoutKey
	env[authconfig.EnvTrustedProxies] = "127.0.0.1"
	return env
}
