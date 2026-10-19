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
)

// sessionEnvironment adds the session secrets to env, which mounts the
// session routes.
func sessionEnvironment(env map[string]string) map[string]string {
	env[authconfig.EnvAccessTokenSecret] = testAccessSecret
	env[authconfig.EnvSessionSuccessorKey] = testSuccessorKey
	return env
}
