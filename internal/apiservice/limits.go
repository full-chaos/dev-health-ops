package apiservice

import (
	"errors"
	"strings"

	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/auth/ratelimitvalkey"
)

// limitStore is the HitStore every limited route counts through: an explicit
// Deps.Limits (a test), else the shared Valkey-backed store when the api has
// a Valkey client (so a limit holds across api replicas, as it does on the
// Python api through REDIS_URL), else the in-process store (development and
// tests; per process, so per replica).
func limitStore(deps Deps) (httpapi.HitStore, error) {
	if deps.Limits != nil {
		return deps.Limits, nil
	}
	if deps.Valkey != nil {
		return ratelimitvalkey.New(deps.Valkey)
	}
	return httpapi.NewMemoryStore(deps.Now), nil
}

// developmentEnvironment is rate_limit.py's _is_dev_or_test: ENVIRONMENT,
// else APP_ENV, else ENV, else "production", compared case-insensitively
// with development, dev, local, test and testing.
func developmentEnvironment(lookup func(string) (string, bool)) bool {
	for _, name := range []string{"ENVIRONMENT", "APP_ENV", "ENV"} {
		if value, ok := lookup(name); ok && value != "" {
			switch strings.ToLower(strings.TrimSpace(value)) {
			case "development", "dev", "local", "test", "testing":
				return true
			}
			return false
		}
	}
	return false
}

// errSharedLimiterRequired is verify_rate_limit_config's refusal: outside
// development an in-process limiter is per replica and so no limit at all.
var errSharedLimiterRequired = errors.New(
	"VALKEY_URI must be set outside development: the in-process rate limiter is per process and ineffective across api replicas; " +
		"set VALKEY_URI, or set ENVIRONMENT=development to suppress")
