package venueoracle

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
)

// perRunPythonEnv is the closed list of Options.PythonEnv variables whose
// value is made for one run and cannot be the same in a recording and its
// replay: the address of a fake server the test starts, a temporary path.
// They are keyed by name only. Every other variable is keyed by name and
// value, so a test that hands the Python plane a per-run value under a name
// that is not listed here fails its frozen replay until the name is listed
// with its reason.
var perRunPythonEnv = map[string]string{
	"CLICKHOUSE_URI":                      "the address of the run's own ClickHouse database",
	"POSTGRES_URI":                        "the address of the run's own PostgreSQL database",
	"PYTHONPATH":                          "holds the checkout's path (the venue's site directory and src)",
	"REDIS_URL":                           "the address of the run's own cache",
	"REQUESTS_CA_BUNDLE":                  "a temporary certificate file of the test's fake TLS server",
	"VENUE_PAGERDUTY_API_BASE_OVERRIDE":   "the address of the test's fake PagerDuty server",
	"VENUE_PAGERDUTY_REVOKE_URL_OVERRIDE": "the address of the test's fake PagerDuty server",
	"VENUE_PAGERDUTY_TOKEN_URL_OVERRIDE":  "the address of the test's fake PagerDuty server",
	"VENUE_PROVIDER_STUB_PORT":            "the port of the test's provider stub",
	"VENUE_STRIPE_API_BASE":               "the address of the test's fake Stripe server",
}

// pythonPlaneEnv is the whole environment Start hands to the Python plane, in
// the one place both its users read: Start builds the plane from it, and the
// golden's key is taken over it, so nothing reaches the Python plane that the
// key does not hold. It is the harness's own settings, the test's JWT key
// (Options.JWTKey), and the test's PythonEnv last (a later entry wins).
// perRun holds the values made for one run (the venue's databases, cache and
// checkout); the key needs none of them, a per-run name is keyed by name only.
func pythonPlaneEnv(options Options, perRun map[string]string) []string {
	return append([]string{"PYTHONPATH=" + perRun["PYTHONPATH"], "POSTGRES_URI=" + perRun["POSTGRES_URI"],
		"JWT_SECRET_KEY=" + options.JWTKey, "OTEL_SDK_DISABLED=true", "DEV_HEALTH_ALLOW_CELERY_RIVER_CUTOVER=1",
		// NullPool: TestClient gives each request its own event loop, and a
		// pooled asyncpg connection cannot cross loops.
		"PGBOUNCER_TRANSACTION_MODE=true", "ENVIRONMENT=test", "REDIS_URL=" + perRun["REDIS_URL"],
		"CLICKHOUSE_URI=" + perRun["CLICKHOUSE_URI"]},
		options.PythonEnv...)
}

// pythonEnvKey is the key of the Python plane's environment (pythonPlaneEnv):
// a digest over the entries by name, each with its value,
// or with its name alone when the name is in perRunPythonEnv. The values
// include test credentials, so only the digest is ever stored. A venue that
// declares no setting has a key too (the digest over nothing): a golden with
// no key is then always one recorded before the key existed, never one whose
// venue declared nothing.
func pythonEnvKey(env []string) (string, error) {
	values := map[string]string{}
	for _, entry := range env {
		name, value, found := strings.Cut(entry, "=")
		if !found || name == "" {
			return "", fmt.Errorf("venue: PythonEnv entry %q is not NAME=VALUE", entry)
		}
		// A repeated name: the later entry wins, as it does in a process environment.
		values[name] = value
	}
	names := make([]string, 0, len(values))
	for name := range values {
		names = append(names, name)
	}
	sort.Strings(names)
	hash := sha256.New()
	for _, name := range names {
		if _, perRun := perRunPythonEnv[name]; perRun {
			fmt.Fprintf(hash, "%d:%s per-run\n", len(name), name)
			continue
		}
		fmt.Fprintf(hash, "%d:%s=%d:%s\n", len(name), name, len(values[name]), values[name])
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}
