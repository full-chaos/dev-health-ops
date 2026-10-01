package venueoracle

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"sort"
	"strings"
)

// The Python plane's environment is explicit. A Python child of the harness
// gets, in this order (a later entry wins): the variables the test process set
// in code (testSetEnv), the plane's own entries (pythonPlaneEnv: the names it
// may inherit, the interpreter's settings, the harness's settings, the test's
// JWT key and PythonEnv), and a call's extra entries. Nothing else of the
// process environment reaches it: not the shell's variables, not the CI
// job's. All of it is in a golden's key (bindPythonEnv, callEnvKey).
//
// Which program runs as the child is fixed with it: the venue's interpreter is
// chosen when the venue is built, every launch goes through pythonCommand,
// which refuses another python3 that a later PATH would give, and the children
// get the inherited names with the values the venue fixed at that moment.
//
// NOT pinned, two limits of telling "set by the test" from "ambient":
//   - a test that sets a variable to the value it already has in the ambient
//     environment is not seen (nothing changed), so the Python plane does not
//     get that variable;
//   - a live venue test that silently relied on an ambient variable of the
//     shell or the CI job loses it; only a run of the gated venue tests shows
//     that. Such a test declares the variable (t.Setenv or PythonEnv).
//   - the interpreter's installed packages: they are the pinned checkout's
//     own environment, built from the lock file of that build, and their
//     bytes are in no key.
//
// NOT covered at all: a Golden.Produce oracle. Its Python child is started by
// the test's own function, this package sets no environment there, and a
// Produce golden holds no key of one. A producer must set a closed
// environment itself and put what shapes its answer into the request
// (ProgramRequest's env).

// processEnvAtInit is the test process's environment when this package was
// initialised, before TestMain and before any test ran: what the shell, the
// CI job or the recorder gave the process. A variable that differs from it
// later was set by test code.
var processEnvAtInit = envMap(os.Environ())

func envMap(env []string) map[string]string {
	out := make(map[string]string, len(env))
	for _, entry := range env {
		if name, value, found := strings.Cut(entry, "="); found && name != "" {
			out[name] = value
		}
	}
	return out
}

// inheritedPythonEnv is the closed list of the names a Python child takes
// from the test process as they are: only what an interpreter needs to start.
// Their values belong to the host, so they are keyed by name (perRunPythonEnv).
// A variable that can change what Python answers is never on this list: it is
// set by value in pythonPlaneEnv, by the test, or not at all.
var inheritedPythonEnv = []string{"HOME", "PATH", "TMPDIR"}

// interpreterPythonEnv is what every Python child runs under, by value: the
// settings of the interpreter that can change its output or its files.
var interpreterPythonEnv = []string{"PYTHONHASHSEED=0", "PYTHONDONTWRITEBYTECODE=1", "TZ=UTC", "LANG=C.UTF-8", "LC_ALL=C.UTF-8"}

// testSetEnv is the variables test code set in the process since this package
// was initialised (t.Setenv, os.Setenv; TestMain included), as NAME=VALUE in
// name order: those whose value differs from processEnvAtInit and those that
// were not in it. The inherited names are left out: the harness puts the
// interpreter's directory on PATH itself.
func testSetEnv() []string { return testSetSince(processEnvAtInit, os.Environ()) }

func testSetSince(before map[string]string, now []string) []string {
	inherited := map[string]bool{}
	for _, name := range inheritedPythonEnv {
		inherited[name] = true
	}
	var out []string
	for name, value := range envMap(now) {
		if earlier, was := before[name]; inherited[name] || (was && earlier == value) {
			continue
		}
		out = append(out, name+"="+value)
	}
	sort.Strings(out)
	return out
}

// testSetDelta is what changed in the test-set variables between start and
// now, as entries for a call's key: a variable set or changed since start with
// its value, and a variable that is gone with a mark instead of a value.
func testSetDelta(start, now []string) []string {
	before, after := envMap(start), envMap(now)
	var out []string
	for name, value := range after {
		if earlier, was := before[name]; !was || earlier != value {
			out = append(out, name+"="+value)
		}
	}
	for name := range before {
		if _, still := after[name]; !still {
			out = append(out, name+"=\x00unset")
		}
	}
	sort.Strings(out)
	return out
}

// perRunPythonEnv is the closed list of the variables of the Python plane whose
// value is made for one run or one host and cannot be the same in a recording
// and its replay: the address of a fake server the test starts, a temporary
// path, the host's own paths. They are keyed by name only. Every other
// variable is keyed by name and value, so a test that hands the Python plane a
// per-run value under a name that is not listed here fails its frozen replay
// until the name is listed with its reason.
var perRunPythonEnv = map[string]string{
	"CLICKHOUSE_URI":                      "the address of the run's own ClickHouse database",
	"HOME":                                "inherited: the home directory of the host's user",
	"PATH":                                "inherited: the host's search path, with the interpreter's directory first",
	"POSTGRES_URI":                        "the address of the run's own PostgreSQL database",
	"PYTHONPATH":                          "holds the checkout's path (the venue's site directory and src)",
	"REDIS_URL":                           "the address of the run's own cache",
	"REQUESTS_CA_BUNDLE":                  "a temporary certificate file of the test's fake TLS server",
	"TMPDIR":                              "inherited: the host's directory for temporary files",
	"VENUE_PAGERDUTY_API_BASE_OVERRIDE":   "the address of the test's fake PagerDuty server",
	"VENUE_PAGERDUTY_REVOKE_URL_OVERRIDE": "the address of the test's fake PagerDuty server",
	"VENUE_PAGERDUTY_TOKEN_URL_OVERRIDE":  "the address of the test's fake PagerDuty server",
	"VENUE_PROVIDER_STUB_PORT":            "the port of the test's provider stub",
	"VENUE_STRIPE_API_BASE":               "the address of the test's fake Stripe server",
}

// legacyPerRunPythonEnv is perRunPythonEnv as the first key version knew it.
var legacyPerRunPythonEnv = map[string]bool{"CLICKHOUSE_URI": true, "POSTGRES_URI": true, "PYTHONPATH": true, "REDIS_URL": true,
	"REQUESTS_CA_BUNDLE": true, "VENUE_PAGERDUTY_API_BASE_OVERRIDE": true, "VENUE_PAGERDUTY_REVOKE_URL_OVERRIDE": true,
	"VENUE_PAGERDUTY_TOKEN_URL_OVERRIDE": true, "VENUE_PROVIDER_STUB_PORT": true, "VENUE_STRIPE_API_BASE": true}

// interpreterEnv is the start of every Python child's environment: the
// inherited names with the values of perRun, then the interpreter's settings.
// With no perRun (the key) every inherited name is there, by name; with one, a
// name the process does not hold is left out.
func interpreterEnv(perRun map[string]string) []string {
	var env []string
	for _, name := range inheritedPythonEnv {
		if value, held := perRun[name]; held || perRun == nil {
			env = append(env, name+"="+value)
		}
	}
	return append(env, interpreterPythonEnv...)
}

// inheritedValues adds to perRun the values the process holds for the
// inherited names.
func inheritedValues(perRun map[string]string) map[string]string {
	for _, name := range inheritedPythonEnv {
		if value, held := os.LookupEnv(name); held {
			perRun[name] = value
		}
	}
	return perRun
}

// pythonPlaneEnv is the environment Start sets for the Python plane, in the
// one place both its users read: Start builds the plane with it, and the
// golden's key is taken over it. It is the inherited names and the
// interpreter's settings (interpreterEnv), the harness's own settings, the
// test's JWT key (Options.JWTKey), and the test's PythonEnv last (a later
// entry wins). perRun holds the values made for one run (the venue's
// databases, cache and checkout, the inherited values); the key needs none of
// them, a per-run name is keyed by name only.
func pythonPlaneEnv(options Options, perRun map[string]string) []string {
	env := append(interpreterEnv(perRun), "PYTHONPATH="+perRun["PYTHONPATH"], "POSTGRES_URI="+perRun["POSTGRES_URI"],
		"JWT_SECRET_KEY="+options.JWTKey, "OTEL_SDK_DISABLED=true", "DEV_HEALTH_ALLOW_CELERY_RIVER_CUTOVER=1",
		// NullPool: TestClient gives each request its own event loop, and a
		// pooled asyncpg connection cannot cross loops.
		"PGBOUNCER_TRANSACTION_MODE=true", "ENVIRONMENT=test", "REDIS_URL="+perRun["REDIS_URL"],
		"CLICKHOUSE_URI="+perRun["CLICKHOUSE_URI"])
	return append(env, options.PythonEnv...)
}

// legacyPlaneEnv is what the first key version was taken over: the entries
// Start set for the Python plane before the child's environment was explicit
// (no inherited names, no interpreter settings, no test-set variables). A
// golden on the closed list of that version (legacy_python_env_goldens.txt) is
// still checked against it, which is the guarantee it was recorded with; it
// goes when the last golden of that list is recorded again.
func legacyPlaneEnv(options Options) []string {
	return append([]string{"PYTHONPATH=", "POSTGRES_URI=",
		"JWT_SECRET_KEY=" + options.JWTKey, "OTEL_SDK_DISABLED=true", "DEV_HEALTH_ALLOW_CELERY_RIVER_CUTOVER=1",
		"PGBOUNCER_TRANSACTION_MODE=true", "ENVIRONMENT=test", "REDIS_URL=", "CLICKHOUSE_URI="},
		options.PythonEnv...)
}

// pythonEnvKeyVersion is the version of the key a golden recorded now holds.
const pythonEnvKeyVersion = 2

// pythonEnvKey is the key of an environment of the Python plane: a digest over
// the entries by name, each with its value, or with its name alone when the
// name is in perRunPythonEnv. The values include test credentials, so only
// the digest is ever stored. An empty environment has a key too (the digest
// over nothing): a golden with no key is then always one recorded before the
// key existed.
func pythonEnvKey(env []string) (string, error) {
	return envKey(env, func(name string) bool { _, perRun := perRunPythonEnv[name]; return perRun })
}

// legacyPythonEnvKey is pythonEnvKey with the per-run names of the first key version.
func legacyPythonEnvKey(env []string) (string, error) {
	return envKey(env, func(name string) bool { return legacyPerRunPythonEnv[name] })
}

func envKey(env []string, perRun func(name string) bool) (string, error) {
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
		if perRun(name) {
			fmt.Fprintf(hash, "%d:%s per-run\n", len(name), name)
			continue
		}
		fmt.Fprintf(hash, "%d:%s=%d:%s\n", len(name), name, len(values[name]), values[name])
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}
