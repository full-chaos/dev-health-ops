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
//   - a golden of the first key version (the closed list): that version keys
//     a per-run name by name whoever supplied its value, and does not hold
//     the variables a test sets in the process. Each such golden is checked
//     by that rule until it is recorded again.
//   - the address a test gives a fake server of its own (the test's per-run
//     names in perRunPythonEnv): it is another one in every run, so it is
//     keyed by name only.
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
	// The names that are never handed on as the test's: the inherited ones
	// (the venue fixes their values itself) and the producer guard's poison,
	// which the harness sets in the process while a producer runs.
	inherited := map[string]bool{producerPoisonName: true}
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

// envEntry is one NAME=VALUE entry of a Python child's environment and who
// supplied its value: the harness, or test code (Options.PythonEnv, the JWT
// key, a call's extra entries, a variable set in the process).
type envEntry struct {
	text   string
	byTest bool
}

func fromHarness(entries ...string) []envEntry { return tagged(entries, false) }

func fromTest(entries ...string) []envEntry { return tagged(entries, true) }

func tagged(entries []string, byTest bool) []envEntry {
	out := make([]envEntry, 0, len(entries))
	for _, entry := range entries {
		out = append(out, envEntry{text: entry, byTest: byTest})
	}
	return out
}

func texts(entries []envEntry) []string {
	out := make([]string, 0, len(entries))
	for _, entry := range entries {
		out = append(out, entry.text)
	}
	return out
}

// perRunName is one name whose value cannot be the same in a recording and
// its replay, who sets that value, and why.
type perRunName struct {
	byTest bool
	reason string
}

// perRunPythonEnv is the ONE closed list of the names that are keyed by name
// only. The rule, closed by default:
//
//   - every entry that reaches a Python child is keyed by name AND value;
//   - an entry is keyed by name only when its name is on this list AND its
//     value was supplied by the side the list names for it: the harness for
//     the values the harness makes for one run or takes from the host, test
//     code for the address of a fake server the test starts;
//   - so a value test code supplies under a harness name (PYTHONPATH,
//     POSTGRES_URI...), through Options.PythonEnv, a call's extra entries or
//     a variable set in the process, is keyed by name and value: it is a
//     declared input, and a per-run value there fails the frozen replay;
//   - a new name, and a new way for an entry to reach the child, is keyed by
//     name and value until someone adds a row here with its reason.
var perRunPythonEnv = map[string]perRunName{
	"CLICKHOUSE_URI": {false, "the address of the run's own ClickHouse database"},
	"HOME":           {false, "inherited: the home directory of the host's user"},
	"PATH":           {false, "inherited: the host's search path, with the interpreter's directory first"},
	"POSTGRES_URI":   {false, "the address of the run's own PostgreSQL database"},
	"PYTHONPATH":     {false, "holds the checkout's path (the venue's site directory and src)"},
	"REDIS_URL":      {false, "the address of the run's own cache"},
	"TMPDIR":         {false, "inherited: the host's directory for temporary files"},

	"REQUESTS_CA_BUNDLE":                  {true, "a temporary certificate file of the test's fake TLS server"},
	"TELEMETRY_ENDPOINT":                  {true, "the address of the test's fake telemetry endpoint"},
	"VENUE_PAGERDUTY_API_BASE_OVERRIDE":   {true, "the address of the test's fake PagerDuty server"},
	"VENUE_PAGERDUTY_REVOKE_URL_OVERRIDE": {true, "the address of the test's fake PagerDuty server"},
	"VENUE_PAGERDUTY_TOKEN_URL_OVERRIDE":  {true, "the address of the test's fake PagerDuty server"},
	"VENUE_PROVIDER_STUB_PORT":            {true, "the port of the test's provider stub"},
	"VENUE_STRIPE_API_BASE":               {true, "the address of the test's fake Stripe server"},
}

// legacyPerRunPythonEnv is perRunPythonEnv as the first key version knew it.
var legacyPerRunPythonEnv = map[string]bool{"CLICKHOUSE_URI": true, "POSTGRES_URI": true, "PYTHONPATH": true, "REDIS_URL": true,
	"REQUESTS_CA_BUNDLE": true, "TELEMETRY_ENDPOINT": true, "VENUE_PAGERDUTY_API_BASE_OVERRIDE": true, "VENUE_PAGERDUTY_REVOKE_URL_OVERRIDE": true,
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
// golden's key is taken over it (planeEntries). It is the inherited names and
// the interpreter's settings (interpreterEnv), the harness's own settings, the
// test's JWT key (Options.JWTKey), and the test's PythonEnv last (a later
// entry wins). perRun holds the values made for one run (the venue's
// databases, cache and checkout, the inherited values); the key needs none of
// them.
func pythonPlaneEnv(options Options, perRun map[string]string) []string {
	return texts(planeEntries(options, perRun))
}

// planeEntries is pythonPlaneEnv with who supplied each value: the JWT key
// and PythonEnv are the test's, the rest is the harness's.
func planeEntries(options Options, perRun map[string]string) []envEntry {
	entries := fromHarness(append(interpreterEnv(perRun), "PYTHONPATH="+perRun["PYTHONPATH"], "POSTGRES_URI="+perRun["POSTGRES_URI"])...)
	entries = append(entries, fromTest("JWT_SECRET_KEY="+options.JWTKey)...)
	entries = append(entries, fromHarness("OTEL_SDK_DISABLED=true", "DEV_HEALTH_ALLOW_CELERY_RIVER_CUTOVER=1",
		// NullPool: TestClient gives each request its own event loop, and a
		// pooled asyncpg connection cannot cross loops.
		"PGBOUNCER_TRANSACTION_MODE=true", "ENVIRONMENT=test", "REDIS_URL="+perRun["REDIS_URL"],
		"CLICKHOUSE_URI="+perRun["CLICKHOUSE_URI"])...)
	return append(entries, fromTest(options.PythonEnv...)...)
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

// venuePythonEnvKey is the key of a venue's Python environment: the variables
// test code had set when the venue was built, then the plane's entries.
func venuePythonEnvKey(testSet []string, options Options) (string, error) {
	return pythonEnvKey(append(fromTest(testSet...), planeEntries(options, nil)...))
}

// planeNames is the names the plane sets itself: a variable of the process
// with one of these names never reaches a Python child (the plane's entry
// comes later and wins), unless a call passes it as an extra entry.
func planeNames(options Options) map[string]bool {
	names := map[string]bool{}
	for _, entry := range planeEntries(options, nil) {
		name, _, _ := strings.Cut(entry.text, "=")
		names[name] = true
	}
	return names
}

// pythonEnvKey is the key of an environment of the Python plane: a digest over
// the entries by name (a later entry of a name wins, as in a process), each
// with its value, or with its name alone under the rule of perRunPythonEnv.
// The values include test credentials, so only the digest is ever stored. An
// empty environment has a key too (the digest over nothing): a golden with no
// key is then always one recorded before the key existed.
func pythonEnvKey(entries []envEntry) (string, error) {
	type held struct {
		value  string
		byTest bool
	}
	values := map[string]held{}
	for _, entry := range entries {
		name, value, found := strings.Cut(entry.text, "=")
		if !found || name == "" {
			return "", fmt.Errorf("venue: PythonEnv entry %q is not NAME=VALUE", entry.text)
		}
		values[name] = held{value, entry.byTest}
	}
	names := make([]string, 0, len(values))
	for name := range values {
		names = append(names, name)
	}
	sort.Strings(names)
	hash := sha256.New()
	for _, name := range names {
		if listed, perRun := perRunPythonEnv[name]; perRun && listed.byTest == values[name].byTest {
			fmt.Fprintf(hash, "%d:%s per-run\n", len(name), name)
			continue
		}
		fmt.Fprintf(hash, "%d:%s=%d:%s\n", len(name), name, len(values[name].value), values[name].value)
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

// legacyPythonEnvKey is the first key version: an entry is keyed by name only
// when its name is one of legacyPerRunPythonEnv, whoever supplied its value.
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
