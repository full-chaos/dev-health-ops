package venueoracle

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

func TestThePythonEnvKeyHoldsEveryDeclaredSettingAndThePerRunNamesOnly(t *testing.T) {
	base := []string{"VENUE_PINNED_NOW=2026-01-01T00:00:00Z", "SETTINGS_ENCRYPTION_KEY=k", "VENUE_STRIPE_API_BASE=http://127.0.0.1:41001"}
	key := func(env []string) string {
		t.Helper()
		out, err := pythonEnvKey(fromTest(env...))
		if err != nil {
			t.Fatal(err)
		}
		return out
	}
	want := key(base)
	if len(want) != 64 {
		t.Fatalf("key %q", want)
	}
	for name, c := range map[string]struct {
		env  []string
		same bool
	}{
		"another order":                    {[]string{base[2], base[0], base[1]}, true},
		"another address of a fake server": {[]string{base[0], base[1], "VENUE_STRIPE_API_BASE=http://127.0.0.1:55555"}, true},
		"a changed value":                  {[]string{"VENUE_PINNED_NOW=2099-01-01T00:00:00Z", base[1], base[2]}, false},
		"a variable more":                  {append(append([]string{}, base...), "TRIAL_DAYS=7"), false},
		"a variable less":                  {base[:2], false},
		"a per-run variable less":          {base[:2], false},
		"a name in another case":           {[]string{"venue_pinned_now=2026-01-01T00:00:00Z", base[1], base[2]}, false},
		"an empty value":                   {[]string{"VENUE_PINNED_NOW=", base[1], base[2]}, false},
		"name and value moved":             {[]string{"VENUE_PINNED_NOW=2026-01-01T00:00:00ZSETTINGS", "_ENCRYPTION_KEY=k", base[2]}, false},
		"a later entry of one name wins":   {[]string{"VENUE_PINNED_NOW=other", base[0], base[1], base[2]}, true},
	} {
		if got := key(c.env); (got == want) != c.same {
			t.Errorf("%s: key equal = %v, want %v", name, got == want, c.same)
		}
	}
	// No setting is a key of its own, not the absence of a key.
	if got := key(nil); len(got) != 64 || got == want || got != key([]string{}) {
		t.Errorf("no settings: key %q", got)
	}
	for _, bad := range []string{"NOEQUALS", "=value"} {
		if _, err := pythonEnvKey(fromTest(bad)); err == nil {
			t.Errorf("%q was keyed", bad)
		}
	}
	// A per-run name that was never executed as one would hide a setting: every listed name has a reason.
	for name, why := range perRunPythonEnv {
		if strings.TrimSpace(why.reason) == "" || strings.TrimSpace(name) == "" {
			t.Errorf("perRunPythonEnv[%q] has no reason", name)
		}
	}
}

// The rule of perRunPythonEnv, on every way an entry reaches a Python child:
// an entry is keyed by name and value unless its name is listed AND the listed
// side supplied the value. Each harness name and each of the test's per-run
// names is tried on each channel, with one unlisted name beside them.
func TestAnEntryIsKeyedByNameOnlyWhenTheListedSideSuppliedIt(t *testing.T) {
	options := Options{JWTKey: "k"}
	var harnessNames, testNames []string
	for name, listed := range perRunPythonEnv {
		if listed.byTest {
			testNames = append(testNames, name)
		} else {
			harnessNames = append(harnessNames, name)
		}
	}
	sort.Strings(harnessNames)
	sort.Strings(testNames)
	if len(harnessNames) == 0 || len(testNames) == 0 {
		t.Fatalf("the list holds %d harness names and %d names of the test: both rules need a name to be tried", len(harnessNames), len(testNames))
	}
	const unlisted = "A_SETTING_NOBODY_LISTED"
	must := func(key string, err error) string {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
		return key
	}
	// same reports whether two values of name give one key on a channel.
	check := func(channel, name string, wantSame bool, key func(entry string) string) {
		t.Helper()
		if same := key(name+"=/first") == key(name+"=/second"); same != wantSame {
			t.Errorf("%s, %s: two values give the same key = %v, want %v", channel, name, same, wantSame)
		}
	}
	base := must(venuePythonEnvKey(nil, options))

	// Channel 1: the test's declared settings (Options.PythonEnv).
	declared := func(entry string) string {
		return must(venuePythonEnvKey(nil, Options{JWTKey: "k", PythonEnv: []string{entry}}))
	}
	for _, name := range harnessNames {
		check("Options.PythonEnv", name, false, declared)
		if declared(name+"=/first") == base {
			t.Errorf("Options.PythonEnv, %s: the test's value is keyed as if the harness had set it", name)
		}
	}
	for _, name := range testNames {
		check("Options.PythonEnv", name, true, declared)
	}
	check("Options.PythonEnv", unlisted, false, declared)

	// Channel 2: a variable test code set in the process before the venue was built.
	inProcess := func(entry string) string { return must(venuePythonEnvKey([]string{entry}, options)) }
	planeSets := planeNames(options)
	for _, name := range harnessNames {
		if !planeSets[name] {
			t.Errorf("%s is a harness name the plane does not set: a process variable of that name would reach the child", name)
			continue
		}
		// The plane's own entry comes later and wins: the variable does not reach the child.
		check("process variable", name, true, inProcess)
		if inProcess(name+"=/first") != base {
			t.Errorf("process variable, %s: a variable the plane overwrites changed the key", name)
		}
	}
	for _, name := range testNames {
		check("process variable", name, true, inProcess)
		if inProcess(name+"=/first") == base {
			t.Errorf("process variable, %s: its presence is not in the key", name)
		}
	}
	check("process variable", unlisted, false, inProcess)

	// Channels 3 and 4: one call's extra entries, and a variable test code
	// sets in the process after the venue was built, through the golden.
	golden, err := openGolden(GoldenSpec{Path: t.TempDir() + "/g.json", PythonBuild: goldenBuild, Recipe: "record it"}, "TestSample", true)
	if err != nil {
		t.Fatal(err)
	}
	if err := golden.bindPythonEnv(options); err != nil {
		t.Fatal(err)
	}
	extra := func(entry string) string { return must(golden.callEnvKey([]string{entry})) }
	for _, name := range harnessNames {
		check("a call's extra entry", name, false, extra)
	}
	for _, name := range testNames {
		check("a call's extra entry", name, true, extra)
	}
	check("a call's extra entry", unlisted, false, extra)

	later := func(name string) func(entry string) string {
		return func(entry string) string {
			_, value, _ := strings.Cut(entry, "=")
			t.Setenv(name, value)
			defer os.Unsetenv(name)
			return must(golden.callEnvKey(nil))
		}
	}
	for _, name := range harnessNames {
		if name == "HOME" || name == "PATH" || name == "TMPDIR" {
			continue // the children get these as the venue fixed them (hostEnv), see TestThePythonChildSeesOnlyItsExplicitEnvironment
		}
		// The plane's own entry wins: the variable does not reach the child, so it is in no call key.
		if key := later(name)(name + "=/first"); key != "" {
			t.Errorf("a variable set after the venue was built, %s: the plane overwrites it and the call key is %q, want none", name, key)
		}
	}
	for _, name := range testNames {
		check("a variable set after the venue was built", name, true, later(name))
		if later(name)(name+"=/first") == "" {
			t.Errorf("a variable set after the venue was built, %s: it is in no call key", name)
		}
	}
	check("a variable set after the venue was built", unlisted, false, later(unlisted))

	// The harness's own channel: its per-run values are keyed by name, every
	// other entry it sets by value, and a name of the test's list is not
	// name-only when the harness supplies it.
	fromSide := func(byTest bool) func(entry string) string {
		return func(entry string) string { return must(pythonEnvKey(tagged([]string{entry}, byTest))) }
	}
	for _, name := range harnessNames {
		check("the harness's entry", name, true, fromSide(false))
		check("the same name from the test", name, false, fromSide(true))
	}
	for _, name := range testNames {
		check("the harness's entry", name, false, fromSide(false))
		check("the same name from the test", name, true, fromSide(true))
	}
	check("the harness's entry", unlisted, false, fromSide(false))
}

// frozenWithEnv writes a frozen golden whose header holds key in the given key
// version (0 = the first version, no mark) and opens it.
func frozenWithEnv(t *testing.T, key string, version int) (*Golden, string) {
	t.Helper()
	request := ProgramRequest("corpus", sampleProgram, []byte("abc"), nil)
	entry := requestKey(request)
	entry.Body = "ABC\n"
	file := goldenFile{Header: goldenHeader{Test: "TestSample", PythonBuild: goldenBuild, ProducerDigest: strings.Repeat("a", 64), Recipe: "record it", PythonEnv: key, PythonEnvVersion: version}, Requests: []goldenRequest{entry}}
	path, digest := writeGoldenFile(t, t.TempDir(), file)
	golden, err := openGolden(GoldenSpec{Path: path, PythonBuild: goldenBuild, SHA256: digest, Recipe: "record it"}, "TestSample", false)
	if err != nil {
		t.Fatal(err)
	}
	return golden, path
}

// currentKey is the key a golden recorded now holds for options, under the
// variables this test process has set.
func currentKey(t *testing.T, options Options) string {
	t.Helper()
	key, err := venuePythonEnvKey(testSetEnv(), options)
	if err != nil {
		t.Fatal(err)
	}
	return key
}

func legacyKey(t *testing.T, options Options) string {
	t.Helper()
	key, err := legacyPythonEnvKey(legacyPlaneEnv(options))
	if err != nil {
		t.Fatal(err)
	}
	return key
}

func TestAFrozenGoldenIsBoundToItsVenuesPythonEnvironment(t *testing.T) {
	declared := Options{JWTKey: "recorded", PythonEnv: []string{"VENUE_PINNED_NOW=2026-01-01T00:00:00Z", "SETTINGS_ENCRYPTION_KEY=k"}}
	key := currentKey(t, declared)

	// Recording: the key of the current version goes into the header, with its version.
	recording, err := openGolden(GoldenSpec{Path: t.TempDir() + "/g.json", PythonBuild: goldenBuild, Recipe: "record it"}, "TestSample", true)
	if err != nil {
		t.Fatal(err)
	}
	if err := recording.bindPythonEnv(declared); err != nil || recording.recorded.Header.PythonEnv != key || recording.recorded.Header.PythonEnvVersion != pythonEnvKeyVersion {
		t.Fatalf("recording: err %v, header key %q version %d, want %q version %d", err, recording.recorded.Header.PythonEnv, recording.recorded.Header.PythonEnvVersion, key, pythonEnvKeyVersion)
	}
	if err := recording.bindPythonEnv(Options{JWTKey: "recorded"}); err == nil || !strings.Contains(err.Error(), "two venues") {
		t.Fatalf("a second venue with another environment was bound to one golden: %v", err)
	}

	other := "recorded under another Python environment"
	for name, c := range map[string]struct {
		recorded string
		version  int
		options  Options
		refusal  string // "" = accepted
	}{
		"the environment the golden was recorded under": {key, 2, declared, ""},
		"the same entries in another order":             {key, 2, Options{JWTKey: "recorded", PythonEnv: []string{declared.PythonEnv[1], declared.PythonEnv[0]}}, ""},
		"a changed value (the pinned clock)":            {key, 2, Options{JWTKey: "recorded", PythonEnv: []string{"VENUE_PINNED_NOW=2099-01-01T00:00:00Z", declared.PythonEnv[1]}}, other},
		"a variable more":                               {key, 2, Options{JWTKey: "recorded", PythonEnv: append(append([]string{}, declared.PythonEnv...), "TRIAL_DAYS=7")}, other},
		"a variable less":                               {key, 2, Options{JWTKey: "recorded", PythonEnv: declared.PythonEnv[:1]}, other},
		"another JWT key":                               {key, 2, Options{JWTKey: "other", PythonEnv: declared.PythonEnv}, other},
		"another interpreter setting":                   {key, 2, Options{JWTKey: "recorded", PythonEnv: append(append([]string{}, declared.PythonEnv...), "PYTHONHASHSEED=7")}, other},
		"a golden with no key":                          {"", 2, declared, "holds no key"},
		"a key version this harness does not know":      {key, 3, declared, "version 3, which this harness does not know"},
		// The first key version: what the harness and the test declared.
		"first version: the declared settings":      {legacyKey(t, declared), 0, declared, ""},
		"first version: another JWT key":            {legacyKey(t, declared), 0, Options{JWTKey: "other", PythonEnv: declared.PythonEnv}, other},
		"first version: a changed value":            {legacyKey(t, declared), 0, Options{JWTKey: "recorded", PythonEnv: declared.PythonEnv[:1]}, other},
		"first version: no key":                     {"", 0, declared, "holds no key"},
		"first version: a current key in its place": {key, 0, declared, other},
		"current version: a first-version key":      {legacyKey(t, declared), 2, declared, other},
	} {
		golden, _ := frozenWithEnv(t, c.recorded, c.version)
		err := golden.bindPythonEnv(c.options)
		if c.refusal == "" && err != nil {
			t.Errorf("%s: refused: %v", name, err)
		}
		if c.refusal != "" && (err == nil || !strings.Contains(err.Error(), c.refusal)) {
			t.Errorf("%s: err = %v, want a refusal holding %q", name, err, c.refusal)
		}
	}

	// A variable the test sets in the process is part of the current key, and of no first-version key.
	t.Run("a variable set by the test", func(t *testing.T) {
		t.Setenv("TRUSTED_PROXIES", "127.0.0.1")
		if currentKey(t, declared) == key {
			t.Fatal("a variable the test set is not in the key")
		}
		golden, _ := frozenWithEnv(t, key, 2)
		if err := golden.bindPythonEnv(declared); err == nil || !strings.Contains(err.Error(), other) {
			t.Fatalf("a golden recorded without the variable was accepted: %v", err)
		}
		legacy, _ := frozenWithEnv(t, legacyKey(t, declared), 0)
		if err := legacy.bindPythonEnv(declared); err != nil {
			t.Fatalf("a first-version golden was refused for a variable its version does not hold: %v", err)
		}
	})

	// A golden recorded under a venue's environment, in a test that builds no venue for it.
	golden, _ := frozenWithEnv(t, key, 2)
	if err := golden.pythonEnvUnboundErr(); err == nil || !strings.Contains(err.Error(), "built no venue") {
		t.Fatalf("a golden with a key and no venue was accepted: %v", err)
	}
	if err := golden.bindPythonEnv(declared); err != nil {
		t.Fatal(err)
	}
	if err := golden.pythonEnvUnboundErr(); err != nil {
		t.Fatalf("a bound golden was refused: %v", err)
	}
	// A second venue of the same test with the same environment uses the same golden.
	if err := golden.bindPythonEnv(Options{JWTKey: "recorded", PythonEnv: []string{declared.PythonEnv[1], declared.PythonEnv[0]}}); err != nil {
		t.Fatalf("a second venue with the same environment was refused: %v", err)
	}
}

func TestTheVariablesATestSetAreToldFromTheAmbientOnes(t *testing.T) {
	before := map[string]string{"AMBIENT": "a", "CHANGED": "old", "PATH": "/bin", "HOME": "/home/x", "SAME": "s"}
	now := []string{"AMBIENT=a", "CHANGED=new", "NEW=1", "EMPTY=", "PATH=/venv/bin:/bin", "HOME=/home/x", "TMPDIR=/t", "SAME=s"}
	if got := strings.Join(testSetSince(before, now), " "); got != "CHANGED=new EMPTY= NEW=1" {
		t.Fatalf("test-set variables = %q", got)
	}
	// What changed between two moments of a test, for a call's key.
	start := []string{"A=1", "B=2", "C=3"}
	for name, c := range map[string]struct {
		now  []string
		want string
	}{
		"nothing":        {[]string{"C=3", "B=2", "A=1"}, ""},
		"a new variable": {[]string{"A=1", "B=2", "C=3", "D=4"}, "D=4"},
		"a changed one":  {[]string{"A=1", "B=9", "C=3"}, "B=9"},
		"one gone":       {[]string{"A=1", "C=3"}, "B=\x00unset"},
	} {
		if got := strings.Join(testSetDelta(start, c.now), " "); got != c.want {
			t.Errorf("%s: delta = %q, want %q", name, got, c.want)
		}
	}
	// This process: a variable set here is seen, and gone again when the test that set it ends.
	t.Run("in this process", func(t *testing.T) {
		t.Setenv("VENUEORACLE_SET_BY_THIS_TEST", "x")
		if !strings.Contains(" "+strings.Join(testSetEnv(), " ")+" ", " VENUEORACLE_SET_BY_THIS_TEST=x ") {
			t.Fatalf("a variable this test set is not in %q", testSetEnv())
		}
	})
	if strings.Contains(strings.Join(testSetEnv(), " "), "VENUEORACLE_SET_BY_THIS_TEST") {
		t.Fatal("a variable of an ended test is still counted")
	}
}

func TestACallsKeyHoldsItsExtraEntriesAndWhatTheTestChangedSinceTheVenueWasBuilt(t *testing.T) {
	declared := Options{JWTKey: "k"}
	extra := []string{"STRIPE_SECRET_KEY=", "POSTGRES_URI=postgresql://nobody@127.0.0.1:1/none"}
	extraKey, _ := pythonEnvKey(fromTest(extra...))
	legacyExtraKey, _ := legacyPythonEnvKey(extra)
	// The first version keyed POSTGRES_URI by name whoever supplied it; the
	// current one keys the value a test supplies. A golden of the first
	// version is still checked by the first version's rule (below).
	if extraKey == legacyExtraKey {
		t.Fatal("a database address the test passes to one call is keyed as the first version keyed it: by name only")
	}
	recording, err := openGolden(GoldenSpec{Path: t.TempDir() + "/g.json", PythonBuild: goldenBuild, Recipe: "record it"}, "TestSample", true)
	if err != nil {
		t.Fatal(err)
	}
	if err := recording.bindPythonEnv(declared); err != nil {
		t.Fatal(err)
	}
	call := func(g *Golden, extra []string) string {
		t.Helper()
		key, err := g.callEnvKey(extra)
		if err != nil {
			t.Fatal(err)
		}
		return key
	}
	if call(recording, nil) != "" || call(recording, extra) != extraKey || call(recording, []string{}) == "" {
		t.Fatalf("with nothing changed: %q, %q, %q", call(recording, nil), call(recording, extra), call(recording, []string{}))
	}
	t.Run("a variable set after the venue was built", func(t *testing.T) {
		t.Setenv("EMAIL_PROVIDER", "smtp")
		plain, withExtra := call(recording, nil), call(recording, extra)
		if plain == "" || withExtra == extraKey || plain == withExtra {
			t.Fatalf("a variable set after Start is not in the call's key: %q %q", plain, withExtra)
		}
		t.Setenv("EMAIL_PROVIDER", "other")
		if call(recording, nil) == plain {
			t.Fatal("another value of the variable gives the same call key")
		}
		// A golden of the first key version keeps its rule: the extra entries alone.
		legacy, _ := frozenWithEnv(t, legacyKey(t, declared), 0)
		if err := legacy.bindPythonEnv(declared); err != nil {
			t.Fatal(err)
		}
		if call(legacy, nil) != "" || call(legacy, extra) != legacyExtraKey {
			t.Fatalf("first version call keys: %q %q", call(legacy, nil), call(legacy, extra))
		}
	})
	// The subtest ended and took its variable with it: the call runs under the venue's environment again.
	if call(recording, nil) != "" {
		t.Fatal("a variable of an ended subtest is still in the call's key")
	}
}

// The plane's environment, entry for entry, and the first key version's: a
// change of either list changes every golden's key of that version.
func TestThePlaneEnvironmentAndItsFirstKeyVersion(t *testing.T) {
	options := Options{JWTKey: "jwt", PythonEnv: []string{"TRIAL_DAYS=7", "ENVIRONMENT=other"}}
	perRun := map[string]string{"HOME": "/home/x", "PATH": "/venv/bin:/bin", "TMPDIR": "/t", "PYTHONPATH": "/checkout/src", "POSTGRES_URI": "postgresql+asyncpg://db", "REDIS_URL": "redis://cache", "CLICKHOUSE_URI": "http://ch"}
	want := []string{"HOME=/home/x", "PATH=/venv/bin:/bin", "TMPDIR=/t",
		"PYTHONHASHSEED=0", "PYTHONDONTWRITEBYTECODE=1", "TZ=UTC", "LANG=C.UTF-8", "LC_ALL=C.UTF-8",
		"PYTHONPATH=/checkout/src", "POSTGRES_URI=postgresql+asyncpg://db",
		"JWT_SECRET_KEY=jwt", "OTEL_SDK_DISABLED=true", "DEV_HEALTH_ALLOW_CELERY_RIVER_CUTOVER=1",
		"PGBOUNCER_TRANSACTION_MODE=true", "ENVIRONMENT=test", "REDIS_URL=redis://cache",
		"CLICKHOUSE_URI=http://ch", "TRIAL_DAYS=7", "ENVIRONMENT=other"}
	if got := pythonPlaneEnv(options, perRun); strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("the Python plane's environment changed (every golden of the current key version must then be recorded again):\n got %q\nwant %q", got, want)
	}
	wantLegacy := []string{"PYTHONPATH=", "POSTGRES_URI=", "JWT_SECRET_KEY=jwt", "OTEL_SDK_DISABLED=true", "DEV_HEALTH_ALLOW_CELERY_RIVER_CUTOVER=1",
		"PGBOUNCER_TRANSACTION_MODE=true", "ENVIRONMENT=test", "REDIS_URL=", "CLICKHOUSE_URI=", "TRIAL_DAYS=7", "ENVIRONMENT=other"}
	if got := legacyPlaneEnv(options); strings.Join(got, "\n") != strings.Join(wantLegacy, "\n") {
		t.Fatalf("the first key version's environment changed (no golden of that version would be accepted any more):\n got %q\nwant %q", got, wantLegacy)
	}
	// The first key version of these Options is the digest the harness computed for them before the
	// versions existed (executed on that code): the goldens of that version hold keys made this way.
	if got := legacyKey(t, options); got != "5c83ca122b16bf0986dc02d0498cd5548298baa505516e64b123ca1d3c5034f8" {
		t.Fatalf("the first key version computes %s for these Options: no golden of that version would be accepted", got)
	}
	key := func(o Options, values map[string]string) string {
		out, err := pythonEnvKey(planeEntries(o, values))
		if err != nil {
			t.Fatal(err)
		}
		return out
	}
	// The values made for a run or a host are not in the key; every name of them is listed as per-run.
	if key(options, perRun) != key(options, nil) {
		t.Fatal("the key depends on a per-run value")
	}
	for name := range perRun {
		if _, listed := perRunPythonEnv[name]; !listed {
			t.Fatalf("%s is given a per-run value and is not in perRunPythonEnv", name)
		}
	}
	for _, name := range inheritedPythonEnv {
		if _, listed := perRunPythonEnv[name]; !listed {
			t.Fatalf("the inherited name %s is not keyed by name", name)
		}
	}
	// An inherited name the process does not hold is not handed on; for the key it is always there.
	if got := strings.Join(interpreterEnv(map[string]string{"PATH": "/bin"}), " "); !strings.HasPrefix(got, "PATH=/bin PYTHONHASHSEED=0") {
		t.Fatalf("interpreter environment with only PATH held: %q", got)
	}
	if got := strings.Join(interpreterEnv(nil), " "); !strings.HasPrefix(got, "HOME= PATH= TMPDIR= PYTHONHASHSEED=0") {
		t.Fatalf("interpreter environment for the key: %q", got)
	}
	// A setting of the harness or the interpreter is in the key by value: PythonEnv giving it its own value changes nothing, another value does.
	plain := Options{JWTKey: "jwt"}
	for _, entry := range []string{"ENVIRONMENT=test", "PYTHONHASHSEED=0", "TZ=UTC"} {
		name, _, _ := strings.Cut(entry, "=")
		if key(Options{JWTKey: "jwt", PythonEnv: []string{entry}}, nil) != key(plain, nil) || key(Options{JWTKey: "jwt", PythonEnv: []string{name + "=another"}}, nil) == key(plain, nil) {
			t.Fatalf("%s is not in the key by value", name)
		}
	}
}

// probeChild runs this test again in a child process whose ambient environment
// holds extra, and returns the child's output.
func probeChild(t *testing.T, mark string, extra ...string) (string, error) {
	t.Helper()
	return probeChildAs(t, mark, "1", extra...)
}

// probeChildAs runs this test again in a child process with mark set to value.
func probeChildAs(t *testing.T, mark, value string, extra ...string) (string, error) {
	t.Helper()
	command := exec.Command(os.Args[0], "-test.run=^"+t.Name()+"$", "-test.v")
	command.Env = append(append(os.Environ(), mark+"="+value), extra...)
	output, err := command.CombinedOutput()
	return string(output), err
}

// The real launch of the Python child (runPython), with a stand-in python3
// that prints which variables it sees: only the explicit environment reaches
// it. The child process of this test is the test process of a venue test; what
// the parent gives it is that test's ambient environment.
func TestThePythonChildSeesOnlyItsExplicitEnvironment(t *testing.T) {
	const mark = "VENUEORACLE_CHILD_ENV_PROBE"
	if os.Getenv(mark) == "" {
		output, err := probeChild(t, mark, "VENUE_AMBIENT_ONLY=ambient", "VENUE_AMBIENT_SAME=same", "PYTHONHASHSEED=123", "TZ=Pacific/Auckland", "TRUSTED_PROXIES=ambient")
		if err != nil {
			t.Fatalf("the probe failed: %v\n%s", err, output)
		}
		return
	}
	dir := t.TempDir()
	names := []string{"VENUE_AMBIENT_ONLY", "VENUE_AMBIENT_SAME", "VENUE_SET_BY_TEST", "TRUSTED_PROXIES", "PYTHONHASHSEED", "TZ", "LANG", "HOME", "PATH", "TMPDIR", "JWT_SECRET_KEY", "ENVIRONMENT", "EXTRA_FOR_ONE_CALL", "OTEL_SDK_DISABLED", mark}
	// The stand-in prints what it sees, and keeps it in a file beside itself for the launch that reads no output.
	script := "#!/bin/sh\nout=\"\"\nfor n in " + strings.Join(names, " ") + "; do eval \"v=\\${$n-<unset>}\"; out=\"$out $n=$v\"; done\necho \"$out\" > \"${0%/*}/last.out\"\necho \"$out\"\n"
	if err := os.WriteFile(filepath.Join(dir, "python3"), []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	// As Start does: the interpreter's directory first on PATH.
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv("VENUE_SET_BY_TEST", "by-test")
	t.Setenv("VENUE_AMBIENT_SAME", "same")   // the value it already has: the snapshot cannot see this one
	t.Setenv("TRUSTED_PROXIES", "127.0.0.1") // an ambient variable the test gives another value
	t.Setenv("OTEL_SDK_DISABLED", "false")   // a name the plane sets itself: the plane's value wins, as it did
	// As Start does: the inherited values are fixed when the venue is built.
	host := inheritedValues(map[string]string{})
	venue := &Venue{Python: filepath.Join(dir, "python3"), hostEnv: host, pythonEnv: pythonPlaneEnv(Options{JWTKey: "k"}, inheritedValues(map[string]string{}))}
	// An inherited name the process changes after the venue was built: the
	// children keep the value the venue fixed.
	tmpAtBuild, held := os.LookupEnv("TMPDIR")
	if !held {
		tmpAtBuild = "<unset>" // a name the process did not hold is not handed on
	}
	t.Setenv("TMPDIR", filepath.Join(dir, "changed-after-the-venue-was-built"))
	seen := func(v *Venue) map[string]string {
		out := map[string]string{}
		for _, field := range strings.Fields(string(v.runPython(t, nil, "probe"))) {
			name, value, _ := strings.Cut(field, "=")
			out[name] = value
		}
		return out
	}
	got := seen(venue)
	for name, want := range map[string]string{
		"VENUE_AMBIENT_ONLY": "<unset>",   // ambient: not inherited
		mark:                 "<unset>",   // ambient: not inherited
		"VENUE_SET_BY_TEST":  "by-test",   // set by the test: handed on
		"TRUSTED_PROXIES":    "127.0.0.1", // set by the test over an ambient value: the test's value
		"PYTHONHASHSEED":     "0",         // the interpreter's settings are the harness's, never the ambient ones
		"TZ":                 "UTC",
		"LANG":               "C.UTF-8",
		"JWT_SECRET_KEY":     "k",
		"ENVIRONMENT":        "test",
		"EXTRA_FOR_ONE_CALL": "<unset>",
		"VENUE_AMBIENT_SAME": "<unset>", // the stated limit: set to its ambient value, it is not seen as set by the test
		"OTEL_SDK_DISABLED":  "true",    // the plane's own entry wins over a variable of the same name the test set
	} {
		if got[name] != want {
			t.Errorf("the Python child sees %s=%q, want %q", name, got[name], want)
		}
	}
	if got["HOME"] == "<unset>" || !strings.HasPrefix(got["PATH"], dir) || got["TMPDIR"] != tmpAtBuild {
		t.Errorf("the inherited names did not reach the child as the venue fixed them: HOME=%q PATH=%q TMPDIR=%q (at build %q)", got["HOME"], got["PATH"], got["TMPDIR"], tmpAtBuild)
	}
	// One call's extra entries reach that call's child only, and win over the plane's.
	clone := *venue
	clone.pythonEnv = append(append([]string(nil), venue.pythonEnv...), "EXTRA_FOR_ONE_CALL=x", "ENVIRONMENT=other")
	if extra := seen(&clone); extra["EXTRA_FOR_ONE_CALL"] != "x" || extra["ENVIRONMENT"] != "other" || seen(venue)["EXTRA_FOR_ONE_CALL"] != "<unset>" {
		t.Errorf("a call's extra entries: %v", extra)
	}
	// The other Python child of the harness, the ClickHouse migration: the same rule.
	migration := &Venue{Root: dir, Python: venue.Python, hostEnv: host, clickHouseHTTPURI: "http://default:pw@127.0.0.1:1/source"}
	migration.migrateClickHouse(t, context.Background(), "target")
	raw, err := os.ReadFile(filepath.Join(dir, "last.out"))
	if err != nil {
		t.Fatal(err)
	}
	migrated := map[string]string{}
	for _, field := range strings.Fields(string(raw)) {
		name, value, _ := strings.Cut(field, "=")
		migrated[name] = value
	}
	for name, want := range map[string]string{"VENUE_AMBIENT_ONLY": "<unset>", mark: "<unset>", "VENUE_SET_BY_TEST": "by-test", "PYTHONHASHSEED": "0", "TZ": "UTC", "JWT_SECRET_KEY": "<unset>"} {
		if migrated[name] != want {
			t.Errorf("the migration child sees %s=%q, want %q", name, migrated[name], want)
		}
	}
	if !strings.HasPrefix(migrated["PATH"], dir) || migrated["HOME"] == "<unset>" || migrated["TMPDIR"] != tmpAtBuild {
		t.Errorf("the inherited names did not reach the migration child as the venue fixed them: HOME=%q PATH=%q TMPDIR=%q (at build %q)", migrated["HOME"], migrated["PATH"], migrated["TMPDIR"], tmpAtBuild)
	}
	// What the child sees of the test's variables is what the key holds.
	if set := strings.Join(testSetEnv(), " "); !strings.Contains(set, "VENUE_SET_BY_TEST=by-test") || !strings.Contains(set, "TRUSTED_PROXIES=127.0.0.1") || strings.Contains(set, "VENUE_AMBIENT") || strings.Contains(set, "PATH=") {
		t.Errorf("the test-set variables for the key: %q", set)
	}
}

// standInPython writes an executable python3 into a new directory; run, it
// leaves a file named ran beside itself and prints label.
func standInPython(t *testing.T, label string) (dir, program string) {
	t.Helper()
	dir = t.TempDir()
	program = filepath.Join(dir, "python3")
	if err := os.WriteFile(program, []byte("#!/bin/sh\n: > \"${0%/*}/ran\"\necho "+label+"\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	return dir, program
}

// Which program runs as the Python child is an input too. The name is looked
// up through the process's PATH at launch, and PATH is in no key, so the
// launch is refused unless the lookup gives the interpreter the venue fixed
// when it was built.
func TestAPythonChildIsOnlyTheInterpreterTheVenueFixed(t *testing.T) {
	ctx := context.Background()
	own, program := standInPython(t, "own")
	other, _ := standInPython(t, "other")
	empty := t.TempDir()
	venue := &Venue{Python: program}
	for _, row := range []struct {
		name, path, refusal string
	}{
		{"PATH as the venue left it", own + string(os.PathListSeparator) + other, ""},
		{"another interpreter put first after the venue was built", other + string(os.PathListSeparator) + own, "another interpreter would answer under the same key"},
		{"the venue's directory gone from PATH, another interpreter left", other, "another interpreter would answer under the same key"},
		{"no python3 on PATH any more", empty, "is not found through PATH any more"},
	} {
		t.Setenv("PATH", row.path)
		command, err := venue.pythonCommand(ctx, "-c", "pass")
		switch {
		case row.refusal == "" && (err != nil || command.Path != program):
			t.Errorf("%s: err %v, program %v, want %s", row.name, err, command, program)
		case row.refusal != "" && (err == nil || !strings.Contains(err.Error(), row.refusal)):
			t.Errorf("%s: err = %v, want a refusal holding %q", row.name, err, row.refusal)
		}
	}
	t.Setenv("PATH", own)
	if _, err := (&Venue{}).pythonCommand(ctx, "-c", "pass"); err == nil || !strings.Contains(err.Error(), "fixed no Python interpreter") {
		t.Errorf("a venue with no interpreter: err = %v, want a refusal", err)
	}
}

// The two real launches (the plane's process and the ClickHouse migration),
// each in a child process whose PATH puts another python3 first after the
// venue was built: the launch fails the test, and the other program never runs.
func TestBothPythonLaunchesRefuseAnotherInterpreterOnPath(t *testing.T) {
	const mark = "VENUEORACLE_CHILD_PATH_SWAP"
	if site := os.Getenv(mark); site != "" {
		own, program := standInPython(t, "own")
		t.Setenv("PATH", own+string(os.PathListSeparator)+os.Getenv("PATH"))
		venue := &Venue{Root: own, Python: program, hostEnv: inheritedValues(map[string]string{}), clickHouseHTTPURI: "http://default:pw@127.0.0.1:1/source"}
		venue.pythonEnv = pythonPlaneEnv(Options{JWTKey: "k"}, inheritedValues(map[string]string{}))
		other := os.Getenv(mark + "_OTHER")
		if site != "control-run" && site != "control-migrate" {
			t.Setenv("PATH", other+string(os.PathListSeparator)+os.Getenv("PATH"))
		}
		switch strings.TrimPrefix(site, "control-") {
		case "run":
			fmt.Printf("LAUNCHED %s\n", venue.runPython(t, nil, "probe"))
		case "migrate":
			venue.migrateClickHouse(t, context.Background(), "target")
			fmt.Println("LAUNCHED migrate")
		}
		return
	}
	for _, site := range []string{"run", "migrate"} {
		other, _ := standInPython(t, "other")
		// Control: with PATH as the venue left it, the launch happens.
		output, err := probeChildAs(t, mark, "control-"+site, mark+"_OTHER="+other)
		if err != nil || !strings.Contains(output, "LAUNCHED") {
			t.Fatalf("%s, control: the launch did not happen: %v\n%s", site, err, output)
		}
		output, err = probeChildAs(t, mark, site, mark+"_OTHER="+other)
		if err == nil || !strings.Contains(output, "PATH changed after the venue was built") || strings.Contains(output, "LAUNCHED") {
			t.Errorf("%s: another python3 first on PATH: err %v, want the launch refused\n%s", site, err, output)
		}
		if _, statErr := os.Stat(filepath.Join(other, "ran")); statErr == nil {
			t.Errorf("%s: the other python3 ran", site)
		}
	}
}

// startWithSettingsInChild runs the real Start in a child process, in a frozen
// venue test whose golden was recorded under the Options recorded and which
// declares the Options declared(name), and returns the child's output. The
// context is cancelled, so a Start that gets past the environment check fails
// on its first build step instead of building a venue.
func startWithSettingsInChild(t *testing.T, env string, recorded Options, declared func(name string) (Options, []string), name string) (string, error, bool) {
	t.Helper()
	if which := os.Getenv(env); which != "" {
		t.Setenv("DEV_HEALTH_LIVE_PYTHON_ORACLES", "1")
		t.Setenv(goldenUpdateEnv, "")
		t.Setenv(goldenCandidateEnv, "")
		golden, _ := frozenWithEnv(t, currentKey(t, recorded), pythonEnvKeyVersion)
		options, set := declared(which)
		for _, entry := range set {
			name, value, _ := strings.Cut(entry, "=")
			t.Setenv(name, value)
		}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		options.Root, options.Golden = t.TempDir(), golden
		t.Log("START CALLED")
		Start(t, ctx, options)
		t.Log("START RETURNED")
		return "", nil, true
	}
	command := exec.Command(os.Args[0], "-test.run=^"+t.Name()+"$", "-test.v")
	command.Env = append(os.Environ(), env+"="+name)
	output, err := command.CombinedOutput()
	return string(output), err, false
}

// The real Start: a frozen golden recorded under one environment of the Python
// plane is refused, before anything is built, by a test that gives the plane
// another value (the pinned clock), one variable more, none, another JWT key,
// or a variable set in the process.
func TestStartRefusesAGoldenRecordedUnderAnotherPythonEnvironment(t *testing.T) {
	env := []string{"VENUE_PINNED_NOW=2026-01-01T00:00:00Z", "SETTINGS_ENCRYPTION_KEY=k"}
	recorded := Options{JWTKey: "recorded", PythonEnv: env}
	type declaration struct {
		options Options
		set     []string
	}
	cases := map[string]declaration{
		"a changed value":            {options: Options{JWTKey: recorded.JWTKey, PythonEnv: []string{"VENUE_PINNED_NOW=2099-01-01T00:00:00Z", env[1]}}},
		"a variable more":            {options: Options{JWTKey: recorded.JWTKey, PythonEnv: append(append([]string{}, env...), "TRIAL_DAYS=7")}},
		"no settings":                {options: Options{JWTKey: recorded.JWTKey}},
		"another per-run":            {options: Options{JWTKey: recorded.JWTKey, PythonEnv: append(append([]string{}, env...), "VENUE_STRIPE_API_BASE=http://127.0.0.1:1")}},
		"another JWT key":            {options: Options{JWTKey: "other", PythonEnv: env}},
		"a variable set by t.Setenv": {options: recorded, set: []string{"TRUSTED_PROXIES=127.0.0.1"}},
		"the same (start)":           {options: recorded},
	}
	declared := func(name string) (Options, []string) { return cases[name].options, cases[name].set }
	for name := range cases {
		output, err, inChild := startWithSettingsInChild(t, "VENUEORACLE_PYTHON_ENV_START_CHILD", recorded, declared, name)
		if inChild {
			return
		}
		if !strings.Contains(output, "START CALLED") {
			t.Fatalf("%s: the child did not reach Start:\n%s", name, output)
		}
		refused := strings.Contains(output, "recorded under another Python environment")
		if name == "the same (start)" {
			// The control: with the recorded environment the check lets Start go on.
			if refused {
				t.Errorf("%s: refused:\n%s", name, output)
			}
			continue
		}
		if err == nil || !refused || strings.Contains(output, "START RETURNED") {
			t.Errorf("%s: Start did not refuse (err %v):\n%s", name, err, output)
		}
	}
}

// legacyKeyGoldenCount is how many goldens the closed list holds. It only goes
// down, with every row that is deleted: there is no room above it.
const legacyKeyGoldenCount = 52

// legacyRow is one row of the closed list: a golden of the first key version
// by its repository path, and the sha256 of its bytes.
type legacyRow struct {
	Digest, Path string
}

// legacyKeyGoldens reads the closed list: "<sha256>  <repository path>", one per line.
func legacyKeyGoldens(t *testing.T, path string) []legacyRow {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var rows []legacyRow
	for _, line := range strings.Split(string(raw), "\n") {
		if line = strings.TrimSpace(line); line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		digest, file, found := strings.Cut(line, "  ")
		if !found || len(digest) != 64 || file == "" {
			t.Fatalf("%s: %q is not a row (sha256, two spaces, path)", path, line)
		}
		rows = append(rows, legacyRow{Digest: digest, Path: file})
	}
	return rows
}

// keyVersionProblems walks root for venue goldens (a golden whose header holds
// a key of its Python environment) and lists every breach of the closed list:
// a golden of the first key version that is not listed, a listed golden that
// holds the current version, or has other bytes than its row says, or is gone,
// or is listed twice.
func keyVersionProblems(t *testing.T, root string, listed []legacyRow) (venueGoldens int, problems []string) {
	t.Helper()
	onList := map[string]int{}
	digests := map[string]string{}
	for _, row := range listed {
		onList[row.Path]++
		digests[row.Path] = row.Digest
		if onList[row.Path] == 2 {
			problems = append(problems, row.Path+": listed twice")
		}
	}
	found := map[string]bool{}
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			if name := entry.Name(); name == ".git" || name == "node_modules" || name == ".venv" {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".json") || !strings.Contains(filepath.ToSlash(path), "/testdata/") {
			return nil
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if !strings.Contains(string(raw), `"python_build"`) {
			return nil
		}
		var file goldenFile
		if err := json.Unmarshal(raw, &file); err != nil || file.Header.PythonEnv == "" {
			return nil // not a golden of this harness, or a golden of a program producer: no venue, no key
		}
		venueGoldens++
		relative, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		relative = filepath.ToSlash(relative)
		found[relative] = true
		switch version := file.Header.PythonEnvVersion; {
		case version == pythonEnvKeyVersion && onList[relative] > 0:
			problems = append(problems, relative+": holds the current key version and is still on the list of the first version: delete its row")
		case version == 0 && onList[relative] == 0:
			problems = append(problems, relative+": holds the first key version and is not on the closed list: a golden recorded now holds the current version; record it with this harness")
		case version == 0 && digests[relative] != fmt.Sprintf("%x", sha256.Sum256(raw)):
			problems = append(problems, fmt.Sprintf("%s: is on the closed list with the sha256 %s and its bytes are %x: a listed golden changes only by being recorded again, which gives it the current key version", relative, digests[relative], sha256.Sum256(raw)))
		case version != 0 && version != pythonEnvKeyVersion:
			problems = append(problems, fmt.Sprintf("%s: holds key version %d, which this harness does not know", relative, version))
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	for row := range onList {
		if !found[row] {
			problems = append(problems, row+": on the list of the first key version, and no such venue golden exists: delete its row")
		}
	}
	sort.Strings(problems)
	return venueGoldens, problems
}

// The ratchet of the first key version: only the goldens on the closed list
// may hold it, the list holds nothing else, and it never grows.
func TestOnlyTheListedGoldensHoldTheFirstKeyVersion(t *testing.T) {
	root, err := filepath.Abs("../../..")
	if err != nil {
		t.Fatal(err)
	}
	listed := legacyKeyGoldens(t, filepath.Join("testdata", "legacy_python_env_goldens.txt"))
	if len(listed) != legacyKeyGoldenCount {
		t.Fatalf("the closed list holds %d goldens and legacyKeyGoldenCount says %d: the list only shrinks, and the count goes down with it", len(listed), legacyKeyGoldenCount)
	}
	venueGoldens, problems := keyVersionProblems(t, root, listed)
	if venueGoldens == 0 {
		t.Fatal("the walk found no venue golden: a ratchet that checks nothing passes everything")
	}
	if len(problems) > 0 {
		t.Fatalf("%d venue goldens, the closed list of the first key version is broken:\n%s", venueGoldens, strings.Join(problems, "\n"))
	}
}

// The walk above, on trees with each breach planted.
func TestTheKeyVersionRatchetNamesEachBreach(t *testing.T) {
	golden := func(version int, key string) string {
		header := goldenHeader{Test: "TestX", PythonBuild: goldenBuild, ProducerDigest: strings.Repeat("a", 64), Recipe: "r", PythonEnv: key, PythonEnvVersion: version}
		raw, err := json.Marshal(goldenFile{Header: header})
		if err != nil {
			t.Fatal(err)
		}
		return string(raw)
	}
	key := strings.Repeat("1", 64)
	tree := func(files map[string]string) string {
		root := t.TempDir()
		for path, content := range files {
			full := filepath.Join(root, filepath.FromSlash(path))
			if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(full, []byte(content), 0o644); err != nil {
				t.Fatal(err)
			}
		}
		return root
	}
	files := map[string]string{
		"a/testdata/old.json":     golden(0, key),
		"a/testdata/new.json":     golden(2, key),
		"a/testdata/program.json": golden(0, ""), // a program producer's golden: no key, not a venue golden
		"a/testdata/notes.json":   `{"a":1}`,
	}
	row := func(path string) legacyRow {
		return legacyRow{Digest: fmt.Sprintf("%x", sha256.Sum256([]byte(files[path]))), Path: path}
	}
	old := row("a/testdata/old.json")
	if count, problems := keyVersionProblems(t, tree(files), []legacyRow{old}); count != 2 || len(problems) != 0 {
		t.Fatalf("a correct tree: %d venue goldens, problems %v", count, problems)
	}
	for name, c := range map[string]struct {
		listed []legacyRow
		want   string
	}{
		"a first-version golden that is not listed":      {nil, "a/testdata/old.json: holds the first key version and is not on the closed list"},
		"a listed golden that holds the current version": {[]legacyRow{old, row("a/testdata/new.json")}, "a/testdata/new.json: holds the current key version and is still on the list"},
		"a row with no golden":                           {[]legacyRow{old, {Digest: strings.Repeat("0", 64), Path: "a/testdata/gone.json"}}, "a/testdata/gone.json: on the list of the first key version, and no such venue golden exists"},
		"a row for a program golden":                     {[]legacyRow{old, row("a/testdata/program.json")}, "a/testdata/program.json: on the list of the first key version, and no such venue golden exists"},
		"a row twice":                                    {[]legacyRow{old, old}, "a/testdata/old.json: listed twice"},
		"a listed golden with other bytes than its row":  {[]legacyRow{{Digest: strings.Repeat("0", 64), Path: "a/testdata/old.json"}}, "a/testdata/old.json: is on the closed list with the sha256 0000"},
	} {
		_, problems := keyVersionProblems(t, tree(files), c.listed)
		if len(problems) != 1 || !strings.HasPrefix(problems[0], c.want) {
			t.Errorf("%s: problems = %q, want exactly one, starting %q", name, problems, c.want)
		}
	}
	files["a/testdata/future.json"] = golden(3, key)
	if _, problems := keyVersionProblems(t, tree(files), []legacyRow{old}); len(problems) != 1 || !strings.Contains(problems[0], "holds key version 3") {
		t.Errorf("a golden of an unknown key version: %q", problems)
	}
}

// TestAKeyGeneratedForTheRunIsKeyedByNameWhateverItsValue pins the one name the
// GitHub App venue sets to a new RSA key in every run (the key is generated in
// the test, internal/api/githubapp/venue_oracle_integration_test.go, never read
// from the host). Needed: two runs with two different generated keys are ONE
// Python environment, in the venue's key and in a call's key (the venue sets it
// with t.Setenv). Safe: nothing derived from the key's value reaches the key
// (equal for two values, so no digest of key material is stored), and the
// same name supplied by anyone but the test (the harness, the host) is keyed
// by value as every other setting.
func TestAKeyGeneratedForTheRunIsKeyedByNameWhateverItsValue(t *testing.T) {
	key := func(entries ...envEntry) string {
		t.Helper()
		out, err := pythonEnvKey(entries)
		if err != nil {
			t.Fatal(err)
		}
		return out
	}
	if key(fromTest("GITHUB_APP_PRIVATE_KEY=generated-one", "GITHUB_APP_ID=12345")...) != key(fromTest("GITHUB_APP_PRIVATE_KEY=generated-two", "GITHUB_APP_ID=12345")...) {
		t.Fatal("a private key generated for the run changes the Python environment key")
	}
	if key(fromTest("GITHUB_APP_PRIVATE_KEY=generated-one", "GITHUB_APP_ID=12345")...) == key(fromTest("GITHUB_APP_PRIVATE_KEY=generated-one", "GITHUB_APP_ID=99999")...) {
		t.Fatal("a changed GITHUB_APP_ID no longer changes the key: the whole environment became per-run")
	}
	if key(tagged([]string{"GITHUB_APP_PRIVATE_KEY=host-one"}, false)...) == key(tagged([]string{"GITHUB_APP_PRIVATE_KEY=host-two"}, false)...) {
		t.Fatal("a key supplied by the harness or the host is keyed by name only: its value must be in the key")
	}
	recording, err := openGolden(GoldenSpec{Path: t.TempDir() + "/g.json", PythonBuild: goldenBuild, Recipe: "record it"}, "TestSample", true)
	if err != nil {
		t.Fatal(err)
	}
	if err := recording.bindPythonEnv(Options{JWTKey: "k"}); err != nil {
		t.Fatal(err)
	}
	call := func() string {
		t.Helper()
		got, err := recording.callEnvKey(nil)
		if err != nil {
			t.Fatal(err)
		}
		return got
	}
	t.Setenv("GITHUB_APP_PRIVATE_KEY", "generated-one")
	first := call()
	t.Setenv("GITHUB_APP_PRIVATE_KEY", "generated-two")
	if first == "" || call() != first {
		t.Fatalf("the call key of a variable the test set to a generated key depends on the key (%q then %q)", first, call())
	}
}

// TestThePerRunNamesAreExactlyTheClosedListWithTheirSide pins perRunPythonEnv
// both ways: every name the list holds is named here with who supplies it, and
// every name here is in the list. Adding a name to the list (or taking one out,
// or moving it between the harness/host side and the test side) fails here until
// this table says so too, in the same change as the oracle that needs it.
func TestThePerRunNamesAreExactlyTheClosedListWithTheirSide(t *testing.T) {
	want := map[string]bool{ // name -> supplied by the test
		"CLICKHOUSE_URI": false, "HOME": false, "PATH": false, "POSTGRES_URI": false, "PYTHONPATH": false, "REDIS_URL": false, "TMPDIR": false,
		"GITHUB_APP_PRIVATE_KEY": true, "REQUESTS_CA_BUNDLE": true, "TELEMETRY_ENDPOINT": true,
		"VENUE_PAGERDUTY_API_BASE_OVERRIDE": true, "VENUE_PAGERDUTY_REVOKE_URL_OVERRIDE": true, "VENUE_PAGERDUTY_TOKEN_URL_OVERRIDE": true,
		"SMTP_HOST": true, "SMTP_PORT": true,
		"VENUE_PROVIDER_STUB_PORT": true, "VENUE_STRIPE_API_BASE": true,
	}
	for name, listed := range perRunPythonEnv {
		byTest, ok := want[name]
		if !ok {
			t.Errorf("perRunPythonEnv lists %s, which this table does not: a per-run name needs the oracle that uses it named here", name)
			continue
		}
		if listed.byTest != byTest {
			t.Errorf("%s: the list says byTest=%v, this table says %v", name, listed.byTest, byTest)
		}
		if strings.TrimSpace(listed.reason) == "" {
			t.Errorf("%s has no reason", name)
		}
	}
	for name := range want {
		if _, ok := perRunPythonEnv[name]; !ok {
			t.Errorf("%s is in this table and not in perRunPythonEnv: the oracle that needs it would key a per-run value by value", name)
		}
	}
}

// TestTheAddressOfAFakeSMTPSinkIsKeyedByNameWhateverItsValue pins SMTP_HOST and
// SMTP_PORT, which the auth-flow venue sets to the loopback address and the
// free port of its per-run SMTP sink: two runs with other addresses are one
// Python environment, while another setting still changes the key, and the same
// names supplied by the harness or the host are keyed by value.
func TestTheAddressOfAFakeSMTPSinkIsKeyedByNameWhateverItsValue(t *testing.T) {
	key := func(entries ...envEntry) string {
		t.Helper()
		out, err := pythonEnvKey(entries)
		if err != nil {
			t.Fatal(err)
		}
		return out
	}
	one := key(fromTest("SMTP_HOST=127.0.0.1", "SMTP_PORT=40001", "EMAIL_PROVIDER=smtp")...)
	two := key(fromTest("SMTP_HOST=127.0.0.2", "SMTP_PORT=40002", "EMAIL_PROVIDER=smtp")...)
	if one != two {
		t.Fatal("the address of a per-run SMTP sink changes the Python environment key")
	}
	if one == key(fromTest("SMTP_HOST=127.0.0.1", "SMTP_PORT=40001", "EMAIL_PROVIDER=resend")...) {
		t.Fatal("a changed EMAIL_PROVIDER no longer changes the key")
	}
	for _, name := range []string{"SMTP_HOST", "SMTP_PORT"} {
		if key(tagged([]string{name + "=a"}, false)...) == key(tagged([]string{name + "=b"}, false)...) {
			t.Fatalf("%s supplied by the harness or the host is keyed by name only", name)
		}
	}
}
