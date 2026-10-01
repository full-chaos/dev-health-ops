package venueoracle

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"os/exec"
	"strings"
	"testing"
)

func TestThePythonEnvKeyHoldsEveryDeclaredSettingAndThePerRunNamesOnly(t *testing.T) {
	base := []string{"VENUE_PINNED_NOW=2026-01-01T00:00:00Z", "SETTINGS_ENCRYPTION_KEY=k", "VENUE_STRIPE_API_BASE=http://127.0.0.1:41001"}
	key := func(env []string) string {
		t.Helper()
		out, err := pythonEnvKey(env)
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
		if _, err := pythonEnvKey([]string{bad}); err == nil {
			t.Errorf("%q was keyed", bad)
		}
	}
	// A per-run name that was never executed as one would hide a setting: every listed name has a reason.
	for name, why := range perRunPythonEnv {
		if strings.TrimSpace(why) == "" || strings.TrimSpace(name) == "" {
			t.Errorf("perRunPythonEnv[%q] has no reason", name)
		}
	}
}

// frozenWithEnv writes a frozen golden whose header holds key (none when empty) and opens it.
func frozenWithEnv(t *testing.T, key string) (*Golden, string) {
	t.Helper()
	request := ProgramRequest("corpus", sampleProgram, []byte("abc"), nil)
	entry := requestKey(request)
	entry.Body = "ABC\n"
	file := goldenFile{Header: goldenHeader{Test: "TestSample", PythonBuild: goldenBuild, ProducerDigest: strings.Repeat("a", 64), Recipe: "record it", PythonEnv: key}, Requests: []goldenRequest{entry}}
	path, digest := writeGoldenFile(t, t.TempDir(), file)
	golden, err := openGolden(GoldenSpec{Path: path, PythonBuild: goldenBuild, SHA256: digest, Recipe: "record it"}, "TestSample", false)
	if err != nil {
		t.Fatal(err)
	}
	return golden, path
}

func TestAFrozenGoldenIsBoundToTheVenuesDeclaredPythonSettings(t *testing.T) {
	declared := []string{"VENUE_PINNED_NOW=2026-01-01T00:00:00Z", "SETTINGS_ENCRYPTION_KEY=k"}
	key, _ := pythonEnvKey(declared)
	none, _ := pythonEnvKey(nil)

	// Recording: the key goes into the header.
	recording, err := openGolden(GoldenSpec{Path: t.TempDir() + "/g.json", PythonBuild: goldenBuild, Recipe: "record it"}, "TestSample", true)
	if err != nil {
		t.Fatal(err)
	}
	if err := recording.bindPythonEnv(declared); err != nil || recording.recorded.Header.PythonEnv != key {
		t.Fatalf("recording: err %v, header key %q, want %q", err, recording.recorded.Header.PythonEnv, key)
	}
	if err := recording.bindPythonEnv(declared[:1]); err == nil || !strings.Contains(err.Error(), "two venues") {
		t.Fatalf("a second venue with other settings was bound to one golden: %v", err)
	}

	for name, c := range map[string]struct {
		recorded string
		env      []string
		refusal  string // "" = accepted
	}{
		"the settings the golden was recorded under": {key, declared, ""},
		"the same settings in another order":         {key, []string{declared[1], declared[0]}, ""},
		"no settings, recorded with none":            {none, nil, ""},
		"a first setting on a venue that had none":   {none, declared[:1], "recorded under other Python settings"},
		"a golden from before the key, no settings":  {"", nil, "backfill the key"},
		"a changed value (the pinned clock)":         {key, []string{"VENUE_PINNED_NOW=2099-01-01T00:00:00Z", declared[1]}, "recorded under other Python settings"},
		"a new variable":                             {key, append(append([]string{}, declared...), "TRIAL_DAYS=7"), "recorded under other Python settings"},
		"a removed variable":                         {key, declared[:1], "recorded under other Python settings"},
		"settings removed from the test":             {key, nil, "recorded under other Python settings"},
		"a golden from before the key":               {"", declared, "backfill the key"},
	} {
		golden, _ := frozenWithEnv(t, c.recorded)
		err := golden.bindPythonEnv(c.env)
		if c.refusal == "" && err != nil {
			t.Errorf("%s: refused: %v", name, err)
		}
		if c.refusal != "" && (err == nil || !strings.Contains(err.Error(), c.refusal)) {
			t.Errorf("%s: err = %v, want a refusal holding %q", name, err, c.refusal)
		}
	}

	// A golden recorded under a venue's settings, in a test that builds no venue for it.
	golden, _ := frozenWithEnv(t, key)
	if err := golden.pythonEnvUnboundErr(); err == nil || !strings.Contains(err.Error(), "built no venue") {
		t.Fatalf("a golden with a settings key and no venue was accepted: %v", err)
	}
	if err := golden.bindPythonEnv(declared); err != nil {
		t.Fatal(err)
	}
	if err := golden.pythonEnvUnboundErr(); err != nil {
		t.Fatalf("a bound golden was refused: %v", err)
	}
	// A second venue of the same test with the same settings uses the same golden.
	if err := golden.bindPythonEnv([]string{declared[1], declared[0]}); err != nil {
		t.Fatalf("a second venue with the same settings was refused: %v", err)
	}
}

func TestWithPythonEnvKeyAddsTheKeyAndTouchesNothingElse(t *testing.T) {
	key, _ := pythonEnvKey([]string{"VENUE_PINNED_NOW=2026-01-01T00:00:00Z"})
	_, path := frozenWithEnv(t, "")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	withKey, err := WithPythonEnvKey(raw, key)
	if err != nil {
		t.Fatal(err)
	}
	// Numbers stay the text the file holds: a float64 would call two numbers equal that are not.
	decode := func(raw []byte) map[string]any {
		var out map[string]any
		decoder := json.NewDecoder(bytes.NewReader(raw))
		decoder.UseNumber()
		if err := decoder.Decode(&out); err != nil {
			t.Fatal(err)
		}
		return out
	}
	before, after := decode(raw), decode(withKey)
	header := after["header"].(map[string]any)
	if header["python_env"] != key {
		t.Fatalf("the key is %v, want %s", header["python_env"], key)
	}
	delete(header, "python_env")
	beforeText, _ := json.Marshal(before)
	afterText, _ := json.Marshal(after)
	if string(beforeText) != string(afterText) {
		t.Fatalf("more than the key differs:\n%s\n%s", beforeText, afterText)
	}
	// The result is a golden a frozen run of the test with those settings opens and accepts.
	sum := sha256.Sum256(withKey)
	if err := os.WriteFile(path, withKey, 0o644); err != nil {
		t.Fatal(err)
	}
	golden, err := openGolden(GoldenSpec{Path: path, PythonBuild: goldenBuild, SHA256: hex.EncodeToString(sum[:]), Recipe: "record it"}, "TestSample", false)
	if err != nil {
		t.Fatal(err)
	}
	if err := golden.bindPythonEnv([]string{"VENUE_PINNED_NOW=2026-01-01T00:00:00Z"}); err != nil {
		t.Fatalf("the golden with its key is refused by its own settings: %v", err)
	}

	for name, c := range map[string]struct {
		raw     []byte
		key     string
		refusal string
	}{
		"a file in another form":            {append([]byte(" "), raw...), key, "not in the form"},
		"a golden that already holds a key": {withKey, key, "already holds"},
		"a key that is not a key":           {raw, "yes", "is not a key"},
		"no key":                            {raw, "", "is not a key"},
		"not a golden":                      {[]byte("[]"), key, "not a golden file"},
	} {
		if out, err := WithPythonEnvKey(c.raw, c.key); err == nil || !strings.Contains(err.Error(), c.refusal) || out != nil {
			t.Errorf("%s: err = %v, want a refusal holding %q", name, err, c.refusal)
		}
	}
}

// startWithSettingsInChild runs the real Start in a child process, in a frozen
// venue test whose golden was recorded under the Options recorded and which
// declares the Options declared(name), and returns the child's output. The
// context is cancelled, so a Start that gets past the settings check fails on
// its first build step instead of building a venue.
func startWithSettingsInChild(t *testing.T, env string, recorded Options, declared func(name string) Options, name string) (string, error, bool) {
	t.Helper()
	if which := os.Getenv(env); which != "" {
		t.Setenv("DEV_HEALTH_LIVE_PYTHON_ORACLES", "1")
		t.Setenv(goldenUpdateEnv, "")
		t.Setenv(goldenCandidateEnv, "")
		key, err := pythonEnvKey(pythonPlaneEnv(recorded, nil))
		if err != nil {
			t.Fatal(err)
		}
		golden, _ := frozenWithEnv(t, key)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		options := declared(which)
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
// plane is refused, before anything is built, by a test that declares another
// value (the pinned clock), one variable more, none, or another JWT key.
func TestStartRefusesAGoldenRecordedUnderOtherPythonSettings(t *testing.T) {
	env := []string{"VENUE_PINNED_NOW=2026-01-01T00:00:00Z", "SETTINGS_ENCRYPTION_KEY=k"}
	recorded := Options{JWTKey: "recorded", PythonEnv: env}
	cases := map[string]Options{
		"a changed value":  {JWTKey: recorded.JWTKey, PythonEnv: []string{"VENUE_PINNED_NOW=2099-01-01T00:00:00Z", env[1]}},
		"a variable more":  {JWTKey: recorded.JWTKey, PythonEnv: append(append([]string{}, env...), "TRIAL_DAYS=7")},
		"no settings":      {JWTKey: recorded.JWTKey},
		"another per-run":  {JWTKey: recorded.JWTKey, PythonEnv: append(append([]string{}, env...), "VENUE_STRIPE_API_BASE=http://127.0.0.1:1")},
		"another JWT key":  {JWTKey: "other", PythonEnv: env},
		"the same (start)": recorded,
	}
	declared := func(name string) Options { return cases[name] }
	for name := range cases {
		output, err, inChild := startWithSettingsInChild(t, "VENUEORACLE_PYTHON_ENV_START_CHILD", recorded, declared, name)
		if inChild {
			return
		}
		if !strings.Contains(output, "START CALLED") {
			t.Fatalf("%s: the child did not reach Start:\n%s", name, output)
		}
		refused := strings.Contains(output, "recorded under other Python settings")
		if name == "the same (start)" {
			// The control: with the recorded settings the check lets Start go on.
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

// The key is taken over the whole environment the Python plane gets: the
// harness's own settings, the JWT key and PythonEnv. And that environment is
// the one Start built before a golden kept its key, entry for entry: a golden
// from before the key can only be given the key of today's environment
// because the two are the same.
func TestThePlaneEnvironmentIsKeyedWholeAndIsTheOneOfTheGoldensFromBeforeTheKey(t *testing.T) {
	options := Options{JWTKey: "jwt", PythonEnv: []string{"TRIAL_DAYS=7", "ENVIRONMENT=other"}}
	perRun := map[string]string{"PYTHONPATH": "/checkout/src", "POSTGRES_URI": "postgresql+asyncpg://db", "REDIS_URL": "redis://cache", "CLICKHOUSE_URI": "http://ch"}
	want := []string{"PYTHONPATH=/checkout/src", "POSTGRES_URI=postgresql+asyncpg://db",
		"JWT_SECRET_KEY=jwt", "OTEL_SDK_DISABLED=true", "DEV_HEALTH_ALLOW_CELERY_RIVER_CUTOVER=1",
		"PGBOUNCER_TRANSACTION_MODE=true", "ENVIRONMENT=test", "REDIS_URL=redis://cache",
		"CLICKHOUSE_URI=http://ch", "TRIAL_DAYS=7", "ENVIRONMENT=other"}
	if got := pythonPlaneEnv(options, perRun); strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("the Python plane's environment changed:\n got %q\nwant %q\na golden from before the key was recorded under the second one: it can no longer be given a key (remove the backfill verb), and every keyed golden must be recorded again", got, want)
	}
	key := func(o Options, values map[string]string) string {
		out, err := pythonEnvKey(pythonPlaneEnv(o, values))
		if err != nil {
			t.Fatal(err)
		}
		return out
	}
	base := key(options, nil)
	// The values made for a run are not in the key; every name of them is listed as per-run.
	if key(options, perRun) != base {
		t.Fatal("the key depends on a per-run value")
	}
	for name := range perRun {
		if _, listed := perRunPythonEnv[name]; !listed {
			t.Fatalf("%s is given a per-run value and is not in perRunPythonEnv", name)
		}
	}
	for name, other := range map[string]Options{
		"another JWT key":       {JWTKey: "other", PythonEnv: options.PythonEnv},
		"a PythonEnv value":     {JWTKey: "jwt", PythonEnv: []string{"TRIAL_DAYS=8", "ENVIRONMENT=other"}},
		"an override taken out": {JWTKey: "jwt", PythonEnv: []string{"TRIAL_DAYS=7"}},
	} {
		if key(other, nil) == base {
			t.Errorf("%s: the same key", name)
		}
	}
	// A harness setting is in the key by value: PythonEnv setting it to its own value changes nothing, another value does.
	plain := Options{JWTKey: "jwt"}
	if key(Options{JWTKey: "jwt", PythonEnv: []string{"ENVIRONMENT=test"}}, nil) != key(plain, nil) || key(Options{JWTKey: "jwt", PythonEnv: []string{"ENVIRONMENT=production"}}, nil) == key(plain, nil) {
		t.Fatal("the harness setting ENVIRONMENT is not in the key by value")
	}
}
