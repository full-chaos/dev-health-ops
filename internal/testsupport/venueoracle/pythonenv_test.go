package venueoracle

import (
	"bytes"
	"context"
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
	t.Setenv(goldenBackfillPythonEnv, "")
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

func TestTheBackfillAddsOnlyTheKeyToTheHeader(t *testing.T) {
	declared := []string{"VENUE_PINNED_NOW=2026-01-01T00:00:00Z"}
	key, _ := pythonEnvKey(declared)
	t.Setenv(goldenBackfillPythonEnv, "1")
	golden, path := frozenWithEnv(t, "")
	if err := golden.bindPythonEnv(declared); err != nil {
		t.Fatalf("the backfill run refused a golden from before the key: %v", err)
	}
	if _, err := golden.writeBackfillCandidate(true); err == nil {
		t.Fatal("a failed run wrote a candidate")
	}
	if _, err := os.Stat(path + GoldenCandidateSuffix); err == nil {
		t.Fatal("a failed run left a candidate")
	}
	digest, err := golden.writeBackfillCandidate(false)
	if err != nil || len(digest) != 64 {
		t.Fatalf("candidate: %v %q", err, digest)
	}
	rawBefore, _ := os.ReadFile(path)
	rawAfter, _ := os.ReadFile(path + GoldenCandidateSuffix)
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
	before, after := decode(rawBefore), decode(rawAfter)
	header := after["header"].(map[string]any)
	if header["python_env"] != key {
		t.Fatalf("the candidate's key is %v, want %s", header["python_env"], key)
	}
	delete(header, "python_env")
	beforeText, _ := json.Marshal(before)
	afterText, _ := json.Marshal(after)
	if string(beforeText) != string(afterText) {
		t.Fatalf("the candidate differs in more than the key:\n%s\n%s", beforeText, afterText)
	}

	// A file this harness did not write in this form is not touched.
	if err := os.WriteFile(path, append([]byte(" "), rawBefore...), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := golden.writeBackfillCandidate(false); err == nil || !strings.Contains(err.Error(), "not in the form") {
		t.Fatalf("a golden in another form was backfilled: %v", err)
	}

	// The backfill switch never loosens a golden that holds another key.
	other, _ := frozenWithEnv(t, strings.Repeat("b", 64))
	if err := other.bindPythonEnv(declared); err == nil || !strings.Contains(err.Error(), "recorded under other Python settings") {
		t.Fatalf("the backfill switch accepted a golden recorded under other settings: %v", err)
	}
}

// The real Start, in a child process: a frozen golden recorded under one set of
// Python settings is refused, before anything is built, by a test that declares
// another value (the pinned clock), one variable more, or none.
func TestStartRefusesAGoldenRecordedUnderOtherPythonSettings(t *testing.T) {
	declared := []string{"VENUE_PINNED_NOW=2026-01-01T00:00:00Z", "SETTINGS_ENCRYPTION_KEY=k"}
	cases := map[string][]string{
		"a changed value":  {"VENUE_PINNED_NOW=2099-01-01T00:00:00Z", declared[1]},
		"a variable more":  append(append([]string{}, declared...), "TRIAL_DAYS=7"),
		"no settings":      nil,
		"another per-run":  append(append([]string{}, declared...), "VENUE_STRIPE_API_BASE=http://127.0.0.1:1"),
		"the same (start)": declared,
	}
	const childEnv = "VENUEORACLE_PYTHON_ENV_START_CHILD"
	if name := os.Getenv(childEnv); name != "" {
		t.Setenv("DEV_HEALTH_LIVE_PYTHON_ORACLES", "1")
		t.Setenv(goldenUpdateEnv, "")
		t.Setenv(goldenCandidateEnv, "")
		t.Setenv(goldenBackfillPythonEnv, "")
		key, err := pythonEnvKey(declared)
		if err != nil {
			t.Fatal(err)
		}
		golden, _ := frozenWithEnv(t, key)
		// A cancelled context: a Start that gets past the settings check fails on
		// its first build step instead of building a venue.
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		t.Log("START CALLED")
		Start(t, ctx, Options{Root: t.TempDir(), JWTKey: "k", Golden: golden, PythonEnv: cases[name]})
		t.Log("START RETURNED")
		return
	}
	for name := range cases {
		command := exec.Command(os.Args[0], "-test.run=^"+t.Name()+"$", "-test.v")
		command.Env = append(os.Environ(), childEnv+"="+name)
		raw, err := command.CombinedOutput()
		output := string(raw)
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
