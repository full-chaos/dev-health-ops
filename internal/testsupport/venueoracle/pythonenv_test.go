package venueoracle

import (
	"encoding/json"
	"os"
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
	var before, after map[string]any
	rawBefore, _ := os.ReadFile(path)
	rawAfter, _ := os.ReadFile(path + GoldenCandidateSuffix)
	if json.Unmarshal(rawBefore, &before) != nil || json.Unmarshal(rawAfter, &after) != nil {
		t.Fatal("decode")
	}
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
