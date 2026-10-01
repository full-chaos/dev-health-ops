package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// fakeOracle stands in for `go test`: the record run (UPDATE=1) writes a
// candidate the way Finish does, the replay run (CANDIDATE=1) reads it.
type fakeOracle struct {
	dir            string
	candidateBody  string
	recordErr      error // returned by the record run AFTER it wrote the candidate (a cleanup that failed after Finish)
	replayErr      error
	recordWrites   bool
	calls          []string
	replayReadBody string
	// afterReplay runs after the replay read the candidate (a cleanup that replaces it).
	afterReplay func() error
	// pythonRoot, and what the record run saw of it.
	pythonRoot        string
	bytecodeAtRecord  bool
	dontWriteBytecode bool
}

func (f *fakeOracle) run(cfg Config, env []string) error {
	has := func(name string) bool {
		for _, entry := range env {
			if entry == name+"=1" {
				return true
			}
		}
		return false
	}
	switch {
	case has("DHO_VENUE_GOLDEN_UPDATE"):
		f.calls = append(f.calls, "record")
		_, err := os.Stat(filepath.Join(f.pythonRoot, "src", "app", "__pycache__"))
		f.bytecodeAtRecord = err == nil
		f.dontWriteBytecode = has("PYTHONDONTWRITEBYTECODE")
		if f.recordWrites {
			if err := os.MkdirAll(filepath.Join(f.dir, "testdata"), 0o755); err != nil {
				return err
			}
			if err := os.WriteFile(filepath.Join(f.dir, "testdata", "g.json.recording"), []byte(f.candidateBody), 0o644); err != nil {
				return err
			}
		}
		return f.recordErr
	case has("DHO_VENUE_GOLDEN_CANDIDATE"):
		f.calls = append(f.calls, "replay")
		raw, err := os.ReadFile(filepath.Join(f.dir, "testdata", "g.json.recording"))
		if err != nil {
			return err
		}
		f.replayReadBody = string(raw)
		if f.afterReplay != nil {
			if err := f.afterReplay(); err != nil {
				return err
			}
		}
		return f.replayErr
	}
	return errors.New("unexpected run")
}

func fixture(t *testing.T, existingGolden string) (Config, *fakeOracle, string) {
	t.Helper()
	root := t.TempDir()
	dir := filepath.Join(root, "pkg")
	if err := os.MkdirAll(filepath.Join(dir, "testdata"), 0o755); err != nil {
		t.Fatal(err)
	}
	if existingGolden != "" {
		if err := os.WriteFile(filepath.Join(dir, "testdata", "g.json"), []byte(existingGolden), 0o644); err != nil {
			t.Fatal(err)
		}
		testFile := "package pkg\nconst pin = \"" + digest([]byte(existingGolden)) + "\"\n"
		if err := os.WriteFile(filepath.Join(dir, "x_test.go"), []byte(testFile), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if existingGolden == "" {
		if err := os.WriteFile(filepath.Join(dir, "x_test.go"), []byte("package pkg\nconst pin = \"PIN:g\"\n"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	fake := &fakeOracle{dir: dir, candidateBody: "NEW GOLDEN\n", recordWrites: true}
	pythonRoot := t.TempDir()
	if err := os.MkdirAll(filepath.Join(pythonRoot, "src", "app", "__pycache__"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(pythonRoot, "src", "app", "__pycache__", "m.cpython-314.pyc"), []byte("stale"), 0o644); err != nil {
		t.Fatal(err)
	}
	fake.pythonRoot = pythonRoot
	return Config{Root: root, Package: "./pkg/", Test: "^TestX$", PythonRoot: pythonRoot, Run: fake.run}, fake, dir
}

func exists(path string) bool { _, err := os.Stat(path); return err == nil }

func TestAGoldenLandsOnlyAfterTheFreshProcessReplayPasses(t *testing.T) {
	cfg, fake, dir := fixture(t, "OLD GOLDEN\n")
	result, err := Record(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(fake.calls, ",") != "record,replay" || fake.replayReadBody != "NEW GOLDEN\n" {
		t.Fatalf("calls %v, replay read %q: the replay must run after the record run and read the candidate", fake.calls, fake.replayReadBody)
	}
	got, _ := os.ReadFile(filepath.Join(dir, "testdata", "g.json"))
	if string(got) != "NEW GOLDEN\n" || exists(filepath.Join(dir, "testdata", "g.json.recording")) {
		t.Fatalf("the golden is %q or the candidate remains", got)
	}
	if len(result.Promoted) != 1 || len(result.Promoted[0].Pinned) != 1 {
		t.Fatalf("result = %+v", result)
	}
	pinned, _ := os.ReadFile(filepath.Join(dir, "x_test.go"))
	if !strings.Contains(string(pinned), digest([]byte("NEW GOLDEN\n"))) || strings.Contains(string(pinned), digest([]byte("OLD GOLDEN\n"))) {
		t.Fatalf("the test still pins the old digest: %s", pinned)
	}
}

// A failing cleanup registered before OpenGolden runs after Finish wrote the
// candidate and fails the recording run: the golden must not land.
func TestAFailingCleanupAfterFinishLeavesTheGoldenUntouched(t *testing.T) {
	cfg, fake, dir := fixture(t, "OLD GOLDEN\n")
	fake.recordErr = errors.New("cleanup registered before OpenGolden failed")
	if _, err := Record(context.Background(), cfg); err == nil || !strings.Contains(err.Error(), "recording run failed") {
		t.Fatalf("err = %v", err)
	}
	got, _ := os.ReadFile(filepath.Join(dir, "testdata", "g.json"))
	if string(got) != "OLD GOLDEN\n" {
		t.Fatalf("the golden changed to %q", got)
	}
	if exists(filepath.Join(dir, "testdata", "g.json.recording")) {
		t.Fatal("the candidate of a failed recording was left on disk")
	}
	if strings.Join(fake.calls, ",") != "record" {
		t.Fatalf("the replay ran after a failed recording: %v", fake.calls)
	}
}

func TestAFailingReplayLeavesTheGoldenUntouched(t *testing.T) {
	cfg, fake, dir := fixture(t, "OLD GOLDEN\n")
	fake.replayErr = errors.New("the candidate does not replay")
	if _, err := Record(context.Background(), cfg); err == nil || !strings.Contains(err.Error(), "replay") {
		t.Fatalf("err = %v", err)
	}
	got, _ := os.ReadFile(filepath.Join(dir, "testdata", "g.json"))
	if string(got) != "OLD GOLDEN\n" || exists(filepath.Join(dir, "testdata", "g.json.recording")) {
		t.Fatalf("golden %q, candidate present %v", got, exists(filepath.Join(dir, "testdata", "g.json.recording")))
	}
}

func TestARecordingThatWroteNoCandidateIsAnError(t *testing.T) {
	cfg, fake, _ := fixture(t, "OLD GOLDEN\n")
	fake.recordWrites = false
	if _, err := Record(context.Background(), cfg); err == nil || !strings.Contains(err.Error(), "no candidate") {
		t.Fatalf("err = %v", err)
	}
}

func TestACandidateFromAnEarlierRunIsRefused(t *testing.T) {
	cfg, _, dir := fixture(t, "OLD GOLDEN\n")
	if err := os.WriteFile(filepath.Join(dir, "testdata", "g.json.recording"), []byte("stale"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Record(context.Background(), cfg); err == nil || !strings.Contains(err.Error(), "earlier run") {
		t.Fatalf("err = %v", err)
	}
}

// A golden that does not exist yet is pinned through its placeholder, so the
// frozen replay accepts what the verb promoted.
func TestANewGoldenIsPromotedAndPinnedThroughItsPlaceholder(t *testing.T) {
	cfg, _, dir := fixture(t, "")
	result, err := Record(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Promoted) != 1 || result.Promoted[0].OldDigest != "" || len(result.Promoted[0].Pinned) != 1 {
		t.Fatalf("result = %+v", result)
	}
	pinned, _ := os.ReadFile(filepath.Join(dir, "x_test.go"))
	if !strings.Contains(string(pinned), digest([]byte("NEW GOLDEN\n"))) || strings.Contains(string(pinned), "PIN:g") {
		t.Fatalf("the placeholder was not replaced by the digest: %s", pinned)
	}
}

func TestAGoldenNoTestPinsIsNotPromoted(t *testing.T) {
	cfg, _, dir := fixture(t, "")
	if err := os.WriteFile(filepath.Join(dir, "x_test.go"), []byte("package pkg\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Record(context.Background(), cfg); err == nil || !strings.Contains(err.Error(), "no test in") {
		t.Fatalf("err = %v", err)
	}
	if exists(filepath.Join(dir, "testdata", "g.json")) || exists(filepath.Join(dir, "testdata", "g.json.recording")) {
		t.Fatal("a golden no test pins was promoted, or its candidate left behind")
	}
}

// A cleanup that succeeds during the replay may replace the candidate after the
// replay read it: only the replayed bytes may be promoted.
func TestACandidateChangedAfterItsReplayIsNotPromoted(t *testing.T) {
	cfg, fake, dir := fixture(t, "OLD GOLDEN\n")
	fake.afterReplay = func() error {
		return os.WriteFile(filepath.Join(dir, "testdata", "g.json.recording"), []byte("CLEANUP REPLACEMENT\n"), 0o644)
	}
	if _, err := Record(context.Background(), cfg); err == nil || !strings.Contains(err.Error(), "changed after it was replayed") {
		t.Fatalf("err = %v", err)
	}
	got, _ := os.ReadFile(filepath.Join(dir, "testdata", "g.json"))
	if string(got) != "OLD GOLDEN\n" {
		t.Fatalf("the golden is %q", got)
	}
}

// A write that fails after the golden was written puts everything back.
func TestAPromotionThatFailsHalfwayRestoresEveryFile(t *testing.T) {
	cfg, _, dir := fixture(t, "OLD GOLDEN\n")
	before, _ := os.ReadFile(filepath.Join(dir, "x_test.go"))
	// The package directory is not writable: the test file's temporary file
	// cannot be created, after the golden (in testdata) was already replaced.
	if err := os.Chmod(dir, 0o555); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(dir, 0o755) })
	if _, err := Record(context.Background(), cfg); err == nil || !strings.Contains(err.Error(), "put back") {
		if err == nil || os.Geteuid() == 0 {
			t.Skip("cannot make the package directory unwritable here")
		}
		t.Fatalf("err = %v", err)
	}
	_ = os.Chmod(dir, 0o755)
	got, _ := os.ReadFile(filepath.Join(dir, "testdata", "g.json"))
	pinned, _ := os.ReadFile(filepath.Join(dir, "x_test.go"))
	if string(got) != "OLD GOLDEN\n" || string(pinned) != string(before) {
		t.Fatalf("the promotion left the golden %q and the test %q", got, pinned)
	}
}

// The default runner: `go test -tags=integration -run <regexp> <pkg>` in the
// root, with the extra environment.
func TestTheDefaultRunnerRunsGoTestWithTheEnvironment(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "pkg")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	files := map[string]string{
		filepath.Join(root, "go.mod"): "module probe\n\ngo 1.22\n",
		filepath.Join(dir, "probe_test.go"): "//go:build integration\n\npackage pkg\n\nimport (\n\t\"os\"\n\t\"testing\"\n)\n\n" +
			"func TestProbe(t *testing.T) {\n\tseen := os.Getenv(\"DHO_VENUE_GOLDEN_UPDATE\") + \"|\" + os.Getenv(\"GOLDENRECORD_AMBIENT_PROBE\") + \"|\" + os.Getenv(\"GOLDENRECORD_NAMED_PROBE\") + \"|\" + os.Getenv(\"TESTCONTAINERS_PROBE\") + os.Getenv(\"DEV_HEALTH_UNLISTED_PROBE\") + os.Getenv(\"DEV_HEALTH_PYTHON\") + \"|\" + os.Getenv(\"TESTCONTAINERS_RYUK_DISABLED\") + \"|\" + os.Getenv(\"LC_ALL\")\n\tif err := os.WriteFile(\"seen.txt\", []byte(seen), 0o644); err != nil {\n\t\tt.Fatal(err)\n\t}\n}\n\n" +
			"func TestNotSelected(t *testing.T) {\n\tt.Fatal(\"the -run regexp was ignored\")\n}\n",
	}
	for path, content := range files {
		if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	// An ambient variable outside the passed names must not reach the tests
	// (it could shape a producer's answers unseen), whatever family its name
	// belongs to: no prefix is passed. One named with PassEnv, one passed by
	// exact name and the verb's own must reach them; the locale is fixed.
	t.Setenv("GOLDENRECORD_AMBIENT_PROBE", "leak")
	t.Setenv("GOLDENRECORD_NAMED_PROBE", "named")
	t.Setenv("TESTCONTAINERS_PROBE", "leak-testcontainers-family")
	t.Setenv("DEV_HEALTH_UNLISTED_PROBE", "leak-dev-health-family")
	t.Setenv("DEV_HEALTH_PYTHON", "/leak/interpreter")
	t.Setenv("TESTCONTAINERS_RYUK_DISABLED", "exact")
	t.Setenv("LC_ALL", "tr_TR.UTF-8")
	cfg := Config{Root: root, Package: "./pkg/", Test: "^TestProbe$", PassEnv: []string{"GOLDENRECORD_NAMED_PROBE"}}
	if err := goTest(cfg, []string{"DHO_VENUE_GOLDEN_UPDATE=1"}); err != nil {
		t.Fatalf("the default runner failed: %v", err)
	}
	seen, err := os.ReadFile(filepath.Join(dir, "seen.txt"))
	if err != nil || string(seen) != "1||named||exact|C.UTF-8" {
		t.Fatalf("the test saw the environment %q (%v), want the verb's variable, no ambient probe, the named one, no variable passed by family, the exact-name one, and the fixed locale", seen, err)
	}
}

// The record run is told which names -pass-env passed (sorted), so each
// golden can require exactly the names it declares.
func TestTheRecordRunIsToldThePassedNames(t *testing.T) {
	cfg, fake, _ := fixture(t, "")
	cfg.PassEnv = []string{"B_NAME", "A_NAME"}
	var seen string
	run := cfg.Run
	cfg.Run = func(c Config, env []string) error {
		for _, entry := range env {
			if strings.HasPrefix(entry, passedEnvName+"=") && seen == "" {
				seen = entry
			}
		}
		return run(c, env)
	}
	if _, err := Record(context.Background(), cfg); err != nil {
		t.Fatal(err)
	}
	if seen != passedEnvName+"=A_NAME,B_NAME" || len(fake.calls) == 0 {
		t.Fatalf("the record run saw %q", seen)
	}
}

func TestTheRecordingEnvironmentKeepsOnlyThePassedVariables(t *testing.T) {
	ambient := []string{"PATH=/bin", "HOME=/h", "GOCACHE=/c", "GOOGLE_APPLICATION_CREDENTIALS=/k", "LOG_LEVEL=DEBUG", "POSTGRES_URI=postgres://x",
		"DOCKER_HOST=unix:///d", "DOCKER_UNLISTED=x", "DEV_HEALTH_PYTHON=/p", "DEV_HEALTH_STRIPE_TEST_MODE=1", "DHO_VENUE_GOLDEN_UPDATE=1",
		"LANG=tr_TR.UTF-8", "EMPTY=", "STRIPE_KEY_NAME=v"}
	got := strings.Join(recordingEnv(ambient, []string{"STRIPE_KEY_NAME"}), "\n")
	want := "PATH=/bin\nHOME=/h\nGOCACHE=/c\nDOCKER_HOST=unix:///d\nSTRIPE_KEY_NAME=v\nLANG=C.UTF-8\nLC_ALL=C.UTF-8"
	if got != want {
		t.Fatalf("recording environment:\n%s\nwant:\n%s", got, want)
	}
}

// A stale bytecode cache in the pinned checkout can run code older than its
// source: the verb clears it and records with bytecode writing switched off.
func TestTheVerbClearsTheBytecodeCacheAndRecordsWithoutWritingBytecode(t *testing.T) {
	cfg, fake, _ := fixture(t, "OLD GOLDEN\n")
	if _, err := Record(context.Background(), cfg); err != nil {
		t.Fatal(err)
	}
	if fake.bytecodeAtRecord {
		t.Fatal("the record run started with a stale __pycache__ in the pinned checkout")
	}
	if !fake.dontWriteBytecode {
		t.Fatal("the record run did not switch bytecode writing off")
	}
}

func goldenJSON(requests []string, rows []string) string {
	var parts []string
	for _, name := range requests {
		parts = append(parts, `{"name":"`+name+`"}`)
	}
	rowParts := make([]string, 0, len(rows))
	for _, name := range rows {
		rowParts = append(rowParts, `"`+name+`":{"rows":"x"}`)
	}
	return `{"requests":[` + strings.Join(parts, ",") + `],"rows":{` + strings.Join(rowParts, ",") + `}}` + "\n"
}

// A re-record that lost a request or a row comparison (an accidental edit of the
// test) must not erase that baseline unless the author says so.
func TestAReRecordThatDropsCoverageIsRefusedUnlessAllowed(t *testing.T) {
	old := goldenJSON([]string{"a", "b"}, []string{"orgs"})
	shrunk := goldenJSON([]string{"a"}, nil)
	cfg, fake, dir := fixture(t, old)
	fake.candidateBody = shrunk
	_, err := Record(context.Background(), cfg)
	if err == nil || !strings.Contains(err.Error(), `request "b"`) || !strings.Contains(err.Error(), `row comparison "orgs"`) || !strings.Contains(err.Error(), "-allow-drop") {
		t.Fatalf("a re-record that dropped coverage was accepted: %v", err)
	}
	got, _ := os.ReadFile(filepath.Join(dir, "testdata", "g.json"))
	if string(got) != old || exists(filepath.Join(dir, "testdata", "g.json.recording")) {
		t.Fatalf("the golden changed to %q or the candidate remains", got)
	}
	// An intended removal is allowed explicitly.
	cfg, fake, dir = fixture(t, old)
	fake.candidateBody = shrunk
	cfg.AllowDrop = true
	if _, err := Record(context.Background(), cfg); err != nil {
		t.Fatalf("an allowed drop was refused: %v", err)
	}
	got, _ = os.ReadFile(filepath.Join(dir, "testdata", "g.json"))
	if string(got) != shrunk {
		t.Fatalf("the golden is %q", got)
	}
	// Added coverage is never a drop.
	cfg, fake, _ = fixture(t, old)
	fake.candidateBody = goldenJSON([]string{"a", "b", "c"}, []string{"orgs", "more"})
	if _, err := Record(context.Background(), cfg); err != nil {
		t.Fatalf("added coverage was refused: %v", err)
	}
}

// backfillFixture is a package with one pinned golden and a runner standing in
// for `go test`: the backfill run writes candidate (when not empty), the replay
// run reads it.
func backfillFixture(t *testing.T, golden, candidate string, replayErr error) (Config, string, *[]string) {
	t.Helper()
	root := t.TempDir()
	dir := filepath.Join(root, "pkg")
	if err := os.MkdirAll(filepath.Join(dir, "testdata"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "testdata", "g.json"), []byte(golden), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "x_test.go"), []byte("package pkg\nconst pin = \""+digest([]byte(golden))+"\"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	var calls []string
	cfg := Config{Root: root, Package: "./pkg/", Test: "^TestProbe$"}
	cfg.Run = func(_ Config, env []string) error {
		for _, entry := range env {
			switch entry {
			case "DHO_VENUE_GOLDEN_UPDATE=1":
				t.Fatal("a backfill ran a recording")
			case "DHO_VENUE_GOLDEN_BACKFILL_PYTHON_ENV=1":
				calls = append(calls, "backfill")
				if candidate == "" {
					return nil
				}
				return os.WriteFile(filepath.Join(dir, "testdata", "g.json.recording"), []byte(candidate), 0o644)
			case "DHO_VENUE_GOLDEN_CANDIDATE=1":
				calls = append(calls, "replay")
				return replayErr
			}
		}
		return errors.New("unexpected run")
	}
	return cfg, dir, &calls
}

const (
	backfillGolden = "{\n  \"header\": {\n    \"test\": \"T\"\n  },\n  \"requests\": [\n    {\n      \"name\": \"a\",\n      \"status\": 200,\n      \"body\": \"{\\\"n\\\": 1.50}\"\n    }\n  ]\n}\n"
	backfillKey    = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
)

func withKey(golden string) string {
	return strings.Replace(golden, "\"test\": \"T\"\n", "\"test\": \"T\",\n    \"python_env\": \""+backfillKey+"\"\n", 1)
}

func TestTheBackfillPromotesAGoldenWithOnlyTheKeyAdded(t *testing.T) {
	cfg, dir, calls := backfillFixture(t, backfillGolden, withKey(backfillGolden), nil)
	result, err := BackfillPythonEnv(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(*calls, ",") != "backfill,replay" || len(result.Promoted) != 1 {
		t.Fatalf("calls %v, result %+v", *calls, result)
	}
	landed, _ := os.ReadFile(filepath.Join(dir, "testdata", "g.json"))
	pinned, _ := os.ReadFile(filepath.Join(dir, "x_test.go"))
	if string(landed) != withKey(backfillGolden) || !strings.Contains(string(pinned), digest(landed)) || exists(filepath.Join(dir, "testdata", "g.json.recording")) {
		t.Fatalf("the golden with its key did not land with its pin:\n%s\n%s", landed, pinned)
	}
}

func TestTheBackfillTouchesNothingButTheKey(t *testing.T) {
	for name, candidate := range map[string]string{
		"an answer changed":       strings.Replace(withKey(backfillGolden), "1.50", "1.5", 1),
		"a status changed":        strings.Replace(withKey(backfillGolden), "200", "201", 1),
		"a request dropped":       strings.Replace(withKey(backfillGolden), "\"requests\": [\n    {\n      \"name\": \"a\",\n      \"status\": 200,\n      \"body\": \"{\\\"n\\\": 1.50}\"\n    }\n  ]", "\"requests\": []", 1),
		"another header field":    strings.Replace(withKey(backfillGolden), "\"test\": \"T\"", "\"test\": \"T2\"", 1),
		"no key":                  backfillGolden,
		"a key that is not a key": strings.Replace(withKey(backfillGolden), backfillKey, "yes", 1),
	} {
		cfg, dir, calls := backfillFixture(t, backfillGolden, candidate, nil)
		if _, err := BackfillPythonEnv(context.Background(), cfg); err == nil {
			t.Errorf("%s: promoted", name)
		}
		landed, _ := os.ReadFile(filepath.Join(dir, "testdata", "g.json"))
		if string(landed) != backfillGolden || exists(filepath.Join(dir, "testdata", "g.json.recording")) || strings.Contains(strings.Join(*calls, ","), "replay") {
			t.Errorf("%s: the golden changed, a candidate stayed, or the replay ran (%v)", name, *calls)
		}
	}
	// A golden that already holds a key is never given another.
	cfg, _, _ := backfillFixture(t, withKey(backfillGolden), strings.Replace(withKey(backfillGolden), backfillKey, strings.Repeat("f", 64), 1), nil)
	if _, err := BackfillPythonEnv(context.Background(), cfg); err == nil || !strings.Contains(err.Error(), "already holds") {
		t.Fatalf("a key was replaced: %v", err)
	}
}

func TestTheBackfillNeedsThePassingFrozenRunAndTheReplay(t *testing.T) {
	cfg, dir, _ := backfillFixture(t, backfillGolden, withKey(backfillGolden), errors.New("replay failed"))
	if _, err := BackfillPythonEnv(context.Background(), cfg); err == nil || !strings.Contains(err.Error(), "replay") {
		t.Fatalf("err = %v", err)
	}
	landed, _ := os.ReadFile(filepath.Join(dir, "testdata", "g.json"))
	if string(landed) != backfillGolden || exists(filepath.Join(dir, "testdata", "g.json.recording")) {
		t.Fatal("a failed replay changed the golden or left its candidate")
	}
	// No candidate: nothing needed the key, nothing is changed, no replay.
	cfg, _, calls := backfillFixture(t, backfillGolden, "", nil)
	result, err := BackfillPythonEnv(context.Background(), cfg)
	if err != nil || len(result.Promoted) != 0 || strings.Join(*calls, ",") != "backfill" {
		t.Fatalf("nothing to backfill: err %v, result %+v, calls %v", err, result, *calls)
	}
}
