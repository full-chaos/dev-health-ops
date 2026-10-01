package main

import (
	"archive/tar"
	"bytes"
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

// backfillGolden is a venue golden in the form the harness writes, recorded
// before a golden kept the key of its venue's Python settings.
func backfillGolden(test string) string {
	return "{\n  \"header\": {\n    \"test\": \"" + test + "\",\n    \"python_build\": \"" + strings.Repeat("0123456789", 4) + "\",\n    \"producer_digest\": \"" + strings.Repeat("a", 64) + "\",\n    \"recipe\": \"record it\"\n  },\n" +
		"  \"requests\": [\n    {\n      \"name\": \"a\",\n      \"method\": \"GET\",\n      \"path\": \"/x\",\n      \"body_sha256\": \"\",\n      \"request_headers_sha256\": \"\",\n      \"status\": 200,\n" +
		"      \"headers\": {\n        \"content-type\": \"application/json\"\n      },\n      \"body\": \"{\\\"n\\\": 1.50}\"\n    }\n  ]\n}\n"
}

var (
	keyNow   = strings.Repeat("1", 64)
	keyOther = strings.Repeat("2", 64)
)

func withKey(golden, key string) string {
	return strings.Replace(golden, "\"recipe\": \"record it\"\n", "\"recipe\": \"record it\",\n    \"python_env\": \""+key+"\"\n", 1)
}

// backfillRun is a package with pinned goldens and stand-ins for what the
// verb reads: the keys the tests declare now (tree = the root) and at a
// recording commit (tree = "tree of <commit>"), git, and go test.
type backfillRun struct {
	cfg       Config
	dir       string
	now       map[string]string
	then      map[string]map[string]string // commit -> test -> key
	commits   map[string]string            // golden file name -> commit
	calls     []string
	replayErr error
	// duringReplay runs inside the replay, as a test that writes its candidate again would.
	duringReplay func()
}

func newBackfillRun(t *testing.T, goldens map[string]string) *backfillRun {
	t.Helper()
	root := t.TempDir()
	run := &backfillRun{dir: filepath.Join(root, "pkg"), now: map[string]string{}, then: map[string]map[string]string{}, commits: map[string]string{}}
	if err := os.MkdirAll(filepath.Join(run.dir, "testdata"), 0o755); err != nil {
		t.Fatal(err)
	}
	pins := "package pkg\n"
	for name, golden := range goldens {
		if err := os.WriteFile(filepath.Join(run.dir, "testdata", name), []byte(golden), 0o644); err != nil {
			t.Fatal(err)
		}
		pins += "const pin_" + strings.TrimSuffix(name, ".json") + " = \"" + digest([]byte(golden)) + "\"\n"
		run.commits[name] = "c1"
	}
	if err := os.WriteFile(filepath.Join(run.dir, "x_test.go"), []byte(pins), 0o644); err != nil {
		t.Fatal(err)
	}
	run.cfg = Config{Root: root, Package: "./pkg/", Test: "^Test"}
	run.cfg.Keys = func(_ Config, tree string) (map[string]string, error) {
		if tree == root {
			run.calls = append(run.calls, "keys now")
			if run.now == nil {
				return nil, errors.New("does not build")
			}
			return run.now, nil
		}
		run.calls = append(run.calls, "keys in "+tree)
		keys, ok := run.then[strings.TrimPrefix(tree, "tree of ")]
		if !ok {
			return nil, errors.New("does not build")
		}
		return keys, nil
	}
	run.cfg.RecordingCommit = func(_ Config, golden string) (string, error) {
		commit := run.commits[filepath.Base(golden)]
		if commit == "" {
			return "", errors.New("no commit holds the golden as it is on disk")
		}
		return commit, nil
	}
	run.cfg.Tree = func(_ Config, commit string) (string, func(), error) {
		if commit == "gone" {
			return "", nil, errors.New("unknown revision")
		}
		run.calls = append(run.calls, "tree "+commit)
		return "tree of " + commit, func() { run.calls = append(run.calls, "cleanup "+commit) }, nil
	}
	run.cfg.Run = func(cfg Config, env []string) error {
		if strings.Join(env, " ") != strings.Join([]string{env[0], env[1], "DHO_VENUE_GOLDEN_CANDIDATE=1"}, " ") || len(env) != 3 {
			t.Fatalf("the verb ran go test with %q: it replays candidates and does nothing else", env)
		}
		run.calls = append(run.calls, "replay "+cfg.Test)
		if run.duringReplay != nil {
			run.duringReplay()
		}
		return run.replayErr
	}
	return run
}

// untouched reports whether every golden and the pins are as the fixture wrote them and no candidate is on disk.
func (run *backfillRun) untouched(t *testing.T, goldens map[string]string) bool {
	t.Helper()
	pins, _ := os.ReadFile(filepath.Join(run.dir, "x_test.go"))
	for name, golden := range goldens {
		landed, _ := os.ReadFile(filepath.Join(run.dir, "testdata", name))
		if string(landed) != golden || !strings.Contains(string(pins), digest([]byte(golden))) || exists(filepath.Join(run.dir, "testdata", name+candidateSuffix)) {
			return false
		}
	}
	return true
}

func TestTheBackfillAddsTheKeyWhenTheTestDeclaredTheSameSettingsAtTheRecording(t *testing.T) {
	goldens := map[string]string{"g.json": backfillGolden("TestProbe"), "sub.json": backfillGolden("TestOther/case"), "keyed.json": withKey(backfillGolden("TestKeyed"), keyOther),
		"program.json": backfillGolden("TestProgram")}
	run := newBackfillRun(t, goldens)
	run.now = map[string]string{"TestProbe": keyNow, "TestOther/case": keyOther, "TestKeyed": keyNow}
	run.then["c1"] = map[string]string{"TestProbe": keyNow, "TestOther/case": keyOther}
	result, err := BackfillPythonEnv(context.Background(), run.cfg)
	if err != nil {
		t.Fatal(err)
	}
	// One tree for the one recording commit, removed again; the replay runs the tests of the backfilled goldens only.
	if got, want := strings.Join(run.calls, ", "), "keys now, tree c1, keys in tree of c1, cleanup c1, replay ^(TestOther|TestProbe)$"; got != want {
		t.Fatalf("calls:\n %s\nwant\n %s", got, want)
	}
	if len(result.Promoted) != 2 || len(result.Shown) != 2 || !strings.Contains(result.Shown[0], "testdata/g.json: recorded at c1; its test TestProbe") {
		t.Fatalf("promoted %+v, shown %q", result.Promoted, result.Shown)
	}
	pins, _ := os.ReadFile(filepath.Join(run.dir, "x_test.go"))
	for name, key := range map[string]string{"g.json": keyNow, "sub.json": keyOther} {
		landed, _ := os.ReadFile(filepath.Join(run.dir, "testdata", name))
		if string(landed) != withKey(goldens[name], key) || !strings.Contains(string(pins), digest(landed)) || exists(filepath.Join(run.dir, "testdata", name+candidateSuffix)) {
			t.Errorf("%s did not land with its key and its pin:\n%s\n%s", name, landed, pins)
		}
	}
	// A golden that holds a key, and a golden of a test that builds no venue, are not touched.
	for _, name := range []string{"keyed.json", "program.json"} {
		landed, _ := os.ReadFile(filepath.Join(run.dir, "testdata", name))
		if string(landed) != goldens[name] {
			t.Errorf("%s was changed", name)
		}
	}
	// Nothing left to do: no tree is built and no test runs.
	run.calls = nil
	result, err = BackfillPythonEnv(context.Background(), run.cfg)
	if err != nil || len(result.Promoted) != 0 || strings.Join(run.calls, ", ") != "keys now" {
		t.Fatalf("a second run: err %v, promoted %+v, calls %v", err, result.Promoted, run.calls)
	}
}

func TestTheBackfillRefusesAGoldenItCannotShowWasRecordedUnderTheSettingsOfToday(t *testing.T) {
	goldens := map[string]string{"g.json": backfillGolden("TestProbe"), "h.json": backfillGolden("TestSecond")}
	for name, c := range map[string]struct {
		edit    func(run *backfillRun)
		refusal string
	}{
		"the test declares another value now":      {func(run *backfillRun) { run.now["TestProbe"] = keyOther }, "g.json was recorded at c1 under other Python settings than its test TestProbe declares now"},
		"the second golden's settings changed":     {func(run *backfillRun) { run.then["c1"]["TestSecond"] = keyOther }, "h.json was recorded at c1 under other Python settings"},
		"the test built no venue at the recording": {func(run *backfillRun) { delete(run.then["c1"], "TestProbe") }, "its test TestProbe declared no venue settings at its recording commit c1"},
		"no commit holds the golden":               {func(run *backfillRun) { run.commits["g.json"] = "" }, "its recording commit was not found"},
		"the recording commit has no tree":         {func(run *backfillRun) { run.commits["h.json"] = "gone" }, "the tree of its recording commit gone could not be built"},
		"the recording commit does not build":      {func(run *backfillRun) { delete(run.then, "c1") }, "declared at its recording commit c1 could not be read"},
		"the tests of today do not build":          {func(run *backfillRun) { run.now = nil }, "declare now could not be read"},
		"the replay fails":                         {func(run *backfillRun) { run.replayErr = errors.New("frozen test failed") }, "fresh-process replay of the candidates failed"},
		"the replay changes a candidate": {func(run *backfillRun) {
			run.duringReplay = func() {
				_ = os.WriteFile(filepath.Join(run.dir, "testdata", "g.json"+candidateSuffix), []byte(withKey(strings.Replace(backfillGolden("TestProbe"), "1.50", "1.5", 1), keyNow)), 0o644)
			}
		}, "changed after it was replayed"},
	} {
		run := newBackfillRun(t, goldens)
		run.now = map[string]string{"TestProbe": keyNow, "TestSecond": keyNow}
		run.then["c1"] = map[string]string{"TestProbe": keyNow, "TestSecond": keyNow}
		c.edit(run)
		_, err := BackfillPythonEnv(context.Background(), run.cfg)
		if err == nil || !strings.Contains(err.Error(), c.refusal) {
			t.Errorf("%s: err = %v, want a refusal holding %q", name, err, c.refusal)
		}
		// One golden that cannot be shown stops the run for every golden.
		if !run.untouched(t, goldens) {
			t.Errorf("%s: a golden or a pin changed, or a candidate stayed", name)
		}
		if replayed := strings.Contains(strings.Join(run.calls, ", "), "replay"); replayed != strings.HasPrefix(name, "the replay") {
			t.Errorf("%s: replay ran = %v (%v)", name, replayed, run.calls)
		}
	}
	// A golden that is not in the harness's own form is not edited.
	odd := map[string]string{"g.json": strings.Replace(backfillGolden("TestProbe"), "\"status\": 200", "\"status\":  200", 1)}
	run := newBackfillRun(t, odd)
	run.now = map[string]string{"TestProbe": keyNow}
	run.then["c1"] = map[string]string{"TestProbe": keyNow}
	if _, err := BackfillPythonEnv(context.Background(), run.cfg); err == nil || !strings.Contains(err.Error(), "not in the form") || !run.untouched(t, odd) {
		t.Fatalf("a golden in another form: err %v", err)
	}
}

func TestOnlyThePythonEnvKeyMayDifferBetweenAGoldenAndItsBackfill(t *testing.T) {
	golden := backfillGolden("TestProbe")
	good := withKey(golden, keyNow)
	if err := onlyPythonEnvAdded([]byte(golden), []byte(good)); err != nil {
		t.Fatalf("the golden with its key: %v", err)
	}
	for name, candidate := range map[string]string{
		"an answer changed":       strings.Replace(good, "1.50", "1.5", 1),
		"a status changed":        strings.Replace(good, "200", "201", 1),
		"a request dropped":       good[:strings.Index(good, "  \"requests\"")] + "  \"requests\": []\n}\n",
		"another header field":    strings.Replace(good, "\"test\": \"TestProbe\"", "\"test\": \"TestOther\"", 1),
		"no key":                  golden,
		"a key that is not a key": strings.Replace(good, keyNow, "yes", 1),
	} {
		if err := onlyPythonEnvAdded([]byte(golden), []byte(candidate)); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	if err := onlyPythonEnvAdded([]byte(good), []byte(strings.Replace(good, keyNow, keyOther, 1))); err == nil || !strings.Contains(err.Error(), "already holds") {
		t.Fatalf("a key was replaced: %v", err)
	}
}

func TestTheRecordingCommitIsTheNewestOneThatHoldsTheGoldenAsItIsOnDisk(t *testing.T) {
	blobs := map[string]string{"c3": "reformatted", "c2": "recorded", "c1": "recorded-first"}
	at := func(commit string) (string, bool) { blob, ok := blobs[commit]; return blob, ok }
	for name, c := range map[string]struct {
		want    string
		history []string
		commit  string
		found   bool
	}{
		"the last change holds it":           {"reformatted", []string{"c3", "c2", "c1"}, "c3", true},
		"the file on disk is an older state": {"recorded", []string{"c3", "c2", "c1"}, "c2", true},
		"no commit holds it (uncommitted)":   {"edited", []string{"c3", "c2", "c1"}, "", false},
		"a commit whose blob cannot be read": {"recorded", []string{"c9", "c2"}, "c2", true},
		"no history":                         {"recorded", nil, "", false},
	} {
		if commit, found := newestCommitHolding(c.want, c.history, at); commit != c.commit || found != c.found {
			t.Errorf("%s: %q %v, want %q %v", name, commit, found, c.commit, c.found)
		}
	}
}

func TestATreeIsUnpackedFromAnArchiveAndNeverOutsideItsDirectory(t *testing.T) {
	archive := func(entries ...tar.Header) *bytes.Buffer {
		var out bytes.Buffer
		writer := tar.NewWriter(&out)
		for _, header := range entries {
			content := []byte("content of " + header.Name)
			if header.Typeflag != tar.TypeReg {
				content = nil
			}
			header.Size = int64(len(content))
			if err := writer.WriteHeader(&header); err != nil {
				t.Fatal(err)
			}
			if _, err := writer.Write(content); err != nil {
				t.Fatal(err)
			}
		}
		if err := writer.Close(); err != nil {
			t.Fatal(err)
		}
		return &out
	}
	dir := t.TempDir()
	err := extractTar(archive(
		tar.Header{Name: "pax_global_header", Typeflag: tar.TypeXGlobalHeader},
		tar.Header{Name: "internal/", Typeflag: tar.TypeDir, Mode: 0o755},
		tar.Header{Name: "internal/pkg/a.go", Typeflag: tar.TypeReg, Mode: 0o644},
		tar.Header{Name: "scripts/run.sh", Typeflag: tar.TypeReg, Mode: 0o755},
		tar.Header{Name: "link", Typeflag: tar.TypeSymlink, Linkname: "internal/pkg/a.go"},
	), dir)
	if err != nil {
		t.Fatal(err)
	}
	if raw, _ := os.ReadFile(filepath.Join(dir, "internal", "pkg", "a.go")); string(raw) != "content of internal/pkg/a.go" {
		t.Fatalf("file content %q", raw)
	}
	if info, err := os.Stat(filepath.Join(dir, "scripts", "run.sh")); err != nil || info.Mode()&0o100 == 0 {
		t.Fatalf("the script is not executable: %v", err)
	}
	if target, _ := os.Readlink(filepath.Join(dir, "link")); target != "internal/pkg/a.go" {
		t.Fatalf("link target %q", target)
	}
	for name, header := range map[string]tar.Header{
		"a path that leaves the tree": {Name: "../escape.go", Typeflag: tar.TypeReg, Mode: 0o644},
		"an absolute path":            {Name: "/etc/escape", Typeflag: tar.TypeReg, Mode: 0o644},
		"a device":                    {Name: "dev", Typeflag: tar.TypeChar},
	} {
		outside := t.TempDir()
		if err := extractTar(archive(header), filepath.Join(outside, "tree")); err == nil {
			t.Errorf("%s: unpacked", name)
		}
		if exists(filepath.Join(outside, "escape.go")) {
			t.Errorf("%s: a file was written outside the tree", name)
		}
	}
}

// The report is put into venueoracle.Start by text: a Start whose head has
// another form must fail here, not when a backfill is run.
func TestTheStartOfThisTreeHasTheFormTheReportIsPutInto(t *testing.T) {
	source, err := os.ReadFile(filepath.Join("..", "venueoracle.go"))
	if err != nil {
		t.Fatal(err)
	}
	if found := startHead.FindAllIndex(source, -1); len(found) != 1 {
		t.Fatalf("venueoracle.Start matches the report's insertion point %d times, want once", len(found))
	}
	if !strings.Contains(reportSource, "pythonEnvKey(pythonPlaneEnv(options, nil))") || !strings.Contains(string(source), "bindPythonEnv(pythonPlaneEnv(options, nil))") {
		t.Fatal("the report and Start no longer take the key over the same environment")
	}
}

// The harness of a recording commit must have handed the Python plane the
// environment the working tree's harness does: the legacy statement in Start
// for a commit from before the key, the same pythonPlaneEnv after it.
func TestTheHarnessOfARecordingCommitMustHandThePythonPlaneTheSameEnvironment(t *testing.T) {
	root, err := filepath.Abs(filepath.Join("..", "..", "..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	ours, err := os.ReadFile(filepath.Join(root, "internal", "testsupport", "venueoracle", "pythonenv.go"))
	if err != nil {
		t.Fatal(err)
	}
	if planeEnvFunction.Find(ours) == nil {
		t.Fatal("pythonPlaneEnv of this tree is not found: the check would compare nothing")
	}
	tree := func(start, env string) string {
		dir := filepath.Join(t.TempDir(), "internal", "testsupport", "venueoracle")
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "venueoracle.go"), []byte(start), 0o644); err != nil {
			t.Fatal(err)
		}
		if env != "" {
			if err := os.WriteFile(filepath.Join(dir, "pythonenv.go"), []byte(env), 0o644); err != nil {
				t.Fatal(err)
			}
		}
		return filepath.Dir(filepath.Dir(filepath.Dir(dir)))
	}
	legacy := "package venueoracle\n\nfunc Start() {\n\t// a comment\n\t" + legacyPlaneEnv + "\n}\n"
	for name, c := range map[string]struct {
		start, env string
		refusal    string // "" = accepted
	}{
		"a commit from before the key":          {legacy, "", ""},
		"the same statement, other white space": {strings.ReplaceAll(legacy, "\n\t\t", "\n  "), "", ""},
		"a harness setting had another value":   {strings.Replace(legacy, "ENVIRONMENT=test", "ENVIRONMENT=dev", 1), "", "another environment than the one known"},
		"a harness setting more":                {strings.Replace(legacy, `"ENVIRONMENT=test", `, `"ENVIRONMENT=test", "EXTRA=1", `, 1), "", "another environment than the one known"},
		"a commit with this tree's function":    {"package venueoracle\n", string(ours), ""},
		"a commit with another function":        {"package venueoracle\n", strings.Replace(string(ours), "ENVIRONMENT=test", "ENVIRONMENT=dev", 1), "another environment than the harness of the working tree"},
		"a commit with neither":                 {"package venueoracle\n", "", "in a way this tool does not know"},
		"a commit with a file and no function":  {"package venueoracle\n", "package venueoracle\n", "another environment than the harness of the working tree"},
	} {
		err := harnessEnvErr(root, tree(c.start, c.env))
		if c.refusal == "" && err != nil {
			t.Errorf("%s: refused: %v", name, err)
		}
		if c.refusal != "" && (err == nil || !strings.Contains(err.Error(), c.refusal)) {
			t.Errorf("%s: err = %v, want a refusal holding %q", name, err, c.refusal)
		}
	}
	// The default reader of the declared keys makes that check before it runs any test of a recording commit's tree.
	other := tree(strings.Replace(legacy, "ENVIRONMENT=test", "ENVIRONMENT=dev", 1), "")
	if keys, err := declaredKeys(Config{Root: root, Package: "./internal/testsupport/venueoracle/", Test: "^TestNone$"}, other); err == nil || !strings.Contains(err.Error(), "another environment than the one known") || keys != nil {
		t.Fatalf("the keys of a tree whose harness handed Python another environment were read: %v %v", keys, err)
	}
}
