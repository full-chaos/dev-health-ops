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
			"func TestProbe(t *testing.T) {\n\tif err := os.WriteFile(\"seen.txt\", []byte(os.Getenv(\"DHO_VENUE_GOLDEN_UPDATE\")), 0o644); err != nil {\n\t\tt.Fatal(err)\n\t}\n}\n\n" +
			"func TestNotSelected(t *testing.T) {\n\tt.Fatal(\"the -run regexp was ignored\")\n}\n",
	}
	for path, content := range files {
		if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	cfg := Config{Root: root, Package: "./pkg/", Test: "^TestProbe$"}
	if err := goTest(cfg, []string{"DHO_VENUE_GOLDEN_UPDATE=1"}); err != nil {
		t.Fatalf("the default runner failed: %v", err)
	}
	seen, err := os.ReadFile(filepath.Join(dir, "seen.txt"))
	if err != nil || string(seen) != "1" {
		t.Fatalf("the test did not see the environment: %q %v", seen, err)
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
