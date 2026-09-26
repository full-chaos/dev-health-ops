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
	fake := &fakeOracle{dir: dir, candidateBody: "NEW GOLDEN\n", recordWrites: true}
	return Config{Root: root, Package: "./pkg/", Test: "^TestX$", PythonRoot: "/pinned", Run: fake.run}, fake, dir
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
