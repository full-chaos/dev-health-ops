//go:build !race

package chartgates_test

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func overlayFixture(t *testing.T) (string, overlayCase) {
	t.Helper()
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "source.go"), []byte("package fixture\n// original\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	return root, overlayCase{name: "fixture", file: "source.go", old: "original", repl: "defect", run: "^TestFixture$", want: "defect"}
}

// These cells stay serial: hook replacement finishes before parallel tests resume.
func TestOverlaySemaphoreBoundsAndIsolatesFixtures(t *testing.T) {
	root, base := overlayFixture(t)
	old := overlayGoTest
	t.Cleanup(func() { overlayGoTest = old })
	limit := cap(overlayGoTestSlots)
	if want := min(runtime.GOMAXPROCS(0), 4); limit != want {
		t.Fatalf("semaphore cap=%d, want %d", limit, want)
	}
	var active, peak atomic.Int32
	var mu sync.Mutex
	seen := map[string]bool{}
	paths := map[string]bool{}
	release := make(chan struct{})
	var releaseOnce sync.Once
	// A bounded wait makes a broken semaphore fail instead of hanging the binary.
	timer := time.AfterFunc(5*time.Second, func() { releaseOnce.Do(func() { close(release) }) })
	defer timer.Stop()
	overlayGoTest = func(dir string, args ...string) ([]byte, error) {
		live := active.Add(1)
		defer active.Add(-1)
		for prev := peak.Load(); live > prev && !peak.CompareAndSwap(prev, live); prev = peak.Load() {
		}
		if live == int32(limit) {
			time.AfterFunc(30*time.Millisecond, func() { releaseOnce.Do(func() { close(release) }) })
		}
		<-release
		if dir != root {
			return nil, fmt.Errorf("unexpected root: %s", dir)
		}
		var overlay string
		for _, arg := range args {
			if value, ok := strings.CutPrefix(arg, "-overlay="); ok {
				overlay = value
			}
		}
		data, err := os.ReadFile(overlay)
		if err != nil {
			return nil, err
		}
		var config struct{ Replace map[string]string }
		if err := json.Unmarshal(data, &config); err != nil {
			return nil, err
		}
		mutated := config.Replace[filepath.Join(root, base.file)]
		data, err = os.ReadFile(mutated)
		if err != nil {
			return nil, err
		}
		mu.Lock()
		defer mu.Unlock()
		if paths[overlay] || paths[mutated] {
			return nil, errors.New("overlay path reused")
		}
		paths[overlay], paths[mutated] = true, true
		seen[string(data)] = true
		return data, errors.New("fixture guard refused defect")
	}
	var done sync.WaitGroup
	for i := range 3 * limit {
		done.Add(1)
		go func() {
			defer done.Done()
			c := base
			c.repl = fmt.Sprintf("defect-%d", i)
			out, failed := runOverlayAtRoot(t, root, c)
			if !failed || out != "package fixture\n// "+c.repl+"\n" {
				t.Errorf("case %d: failed=%v output=%q", i, failed, out)
			}
		}()
	}
	done.Wait()
	if got := peak.Load(); got != int32(limit) {
		t.Errorf("max concurrent execs=%d, want %d", got, limit)
	}
	if len(seen) != 3*limit {
		t.Errorf("distinct defect outputs=%d, want %d", len(seen), 3*limit)
	}
	if len(overlayGoTestSlots) != 0 {
		t.Errorf("semaphore leaked %d slots", len(overlayGoTestSlots))
	}
}

func TestOverlaySemaphoreReleasesOnFatalFixture(t *testing.T) {
	runOverlayFatalChild(t, "fatal", "fixture exec fatal", "AFTER_FATAL_SLOT_RELEASED")
}

func TestOverlayMissingPlantIsFatalFixture(t *testing.T) {
	runOverlayFatalChild(t, "missing", "the defect did not land", "MISSING_PLANT_SKIPPED_EXEC")
}

func runOverlayFatalChild(t *testing.T, mode string, markers ...string) {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^TestOverlayFatalFixtureChild$", "-test.v", "-test.timeout=15s")
	cmd.Env = append(os.Environ(), "CHARTGATES_OVERLAY_FIXTURE_CHILD="+mode)
	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("expected fixture child failure, output:\n%s", out)
	}
	for _, marker := range markers {
		if !strings.Contains(string(out), marker) {
			t.Fatalf("missing %q in child output:\n%s", marker, out)
		}
	}
}

// Only a fixture subprocess enters this cell; its intentional Fatal must not fail the parent binary.
func TestOverlayFatalFixtureChild(t *testing.T) {
	mode := os.Getenv("CHARTGATES_OVERLAY_FIXTURE_CHILD")
	if mode == "" {
		return // a pass, not a skip: ci/race_excluded_tests.tsv lists this test and a skip fails its receipt
	}
	root, c := overlayFixture(t)
	old := overlayGoTest
	t.Cleanup(func() { overlayGoTest = old })
	calls := 0
	// Fill all but one slot so a single leaked acquisition blocks the next exec.
	for i := 1; i < cap(overlayGoTestSlots); i++ {
		overlayGoTestSlots <- struct{}{}
	}
	t.Cleanup(func() {
		for len(overlayGoTestSlots) > 0 {
			<-overlayGoTestSlots
		}
	})
	t.Run("fatal", func(t *testing.T) {
		overlayGoTest = func(string, ...string) ([]byte, error) {
			calls++
			t.Fatal("fixture exec fatal")
			return nil, nil
		}
		if mode == "missing" {
			c.old = "absent text"
		}
		runOverlayAtRoot(t, root, c)
	})
	if mode == "missing" {
		if calls != 0 {
			t.Fatalf("missing plant executed %d commands", calls)
		}
		t.Log("MISSING_PLANT_SKIPPED_EXEC")
		return
	}
	finished := make(chan struct{})
	overlayGoTest = func(string, ...string) ([]byte, error) { return []byte("after fatal"), nil }
	go func() { defer close(finished); runOverlayAtRoot(t, root, c) }()
	select {
	case <-finished:
		t.Log("AFTER_FATAL_SLOT_RELEASED")
	case <-time.After(time.Second):
		t.Fatal("semaphore was not released after Fatal")
	}
}
