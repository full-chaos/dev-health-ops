package containers

import (
	"context"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

func moduleRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("go.mod not found above the test directory")
		}
		dir = parent
	}
}

// A remote Instance has a nil Container, so a direct Container.Terminate call
// panics and skips the scratch-database cleanup. Tests must use Instance.Close.
func TestNoDirectContainerTerminateInTests(t *testing.T) {
	root := moduleRoot(t)
	needle := ".Container." + "Terminate("
	harnessFile := filepath.Join(root, "internal", "testsupport", "containers", "harness.go")
	if _, err := os.Stat(harnessFile); err != nil {
		t.Fatalf("harness file not found: %v", err)
	}

	scanned := 0
	var hits []string
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			switch d.Name() {
			case ".git", "node_modules", "vendor":
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, "_test.go") || path == harnessFile {
			return nil
		}
		scanned++
		body, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		for i, line := range strings.Split(string(body), "\n") {
			if strings.Contains(line, needle) {
				rel, _ := filepath.Rel(root, path)
				hits = append(hits, rel+":"+strconv.Itoa(i+1))
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk: %v", err)
	}
	if scanned == 0 {
		t.Fatal("scanned zero test files; the guard is blind")
	}
	for _, h := range hits {
		t.Errorf("direct Container.Terminate in a test (use Instance.Close): %s", h)
	}
}

func TestCloseWithNilContainerRunsRemoteCleanup(t *testing.T) {
	dropped := 0
	inst := &Instance{cleanup: func(context.Context) error {
		dropped++
		return nil
	}}
	if err := inst.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if dropped != 1 {
		t.Fatalf("remote scratch cleanup ran %d times, want 1", dropped)
	}
}
