package config

import (
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

// routeSwitchPattern is built from parts on purpose. This file is itself
// scanned by the walk below, and a literal would make the guard report its own
// source as a violation.
var routeSwitchPattern = regexp.MustCompile(`WORKER_[A-Z_]+` + "_" + `ENABLED`)

// TestNoRouteEnablementVariableIsReadAnywhere is CHAOS-4054's literal
// acceptance criterion, enforced in CI rather than asserted once by hand: no
// production source in either runtime may name a route enablement environment
// variable, because that plane no longer exists.
//
// Scope is production source only. A test may still NAME one of these
// variables — that is how we prove the names are inert (see
// TestNoRouteEnablementSurfaceExists) — but no shipped code may read one.
func TestNoRouteEnablementVariableIsReadAnywhere(t *testing.T) {
	t.Parallel()

	repositoryRoot := filepath.Join("..", "..", "..")
	roots := []string{
		filepath.Join(repositoryRoot, "internal"),
		filepath.Join(repositoryRoot, "cmd"),
		filepath.Join(repositoryRoot, "src", "dev_health_ops"),
	}

	var offenders []string
	for _, root := range roots {
		if _, err := os.Stat(root); err != nil {
			t.Fatalf("guard cannot see %s: %v", root, err)
		}
		err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if entry.IsDir() {
				return nil
			}
			name := entry.Name()
			switch {
			case strings.HasSuffix(name, "_test.go"), strings.HasSuffix(name, "_test.py"):
				return nil
			case !strings.HasSuffix(name, ".go") && !strings.HasSuffix(name, ".py"):
				return nil
			}
			// Alembic revisions are an append-only historical record: they
			// describe what the schema did at the time they were written and
			// are never edited afterwards.
			if strings.Contains(filepath.ToSlash(path), "/alembic/versions/") {
				return nil
			}
			content, readErr := os.ReadFile(path)
			if readErr != nil {
				return readErr
			}
			for index, line := range strings.Split(string(content), "\n") {
				if routeSwitchPattern.MatchString(line) {
					offenders = append(offenders,
						filepath.ToSlash(path)+":"+strconv.Itoa(index+1)+": "+strings.TrimSpace(line))
				}
			}
			return nil
		})
		if err != nil {
			t.Fatalf("walk %s: %v", root, err)
		}
	}
	if len(offenders) > 0 {
		t.Fatalf("route enablement variables must not exist in production source:\n%s",
			strings.Join(offenders, "\n"))
	}
}
