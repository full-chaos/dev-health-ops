// Package routescan holds the line-shift fixture of the endpoint-profile anchor cells.
package routescan

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// ShiftLines is how many comment lines ShiftedTree inserts above each named func.
const ShiftLines = 9

// ShiftedTree copies the non-test .go files under dirs and the extra files (all repo-relative to repoRoot) into a
// temporary root, then inserts ShiftLines comment lines above the doc comment of the func each shifts entry names
// (file -> the start of its func line, e.g. "func Routes("). The copy is the incident form: no code changes, every
// line below the insert moves.
func ShiftedTree(t testing.TB, repoRoot string, dirs, extra []string, shifts map[string]string) string {
	t.Helper()
	root := t.TempDir()
	copyFile := func(rel string) {
		raw, err := os.ReadFile(filepath.Join(repoRoot, rel))
		if err != nil {
			t.Fatal(err)
		}
		target := filepath.Join(root, rel)
		if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(target, raw, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	for _, dir := range dirs {
		err := filepath.WalkDir(filepath.Join(repoRoot, dir), func(path string, entry fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			rel, err := filepath.Rel(repoRoot, path)
			if err != nil {
				return err
			}
			copyFile(rel)
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	for _, rel := range extra {
		if _, err := os.Stat(filepath.Join(root, rel)); err == nil {
			continue
		}
		copyFile(rel)
	}
	for rel, marker := range shifts {
		if err := shiftAbove(filepath.Join(root, rel), marker); err != nil {
			t.Fatal(err)
		}
	}
	return root
}

func shiftAbove(path, marker string) error {
	raw, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	lines := strings.Split(string(raw), "\n")
	at := -1
	for i, line := range lines {
		if strings.HasPrefix(line, marker) {
			if at >= 0 {
				return fmt.Errorf("%s: %q starts two lines", path, marker)
			}
			at = i
		}
	}
	if at < 0 {
		return fmt.Errorf("%s: no line starts with %q", path, marker)
	}
	for at > 0 && strings.HasPrefix(lines[at-1], "//") {
		at--
	}
	insert := make([]string, ShiftLines)
	for i := range insert {
		insert[i] = fmt.Sprintf("// line-shift fixture %d", i+1)
	}
	lines = append(lines[:at], append(insert, lines[at:]...)...)
	return os.WriteFile(path, []byte(strings.Join(lines, "\n")), 0o644)
}
