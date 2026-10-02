package goldenscan

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// RepoPath is the repository path (slash-separated) of a file given as the test names it: absolute, or relative to the working
// directory. The repository is the nearest directory above the file that holds go.mod.
func RepoPath(path string) (string, error) {
	absolute, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	for dir := filepath.Dir(absolute); ; dir = filepath.Dir(dir) {
		if _, statErr := os.Stat(filepath.Join(dir, "go.mod")); statErr == nil {
			relative, err := filepath.Rel(dir, absolute)
			if err != nil {
				return "", err
			}
			return filepath.ToSlash(relative), nil
		}
		if filepath.Dir(dir) == dir {
			return "", fmt.Errorf("%s is not inside a Go module (no go.mod above it)", path)
		}
	}
}

// CheckGolden is the check the record verb makes of a candidate before it writes it: the golden at path (as the test names it) with
// the bytes it would hold, against the embedded allowlist. It returns one error that names every problem, never a value.
func CheckGolden(path string, golden []byte) error {
	rows, err := Allowlist()
	if err != nil {
		return err
	}
	relative, err := RepoPath(path)
	if err != nil {
		relative = filepath.ToSlash(path) // outside a module (a test's temp directory): no allowlist row can match it
	}
	problems, err := Check(relative, golden, rows)
	if err != nil {
		return err
	}
	if len(problems) > 0 {
		return fmt.Errorf("recording %s: the secret scan of the candidate (every leaf, packed bodies and JSON texts unpacked) refuses it; no candidate is written:\n  %s", path, strings.Join(problems, "\n  "))
	}
	return nil
}
