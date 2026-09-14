package goapiproof

import (
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

// retiredEdgeBearerEnvVarPattern is built from parts on purpose. This file
// is itself scanned by the walk below, and a literal would make the guard
// report its own source as a violation.
var retiredEdgeBearerEnvVarPattern = regexp.MustCompile(
	`GO_API_PROVE` + `_BEARER` + `\b`)

// TestRetiredEdgeBearerEnvVarIsNeverReadAgain guards the edge-token
// retirement: go-api-prove's edge credential now comes ONLY from
// -edge-bearer-exec (a minted, short-lived access token). The static
// hand-minted bearer env var this command used to also accept is gone, and
// no production source may reintroduce a read of it -- reintroducing the
// name is exactly how a "temporary" fallback comes back to life.
//
// Scope is production source only. A test file may still name the retired
// variable (to document why it is gone, or to assert a refusal message),
// which is why _test.go files are excluded from the walk.
func TestRetiredEdgeBearerEnvVarIsNeverReadAgain(t *testing.T) {
	t.Parallel()

	repositoryRoot := filepath.Join("..", "..")
	roots := []string{
		filepath.Join(repositoryRoot, "internal"),
		filepath.Join(repositoryRoot, "cmd"),
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
			if !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
				return nil
			}
			content, readErr := os.ReadFile(path)
			if readErr != nil {
				return readErr
			}
			for index, line := range strings.Split(string(content), "\n") {
				if retiredEdgeBearerEnvVarPattern.MatchString(line) {
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
		t.Fatalf("the retired edge bearer env var must not be read anywhere; -edge-bearer-exec is the only source:\n%s",
			strings.Join(offenders, "\n"))
	}
}
