package testopspooled

import (
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/moduleroot"
)

// The pooled source and expressions have ONE definition, this package. A second copy of their text (the sample names are
// the marker) in any other non-test Go file of the module is how the tile and the report would drift apart again.
var markers = []string{DurationSample, QueueSample, "AS " + PipelineRunsAlias, "AS " + SuiteRunsAlias}

func copiesOutside(t *testing.T, root string, skipDir string) []string {
	t.Helper()
	var found []string
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			if entry.Name() == ".git" || entry.Name() == "node_modules" || path == skipDir {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		body, readErr := os.ReadFile(path)
		if readErr != nil {
			return readErr
		}
		for _, marker := range markers {
			if strings.Contains(string(body), marker) {
				found = append(found, path+": "+marker)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return found
}

func TestNoSecondCopyOfThePooledDefinitionExists(t *testing.T) {
	root, err := moduleroot.Root()
	if err != nil {
		t.Fatal(err)
	}
	if found := copiesOutside(t, filepath.Join(root, "internal"), filepath.Join(root, "internal", "queryapi", "testopspooled")); len(found) != 0 {
		t.Fatalf("the pooled TestOps definition is copied outside internal/queryapi/testopspooled (use the package):\n%s", strings.Join(found, "\n"))
	}
}

// A planted second copy fails the census.
func TestCensusFlagsAPlantedSecondCopy(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "copy.go"), []byte("package x\n\nconst q = `SELECT quantileExactInclusive(0.95)(duration_sample) FROM t`\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if found := copiesOutside(t, dir, ""); len(found) != 1 {
		t.Fatalf("a planted copy was not flagged: %v", found)
	}
}
