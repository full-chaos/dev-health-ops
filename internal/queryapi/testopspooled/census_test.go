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

// The newest-attempt RULE has one text, the fragments of this package. Today's private copies of it (the daily readers) are
// allowed by name until the change that moves them here deletes them; the allowlist may only shrink, so an entry whose file no
// longer holds the rule fails the floor below.
// The marker is the rule's own text. The writer that fills legacy_suite_ids names the column too, and is not a copy of the rule.
var ruleMarkers = []string{"report_slot != ''"}

// The daily readers read the fragments now: no private copy of the rule is allowed anywhere.
var privateRuleCopies = map[string]bool{}

func ruleCopies(t *testing.T, root string) map[string]bool {
	t.Helper()
	found := map[string]bool{}
	skip := filepath.Join(root, "internal", "queryapi", "testopspooled")
	err := filepath.WalkDir(filepath.Join(root, "internal"), func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			if path == skip {
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
		for _, marker := range ruleMarkers {
			if strings.Contains(string(body), marker) {
				rel, _ := filepath.Rel(root, path)
				found[rel] = true
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return found
}

func TestNoUnlistedCopyOfTheNewestAttemptRuleExists(t *testing.T) {
	root, err := moduleroot.Root()
	if err != nil {
		t.Fatal(err)
	}
	found := ruleCopies(t, root)
	for path := range found {
		if !privateRuleCopies[path] {
			t.Errorf("%s holds a copy of the newest-attempt rule: use SuiteRowsNewestAttempt / CoverageRowsNewestAttempt", path)
		}
	}
	for path := range privateRuleCopies {
		if !found[path] {
			t.Errorf("%s is allowlisted but no longer holds the rule: remove it from privateRuleCopies", path)
		}
	}
}

// A planted extra copy fails the census.
func TestRuleCensusFlagsAPlantedExtraCopy(t *testing.T) {
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "internal", "x"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "internal", "x", "copy.go"), []byte("package x\n\nconst q = `AND (report_slot != '' AND 1)`\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	found := ruleCopies(t, dir)
	if len(found) != 1 || privateRuleCopies["internal/x/copy.go"] {
		t.Fatalf("a planted copy was not flagged as an unlisted copy: %v", found)
	}
}
