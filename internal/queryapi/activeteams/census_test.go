package activeteams

import (
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// labelLookups are the query-api files that read the teams table only to
// LABEL ids they already hold (an id from a metrics row, a scope, a request).
// Such a lookup must keep resolving a retired id to its name, so it does not
// apply the listing rule. Each entry says why; a stale entry fails the test.
var labelLookups = map[string]string{
	"quadrant/quadrant.go":       "label lookup for the ids of the quadrant rows",
	"scopelabel/scopelabel.go":   "label lookup for the scope ids of the request",
	"home/queries_signals.go":    "label lookup for the scope ids of the risk rows",
	"analytics/catalog.go":       "lists teams itself with the same rule inline (is_active = 1 on teams FINAL); asserted below",
	"activeteams/activeteams.go": "defines the rule",
	// Lists every team (inactive twins included) to build a repo-pattern
	// resolver. Its SQL text is pinned by a golden captured from the Python
	// reference, so adding the rule is a deliberate divergence from that
	// oracle and waits for an owner decision. Visible here on purpose.
	"cognitiveload/cognitiveload.go": "PENDING DECISION: SQL text pinned by the frozen Python golden",
}

var (
	readsTeams = regexp.MustCompile(`(?i)(FROM|JOIN)\s+teams\b`)
	listsIDs   = regexp.MustCompile(`(?i)DISTINCT\s+team_id\b`)
)

// TestEveryTeamListReaderAppliesTheActiveTeamRule fails when a query-api file
// reads the teams table, or lists distinct team ids, without the shared rule
// (this package) and without a documented label-lookup reason.
func TestEveryTeamListReaderAppliesTheActiveTeamRule(t *testing.T) {
	root := ".."
	seen := map[string]bool{}
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		src := string(raw)
		rel, _ := filepath.Rel(root, path)
		rel = filepath.ToSlash(rel)
		reads, lists := readsTeams.MatchString(src), listsIDs.MatchString(src)
		if !reads && !lists {
			return nil
		}
		if _, ok := labelLookups[rel]; ok {
			seen[rel] = true
			if lists {
				t.Errorf("%s lists distinct team ids but is declared a label lookup", rel)
			}
			return nil
		}
		if !strings.Contains(src, "activeteams.") {
			t.Errorf("%s reads teams or lists team ids without the shared activeteams rule", rel)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk: %v", err)
	}
	for rel := range labelLookups {
		if !seen[rel] {
			t.Errorf("stale label-lookup entry %s: file no longer reads teams", rel)
		}
	}
	raw, err := os.ReadFile(filepath.Join(root, "analytics/catalog.go"))
	if err != nil {
		t.Fatal(err)
	}
	if !regexp.MustCompile(`(?s)FROM teams FINAL\s+WHERE org_id = \{org_id:String\}\s+AND is_active = 1`).Match(raw) {
		t.Error("analytics/catalog.go no longer applies teams FINAL + is_active = 1")
	}
}
