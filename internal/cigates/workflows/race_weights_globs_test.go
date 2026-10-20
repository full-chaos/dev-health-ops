package workflows_test

import (
	"strings"
	"testing"
)

// TestGoYmlPathFiltersCoverCiTablesByGlob: both `paths` lists of go.yml (push, pull_request) hold the ci globs, and no
// enumerated name of a file a glob already covers, so a new ci table or weight file never edits go.yml.
func TestGoYmlPathFiltersCoverCiTablesByGlob(t *testing.T) {
	root := rootOf(t)
	doc := loadWf(t, root, "go.yml")
	globs := []string{"ci/*.tsv", "ci/*.awk", "ci/*.d/**"}
	for _, event := range []string{"push", "pull_request"} {
		paths := map[string]bool{}
		for _, p := range asList(asMap(asMap(doc["on"])[event])["paths"]) {
			paths[asString(p)] = true
		}
		for _, g := range globs {
			if !paths[g] {
				t.Errorf("go.yml %s.paths lacks %q", event, g)
			}
		}
		for p := range paths {
			covered := (strings.HasPrefix(p, "ci/") && !strings.Contains(strings.TrimPrefix(p, "ci/"), "/") && (strings.HasSuffix(p, ".tsv") || strings.HasSuffix(p, ".awk"))) ||
				(strings.HasPrefix(p, "ci/") && strings.Contains(p, ".d/") && !strings.HasSuffix(p, "**"))
			if covered && !strings.ContainsAny(p, "*") {
				t.Errorf("go.yml %s.paths names %q, which a ci glob already covers; list globs only", event, p)
			}
		}
	}
}
