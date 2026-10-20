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

// TestGoYmlPathFiltersSelectNewCiFiles: behaviour, not literals. Each changed file a pull request can add (a weight file
// for a new package, one for a root package, a new ci table, a new awk script, a new row directory) must match the go.yml
// filter of both events, and no filter entry may be a `!` exclusion that could stop such a file from selecting the workflow.
func TestGoYmlPathFiltersSelectNewCiFiles(t *testing.T) {
	root := rootOf(t)
	changed := []string{
		"ci/go_race_weights.d/internal__zz-new-package.tsv",
		"ci/go_race_weights.d/_root.tsv",
		"ci/zz_new_table.tsv",
		"ci/zz_new_shard.awk",
		"ci/zz_new_rows.d/anything.txt",
	}
	for _, event := range []string{"push", "pull_request"} {
		paths := goPaths(t, root, event)
		for _, p := range paths {
			if strings.HasPrefix(p, "!") {
				t.Errorf("go.yml %s.paths holds the exclusion %q: a weight file change could skip the workflow", event, p)
			}
		}
		for _, f := range changed {
			if !matchesAny(t, f, paths) {
				t.Errorf("go.yml %s.paths does not select a change to %s", event, f)
			}
		}
	}
}
