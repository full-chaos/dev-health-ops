package teamattribution

import (
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/moduleroot"
)

var (
	isPrimaryToken = regexp.MustCompile(`\b(?:[a-z_]+\.)?is_primary\b`)
	// What follows the token when it is compared.
	isPrimaryOperator = regexp.MustCompile(`^\s*(?:=|!=|<>|>=|<=|>|<|(?i:not\s+in|in)\b)`)
	// The two allowed predicate forms (AttributionPrimary, and a team-scoped
	// read of AttributionPrimary + AttributionCoOwner).
	isPrimaryAllowed = regexp.MustCompile(`^\s*(?:=\s*1\b|IN\s*\(\s*1\s*,\s*2\s*\))`)
	// A bare truthy use: the token right after WHERE/AND/OR/NOT/if( with no
	// operator after it.
	isPrimaryTruthyBefore = regexp.MustCompile(`(?i)(?:\bwhere|\band|\bor|\bnot|\bif\s*\()\s*$`)
	// An aggregate over the flag.
	isPrimaryAggregateBefore = regexp.MustCompile(`(?i)\b(?:argmax|argmin|any|anylast|max|min|sum|avg|groupuniqarray)\s*\(\s*$`)
	// Go code that reads a stored flag as "not zero".
	isPrimaryGoTruthy = regexp.MustCompile(`(?i)\bisprimary\s*(?:!=\s*0|>\s*0|>=\s*1)`)
)

// isPrimaryCensusExclusions: lines that use an is_primary column of ANOTHER
// table in a file that also reads work_item_team_attributions. Each must
// still match, so a stale entry fails.
var isPrimaryCensusExclusions = map[string]string{
	"internal/teamattribution/cascade.go\x00argMax(o.is_primary, (o.version_at, o.valid_from)) AS is_primary,": "team_project_ownership / team_repo_ownership / team_memberships fact loaders (newest version of an ownership row)",
	"internal/teamattribution/cascade.go\x00argMax(v.is_primary, v.updated_at) AS is_primary,":                 "the same fact loaders, inner level",
}

// TestWorkItemTeamAttributionIsPrimaryPredicateCensus holds every reader of
// work_item_team_attributions to the two forms of is_primary: `= 1` (the one
// primary row: org totals, rollups, any read without a team filter) and
// `IN (1, 2)` (a team-scoped read, which also takes the co-owner rows). Any
// other form -- `!= 0`, `> 0`, a bare truthy flag, an aggregate over it, a Go
// `!= 0` on a scanned flag -- would read a co-owner row as primary and count
// an item of a project of several teams more than once. Production Go files
// only; the Python tree no longer reads this table at runtime.
func TestWorkItemTeamAttributionIsPrimaryPredicateCensus(t *testing.T) {
	root, err := moduleroot.Root()
	if err != nil {
		t.Fatal(err)
	}
	var violations []string
	allowedSites, filesRead := 0, 0
	excluded := map[string]int{}
	err = filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		rel, _ := filepath.Rel(root, path)
		rel = filepath.ToSlash(rel)
		if entry.IsDir() {
			if strings.HasPrefix(entry.Name(), ".") || rel == "third_party" || rel == "node_modules" || entry.Name() == "testdata" {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(rel, ".go") || strings.HasSuffix(rel, "_test.go") {
			return nil
		}
		content, readErr := os.ReadFile(path)
		if readErr != nil {
			return readErr
		}
		if !strings.Contains(string(content), "work_item_team_attributions") {
			return nil
		}
		filesRead++
		for number, line := range strings.Split(string(content), "\n") {
			trimmed := strings.TrimSpace(line)
			if strings.HasPrefix(trimmed, "//") {
				continue
			}
			if key := rel + "\x00" + trimmed; isPrimaryCensusExclusions[key] != "" {
				excluded[key]++
				continue
			}
			site := func(reason string) {
				violations = append(violations, rel+":"+strconv.Itoa(number+1)+": "+reason+": "+trimmed)
			}
			if isPrimaryGoTruthy.MatchString(line) {
				site("a stored is_primary read as not-zero")
			}
			for _, match := range isPrimaryToken.FindAllStringIndex(line, -1) {
				before, after := line[:match[0]], line[match[1]:]
				switch {
				case isPrimaryAggregateBefore.MatchString(before):
					site("an aggregate over is_primary")
				case isPrimaryOperator.MatchString(after):
					if isPrimaryAllowed.MatchString(after) {
						allowedSites++
					} else {
						site("an is_primary predicate other than `= 1` or `IN (1, 2)`")
					}
				case isPrimaryTruthyBefore.MatchString(before):
					site("a bare truthy is_primary")
				}
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	for key, reason := range isPrimaryCensusExclusions {
		if excluded[key] == 0 {
			t.Errorf("stale exclusion (%s): %q matches no line", reason, strings.ReplaceAll(key, "\x00", ": "))
		}
	}
	// A census that reads nothing proves nothing.
	if filesRead < 10 || allowedSites < 14 {
		t.Fatalf("census read %d files and %d allowed predicate sites: the walk did not reach the readers", filesRead, allowedSites)
	}
	for _, violation := range violations {
		t.Error(violation)
	}
}
