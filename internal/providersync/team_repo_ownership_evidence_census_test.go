package providersync

import (
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/moduleroot"
)

// The two census facts the absence proof of inferred_repo_ownership stands on
// (CHAOS-9349): the derivation is the only writer of new inferred rows (its
// scope), and every statement that removes a row of an input table is named
// with why a recorded key of a removed row cannot hold a close for ever, or why
// its removal is witnessed.

// teamRepoOwnershipWriters is every production file that writes
// team_repo_ownership rows, statically or through a table list.
var teamRepoOwnershipWriters = map[string]string{
	"internal/providersync/team_repo_ownership_derivation_clickhouse.go": "the sole writer of new inferred rows (TeamRepoOwnershipDerivationService)",
	"internal/providersync/github_team_catalog_effects_clickhouse.go":    "writes provider_access rows of the GitHub catalog, never source inferred",
	"internal/providersync/jira_project_as_team_retire.go": "closes the open inferred rows of a retired Jira project-as-team id: the team is gone, " +
		"a removal of the team, not a close on a missing input row",
	"internal/providersync/ownership_interval_compaction.go": "folds adjacent closed rows of one shape into one; opens and closes no claim",
	"internal/providersync/catalog_evidence_start.go":        "re-states the evidence start of a catalog link (provider_access, native); never source inferred",
	"internal/providersync/team_id_carry.go":                 "moves the open rows of a bare team id to its prefixed id, with their evidence keys (planRepoOwnershipEvidence)",
}

// teamRepoOwnershipInputRemovers is every production file that removes rows of
// an input table of the derivation, statically or through a dynamic table name.
var teamRepoOwnershipInputRemovers = map[string]string{
	"internal/jobs/workgraph/issueprlinks/fallback_clickhouse.go": "work_graph_issue_pr: RetractOutranked removes a fallback row that a native " +
		"link of its pull request or issue outranks, or whose issue side is a pull request; the run holds the witness (teamRepoOwnershipEvidenceHeld.holds)",
	"internal/providersync/linear_stale_project_ownership_cleanup.go": "team_project_ownership: deletes a stale Linear row only beside a sibling of the " +
		"same (org_id, project_id) and team, so the key (provider, project_id, team_id) stays",
	"internal/providersync/ownership_interval_compaction.go": "team_project_ownership (dynamic spec.table): deletes only the tombstone versions of a " +
		"folded run; the key keeps its first row",
	"internal/providersync/work_item_dependency_drain.go": "work_item_dependencies (dynamic WorkItemDependencyDrainTable): deletes only ended rows of " +
		"the two blocking types, which are never inheritable, so never evidence",
	"internal/apiservice/admin/orgdeletion.go": "every org table (dynamic): a whole-organization delete, the claims and their evidence go with it",
	"internal/auth/restorefence/specs.go":      "Postgres credential tables only (dynamic): no input table of the derivation",
}

var (
	teamRepoOwnershipWriteStatic  = regexp.MustCompile("INSERT INTO `?team_repo_ownership`?[\\s(]|\\bteamRepoOwnership(?:Share)?Insert\\b")
	teamRepoOwnershipWriteDynamic = regexp.MustCompile(`"INSERT INTO "\s*\+`)
	teamRepoOwnershipTableLiteral = regexp.MustCompile(`"team_repo_ownership"`)
	teamRepoOwnershipInputTables  = `(?:work_items|work_item_dependencies|work_graph_issue_pr|team_project_ownership|teams)`
	teamRepoOwnershipRemoveStatic = regexp.MustCompile("(?i)(?:ALTER\\s+TABLE\\s+`?" + teamRepoOwnershipInputTables + "`?\\s+DELETE\\b|DELETE\\s+FROM\\s+`?" +
		teamRepoOwnershipInputTables + "`?\\b|TRUNCATE(?:\\s+TABLE)?(?:\\s+IF\\s+EXISTS)?\\s+`?" + teamRepoOwnershipInputTables + "`?\\b)")
	teamRepoOwnershipRemoveDynamic = regexp.MustCompile("(?i)(?:\"(?:ALTER TABLE|DELETE FROM|TRUNCATE TABLE|TRUNCATE)\\s*`?\"\\s*\\+|(?:ALTER TABLE|DELETE FROM|TRUNCATE TABLE|TRUNCATE)\\s+`?%s)")
)

var (
	teamRepoOwnershipSourcesOnce sync.Once
	teamRepoOwnershipSources     map[string]string
	teamRepoOwnershipSourcesErr  error
)

// teamRepoOwnershipProductionSources reads every non-test Go file of the
// module's internal and cmd trees, once for both censuses.
func teamRepoOwnershipProductionSources(t *testing.T) map[string]string {
	t.Helper()
	teamRepoOwnershipSourcesOnce.Do(func() {
		teamRepoOwnershipSources, teamRepoOwnershipSourcesErr = readTeamRepoOwnershipProductionSources()
	})
	if teamRepoOwnershipSourcesErr != nil {
		t.Fatal(teamRepoOwnershipSourcesErr)
	}
	if len(teamRepoOwnershipSources) < 500 {
		t.Fatalf("the census read %d source files: it measured nothing", len(teamRepoOwnershipSources))
	}
	return teamRepoOwnershipSources
}

func readTeamRepoOwnershipProductionSources() (map[string]string, error) {
	root, err := moduleroot.Root()
	if err != nil {
		return nil, err
	}
	sources := map[string]string{}
	for _, tree := range []string{"internal", "cmd"} {
		err := filepath.WalkDir(filepath.Join(root, tree), func(path string, entry fs.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			if entry.IsDir() {
				if entry.Name() == "testdata" || strings.HasPrefix(entry.Name(), ".") {
					return filepath.SkipDir
				}
				return nil
			}
			if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			raw, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			rel, _ := filepath.Rel(root, path)
			sources[filepath.ToSlash(rel)] = string(raw)
			return nil
		})
		if err != nil {
			return nil, err
		}
	}
	return sources, nil
}

func censusKeys(found map[string]bool) []string {
	out := make([]string, 0, len(found))
	for key := range found {
		out = append(out, key)
	}
	sort.Strings(out)
	return out
}

func namedKeys(named map[string]string, t *testing.T) []string {
	out := make([]string, 0, len(named))
	for key, reason := range named {
		if strings.TrimSpace(reason) == "" {
			t.Errorf("%s is named with no reason", key)
		}
		out = append(out, key)
	}
	sort.Strings(out)
	return out
}

// TestInferredRepoOwnershipHasOneWriter: a new writer of team_repo_ownership
// is named here, and only the derivation writes new inferred rows (the scope
// of the kind inferred_repo_ownership).
func TestInferredRepoOwnershipHasOneWriter(t *testing.T) {
	sources := teamRepoOwnershipProductionSources(t)
	found := map[string]bool{}
	for path, source := range sources {
		// The literal checks only skip the regexes on files that cannot match them.
		if !strings.Contains(source, "team_repo_ownership") && !strings.Contains(source, "teamRepoOwnership") {
			continue
		}
		if teamRepoOwnershipWriteStatic.MatchString(source) ||
			(teamRepoOwnershipWriteDynamic.MatchString(source) && teamRepoOwnershipTableLiteral.MatchString(source)) {
			found[path] = true
		}
	}
	if got, want := censusKeys(found), namedKeys(teamRepoOwnershipWriters, t); !reflect.DeepEqual(got, want) {
		t.Fatalf("the writers of team_repo_ownership changed.\n got  %v\n want %v\nName a writer with why it writes no new inferred row.", got, want)
	}
	// A row is written with source inferred by an Append of the literal; the
	// compaction names the source in its spec (it folds existing rows).
	for path := range found {
		writesInferred := strings.Contains(sources[path], `"inferred",`) && !strings.Contains(sources[path], `source: "inferred"`)
		if writesInferred != (path == "internal/providersync/team_repo_ownership_derivation_clickhouse.go") {
			t.Errorf("%s: writes the source value \"inferred\" = %v; only the derivation writes new inferred rows", path, writesInferred)
		}
	}
}

// teamRepoOwnershipRemoveWords are the spellings of the verbs a remover form
// holds; only a line that holds one is matched against the case-blind forms
// (a whole-file case-blind match of every source is too slow under -race).
var teamRepoOwnershipRemoveWords = []string{"DELETE", "delete", "Delete", "TRUNCATE", "truncate", "Truncate"}

func teamRepoOwnershipRemovesRows(source string) bool {
	holds := func(text string) bool {
		for _, word := range teamRepoOwnershipRemoveWords {
			if strings.Contains(text, word) {
				return true
			}
		}
		return false
	}
	if !holds(source) {
		return false
	}
	for _, line := range strings.Split(source, "\n") {
		if holds(line) && (teamRepoOwnershipRemoveStatic.MatchString(line) || teamRepoOwnershipRemoveDynamic.MatchString(line)) {
			return true
		}
	}
	return false
}

// TestTeamRepoOwnershipInputRemoverCensus: every statement that removes rows
// of an input table of the derivation (and every dynamic remover, whatever
// table it names) is named with its witness.
func TestTeamRepoOwnershipInputRemoverCensus(t *testing.T) {
	found := map[string]bool{}
	for path, source := range teamRepoOwnershipProductionSources(t) {
		if teamRepoOwnershipRemovesRows(source) {
			found[path] = true
		}
	}
	if got, want := censusKeys(found), namedKeys(teamRepoOwnershipInputRemovers, t); !reflect.DeepEqual(got, want) {
		t.Fatalf("the removers of the derivation's input rows changed.\n got  %v\n want %v\n"+
			"Name a remover with why a recorded key of a row it removes cannot hold a close for ever, or how the run witnesses the removal.", got, want)
	}
}

// The census regexes find the forms they are for.
func TestTeamRepoOwnershipCensusFormsAreFound(t *testing.T) {
	for _, form := range []string{
		"ALTER TABLE work_items DELETE WHERE org_id = ?",
		"ALTER TABLE `teams` DELETE WHERE id = ?",
		"TRUNCATE TABLE work_graph_issue_pr",
		"DELETE FROM team_project_ownership WHERE org_id = ?",
		"const x = `ALTER TABLE work_graph_issue_pr DELETE\nWHERE org_id = ?`",
	} {
		if !teamRepoOwnershipRemovesRows(form) {
			t.Errorf("static remover form not found: %s", form)
		}
	}
	for _, form := range []string{`"ALTER TABLE "+spec.table`, "\"DELETE FROM %s WHERE %s\"", "\"ALTER TABLE `%s` DELETE WHERE %s\""} {
		if !teamRepoOwnershipRemoveDynamic.MatchString(form) {
			t.Errorf("dynamic remover form not found: %s", form)
		}
	}
	if teamRepoOwnershipRemoveStatic.MatchString("ALTER TABLE work_items ADD COLUMN IF NOT EXISTS x UInt8") {
		t.Error("an ADD COLUMN is taken as a remover")
	}
	if !teamRepoOwnershipWriteStatic.MatchString("INSERT INTO team_repo_ownership (org_id)") ||
		teamRepoOwnershipWriteStatic.MatchString("INSERT INTO team_repo_ownership_evidence\n(org_id)") {
		t.Error("the writer form is not exact")
	}
}
