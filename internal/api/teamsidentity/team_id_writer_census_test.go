package teamsidentity

import (
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// teamIDWriters is every production line that writes a row of a table
// keyed by a team id, per file, and how that file's ids reach the keyed
// form. A new writer fails the census until it is listed here with its
// route: an admin writer goes through the write seam (keyTeamIDs, below);
// the other routes build the id from a provider's own key with teamid.Of
// behind the carry. See docs/contribute/architecture/team-attribution.md
// "Team ids".
var teamIDWriters = map[string]struct {
	count int
	route string
}{
	"internal/api/teamsidentity/store.go":                                  {2, "admin: write seam (keyTeamIDs) at the handler; insertTeamRow refuses a bare id"},
	"internal/api/teamsidentity/drift.go":                                  {2, "admin import: write seam over teamid.Of(provider, id)"},
	"internal/api/teamsidentity/drift_apply.go":                            {2, "admin drift decision: write seam on the path id; an open row with a bare id is refused"},
	"internal/streamhandlers/external_clickhouse.go":                       {2, "team.v1 / identity.v1: teamid.Of(system, id) and teamid.CheckPushed, behind the batch carry"},
	"internal/atlassianteams/write.go":                                     {3, "native: teamid.Of on the Atlassian team id, behind the carry (synccli)"},
	"internal/providersync/github_team_catalog_effects_clickhouse.go":      {3, "native catalog: teamid.Of, behind CarryFirstTeamCatalogCollector"},
	"internal/providersync/gitlab_team_catalog_effects_clickhouse.go":      {3, "native catalog: teamid.Of, behind CarryFirstTeamCatalogCollector"},
	"internal/providersync/jira_team_catalog_effects_clickhouse.go":        {3, "native catalog: teamid.Of, behind CarryFirstTeamCatalogCollector"},
	"internal/providersync/linear_reference_catalog_effects_clickhouse.go": {3, "native catalog: teamid.Of, behind CarryFirstTeamCatalogCollector"},
	"internal/providersync/team_drift_review.go":                           {2, "native catalog drift staging: the collector's keyed ids"},
	"internal/providersync/jira_project_as_team_retire.go":                 {3, "retire: closes and deactivates stored rows, no new id"},
	"internal/providersync/team_repo_ownership_derivation_clickhouse.go":   {1, "derivation: ids of stored active team rows"},
}

// teamIDDynamicWriters is every production line that builds an INSERT from a
// table name held in a variable, and why it may write a team-keyed table
// without the seam or does not write one.
var teamIDDynamicWriters = map[string]struct {
	count int
	route string
}{
	"internal/providersync/team_id_carry.go": {1, "the carry: keyed ids only (teamIDCarryGuard)"},
	"internal/chmigrate/apply.go":            {2, "schema migrations: no team row"},
	"internal/providerfoundation/sinks.go":   {1, "raw provider record tables: no team-keyed table"},
	// The stale-key rule of the daily metric families writes a row of zeros
	// over a stored key of a derived daily table. It stores the team id that
	// the superseded row holds, also a bare id of a team that was replaced: a
	// keyed id would be another key and would leave the old row in place. It
	// writes no team row and no link row (package teamkeytables).
	"internal/jobs/metrics/daily/stale_team_keys.go": {1, "stale-key rule: the stored id of the superseded row, daily metric tables only"},
	// Exception: `dho fixtures generate` writes contrived CI data, the frozen
	// fixture world's rows as they are, into an organization that holds no
	// synced data (it refuses one without --allow-mixed-org). Its team ids
	// are the frozen world's; a carry or the seam keys them at the first
	// real write of that organization.
	"internal/fixturescli/generate.go":  {1, "exception: dho fixtures generate, contrived CI data"},
	"internal/fixturescli/synthetic.go": {1, "exception: dho fixtures generate, contrived CI data"},
}

var (
	teamIDTableInsert   = regexp.MustCompile("(?i)INSERT INTO `?(teams|identities|team_memberships|team_project_ownership|team_repo_ownership|team_sync_policies|team_drift_changes|manual_attribution_fallbacks|team_provider_observations)\\b")
	teamIDDynamicInsert = regexp.MustCompile("(?i)\"INSERT INTO `?\"\\s*\\+")
	// adminTeamIDWrite is a call that writes a team id (or a row that names
	// one) in this package.
	adminTeamIDWrite = regexp.MustCompile(`\.(DeleteTeam|CreateOrUpdateTeam|SetMembers|AddMembers|RemoveMembers|CreateOrUpdateIdentity|insertTeamRow|insertIdentityRow|insertTeamMembership|insertManualFallback|applyChange|applyIdentityMembershipChange|expireConflict|projectTeam|insertObservation|insertChanges|markPending|insertDecisionRows)\(`)
)

// TestEveryTeamIDWriterGoesThroughTheWriteSeamCensus fails when a line that
// writes a team-keyed table appears outside the writers above, and when a
// function of this package that writes a team id is not a store method and
// does not run the write seam before its first write.
func TestEveryTeamIDWriterGoesThroughTheWriteSeamCensus(t *testing.T) {
	root, err := filepath.Abs(filepath.Join("..", "..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	found, dynamic := map[string]int{}, map[string]int{}
	for _, dir := range []string{"internal", "cmd"} {
		err := filepath.WalkDir(filepath.Join(root, dir), func(path string, entry fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			data, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			rel, err := filepath.Rel(root, path)
			if err != nil {
				return err
			}
			rel = filepath.ToSlash(rel)
			for _, line := range strings.Split(string(data), "\n") {
				if strings.HasPrefix(strings.TrimSpace(line), "//") {
					continue
				}
				if n := len(teamIDTableInsert.FindAllString(line, -1)); n > 0 {
					found[rel] += n
				}
				if teamIDDynamicInsert.MatchString(line) {
					dynamic[rel]++
				}
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	if len(found) == 0 || len(dynamic) == 0 {
		t.Fatal("no team id writer found: the scan read nothing")
	}
	for file, count := range found {
		if want, ok := teamIDWriters[file]; !ok || want.count != count {
			t.Errorf("%s: %d team-keyed writes, want %d: route a new writer through the write seam (providersync.KeyTeamIDsForWrite) or teamid.Of behind the carry, then list it here", file, count, want.count)
		}
	}
	for file, want := range teamIDWriters {
		if found[file] != want.count {
			t.Errorf("%s: %d team-keyed writes, want %d (%s)", file, found[file], want.count, want.route)
		}
	}
	for file, count := range dynamic {
		if teamIDDynamicWriters[file].count != count {
			t.Errorf("%s: %d INSERTs built from a table variable, want %d: list it with its route", file, count, teamIDDynamicWriters[file].count)
		}
	}
	for file, want := range teamIDDynamicWriters {
		if dynamic[file] != want.count {
			t.Errorf("%s: %d INSERTs built from a table variable, want %d (%s)", file, dynamic[file], want.count, want.route)
		}
	}

	// Every function of this package that writes a team id runs the seam
	// first, or is a store method behind a handler that did.
	handlers := 0
	for file, source := range packageSources(t, filepath.Join(root, "internal", "api", "teamsidentity")) {
		for _, fn := range splitFuncs(source) {
			first := adminTeamIDWrite.FindStringIndex(fn.body)
			if first == nil || strings.HasPrefix(fn.decl, "func (s Store)") {
				continue
			}
			handlers++
			seam := strings.Index(fn.body, "h.keyTeamIDs(")
			if seam < 0 || seam > first[0] {
				t.Errorf("%s: %s writes a team id without running h.keyTeamIDs before it", file, fn.decl)
			}
		}
	}
	if handlers != 8 {
		t.Errorf("%d functions outside the store write a team id, want 8 (createOrUpdateTeam, updateTeam, deleteTeam, createOrUpdateIdentity, confirmMembers, confirmInferredMembers, importTeams, decideChanges)", handlers)
	}
	// The store refuses a bare id before the write it guards.
	store := packageSources(t, filepath.Join(root, "internal", "api", "teamsidentity"))
	for file, fnName := range map[string]string{
		"store.go":       "func (s Store) insertTeamRow(",
		"drift_apply.go": "func (s Store) insertTeamMembership(",
	} {
		guardedBeforeBatch(t, store[file], fnName)
	}
	guardedBeforeBatch(t, store["drift_apply.go"], "func (s Store) insertManualFallback(")
}

func guardedBeforeBatch(t *testing.T, source, decl string) {
	t.Helper()
	for _, fn := range splitFuncs(source) {
		if !strings.HasPrefix(fn.decl, decl) {
			continue
		}
		guard, batch := strings.Index(fn.body, "checkKeyedTeamID("), strings.Index(fn.body, "PrepareBatch(")
		if guard < 0 || batch < 0 || guard > batch {
			t.Errorf("%s: checkKeyedTeamID does not run before PrepareBatch", decl)
		}
		return
	}
	t.Errorf("%s not found", decl)
}

type sourceFunc struct{ decl, body string }

func splitFuncs(source string) []sourceFunc {
	var out []sourceFunc
	parts := strings.Split(source, "\nfunc ")
	for _, part := range parts[1:] {
		decl, body, _ := strings.Cut(part, "\n")
		out = append(out, sourceFunc{decl: "func " + decl, body: body})
	}
	return out
}

func packageSources(t *testing.T, dir string) map[string]string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]string{}
	for _, entry := range entries {
		name := entry.Name()
		if !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		data, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			t.Fatal(err)
		}
		out[name] = string(data)
	}
	if len(out) == 0 {
		t.Fatal("no source read")
	}
	return out
}
