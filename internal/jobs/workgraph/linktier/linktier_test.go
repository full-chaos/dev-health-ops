package linktier

import (
	"os"
	"strings"
	"testing"
)

// Every table that ranks a link tier writes the same ranks as Rank.
func TestMigrationVersionRanksAreTheExportedRanks(t *testing.T) {
	t.Parallel()
	if !(Rank(Native) > Rank(ExplicitText) &&
		Rank(ExplicitText) > Rank(Heuristic) &&
		Rank(Heuristic) > Rank("unknown")) {
		t.Fatal("tier ranks must run native > explicit_text > heuristic > unknown")
	}
	for path, args := range map[string][2]string{
		"../../../chmigrate/sql/131_deployment_incident_edges_repo_key.sql": {"source", "computed_at"},
		"../../../chmigrate/sql/132_pr_commit_native_wins.sql":              {"provenance", "last_synced"},
		"../../../chmigrate/baseline/head.json":                             {"provenance", "last_synced"},
	} {
		body, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		want := VersionRankSQL(args[0], args[1])
		if !strings.Contains(string(body), want) {
			t.Errorf("%s does not hold the version_rank expression %q", path, want)
		}
	}
}
