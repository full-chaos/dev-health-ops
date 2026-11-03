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
	// The head baseline holds both tables (work_graph_deployment_incident_edges, migration 131, and
	// work_graph_pr_commit, migration 132) as the server shows them.
	const path = "../../../chmigrate/baseline/head.json"
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, args := range [][2]string{{"source", "computed_at"}, {"provenance", "last_synced"}} {
		want := VersionRankSQL(args[0], args[1])
		if !strings.Contains(string(body), want) {
			t.Errorf("%s does not hold the version_rank expression %q", path, want)
		}
	}
}
