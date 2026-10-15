package edges

import (
	"os"
	"strings"
	"testing"
)

// Every table that ranks a link tier writes the same ranks as ProvenanceRank.
func TestMigrationVersionRanksAreTheExportedRanks(t *testing.T) {
	t.Parallel()
	if !(ProvenanceRank(ProvenanceNative) > ProvenanceRank(ProvenanceExplicitText) &&
		ProvenanceRank(ProvenanceExplicitText) > ProvenanceRank(ProvenanceHeuristic) &&
		ProvenanceRank(ProvenanceHeuristic) > ProvenanceRank("unknown")) {
		t.Fatal("tier ranks must run native > explicit_text > heuristic > unknown")
	}
	for path, args := range map[string][2]string{
		"../../../chmigrate/sql/131_deployment_incident_edges_repo_key.sql": {"source", "computed_at"},
		"../../../chmigrate/baseline/head.json":                             {"provenance", "last_synced"},
	} {
		body, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		want := ProvenanceVersionRankSQL(args[0], args[1])
		if !strings.Contains(string(body), want) {
			t.Errorf("%s does not hold the version_rank expression %q", path, want)
		}
	}
}
