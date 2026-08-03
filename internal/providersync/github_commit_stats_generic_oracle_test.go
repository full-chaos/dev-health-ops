package providersync

import (
	"testing"
	"time"
)

var oracleCommitStatsNormalizedAt = time.Date(2026, 7, 23, 12, 30, 0, 0, time.UTC)

var oracleCommitStatsGoOnlyFields = map[string]string{
	"org_id": "carried from the Go claim to scope ClickHouse's tenant-partitioned replacing key",
}

func buildCommitStatsRowForOracle(t *testing.T, input map[string]any) commitStatsRow {
	t.Helper()
	file, ok := input["raw_file"].(map[string]any)
	if !ok {
		t.Fatalf("oracle case missing raw_file: %v", input)
	}
	filename, _ := file["filename"].(string)
	additions, _ := file["additions"].(float64)
	deletions, _ := file["deletions"].(float64)
	return commitStatsRow{
		OrgID: "org-1", RepoID: input["repo_id"].(string),
		CommitHash: input["commit_hash"].(string), FilePath: filename,
		Additions: int32(additions), Deletions: int32(deletions),
		OldFileMode: "unknown", NewFileMode: "unknown", LastSynced: oracleCommitStatsNormalizedAt,
	}
}

func TestGenericOracleMatchesLivePythonForCommitStatsRowConstruction(t *testing.T) {
	compareRowsAgainstPythonOracle(
		t,
		"github/commit-stats/row",
		[]oracleCase{{
			ID: "commit_file_stat_defaults_modes_at_sink",
			Input: map[string]any{
				"repo_id": "c7198fbc-1945-3717-05d8-eb78866b4e79", "commit_hash": "abc123",
				"raw_file": map[string]any{"filename": "src/main.go", "additions": float64(4), "deletions": float64(2)},
			},
		}},
		buildCommitStatsRowForOracle,
		oracleCommitStatsGoOnlyFields,
	)
}
