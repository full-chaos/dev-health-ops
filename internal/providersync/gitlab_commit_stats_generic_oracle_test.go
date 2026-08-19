package providersync

import (
	"bytes"
	"encoding/json"
	"testing"
	"time"
)

var oracleGitLabCommitStatsNormalizedAt = time.Date(2026, 8, 3, 15, 0, 0, 0, time.UTC)

func buildGitLabCommitStatsRowForOracle(t *testing.T, input map[string]any) commitStatsRow {
	t.Helper()
	encoded, err := json.Marshal(input["raw_detail"])
	if err != nil {
		t.Fatal(err)
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	var detail gitLabCommitStatsPayload
	if err := decoder.Decode(&detail); err != nil {
		t.Fatal(err)
	}
	row, err := normalizeGitLabCommitStats(
		nativeTestClaim("gitlab", "commit-stats"), input["repo_id"].(string),
		input["commit_hash"].(string), detail, oracleGitLabCommitStatsNormalizedAt,
	)
	if err != nil {
		t.Fatal(err)
	}
	return row
}

func TestGenericOracleMatchesLivePythonForGitLabCommitStatsRowConstruction(t *testing.T) {
	compareRowsAgainstPythonOracle(
		t, "gitlab/commit-stats/row",
		[]oracleCase{
			{ID: "numeric_strings", Input: map[string]any{
				"repo_id": "4978eedc-8575-a471-4358-47b88c867ce8", "commit_hash": "abc123",
				"raw_detail": map[string]any{"stats": map[string]any{"additions": "4", "deletions": "2"}},
			}},
			{ID: "missing_stats_defaults_zero", Input: map[string]any{
				"repo_id": "4978eedc-8575-a471-4358-47b88c867ce8", "commit_hash": "def456",
				"raw_detail": map[string]any{"stats": nil},
			}},
			{ID: "python_int_coercions", Input: map[string]any{
				"repo_id": "4978eedc-8575-a471-4358-47b88c867ce8", "commit_hash": "ghi789",
				"raw_detail": map[string]any{"stats": map[string]any{"additions": " +7 ", "deletions": 3.8}},
			}},
			{ID: "invalid_and_boolean_coercions", Input: map[string]any{
				"repo_id": "4978eedc-8575-a471-4358-47b88c867ce8", "commit_hash": "jkl012",
				"raw_detail": map[string]any{"stats": map[string]any{"additions": "not-a-number", "deletions": true}},
			}},
		},
		buildGitLabCommitStatsRowForOracle,
		map[string]string{
			"org_id":        "carried from the Go claim to scope ClickHouse's tenant-partitioned replacing key",
			"old_file_mode": "supplied by the Python ClickHouse sink default",
			"new_file_mode": "supplied by the Python ClickHouse sink default",
			"last_synced":   "stamped from the persisted occurrence instant by the Go complete-route handler",
		},
	)
}
