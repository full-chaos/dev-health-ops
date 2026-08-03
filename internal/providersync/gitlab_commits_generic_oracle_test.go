package providersync

import (
	"bytes"
	"encoding/json"
	"testing"
	"time"
)

var oracleGitLabCommitsGoOnlyFields = map[string]string{
	"org_id":      "carried from the tenant-scoped Go claim",
	"last_synced": "stamped by the Go complete-route handler",
}

func buildGitLabCommitRowForOracle(t *testing.T, input map[string]any) gitCommitRow {
	t.Helper()
	encoded, err := json.Marshal(input["raw_commit"])
	if err != nil {
		t.Fatal(err)
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	var raw gitLabCommitPayload
	if err := decoder.Decode(&raw); err != nil {
		t.Fatal(err)
	}
	fixedNow, err := time.Parse(time.RFC3339Nano, input["now"].(string))
	if err != nil {
		t.Fatal(err)
	}
	row, err := (GitLabCommitsRouteHandler{Now: func() time.Time { return fixedNow }}).normalizeCommit(
		nativeTestClaim("gitlab", "commits"),
		input["repo_id"].(string),
		raw,
		time.Date(2026, 8, 3, 12, 0, 0, 0, time.UTC),
	)
	if err != nil {
		t.Fatal(err)
	}
	return row
}

func TestGenericOracleMatchesLivePythonForGitLabCommitsRowConstruction(t *testing.T) {
	cases := []oracleCase{
		{
			ID: "nullable_and_fallback_values",
			Input: map[string]any{
				"repo_id": "c7198fbc-1945-3717-05d8-eb78866b4e79",
				"now":     "2026-08-03T11:12:13.456Z",
				"raw_commit": map[string]any{
					"id": "abc", "message": nil, "author_name": nil,
					"authored_date": nil, "committer_name": "", "committed_date": "invalid",
					"parent_ids": []any{"parent-a", "parent-b"},
				},
			},
		},
		{
			ID: "canonical_values_and_short_id_fallback",
			Input: map[string]any{
				"repo_id": "c7198fbc-1945-3717-05d8-eb78866b4e79",
				"now":     "2026-08-03T11:12:13.456Z",
				"raw_commit": map[string]any{
					"id": nil, "short_id": "def", "message": "preserved",
					"author_name": "Author", "authored_date": "2026-08-01T10:00:00Z",
					"committer_name": "Committer", "committed_date": "2026-08-01T11:00:00+00:00",
					"parent_ids": []any{},
				},
			},
		},
	}
	compareRowsAgainstPythonOracle(
		t,
		"gitlab/commits/row",
		cases,
		buildGitLabCommitRowForOracle,
		oracleGitLabCommitsGoOnlyFields,
	)
}
