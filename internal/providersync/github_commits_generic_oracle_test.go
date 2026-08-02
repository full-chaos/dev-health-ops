package providersync

import (
	"bytes"
	"encoding/json"
	"testing"
	"time"
)

var oracleCommitsGoOnlyFields = map[string]string{
	"org_id":      "carried from the tenant-scoped Go claim",
	"last_synced": "stamped by the Go complete-route handler",
}

func buildCommitRowForOracle(t *testing.T, input map[string]any) gitCommitRow {
	t.Helper()
	encoded, err := json.Marshal(input["raw_commit"])
	if err != nil {
		t.Fatal(err)
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	var raw gitHubCommitPayload
	if err := decoder.Decode(&raw); err != nil {
		t.Fatal(err)
	}
	row, ok := normalizeGitHubCommit(nativeTestClaim("github", "commits"), input["repo_id"].(string), raw, time.Date(2026, 7, 26, 12, 0, 0, 0, time.UTC))
	if !ok {
		t.Fatal("oracle commit did not produce a row")
	}
	return row
}

func TestGenericOracleMatchesLivePythonForCommitsRowConstruction(t *testing.T) {
	raw := map[string]any{
		"sha": "abc", "author": map[string]any{"login": "api-author", "email": "api@example.com"},
		"committer": map[string]any{"login": "api-committer"}, "parents": []any{map[string]any{}, map[string]any{}},
		"commit": map[string]any{"message": "message", "author": map[string]any{"name": "embedded author", "email": "embedded@example.com", "date": "2026-07-25T10:00:00Z"}, "committer": map[string]any{"name": "embedded committer", "email": nil, "date": "2026-07-25T11:00:00Z"}},
	}
	compareRowsAgainstPythonOracle(t, "github/commits/row", []oracleCase{{ID: "github_user_precedence", Input: map[string]any{"repo_id": "c7198fbc-1945-3717-05d8-eb78866b4e79", "raw_commit": raw}}}, buildCommitRowForOracle, oracleCommitsGoOnlyFields)
}
