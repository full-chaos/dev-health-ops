package providersync

import (
	"bytes"
	"encoding/json"
	"testing"
	"time"
)

var oracleDeploymentsGoOnlyFields = map[string]string{
	"org_id":      "carried from the Go claim to keep ClickHouse writes tenant-scoped",
	"last_synced": "stamped from normalizedAt by the Go complete-route handler",
}

var oracleDeploymentsNormalizedAt = time.Date(2026, 7, 23, 12, 30, 0, 0, time.UTC)

func buildDeploymentRowForOracle(t *testing.T, input map[string]any) deploymentRow {
	t.Helper()
	encoded, err := json.Marshal(input["raw_deployment"])
	if err != nil {
		t.Fatal(err)
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	var deployment gitHubDeploymentPayload
	if err := decoder.Decode(&deployment); err != nil {
		t.Fatal(err)
	}
	var releases []gitHubReleasePayload
	encoded, err = json.Marshal(input["releases"])
	if err != nil {
		t.Fatal(err)
	}
	decoder = json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	if err := decoder.Decode(&releases); err != nil {
		t.Fatal(err)
	}
	row, ok := normalizeGitHubDeployment(nativeTestClaim("github", "deployments"), input["repo_id"].(string), deployment, releases, oracleDeploymentsNormalizedAt)
	if !ok {
		t.Fatal("oracle deployment did not produce a row")
	}
	encoded, err = json.Marshal(input["pulls"])
	if err != nil {
		t.Fatal(err)
	}
	decoder = json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	var pulls []gitHubPullPayload
	if err := decoder.Decode(&pulls); err != nil {
		t.Fatal(err)
	}
	if deployment.SHA != nil {
		row.PullRequestNumber, row.MergedAt = chooseDeploymentPullRequest(pulls, *deployment.SHA)
	}
	return row
}

func oracleDeploymentCases() []oracleCase {
	return []oracleCase{
		{ID: "release_tag_with_merged_pull", Input: map[string]any{"repo_id": "c7198fbc-1945-3717-05d8-eb78866b4e79", "releases": []map[string]any{{"tag_name": "v1.2.3"}}, "pulls": []map[string]any{{"number": 42, "merge_commit_sha": "abc", "merged_at": "2026-07-21T10:00:00Z"}}, "raw_deployment": map[string]any{"id": 101, "state": "success", "environment": "production", "created_at": "2026-07-22T10:00:00Z", "ref": "v1.2.3", "sha": "abc"}}},
		{ID: "fallback", Input: map[string]any{"repo_id": "c7198fbc-1945-3717-05d8-eb78866b4e79", "releases": []map[string]any{}, "pulls": []map[string]any{}, "raw_deployment": map[string]any{"id": "102", "status": "pending", "environment": nil, "created_at": "2026-07-22T11:00:00Z"}}},
	}
}

func TestGenericOracleMatchesLivePythonForDeploymentsRowConstruction(t *testing.T) {
	compareRowsAgainstPythonOracle(t, "github/deployments/row", oracleDeploymentCases(), buildDeploymentRowForOracle, oracleDeploymentsGoOnlyFields)
}
