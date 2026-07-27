package providersync

import (
	"bytes"
	"encoding/json"
	"testing"
	"time"
)

var oracleCICDGoOnlyFields = map[string]string{
	"last_synced": "stamped from normalizedAt by the Go complete-route handler after Python's build_ci_pipeline_run boundary",
	"org_id":      "carried from the Go claim to keep ClickHouse writes tenant-scoped",
}

var oracleCICDNormalizedAt = time.Date(2026, 7, 23, 12, 30, 0, 0, time.UTC)

func buildCICDRowForOracle(t *testing.T, input map[string]any) ciPipelineRunRow {
	t.Helper()
	rawRun, ok := input["raw_run"]
	if !ok {
		t.Fatalf("oracle case missing raw_run: %v", input)
	}
	encoded, err := json.Marshal(rawRun)
	if err != nil {
		t.Fatalf("marshal raw_run: %v", err)
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	var workflow gitHubWorkflowRunPayload
	if err := decoder.Decode(&workflow); err != nil {
		t.Fatalf("unmarshal workflow run: %v", err)
	}
	row, ok := normalizeGitHubWorkflowRun(
		nativeTestClaim("github", "cicd"), input["repo_id"].(string), workflow, oracleCICDNormalizedAt,
	)
	if !ok {
		t.Fatal("oracle workflow run did not produce a row")
	}
	return row
}

func oracleCICDCases() []oracleCase {
	return []oracleCase{
		{
			ID: "completed_run_with_retries",
			Input: map[string]any{
				"repo_id": "c7198fbc-1945-3717-05d8-eb78866b4e79",
				"raw_run": map[string]any{
					"id": 101, "conclusion": "success", "status": "completed",
					"created_at":     "2026-07-22T10:00:00Z",
					"run_started_at": "2026-07-22T10:01:00Z",
					"updated_at":     "2026-07-22T10:05:00Z", "run_attempt": 3,
				},
			},
		},
		{
			ID: "queued_run_falls_back_to_created_at",
			Input: map[string]any{
				"repo_id": "c7198fbc-1945-3717-05d8-eb78866b4e79",
				"raw_run": map[string]any{
					"id": "102", "status": "queued", "created_at": "2026-07-22T11:00:00Z",
					"run_started_at": nil, "updated_at": nil, "run_attempt": "not-a-number",
				},
			},
		},
	}
}

func TestGenericOracleMatchesLivePythonForCICDRowConstruction(t *testing.T) {
	compareRowsAgainstPythonOracle(
		t, "github/cicd/row", oracleCICDCases(), buildCICDRowForOracle, oracleCICDGoOnlyFields,
	)
}
