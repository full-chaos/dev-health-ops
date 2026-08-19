package providersync

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
)

var githubWorkItemOracleNormalizedAt = time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC)

func TestGitHubIssueWorkItemMatchesLivePythonProductionRow(t *testing.T) {
	compareRowsAgainstPythonOracle(
		t,
		"github/work-items/issue",
		[]oracleCase{
			{
				ID: "open_labeled_issue",
				Input: map[string]any{
					"repo_full_name": "acme/api",
					"repo_id":        "c7198fbc-1945-3717-05d8-eb78866b4e79",
					"org_id":         "org-acme",
					"raw_issue": map[string]any{
						"number": 42, "title": "Repair delivery path",
						"body": "Closes CHAOS-42", "state": "open",
						"created_at": "2026-08-01T08:00:00.123456Z",
						"updated_at": "2026-08-03T09:30:00.654321Z",
						"labels": []any{
							map[string]any{"name": "doing"},
							map[string]any{"name": "bug"},
							map[string]any{"name": "p1"},
						},
						"assignees": []any{
							map[string]any{"email": "DEV@EXAMPLE.COM", "login": "dev"},
							map[string]any{"login": "octocat"},
						},
						"user":     map[string]any{"login": "reporter"},
						"html_url": "https://github.com/acme/api/issues/42",
					},
				},
			},
			{
				ID: "closed_issue",
				Input: map[string]any{
					"repo_full_name": "acme/api",
					"repo_id":        "c7198fbc-1945-3717-05d8-eb78866b4e79",
					"org_id":         "org-acme",
					"raw_issue": map[string]any{
						"number": 7, "title": "Close duplicate", "state": "closed",
						"created_at": "2026-07-01T00:00:00Z",
						"updated_at": "2026-07-03T00:00:00Z",
						"closed_at":  "2026-07-03T00:00:00Z",
						"labels":     []any{map[string]any{"name": "duplicate"}},
						"assignees":  []any{},
					},
				},
			},
			{
				ID: "empty_optional_text",
				Input: map[string]any{
					"repo_full_name": "acme/api",
					"repo_id":        "c7198fbc-1945-3717-05d8-eb78866b4e79",
					"org_id":         "org-acme",
					"raw_issue": map[string]any{
						"number": 8, "title": "No description", "body": "", "state": "open",
						"created_at": "2026-07-04T00:00:00Z",
						"updated_at": "2026-07-05T00:00:00Z",
						"labels":     []any{}, "assignees": []any{},
					},
				},
			},
		},
		buildGitHubIssueWorkItemOracleRow,
		githubWorkItemWriteStampGoOnly,
	)
}

// githubWorkItemWriteStampGoOnly declares the one field the Go work-item and
// transition rows carry that the Python semantic row structurally cannot.
//
// Python's WorkItem/WorkItemStatusTransition dataclasses have no last_synced:
// the sink stamps that column from wall-clock at insert time
// (metrics/sinks/clickhouse/work_graph.py:661 and :749). Go carries the unit's
// normalizedAt in the row instead, so the effect payload — and therefore a
// recovery-snapshot replay of it — is byte-identical across attempts, which a
// wall-clock stamp can never be. The stored column keeps its meaning; only its
// determinism changes.
var githubWorkItemWriteStampGoOnly = map[string]string{
	"last_synced": "Python's semantic row has no last_synced at all -- its sink " +
		"stamps the column from wall-clock at insert time, which no retry can " +
		"reproduce. Go carries the unit's normalizedAt in the row so a recovery " +
		"snapshot replays to identical bytes and the readback can answer Exact.",
}

func TestGitHubSprintMatchesLivePythonProductionRow(t *testing.T) {
	compareRowsAgainstPythonOracle(
		t,
		"github/work-items/sprint",
		[]oracleCase{
			{
				ID: "closed_large_id",
				Input: map[string]any{
					"repo_full_name": "acme/api", "org_id": "org-acme",
					"raw_milestone": map[string]any{
						"id": json.Number("9007199254740993"), "number": 7,
						"title": "August", "state": "closed",
						"created_at": "2026-08-01T00:00:00Z",
						"due_on":     "2026-08-31T23:59:59.987654Z",
					},
				},
			},
			{
				ID: "active_number_fallback",
				Input: map[string]any{
					"repo_full_name": "acme/api", "org_id": "org-acme",
					"raw_milestone": map[string]any{
						"id": 0, "number": 8, "title": "September", "state": "open",
						"created_at": "2026-09-01T00:00:00Z", "due_on": nil,
					},
				},
			},
		},
		buildGitHubSprintOracleRow,
		nil,
	)
}

func buildGitHubIssueWorkItemOracleRow(t *testing.T, input map[string]any) githubWorkItemRow {
	t.Helper()
	raw, err := json.Marshal(input["raw_issue"])
	if err != nil {
		t.Fatal(err)
	}
	repoID, err := uuid.Parse(input["repo_id"].(string))
	if err != nil {
		t.Fatal(err)
	}
	claim := githubWorkItemOracleClaim()
	claim.OrgID = input["org_id"].(string)
	row, err := normalizeGitHubIssueWorkItem(
		claim, input["repo_full_name"].(string), repoID, raw, nil,
		githubWorkItemOracleNormalizedAt,
	)
	if err != nil {
		t.Fatal(err)
	}
	return row
}

func buildGitHubSprintOracleRow(t *testing.T, input map[string]any) githubSprintRow {
	t.Helper()
	raw, err := json.Marshal(input["raw_milestone"])
	if err != nil {
		t.Fatal(err)
	}
	claim := githubWorkItemOracleClaim()
	claim.OrgID = input["org_id"].(string)
	row, err := normalizeGitHubSprint(
		claim, input["repo_full_name"].(string), raw,
		githubWorkItemOracleNormalizedAt,
	)
	if err != nil {
		t.Fatal(err)
	}
	return row
}
