package providersync

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

var oracleReviewClaim = Claim{Unit: Unit{
	OrgID: "org-oracle", Provider: "github", Dataset: "pr-reviews",
	SourceExternalID: "octo/widgets",
}}

var oracleReviewNormalizedAt = time.Date(2026, 7, 25, 0, 0, 0, 0, time.UTC)

func oracleReviewCases() []oracleCase {
	return []oracleCase{
		{ID: "approved", Input: map[string]any{
			"repo_id": "c7198fbc-1945-3717-05d8-eb78866b4e79", "number": 42,
			"created_at": "2026-07-10T09:00:00Z",
			"review":     map[string]any{"id": 9007199254740993, "reviewer": "octocat", "state": "APPROVED", "submitted_at": "2026-07-11T10:30:00Z"},
		}},
		{ID: "fallback_time_and_reviewer", Input: map[string]any{
			"repo_id": "c7198fbc-1945-3717-05d8-eb78866b4e79", "number": 7,
			"created_at": "2026-07-12T09:00:00Z",
			"review":     map[string]any{"id": "R_kwDO", "reviewer": nil, "state": "CHANGES_REQUESTED", "submitted_at": nil},
		}},
	}
}

func buildReviewRowForOracle(t *testing.T, input map[string]any) pullRequestReviewRow {
	t.Helper()
	reviewMap := input["review"].(map[string]any)
	wire := map[string]any{
		"id": reviewMap["id"], "author": map[string]any{"login": reviewMap["reviewer"]},
		"state": reviewMap["state"], "submitted_at": reviewMap["submitted_at"],
	}
	encoded, err := json.Marshal(wire)
	if err != nil {
		t.Fatal(err)
	}
	decoder := json.NewDecoder(strings.NewReader(string(encoded)))
	decoder.UseNumber()
	var payload gitHubReviewPayload
	if err := decoder.Decode(&payload); err != nil {
		t.Fatal(err)
	}
	created, err := time.Parse(time.RFC3339, input["created_at"].(string))
	if err != nil {
		t.Fatal(err)
	}
	row, err := normalizeGitHubPullRequestReview(
		oracleReviewClaim, input["repo_id"].(string), int(input["number"].(int)),
		payload,
		created, oracleReviewNormalizedAt,
	)
	if err != nil {
		t.Fatal(err)
	}
	return row
}

func TestGitHubPullRequestReviewRowMatchesLivePythonProducer(t *testing.T) {
	compareRowsAgainstPythonOracle(
		t, "github/prreviews/row", oracleReviewCases(), buildReviewRowForOracle,
		map[string]string{
			"last_synced": "stamped from the frozen native normalizedAt",
			"source_id":   "native provider effects never set external-ingest source_id",
			"org_id":      "stamped from the authoritative unit claim",
		},
	)
}

func TestNormalizeGitHubPullRequestReviewFailsClosed(t *testing.T) {
	row, err := normalizeGitHubPullRequestReview(
		oracleReviewClaim, "", 0, gitHubReviewPayload{}, time.Time{}, time.Time{},
	)
	if err == nil || row != (pullRequestReviewRow{}) {
		t.Fatalf("row=%+v err=%v", row, err)
	}
}
