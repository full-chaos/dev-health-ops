package providersync

import (
	"bytes"
	"encoding/json"
	"testing"
	"time"
)

var oracleGitLabPullRequestNormalizedAt = time.Date(2026, 8, 9, 12, 0, 0, 987000000, time.UTC)

var oracleGitLabPullRequestGoOnlyFields = map[string]string{
	"last_synced": "stamped from the Go collection instant, not the Python builder",
	"source_id":   "native provider effects always write null source_id",
	"org_id":      "stamped from the authoritative tenant claim",
}

func oracleGitLabPullRequestCases() []oracleCase {
	return []oracleCase{
		{
			ID: "approval_note_diff_note_and_approval_backfill",
			Input: map[string]any{
				"repo_id":       "c7198fbc-1945-3717-05d8-eb78866b4e79",
				"normalized_at": "2026-08-09T12:00:00.987Z",
				"mr": map[string]any{
					"iid": 7, "title": "Add API", "description": "body", "state": "opened",
					"author":     map[string]any{"username": "author"},
					"created_at": "2026-07-15T10:00:00Z", "updated_at": "2026-07-20T10:00:00Z",
					"source_branch": "feature", "target_branch": "main", "user_notes_count": 4,
				},
				"approvals": map[string]any{"approved_by": []any{
					map[string]any{"user": map[string]any{"id": 77, "username": "reviewer"}},
					map[string]any{"user": map[string]any{"id": 88, "username": "approver-only"}},
				}},
				"notes": []any{
					map[string]any{"id": 1, "system": true, "body": "approved this merge request", "author": map[string]any{"username": "reviewer"}, "created_at": "2026-07-16T11:00:00Z"},
					map[string]any{"id": 2, "type": "DiffNote", "author": map[string]any{"username": "reviewer2"}, "created_at": "2026-07-17T11:00:00Z"},
					map[string]any{"id": 3, "type": "DiffNote", "author": map[string]any{"username": "author"}, "created_at": "2026-07-18T11:00:00Z"},
					map[string]any{"id": 4, "type": "DiscussionNote", "author": map[string]any{"username": "reviewer3"}, "created_at": "2026-07-18T12:00:00Z"},
				},
			},
		},
		{
			ID: "merged_mr_unapproval_is_dismissed",
			Input: map[string]any{
				"repo_id":       "c7198fbc-1945-3717-05d8-eb78866b4e79",
				"normalized_at": "2026-08-09T12:00:00.987Z",
				"mr": map[string]any{
					"iid": 8, "title": "Ship API", "description": nil, "state": "merged",
					"author":     map[string]any{"username": "author2"},
					"created_at": "2026-07-05T10:00:00Z", "updated_at": "2026-07-10T10:00:00Z",
					"merged_at": "2026-07-22T10:00:00Z", "closed_at": "2026-07-22T10:00:00Z",
					"source_branch": "release", "target_branch": "main", "user_notes_count": 0,
				},
				"approvals": nil,
				"notes": []any{map[string]any{
					"id": 5, "system": true, "body": "unapproved this merge request",
					"author": map[string]any{"username": "reviewer"}, "created_at": "2026-07-23T11:00:00Z",
				}},
			},
		},
		{
			ID: "approval_only_without_timestamp",
			Input: map[string]any{
				"repo_id":       "c7198fbc-1945-3717-05d8-eb78866b4e79",
				"normalized_at": "2026-08-09T12:00:00.987Z",
				"mr": map[string]any{
					"iid": 9, "title": "Review", "description": "review body", "state": "closed",
					"author":     map[string]any{"username": "author3"},
					"created_at": "2026-07-12T10:00:00Z", "updated_at": "2026-07-13T10:00:00Z",
					"closed_at": "2026-07-14T10:00:00Z", "user_notes_count": "2",
				},
				"approvals": map[string]any{"approved_by": []any{map[string]any{
					"user": map[string]any{"id": "91", "username": "reviewer3"},
				}}},
				"notes": []any{},
			},
		},
	}
}

func decodeOracleGitLabPullRequestInput(t *testing.T, input map[string]any) (gitLabMergeRequestPayload, map[string]any, []json.RawMessage) {
	t.Helper()
	encoded, err := json.Marshal(input["mr"])
	if err != nil {
		t.Fatal(err)
	}
	var payload gitLabMergeRequestPayload
	if err := decodeGitLabMergeRequest(encoded, &payload); err != nil {
		t.Fatal(err)
	}
	var approvals map[string]any
	if raw := input["approvals"]; raw != nil {
		encoded, err = json.Marshal(raw)
		if err != nil {
			t.Fatal(err)
		}
		decoder := json.NewDecoder(bytes.NewReader(encoded))
		decoder.UseNumber()
		if err := decoder.Decode(&approvals); err != nil {
			t.Fatal(err)
		}
	}
	notesValue, ok := input["notes"].([]any)
	if !ok {
		t.Fatalf("notes=%T", input["notes"])
	}
	notes := make([]json.RawMessage, 0, len(notesValue))
	for _, note := range notesValue {
		encoded, err = json.Marshal(note)
		if err != nil {
			t.Fatal(err)
		}
		notes = append(notes, encoded)
	}
	return payload, approvals, notes
}

func buildGitLabPullRequestRowForOracle(t *testing.T, input map[string]any) pullRequestRow {
	t.Helper()
	payload, approvals, notes := decodeOracleGitLabPullRequestInput(t, input)
	claim := nativeTestClaim("gitlab", "prs")
	normalizedAt := oracleGitLabPullRequestNormalizedAt
	createdAt := parseGitLabPullTime(payload.CreatedAt)
	mergedAt := parseGitLabPullTime(payload.MergedAt)
	closedAt := parseGitLabPullTime(payload.ClosedAt)
	reviews, firstReviewAt, changesRequested := mapGitLabPullRequestReviews(
		claim, input["repo_id"].(string), mustGitLabOracleIID(t, payload.IID), approvals, notes,
		createdAt, normalizedAt, payload.Author,
	)
	comments, err := gitLabPullRequestInt(payload.UserNotesCount)
	if err != nil {
		t.Fatal(err)
	}
	row, err := normalizeGitLabPullRequest(
		claim, input["repo_id"].(string), payload, createdAt, mergedAt, closedAt,
		gitLabMergeRequestReviewFetch{Rows: reviews, FirstReviewAt: firstReviewAt, ChangesRequestedCount: changesRequested},
		comments, normalizedAt,
	)
	if err != nil {
		t.Fatal(err)
	}
	return row
}

func mustGitLabOracleIID(t *testing.T, value any) int {
	t.Helper()
	iid, err := gitLabPullRequestIID(value)
	if err != nil {
		t.Fatal(err)
	}
	return iid
}

func buildGitLabPullRequestReviewForOracle(t *testing.T, input map[string]any) pullRequestReviewRow {
	t.Helper()
	payload, approvals, notes := decodeOracleGitLabPullRequestInput(t, input)
	claim := nativeTestClaim("gitlab", "pr-reviews")
	createdAt := parseGitLabPullTime(payload.CreatedAt)
	reviews, _, _ := mapGitLabPullRequestReviews(
		claim, input["repo_id"].(string), mustGitLabOracleIID(t, payload.IID), approvals, notes,
		createdAt, oracleGitLabPullRequestNormalizedAt, payload.Author,
	)
	index, ok := input["review_index"].(int)
	if !ok || index < 0 || index >= len(reviews) {
		t.Fatalf("review_index=%v reviews=%d", input["review_index"], len(reviews))
	}
	return reviews[index]
}

func TestGitLabPullRequestRowsMatchLivePythonAcrossAllAliases(t *testing.T) {
	rowCases := oracleGitLabPullRequestCases()[:2]
	for _, pairID := range []string{"gitlab/prs/row", "gitlab/pr-comments/row"} {
		t.Run(pairID, func(t *testing.T) {
			compareRowsAgainstPythonOracle(t, pairID, rowCases, buildGitLabPullRequestRowForOracle, oracleGitLabPullRequestGoOnlyFields)
		})
	}
}

func TestGitLabPullRequestReviewRowsMatchLivePython(t *testing.T) {
	cases := oracleGitLabPullRequestCases()
	for index := range cases {
		cases[index].Input["review_index"] = 0
	}
	compareRowsAgainstPythonOracle(t, "gitlab/pr-reviews/row", cases, buildGitLabPullRequestReviewForOracle, oracleGitLabPullRequestGoOnlyFields)
}
