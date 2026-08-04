package providersync

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestNormalizeGitHubIssueWorkItemMatchesProductionBoundary(t *testing.T) {
	t.Parallel()
	normalizedAt := time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC)
	repoID := uuid.MustParse("c7198fbc-1945-3717-05d8-eb78866b4e79")
	claim := githubWorkItemOracleClaim()
	body := json.RawMessage(`{
      "number": 42,
      "title": "Repair delivery path",
      "body": "Closes CHAOS-42",
      "state": "open",
      "created_at": "2026-08-01T08:00:00.123456Z",
      "updated_at": "2026-08-03T09:30:00.654321Z",
      "labels": [
        {"name":"doing"},
        {"name":"bug"},
        {"name":"p1"}
      ],
      "assignees": [
        {"email":"DEV@EXAMPLE.COM","login":"dev"},
        {"login":"octocat"},
        {"name":"Unknown User"}
      ],
      "user": {"login":"reporter"},
      "html_url": "https://github.com/acme/api/issues/42"
    }`)
	row, err := normalizeGitHubIssueWorkItem(
		claim, "acme/api", repoID, body, nil, normalizedAt,
	)
	if err != nil {
		t.Fatal(err)
	}
	wantAssignees := []string{"dev@example.com", "github:octocat", "Unknown User"}
	if row.WorkItemID != "gh:acme/api#42" || row.Type != "bug" ||
		row.Status != "in_progress" || row.StatusRaw == nil || *row.StatusRaw != "open" ||
		row.ProjectID == nil || *row.ProjectID != "acme/api" ||
		row.Reporter == nil || *row.Reporter != "github:reporter" ||
		!reflect.DeepEqual(row.Assignees, wantAssignees) ||
		row.PriorityRaw == nil || *row.PriorityRaw != "high" ||
		row.ServiceClass == nil || *row.ServiceClass != "fixed_date" ||
		row.CreatedAt.Nanosecond() != 123456000 || row.UpdatedAt.Nanosecond() != 654321000 ||
		row.CompletedAt != nil || row.ClosedAt != nil || row.OrgID != claim.OrgID {
		t.Fatalf("row=%+v", row)
	}
}

func TestNormalizeGitHubIssueWorkItemFallsBackLikePython(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 4, 12, 0, 0, 123456000, time.UTC)
	claim := githubWorkItemOracleClaim()
	repoID := uuid.MustParse("c7198fbc-1945-3717-05d8-eb78866b4e79")
	row, err := normalizeGitHubIssueWorkItem(
		claim, "acme/api", repoID,
		json.RawMessage(`{"number":7,"title":"Closed","state":"closed","closed_at":"2026-08-03T10:00:00Z"}`),
		nil, now,
	)
	if err != nil {
		t.Fatal(err)
	}
	if row.Status != "done" || !row.CreatedAt.Equal(now) || !row.UpdatedAt.Equal(now) ||
		row.CompletedAt == nil || row.ClosedAt == nil || !row.CompletedAt.Equal(*row.ClosedAt) ||
		row.Assignees == nil || row.Labels == nil {
		t.Fatalf("row=%+v", row)
	}
}

func TestNormalizeGitHubSprintMatchesProductionBoundary(t *testing.T) {
	t.Parallel()
	claim := githubWorkItemOracleClaim()
	now := time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC)
	row, err := normalizeGitHubSprint(
		claim, "acme/api",
		json.RawMessage(`{
          "id":9007199254740993,
          "number":7,
          "title":"August",
          "state":"closed",
          "created_at":"2026-08-01T00:00:00Z",
          "due_on":"2026-08-31T23:59:59.987654Z"
        }`),
		now,
	)
	if err != nil {
		t.Fatal(err)
	}
	if row.SprintID != "ghms:acme/api:9007199254740993" || row.Name == nil ||
		*row.Name != "August" || row.State == nil || *row.State != "closed" ||
		row.CompletedAt == nil || row.EndedAt == nil || !row.CompletedAt.Equal(*row.EndedAt) ||
		row.EndedAt.Nanosecond() != 987654000 ||
		!row.LastSynced.Equal(now) || row.OrgID != claim.OrgID {
		t.Fatalf("row=%+v", row)
	}
}

func TestGitHubWorkItemCompositeDeclaresEveryPythonBatchFamily(t *testing.T) {
	t.Parallel()
	typeOf := reflect.TypeOf(githubWorkItemRows{})
	want := []string{
		"WorkItems", "StatusTransitions", "Dependencies", "ReopenEvents",
		"Interactions", "Sprints", "AIAttributions",
	}
	got := make([]string, 0, typeOf.NumField())
	for index := 0; index < typeOf.NumField(); index++ {
		got = append(got, typeOf.Field(index).Name)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("batch families=%v want=%v", got, want)
	}
}

func githubWorkItemOracleClaim() Claim {
	return Claim{Unit: Unit{
		ID:        "11111111-1111-4111-8111-111111111111",
		SyncRunID: "22222222-2222-4222-8222-222222222222", OrgID: "org-acme",
		IntegrationID: "33333333-3333-4333-8333-333333333333",
		SourceID:      "44444444-4444-4444-8444-444444444444",
		Provider:      "github", Dataset: "work-items", CostClass: "medium",
		Mode: "incremental", SourceExternalID: "acme/api",
		CredentialID: "55555555-5555-4555-8555-555555555555",
		AuthSource:   "database",
	}, Owner: "66666666-6666-4666-8666-666666666666", Attempt: 1,
		LeaseExpiresAt: time.Date(2026, 8, 4, 13, 0, 0, 0, time.UTC)}
}
