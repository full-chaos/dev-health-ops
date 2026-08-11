package providersync

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
)

var gitlabWorkItemsOracleNormalizedAt = time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC)

var gitlabWorkItemsFetchGoOnlyFields = map[string]string{
	"last_synced": "the Python semantic WorkItem dataclasses have no last_synced field; Go carries deterministic normalizedAt for replay",
}

func TestGitLabFetchWorkItemMatchesLivePythonProducer(t *testing.T) {
	compareRowsAgainstPythonOracle(
		t,
		"gitlab/work-items/issue",
		gitlabWorkItemsOracleCases(),
		buildGitLabFetchWorkItemOracleRow,
		gitlabWorkItemsFetchGoOnlyFields,
	)
}

func TestGitLabFetchStatusTransitionMatchesLivePythonProducer(t *testing.T) {
	compareRowsAgainstPythonOracle(
		t,
		"gitlab/work-items/status-transition",
		[]oracleCase{{
			ID: "label_driven_transition",
			Input: map[string]any{
				"repo_full_name": "acme/api",
				"repo_id":        "c7198fbc-1945-3717-05d8-eb78866b4e79",
				"org_id":         "org-acme",
				"since":          "2026-08-01T00:00:00Z",
				"raw_issue": map[string]any{
					"iid": 42, "title": "Repair delivery path", "state": "opened",
					"created_at": "2026-08-01T08:00:00Z", "updated_at": "2026-08-03T09:30:00Z",
					"labels": []any{"bug"}, "assignees": []any{},
				},
				"label_events": []any{
					map[string]any{"action": "add", "created_at": "2026-08-02T09:00:00Z", "label": map[string]any{"name": "in progress"}},
					map[string]any{"action": "add", "created_at": "2026-08-03T09:00:00Z", "label": map[string]any{"name": "done"}},
				},
				"transition_index": 1,
			},
		}},
		buildGitLabFetchStatusTransitionOracleRow,
		gitlabWorkItemsFetchGoOnlyFields,
	)
}

func gitlabWorkItemsOracleCases() []oracleCase {
	return []oracleCase{
		{
			ID: "open_labeled_issue",
			Input: map[string]any{
				"repo_full_name": "acme/api",
				"repo_id":        "c7198fbc-1945-3717-05d8-eb78866b4e79",
				"org_id":         "org-acme",
				"since":          "2026-08-01T00:00:00Z",
				"raw_issue": map[string]any{
					"iid": 42, "title": "Repair delivery path", "description": "Closes CHAOS-42", "state": "opened",
					"created_at": "2026-08-01T08:00:00.123456Z", "updated_at": "2026-08-03T09:30:00.654321Z",
					"labels":    []any{"doing", "bug", "priority::high"},
					"assignees": []any{map[string]any{"email": "DEV@EXAMPLE.COM", "username": "dev"}, map[string]any{"username": "octocat"}},
					"author":    map[string]any{"username": "reporter"}, "web_url": "https://gitlab.example/acme/api/-/issues/42", "weight": 3,
					"milestone": map[string]any{"id": 7, "title": "August"},
				},
				"label_events": []any{map[string]any{"action": "add", "created_at": "2026-08-02T09:00:00Z", "label": map[string]any{"name": "in progress"}}},
			},
		},
		{
			ID: "closed_issue",
			Input: map[string]any{
				"repo_full_name": "acme/api",
				"repo_id":        "c7198fbc-1945-3717-05d8-eb78866b4e79",
				"org_id":         "org-acme",
				"since":          "2026-07-01T00:00:00Z",
				"raw_issue": map[string]any{
					"iid": 7, "title": "Close duplicate", "state": "closed", "created_at": "2026-07-01T00:00:00Z",
					"updated_at": "2026-07-03T00:00:00Z", "closed_at": "2026-07-03T00:00:00Z", "labels": []any{"duplicate"}, "assignees": []any{},
				},
			},
		},
	}
}

func buildGitLabFetchWorkItemOracleRow(t *testing.T, input map[string]any) gitlabWorkItemRow {
	t.Helper()
	payload := gitlabIssueWorkItemPayloadFromOracle(t, input)
	claim := nativeTestClaim("gitlab", "work-items")
	repoID, err := uuid.Parse(input["repo_id"].(string))
	if err != nil {
		t.Fatal(err)
	}
	labels := gitlabLabelEventsFromOracle(t, input)
	row, _, err := normalizeGitLabIssueWorkItem(
		claim, input["repo_full_name"].(string), repoID, payload, labels,
		loadRealStatusMapping(t), nil, gitlabWorkItemsOracleNormalizedAt,
	)
	if err != nil {
		t.Fatal(err)
	}
	return row
}

func buildGitLabFetchStatusTransitionOracleRow(t *testing.T, input map[string]any) gitlabWorkItemTransitionRow {
	t.Helper()
	payload := gitlabIssueWorkItemPayloadFromOracle(t, input)
	claim := nativeTestClaim("gitlab", "work-items")
	repoID, err := uuid.Parse(input["repo_id"].(string))
	if err != nil {
		t.Fatal(err)
	}
	row, transitions, err := normalizeGitLabIssueWorkItem(
		claim, input["repo_full_name"].(string), repoID, payload, gitlabLabelEventsFromOracle(t, input),
		loadRealStatusMapping(t), nil, gitlabWorkItemsOracleNormalizedAt,
	)
	if err != nil {
		t.Fatal(err)
	}
	_ = row
	index := int(input["transition_index"].(int))
	return transitions[index]
}

func gitlabIssueWorkItemPayloadFromOracle(t *testing.T, input map[string]any) gitlabIssueWorkItemPayload {
	t.Helper()
	raw, err := json.Marshal(input["raw_issue"])
	if err != nil {
		t.Fatal(err)
	}
	var payload gitlabIssueWorkItemPayload
	if err := json.Unmarshal(raw, &payload); err != nil {
		t.Fatal(err)
	}
	return payload
}

func gitlabLabelEventsFromOracle(t *testing.T, input map[string]any) []gitlabLabelEventPayload {
	t.Helper()
	values, _ := input["label_events"].([]any)
	events := make([]gitlabLabelEventPayload, 0, len(values))
	for _, value := range values {
		raw, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		var event gitlabLabelEventPayload
		if err := json.Unmarshal(raw, &event); err != nil {
			t.Fatal(err)
		}
		events = append(events, event)
	}
	return events
}
