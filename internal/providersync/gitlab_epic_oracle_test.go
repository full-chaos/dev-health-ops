package providersync

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

// gitlabEpicOracleCase is one group epic for the Go normalizer and for Python's
// GitLabProvider batch (CHAOS-9857).
func gitlabEpicOracleCase(id string, epic map[string]any, events []any) oracleCase {
	input := map[string]any{
		"repo_full_name": "acme/api",
		"org_id":         "org-acme",
		"raw_epic":       epic,
	}
	if events != nil {
		input["state_events"] = events
	}
	return oracleCase{ID: id, Input: input}
}

func gitlabEpicOracleCases() []oracleCase {
	return []oracleCase{
		gitlabEpicOracleCase("open_epic_with_dates", map[string]any{
			"iid": 4, "title": "Platform epic", "description": "all of it", "state": "opened",
			"created_at": "2026-07-01T08:00:00Z", "updated_at": "2026-07-10T08:00:00Z",
			"start_date": "2026-07-01", "due_date": "2026-09-30",
			"labels": []any{"priority::high", "in progress"}, "author": map[string]any{"username": "bob", "name": "Bob", "email": "BOB@EXAMPLE.COM"},
			"web_url": "https://gitlab.example/groups/acme/-/epics/4",
		}, nil),
		gitlabEpicOracleCase("closed_epic_with_parent_id", map[string]any{
			"iid": 5, "title": "Child epic", "state": "closed",
			"created_at": "2026-07-02T08:00:00Z", "updated_at": "2026-07-12T08:00:00Z", "closed_at": "2026-07-12T08:00:00Z",
			"labels": []any{}, "author": map[string]any{"username": "alice"}, "parent_id": 4,
		}, nil),
		gitlabEpicOracleCase("parent_iid_only", map[string]any{
			"iid": 6, "title": "Grandchild", "state": "opened", "created_at": "2026-07-03T08:00:00Z",
			"labels": []any{"feature"}, "parent_iid": 5,
		}, nil),
		gitlabEpicOracleCase("parent_id_wins_over_parent_iid", map[string]any{
			"iid": 7, "title": "Both parents", "state": "opened", "created_at": "2026-07-03T08:00:00Z",
			"labels": []any{}, "parent_id": 4, "parent_iid": 5,
		}, nil),
		gitlabEpicOracleCase("zero_parent_is_no_parent", map[string]any{
			"iid": 8, "title": "Zero parent", "state": "opened", "created_at": "2026-07-03T08:00:00Z",
			"labels": []any{}, "parent_id": 0, "parent_iid": nil,
		}, nil),
		gitlabEpicOracleCase("empty_description_and_unknown_author", map[string]any{
			"iid": 9, "title": "Bare epic", "description": "", "state": "opened", "created_at": "2026-07-03T08:00:00Z",
			"labels": []any{}, "author": map[string]any{},
		}, nil),
		gitlabEpicOracleCase("string_iid", map[string]any{
			"iid": "11", "title": "String iid", "state": "opened", "created_at": "2026-07-03T08:00:00Z", "labels": []any{},
		}, nil),
	}
}

func gitlabEpicTransitionOracleCases() []oracleCase {
	events := []any{
		map[string]any{"state": "reopened", "created_at": "2026-07-06T08:00:00Z", "user": map[string]any{"username": "bob", "name": "Bob"}},
		map[string]any{"state": "closed", "created_at": "2026-07-05T08:00:00Z", "user": map[string]any{"username": "alice"}},
		map[string]any{"state": "opened", "created_at": "2026-07-01T09:00:00Z"},
		map[string]any{"state": "locked", "created_at": "2026-07-07T08:00:00Z"},
	}
	base := map[string]any{
		"iid": 4, "title": "Platform epic", "state": "opened",
		"created_at": "2026-07-01T08:00:00Z", "updated_at": "2026-07-10T08:00:00Z", "labels": []any{},
	}
	var cases []oracleCase
	// Sorted by time: opened (07-01), closed (07-05), reopened (07-06); the locked event is skipped.
	for index := range 3 {
		c := gitlabEpicOracleCase("sorted_state_events_"+string(rune('a'+index)), base, events)
		c.Input["transition_index"] = index
		cases = append(cases, c)
	}
	return cases
}

func buildGitLabEpicOracleRow(t *testing.T, input map[string]any) gitlabWorkItemRow {
	t.Helper()
	row, _, _, err := normalizeGitLabEpicOracleInput(t, input)
	if err != nil {
		t.Fatal(err)
	}
	return row
}

func normalizeGitLabEpicOracleInput(t *testing.T, input map[string]any) (gitlabWorkItemRow, []gitlabWorkItemTransitionRow, []gitlabWorkItemReopenRow, error) {
	t.Helper()
	claim := nativeTestClaim("gitlab", "work-items")
	claim.OrgID = input["org_id"].(string)
	var payload gitlabEpicPayload
	decodeGitLabEpicOracleValue(t, input["raw_epic"], &payload)
	var events []gitlabStateEventPayload
	if raw, ok := input["state_events"]; ok {
		decodeGitLabEpicOracleValue(t, raw, &events)
	}
	repo := input["repo_full_name"].(string)
	groupPath := repo[:strings.LastIndex(repo, "/")]
	return normalizeGitLabEpicWorkItem(claim, groupPath, payload, events, loadRealStatusMapping(t), nil,
		time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC))
}

// TestGitLabEpicWorkItemMatchesFrozenPythonProvider compares the whole epic row
// with the row of Python's canonical GitLabProvider batch.
func TestGitLabEpicWorkItemMatchesFrozenPythonProvider(t *testing.T) {
	compareRowsAgainstPythonOracle(t, "gitlab/work-items/epic", gitlabEpicOracleCases(),
		buildGitLabEpicOracleRow, gitlabEpicOracleGoOnlyFields)
}

// TestGitLabEpicTransitionMatchesFrozenPythonProvider compares each state-event
// transition of an epic with Python's.
func TestGitLabEpicTransitionMatchesFrozenPythonProvider(t *testing.T) {
	compareRowsAgainstPythonOracle(t, "gitlab/work-items/epic-transition", gitlabEpicTransitionOracleCases(),
		func(t *testing.T, input map[string]any) gitlabWorkItemTransitionRow {
			t.Helper()
			_, transitions, _, err := normalizeGitLabEpicOracleInput(t, input)
			if err != nil {
				t.Fatal(err)
			}
			return transitions[input["transition_index"].(int)]
		}, gitlabWorkItemsFetchGoOnlyFields)
}

var gitlabEpicOracleGoOnlyFields = map[string]string{
	"last_synced": "the Python semantic WorkItem dataclasses have no last_synced field; Go carries deterministic normalizedAt for replay",
}

func decodeGitLabEpicOracleValue(t *testing.T, value any, target any) {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(raw, target); err != nil {
		t.Fatal(err)
	}
}
