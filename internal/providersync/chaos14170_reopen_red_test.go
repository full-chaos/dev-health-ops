package providersync

import (
	"encoding/json"
	"sort"
	"testing"
	"time"

	"github.com/google/uuid"
)

type chaos14170Step struct {
	at time.Time
	to string
}

func chaos14170Last(t *testing.T, steps []chaos14170Step) chaos14170Step {
	t.Helper()
	if len(steps) == 0 {
		t.Fatal("no transitions stored")
	}
	sort.SliceStable(steps, func(i, j int) bool { return steps[i].at.Before(steps[j].at) })
	return steps[len(steps)-1]
}

func chaos14170CheckLastMatches(t *testing.T, status string, steps []chaos14170Step) {
	t.Helper()
	last := chaos14170Last(t, steps)
	if last.to != status {
		t.Fatalf("item status=%q but last transition (%s) to_status=%q; transitions=%v", status, last.at.Format(time.RFC3339), last.to, steps)
	}
}

var chaos14170Now = time.Date(2026, 10, 15, 12, 0, 0, 0, time.UTC)

func chaos14170GitLabIssue(t *testing.T, state, labels, labelEvents, stateEvents string) (string, []chaos14170Step) {
	t.Helper()
	var payload gitlabIssueWorkItemPayload
	if err := json.Unmarshal([]byte(`{"iid":42,"title":"Reopened","state":"`+state+`",
		"created_at":"2026-10-01T00:00:00Z","updated_at":"2026-10-04T00:00:00Z","labels":`+labels+`}`), &payload); err != nil {
		t.Fatal(err)
	}
	var events []gitlabLabelEventPayload
	if err := json.Unmarshal([]byte(labelEvents), &events); err != nil {
		t.Fatal(err)
	}
	var states []gitlabStateEventPayload
	if err := json.Unmarshal([]byte(stateEvents), &states); err != nil {
		t.Fatal(err)
	}
	row, transitions, err := normalizeGitLabIssueWorkItem(
		nativeTestClaim("gitlab", "work-items"), "acme/api", "acme/api", "org:gitlab:1",
		uuid.MustParse("0b7e3d4e-1111-4222-8333-944455556666"), payload, events, states,
		loadRealStatusMapping(t), nil, chaos14170Now,
	)
	if err != nil {
		t.Fatal(err)
	}
	steps := make([]chaos14170Step, 0, len(transitions))
	for _, tr := range transitions {
		steps = append(steps, chaos14170Step{tr.OccurredAt, tr.ToStatus})
	}
	return row.Status, steps
}

// GitLab issue transitions come from resource_label_events only; a "remove" of
// the terminal label maps to no status and is skipped.
func TestChaos14170RedGitLabIssueTerminalLabelRemoved(t *testing.T) {
	for _, label := range []string{"done", "wontfix"} {
		t.Run(label, func(t *testing.T) {
			status, steps := chaos14170GitLabIssue(t, "opened", `[]`, `[
				{"action":"add","created_at":"2026-10-02T00:00:00Z","label":{"name":"`+label+`"}},
				{"action":"remove","created_at":"2026-10-03T00:00:00Z","label":{"name":"`+label+`"}}]`, `[]`)
			chaos14170CheckLastMatches(t, status, steps)
		})
	}
}

// GitLab issue state events (native close/reopen) are folded into the issue
// transitions with the label events.
func TestChaos14170RedGitLabIssueStateCloseThenReopen(t *testing.T) {
	status, steps := chaos14170GitLabIssue(t, "opened", `[]`, `[
		{"action":"add","created_at":"2026-10-01T12:00:00Z","label":{"name":"doing"}},
		{"action":"remove","created_at":"2026-10-01T18:00:00Z","label":{"name":"doing"}}]`, `[
		{"state":"closed","created_at":"2026-10-02T00:00:00Z","user":{"username":"dev"}},
		{"state":"reopened","created_at":"2026-10-03T00:00:00Z","user":{"username":"dev"}}]`)
	chaos14170CheckLastMatches(t, status, steps)
	if len(steps) != 4 || steps[2].to != "done" || steps[3].to != "todo" {
		t.Fatalf("want doing, unlabeled, closed, reopened rows; got %v", steps)
	}
}

func TestChaos14170GitLabIssueStateFoldKeepsLabelStatus(t *testing.T) {
	t.Run("close_with_status_label", func(t *testing.T) {
		status, steps := chaos14170GitLabIssue(t, "closed", `["doing"]`, `[
			{"action":"add","created_at":"2026-10-02T00:00:00Z","label":{"name":"doing"}}]`, `[
			{"state":"closed","created_at":"2026-10-03T00:00:00Z"}]`)
		chaos14170CheckLastMatches(t, status, steps)
		if len(steps) != 1 {
			t.Fatalf("a close that leaves the label status in place adds no row; got %v", steps)
		}
	})
	t.Run("remove_unmapped_label", func(t *testing.T) {
		status, steps := chaos14170GitLabIssue(t, "opened", `[]`, `[
			{"action":"add","created_at":"2026-10-02T00:00:00Z","label":{"name":"doing"}},
			{"action":"remove","created_at":"2026-10-03T00:00:00Z","label":{"name":"frontend"}}]`, `[]`)
		if len(steps) != 1 || steps[0].to != "in_progress" || status != "todo" {
			t.Fatalf("an unmapped label remove adds no row; status=%q steps=%v", status, steps)
		}
	})
	t.Run("remove_while_closed", func(t *testing.T) {
		status, steps := chaos14170GitLabIssue(t, "closed", `[]`, `[
			{"action":"add","created_at":"2026-10-02T00:00:00Z","label":{"name":"done"}},
			{"action":"remove","created_at":"2026-10-04T00:00:00Z","label":{"name":"done"}}]`, `[
			{"state":"closed","created_at":"2026-10-03T00:00:00Z"}]`)
		chaos14170CheckLastMatches(t, status, steps)
		if len(steps) != 1 {
			t.Fatalf("closed then unlabeled done stays done; got %v", steps)
		}
	})
}

func chaos14170GitHubIssue(t *testing.T, issue string, events ...string) (string, []chaos14170Step) {
	t.Helper()
	raw := make([]json.RawMessage, 0, len(events))
	for _, event := range events {
		raw = append(raw, json.RawMessage(event))
	}
	rows, err := normalizeGitHubIssueBundle(
		githubWorkItemOracleClaim(), sameGitHubRepoNames("acme/api"),
		uuid.MustParse("c7198fbc-1945-3717-05d8-eb78866b4e79"), json.RawMessage(issue), raw, nil, nil, chaos14170Now,
	)
	if err != nil || len(rows.WorkItems) != 1 {
		t.Fatalf("rows=%d err=%v", len(rows.WorkItems), err)
	}
	steps := make([]chaos14170Step, 0, len(rows.StatusTransitions))
	for _, tr := range rows.StatusTransitions {
		steps = append(steps, chaos14170Step{tr.OccurredAt, tr.ToStatus})
	}
	return rows.WorkItems[0].Status, steps
}

const chaos14170GitHubOpenIssue = `{"number":7,"title":"T","state":"open",
	"created_at":"2026-10-01T00:00:00Z","updated_at":"2026-10-04T00:00:00Z","labels":[],"assignees":[]}`

// GitHub issue: a "labeled" event with a status label is a transition; the
// matching "unlabeled" event is not read.
func TestChaos14170RedGitHubIssueTerminalLabelRemoved(t *testing.T) {
	for _, label := range []string{"done", "wontfix"} {
		t.Run(label, func(t *testing.T) {
			status, steps := chaos14170GitHubIssue(t, chaos14170GitHubOpenIssue,
				`{"event":"labeled","created_at":"2026-10-02T00:00:00Z","label":{"name":"`+label+`"}}`,
				`{"event":"unlabeled","created_at":"2026-10-03T00:00:00Z","label":{"name":"`+label+`"}}`,
			)
			chaos14170CheckLastMatches(t, status, steps)
		})
	}
}

func TestChaos14170GitHubIssueUnlabeledKeepsStateStatus(t *testing.T) {
	t.Run("closed", func(t *testing.T) {
		status, steps := chaos14170GitHubIssue(t, `{"number":7,"title":"T","state":"closed","closed_at":"2026-10-03T00:00:00Z",
			"created_at":"2026-10-01T00:00:00Z","updated_at":"2026-10-04T00:00:00Z","labels":[],"assignees":[]}`,
			`{"event":"labeled","created_at":"2026-10-02T00:00:00Z","label":{"name":"done"}}`,
			`{"event":"closed","created_at":"2026-10-03T00:00:00Z"}`,
			`{"event":"unlabeled","created_at":"2026-10-04T00:00:00Z","label":{"name":"done"}}`,
		)
		chaos14170CheckLastMatches(t, status, steps)
		if len(steps) != 2 {
			t.Fatalf("closed then unlabeled done stays done; got %v", steps)
		}
	})
	t.Run("unmapped_label", func(t *testing.T) {
		_, steps := chaos14170GitHubIssue(t, chaos14170GitHubOpenIssue,
			`{"event":"labeled","created_at":"2026-10-02T00:00:00Z","label":{"name":"doing"}}`,
			`{"event":"unlabeled","created_at":"2026-10-03T00:00:00Z","label":{"name":"frontend"}}`,
		)
		if len(steps) != 1 {
			t.Fatalf("an unmapped label remove adds no row; got %v", steps)
		}
	})
	t.Run("raw_value", func(t *testing.T) {
		rows, err := normalizeGitHubIssueBundle(
			githubWorkItemOracleClaim(), sameGitHubRepoNames("acme/api"),
			uuid.MustParse("c7198fbc-1945-3717-05d8-eb78866b4e79"), json.RawMessage(chaos14170GitHubOpenIssue),
			[]json.RawMessage{
				json.RawMessage(`{"event":"labeled","created_at":"2026-10-02T00:00:00Z","label":{"name":"wontfix"}}`),
				json.RawMessage(`{"event":"unlabeled","created_at":"2026-10-03T00:00:00Z","label":{"name":"wontfix"}}`),
			}, nil, nil, chaos14170Now,
		)
		if err != nil || len(rows.StatusTransitions) != 2 {
			t.Fatalf("rows=%v err=%v", rows.StatusTransitions, err)
		}
		last := rows.StatusTransitions[1]
		if last.ToStatusRaw == nil || *last.ToStatusRaw != "unlabeled:wontfix" || last.FromStatus != "canceled" ||
			!last.OccurredAt.Equal(time.Date(2026, 10, 3, 0, 0, 0, 0, time.UTC)) {
			t.Fatalf("raw=%v from=%q at=%s", last.ToStatusRaw, last.FromStatus, last.OccurredAt)
		}
	})
}

// GitHub issue on a Projects v2 board: the board row replaces the repository
// row (status from the board), while the transitions are the union of the
// repository events and the board changes. A repository close with no board
// move leaves an open board status after a "done" transition.
func TestChaos14170RedGitHubBoardStatusOpenAfterRepositoryClose(t *testing.T) {
	t.Skip("board status vs repository close: ticket 14184 (filed separately)")
	claim := nativeTestClaim("github", "work-items")
	repository, err := normalizeGitHubIssueBundle(
		claim, sameGitHubRepoNames("acme/api"), uuid.MustParse("c7198fbc-1945-3717-05d8-eb78866b4e79"),
		json.RawMessage(`{"number":9,"title":"T","state":"closed","closed_at":"2026-10-03T00:00:00Z",
			"created_at":"2026-10-01T00:00:00Z","updated_at":"2026-10-03T00:00:00Z","labels":[],"assignees":[]}`),
		[]json.RawMessage{json.RawMessage(`{"event":"closed","created_at":"2026-10-03T00:00:00Z"}`)}, nil, nil, chaos14170Now,
	)
	if err != nil {
		t.Fatal(err)
	}
	var payload gitHubProjectV2ItemPayload
	if err := json.Unmarshal([]byte(`{"id":"PVTI_1","createdAt":"2026-10-01T00:00:00Z",
		"content":{"__typename":"Issue","id":"I_1","number":9,"title":"T","state":"CLOSED",
		"createdAt":"2026-10-01T00:00:00Z","updatedAt":"2026-10-03T00:00:00Z","closedAt":"2026-10-03T00:00:00Z",
		"repository":{"nameWithOwner":"acme/api"},"labels":{"nodes":[]},"assignees":{"nodes":[]}},
		"fieldValues":{"nodes":[{"__typename":"ProjectV2ItemFieldSingleSelectValue","name":"In Progress","field":{"name":"Status"}}]},
		"changes":{"nodes":[{"field":{"name":"Status"},"previousValue":{"name":"Todo"},"newValue":{"name":"In Progress"},"createdAt":"2026-10-02T00:00:00Z"}]}}`), &payload); err != nil {
		t.Fatal(err)
	}
	row, transitions, kept, err := normalizeGitHubProjectV2Item(claim, payload, "ghprojv2:acme/1", nil, chaos14170Now)
	if err != nil || !kept {
		t.Fatalf("kept=%v err=%v", kept, err)
	}
	merged := mergeGitHubProjectV2Rows(repository, githubWorkItemRows{
		WorkItems: []githubWorkItemRow{row}, StatusTransitions: transitions,
	})
	if len(merged.WorkItems) != 1 {
		t.Fatalf("work items=%d", len(merged.WorkItems))
	}
	steps := make([]chaos14170Step, 0, len(merged.StatusTransitions))
	for _, tr := range merged.StatusTransitions {
		if tr.WorkItemID == merged.WorkItems[0].WorkItemID {
			steps = append(steps, chaos14170Step{tr.OccurredAt, tr.ToStatus})
		}
	}
	chaos14170CheckLastMatches(t, merged.WorkItems[0].Status, steps)
}

// Refuting cells: these paths store the reopen today.

func TestChaos14170RefuteGitHubIssueCloseThenReopen(t *testing.T) {
	status, steps := chaos14170GitHubIssue(t, chaos14170GitHubOpenIssue,
		`{"event":"closed","created_at":"2026-10-02T00:00:00Z"}`,
		`{"event":"reopened","created_at":"2026-10-03T00:00:00Z"}`,
	)
	chaos14170CheckLastMatches(t, status, steps)
}

func TestChaos14170RefuteGitHubPullRequestCloseThenReopen(t *testing.T) {
	rows, err := normalizeGitHubPullRequestBundle(
		githubWorkItemOracleClaim(), sameGitHubRepoNames("acme/api"), uuid.MustParse("c7198fbc-1945-3717-05d8-eb78866b4e79"),
		json.RawMessage(`{"number":8,"title":"Change","state":"open","created_at":"2026-10-01T00:00:00Z","labels":[]}`),
		[]json.RawMessage{
			json.RawMessage(`{"event":"closed","created_at":"2026-10-02T00:00:00Z"}`),
			json.RawMessage(`{"event":"reopened","created_at":"2026-10-03T00:00:00Z"}`),
		}, nil, nil, nil, chaos14170Now,
	)
	if err != nil || len(rows.WorkItems) != 1 {
		t.Fatalf("rows=%d err=%v", len(rows.WorkItems), err)
	}
	steps := []chaos14170Step{}
	for _, tr := range rows.StatusTransitions {
		steps = append(steps, chaos14170Step{tr.OccurredAt, tr.ToStatus})
	}
	chaos14170CheckLastMatches(t, rows.WorkItems[0].Status, steps)
}

func TestChaos14170RefuteGitHubBoardDoneThenInProgress(t *testing.T) {
	var payload gitHubProjectV2ItemPayload
	if err := json.Unmarshal([]byte(`{"id":"PVTI_2","createdAt":"2026-10-01T00:00:00Z",
		"content":{"__typename":"DraftIssue","id":"DI_1","title":"Draft",
		"createdAt":"2026-10-01T00:00:00Z","updatedAt":"2026-10-03T00:00:00Z"},
		"fieldValues":{"nodes":[{"__typename":"ProjectV2ItemFieldSingleSelectValue","name":"In Progress","field":{"name":"Status"}}]},
		"changes":{"nodes":[
			{"field":{"name":"Status"},"previousValue":{"name":"In Progress"},"newValue":{"name":"Done"},"createdAt":"2026-10-02T00:00:00Z"},
			{"field":{"name":"Status"},"previousValue":{"name":"Done"},"newValue":{"name":"In Progress"},"createdAt":"2026-10-03T00:00:00Z"}]}}`), &payload); err != nil {
		t.Fatal(err)
	}
	row, transitions, kept, err := normalizeGitHubProjectV2Item(nativeTestClaim("github", "work-items"), payload, "ghprojv2:acme/1", nil, chaos14170Now)
	if err != nil || !kept {
		t.Fatalf("kept=%v err=%v", kept, err)
	}
	steps := []chaos14170Step{}
	for _, tr := range transitions {
		steps = append(steps, chaos14170Step{tr.OccurredAt, tr.ToStatus})
	}
	chaos14170CheckLastMatches(t, row.Status, steps)
}

func TestChaos14170RefuteGitLabStateEventsCloseThenReopen(t *testing.T) {
	var events []gitlabStateEventPayload
	if err := json.Unmarshal([]byte(`[
		{"state":"closed","created_at":"2026-10-02T00:00:00Z"},
		{"state":"reopened","created_at":"2026-10-03T00:00:00Z"}]`), &events); err != nil {
		t.Fatal(err)
	}
	claim := nativeTestClaim("gitlab", "work-items")
	t.Run("merge_request", func(t *testing.T) {
		transitions, _ := normalizeGitLabMRStateEvents(claim, "gitlab:acme/api!8", events, chaos14170Now, nil, chaos14170Now)
		steps := []chaos14170Step{}
		for _, tr := range transitions {
			steps = append(steps, chaos14170Step{tr.OccurredAt, tr.ToStatus})
		}
		chaos14170CheckLastMatches(t, "in_progress", steps)
	})
	t.Run("epic", func(t *testing.T) {
		transitions := normalizeGitLabEpicStateEvents(claim, "gitlab:acme:epic:3", events, chaos14170Now, nil, chaos14170Now)
		steps := []chaos14170Step{}
		for _, tr := range transitions {
			steps = append(steps, chaos14170Step{tr.OccurredAt, tr.ToStatus})
		}
		chaos14170CheckLastMatches(t, "todo", steps)
	})
}

func TestChaos14170RefuteLinearCompletedThenStarted(t *testing.T) {
	var payload linearWorkItemPayload
	if err := json.Unmarshal([]byte(`{"id":"l1","identifier":"ENG-1","title":"T",
		"createdAt":"2026-10-01T00:00:00Z","updatedAt":"2026-10-03T00:00:00Z",
		"state":{"name":"In Progress","type":"started"},
		"history":{"nodes":[
			{"id":"h1","createdAt":"2026-10-02T00:00:00Z","fromState":{"name":"In Progress","type":"started"},"toState":{"name":"Done","type":"completed"}},
			{"id":"h2","createdAt":"2026-10-03T00:00:00Z","fromState":{"name":"Done","type":"completed"},"toState":{"name":"In Progress","type":"started"}}],
			"pageInfo":{"hasNextPage":false,"endCursor":""}}}`), &payload); err != nil {
		t.Fatal(err)
	}
	row, transitions, err := normalizeLinearWorkItem(nativeTestClaim("linear", "work-items"), payload, chaos14170Now)
	if err != nil {
		t.Fatal(err)
	}
	steps := []chaos14170Step{}
	for _, tr := range transitions {
		steps = append(steps, chaos14170Step{tr.OccurredAt, tr.ToStatus})
	}
	chaos14170CheckLastMatches(t, row.Status, steps)
}

func TestChaos14170RefuteJiraDoneThenInProgress(t *testing.T) {
	claim := nativeTestClaim("jira", "work-items")
	raw := map[string]any{
		"key": "OPS-7", "id": "10007", "self": "https://jira.example/rest/api/3/issue/OPS-7",
		"fields": map[string]any{
			"project":   map[string]any{"key": "OPS", "id": "10001", "name": "Operations"},
			"summary":   "Reopened",
			"status":    map[string]any{"name": "In Progress", "statusCategory": map[string]any{"key": "indeterminate"}},
			"issuetype": map[string]any{"name": "Bug"},
			"labels":    []any{},
			"created":   "2026-10-01T00:00:00.000+0000",
			"updated":   "2026-10-03T00:00:00.000+0000",
		},
		"changelog": map[string]any{"histories": []any{
			map[string]any{"id": "1", "created": "2026-10-02T00:00:00.000+0000",
				"items": []any{map[string]any{"field": "status", "fromString": "In Progress", "toString": "Done"}}},
			map[string]any{"id": "2", "created": "2026-10-03T00:00:00.000+0000",
				"items": []any{map[string]any{"field": "status", "fromString": "Done", "toString": "In Progress"}}},
		}},
	}
	row, transitions, err := normalizeJiraWorkItem(claim, jiraWorkItemFixtureInput{Raw: raw, AtlassianShape: true},
		loadRealStatusMapping(t), jiraOracleIdentity, chaos14170Now)
	if err != nil {
		t.Fatal(err)
	}
	steps := []chaos14170Step{}
	for _, tr := range transitions {
		steps = append(steps, chaos14170Step{tr.OccurredAt, tr.ToStatus})
	}
	chaos14170CheckLastMatches(t, row.Status, steps)
}
