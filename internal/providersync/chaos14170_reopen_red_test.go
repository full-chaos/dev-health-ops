package providersync

import (
	"context"
	"encoding/json"
	"sort"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
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

// GitHub issue on a Projects v2 board. The board half is the production
// fetch: the board's one event is its current Status value at updatedAt
// (GitHub serves no past board moves). The status is the last event of the
// merged history. A repository close with no board move must not leave an open
// board status after a "done" transition.
func TestChaos14170RedGitHubBoardStatusOpenAfterRepositoryClose(t *testing.T) {
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
	board := chaos14170BoardFetch(t, `{"id":"PVTI_1","createdAt":"2026-10-01T00:00:00Z",
		"content":{"__typename":"Issue","id":"I_1","number":9,"title":"T","state":"CLOSED",
		"createdAt":"2026-10-01T00:00:00Z","updatedAt":"2026-10-03T00:00:00Z","closedAt":"2026-10-03T00:00:00Z",
		"repository":{"nameWithOwner":"acme/api"},"labels":{"nodes":[],"pageInfo":{"hasNextPage":false,"endCursor":null}},"assignees":{"nodes":[]}},
		"fieldValues":{"nodes":[{"__typename":"ProjectV2ItemFieldSingleSelectValue","name":"In Progress","updatedAt":"2026-10-02T00:00:00Z","field":{"name":"Status"}}],
		"pageInfo":{"hasNextPage":false,"endCursor":null}}}`)
	merged := chaos14170RouteBoardMerge(t, repository, board, nil)
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

// chaos14170BoardFetch runs the production Projects v2 fetch over one board
// item.
func chaos14170BoardFetch(t *testing.T, item string) githubWorkItemRows {
	t.Helper()
	doer := &gitHubProjectV2Doer{t: t, replies: []string{
		`{"data":{"organization":{"projectV2":{"items":{"nodes":[` + item + `],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}}}`,
	}}
	claim := githubWorkItemOracleClaim()
	claim.IntegrationConfig = map[string]any{"github_projects_v2": []any{map[string]any{"org_login": "acme", "project_number": 1}}}
	result, err := (GitHubProjectV2Fetcher{}).Fetch(
		context.Background(), claim, providerfoundation.Credential{Provider: "github", ID: claim.CredentialID},
		githubProjectV2TestClient(t, fakehttp.Client(doer)), chaos14170Now, nil,
	)
	if err != nil || len(result.Rows.WorkItems) != 1 {
		t.Fatalf("board rows=%d err=%v", len(result.Rows.WorkItems), err)
	}
	return result.Rows
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

// A draft on a board moved to Done, then back to In Progress. GitHub serves
// only the current Status value, so each sync stores the value it sees at its
// updatedAt: sync 1 sees Done, sync 2 sees In Progress. The stored history ends
// in the item's status.
func TestChaos14170RefuteGitHubBoardDoneThenInProgress(t *testing.T) {
	draft := func(status, at string) string {
		return `{"id":"PVTI_2","createdAt":"2026-10-01T00:00:00Z",
		"content":{"__typename":"DraftIssue","id":"DI_1","title":"Draft",
		"createdAt":"2026-10-01T00:00:00Z","updatedAt":"` + at + `"},
		"fieldValues":{"nodes":[{"__typename":"ProjectV2ItemFieldSingleSelectValue","name":"` + status + `","updatedAt":"` + at + `","field":{"name":"Status"}}],
		"pageInfo":{"hasNextPage":false,"endCursor":null}}}`
	}
	empty := githubWorkItemRows{WorkItems: []githubWorkItemRow{}, StatusTransitions: []githubWorkItemTransitionRow{}}
	first := chaos14170RouteBoardMerge(t, empty, chaos14170BoardFetch(t, draft("Done", "2026-10-02T00:00:00Z")), nil)
	stored := first.StatusTransitions
	second := chaos14170RouteBoardMerge(t, empty, chaos14170BoardFetch(t, draft("In Progress", "2026-10-03T00:00:00Z")), stored)
	steps := []chaos14170Step{}
	for _, tr := range append(append([]githubWorkItemTransitionRow{}, stored...), second.StatusTransitions...) {
		steps = append(steps, chaos14170Step{tr.OccurredAt, tr.ToStatus})
	}
	if len(steps) != 2 {
		t.Fatalf("stored board events=%v, want Done then In Progress", steps)
	}
	chaos14170CheckLastMatches(t, second.WorkItems[0].Status, steps)
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

func TestChaos14170GitLabIssueStateEventsKeepLifecycle(t *testing.T) {
	var payload gitlabIssueWorkItemPayload
	if err := json.Unmarshal([]byte(`{"iid":42,"title":"Reopened","state":"opened",
		"created_at":"2026-10-01T00:00:00Z","updated_at":"2026-10-04T00:00:00Z","labels":[]}`), &payload); err != nil {
		t.Fatal(err)
	}
	var states []gitlabStateEventPayload
	if err := json.Unmarshal([]byte(`[
		{"state":"closed","created_at":"2026-10-02T00:00:00Z"},
		{"state":"reopened","created_at":"2026-10-03T00:00:00Z"}]`), &states); err != nil {
		t.Fatal(err)
	}
	row, transitions, err := normalizeGitLabIssueWorkItem(
		nativeTestClaim("gitlab", "work-items"), "acme/api", "acme/api", "org:gitlab:1",
		uuid.MustParse("0b7e3d4e-1111-4222-8333-944455556666"), payload, nil, states,
		loadRealStatusMapping(t), nil, chaos14170Now,
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(transitions) != 2 || row.CompletedAt != nil || row.StartedAt != nil {
		t.Fatalf("a reopened issue keeps no completed_at from a state close; rows=%d completed=%v started=%v",
			len(transitions), row.CompletedAt, row.StartedAt)
	}
}

var chaos14170GitHubStatusLabels = []string{"in progress", "doing", "wip", "in review", "needs review", "blocked", "todo", "to do", "ready", "done", "wontfix", "duplicate"}
var chaos14170GitLabStatusLabels = []string{"in progress", "doing", "wip", "in review", "review", "blocked", "todo", "to do", "ready", "done", "wontfix", "duplicate"}

func chaos14170RawCount(t *testing.T, raws []*string, prefix string) int {
	t.Helper()
	count := 0
	for _, raw := range raws {
		if raw != nil && len(*raw) >= len(prefix) && (*raw)[:len(prefix)] == prefix {
			count++
		}
	}
	return count
}

// The removal compares the folded status before and after the event, not the
// last stored row: a close row says done while a status label still wins.
func TestChaos14170GitHubCloseThenStatusLabelRemoved(t *testing.T) {
	closed := `{"number":7,"title":"T","state":"closed","closed_at":"2026-10-03T00:00:00Z",
		"created_at":"2026-10-01T00:00:00Z","updated_at":"2026-10-04T00:00:00Z","labels":[],"assignees":[]}`
	for _, label := range chaos14170GitHubStatusLabels {
		t.Run(label, func(t *testing.T) {
			rows, err := normalizeGitHubIssueBundle(
				githubWorkItemOracleClaim(), sameGitHubRepoNames("acme/api"),
				uuid.MustParse("c7198fbc-1945-3717-05d8-eb78866b4e79"), json.RawMessage(closed),
				[]json.RawMessage{
					json.RawMessage(`{"event":"labeled","created_at":"2026-10-02T00:00:00Z","label":{"name":"` + label + `"}}`),
					json.RawMessage(`{"event":"closed","created_at":"2026-10-03T00:00:00Z"}`),
					json.RawMessage(`{"event":"unlabeled","created_at":"2026-10-04T00:00:00Z","label":{"name":"` + label + `"}}`),
				}, nil, nil, chaos14170Now,
			)
			if err != nil || len(rows.WorkItems) != 1 {
				t.Fatalf("rows=%d err=%v", len(rows.WorkItems), err)
			}
			raws, steps := []*string{}, []chaos14170Step{}
			for _, tr := range rows.StatusTransitions {
				raws = append(raws, tr.ToStatusRaw)
				steps = append(steps, chaos14170Step{tr.OccurredAt, tr.ToStatus})
			}
			want := 1
			if githubIssueStatus("", []string{label}) == "done" {
				want = 0
			}
			if got := chaos14170RawCount(t, raws, "unlabeled:"); got != want {
				t.Fatalf("unlabeled rows=%d want %d; steps=%v", got, want, steps)
			}
			chaos14170CheckLastMatches(t, rows.WorkItems[0].Status, steps)
		})
	}
}

// Two status labels: removing the winning one stores a row, removing the
// losing one or a state change the labels outrank stores none.
func TestChaos14170StatusLabelPairsFoldByPriority(t *testing.T) {
	type pair struct{ high, low string }
	pairs := []pair{}
	order := []string{"done", "wontfix", "blocked", "in review", "in progress", "todo"}
	for i := range order {
		for j := i + 1; j < len(order); j++ {
			pairs = append(pairs, pair{order[i], order[j]})
		}
	}
	for _, p := range pairs {
		t.Run(p.high+">"+p.low, func(t *testing.T) {
			for _, removed := range []string{p.high, p.low} {
				rows, err := normalizeGitHubIssueBundle(
					githubWorkItemOracleClaim(), sameGitHubRepoNames("acme/api"),
					uuid.MustParse("c7198fbc-1945-3717-05d8-eb78866b4e79"), json.RawMessage(chaos14170GitHubOpenIssue),
					[]json.RawMessage{
						json.RawMessage(`{"event":"labeled","created_at":"2026-10-02T00:00:00Z","label":{"name":"` + p.high + `"}}`),
						json.RawMessage(`{"event":"labeled","created_at":"2026-10-02T01:00:00Z","label":{"name":"` + p.low + `"}}`),
						json.RawMessage(`{"event":"unlabeled","created_at":"2026-10-03T00:00:00Z","label":{"name":"` + removed + `"}}`),
					}, nil, nil, chaos14170Now,
				)
				if err != nil {
					t.Fatal(err)
				}
				raws := []*string{}
				var last githubWorkItemTransitionRow
				for _, tr := range rows.StatusTransitions {
					raws = append(raws, tr.ToStatusRaw)
					last = tr
				}
				wantRows := 0
				if removed == p.high {
					wantRows = 1
				}
				if got := chaos14170RawCount(t, raws, "unlabeled:"); got != wantRows {
					t.Fatalf("github remove %q: unlabeled rows=%d want %d", removed, got, wantRows)
				}
				if wantRows == 1 && last.ToStatus != githubIssueStatus("open", []string{p.low}) {
					t.Fatalf("github remove %q: last to=%q", removed, last.ToStatus)
				}
			}
			mapping := loadRealStatusMapping(t)
			for _, removed := range []string{p.high, p.low, ""} {
				events := `[{"action":"add","created_at":"2026-10-02T00:00:00Z","label":{"name":"` + p.high + `"}},
					{"action":"add","created_at":"2026-10-02T01:00:00Z","label":{"name":"` + p.low + `"}}`
				states := `[]`
				if removed == "" {
					states = `[{"state":"closed","created_at":"2026-10-03T00:00:00Z"}]`
				} else {
					events += `,{"action":"remove","created_at":"2026-10-03T00:00:00Z","label":{"name":"` + removed + `"}}`
				}
				_, steps := chaos14170GitLabIssue(t, "opened", `[]`, events+`]`, states)
				wantRows := 2
				if removed == p.high {
					wantRows = 3
				}
				if len(steps) != wantRows {
					t.Fatalf("gitlab remove %q: rows=%d want %d; steps=%v", removed, len(steps), wantRows, steps)
				}
				if wantRows == 3 && chaos14170Last(t, steps).to != mapping.NormalizeStatus("gitlab", "", []string{p.low}, "opened") {
					t.Fatalf("gitlab remove %q: steps=%v", removed, steps)
				}
			}
		})
	}
}

// Without state history the fold reads the issue's current state.
func TestChaos14170GitLabLabelRemovalWithoutStateHistory(t *testing.T) {
	for _, label := range chaos14170GitLabStatusLabels {
		t.Run(label, func(t *testing.T) {
			var payload gitlabIssueWorkItemPayload
			if err := json.Unmarshal([]byte(`{"iid":42,"title":"Closed","state":"closed","closed_at":"2026-10-04T00:00:00Z",
				"created_at":"2026-10-01T00:00:00Z","updated_at":"2026-10-04T00:00:00Z","labels":[]}`), &payload); err != nil {
				t.Fatal(err)
			}
			var events []gitlabLabelEventPayload
			if err := json.Unmarshal([]byte(`[
				{"action":"add","created_at":"2026-10-02T00:00:00Z","label":{"name":"`+label+`"}},
				{"action":"remove","created_at":"2026-10-03T00:00:00Z","label":{"name":"`+label+`"}}]`), &events); err != nil {
				t.Fatal(err)
			}
			row, transitions, err := normalizeGitLabIssueWorkItem(
				nativeTestClaim("gitlab", "work-items"), "acme/api", "acme/api", "org:gitlab:1",
				uuid.MustParse("0b7e3d4e-1111-4222-8333-944455556666"), payload, events, nil,
				loadRealStatusMapping(t), nil, chaos14170Now,
			)
			if err != nil {
				t.Fatal(err)
			}
			steps := []chaos14170Step{}
			for _, tr := range transitions {
				steps = append(steps, chaos14170Step{tr.OccurredAt, tr.ToStatus})
			}
			chaos14170CheckLastMatches(t, row.Status, steps)
		})
	}
}
