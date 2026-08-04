package providersync

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestNormalizeGitHubIssueBundleMatchesPythonEventsCommentsAndDependencies(t *testing.T) {
	t.Parallel()
	claim := githubWorkItemOracleClaim()
	normalizedAt := time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC)
	repoID := uuid.MustParse("c7198fbc-1945-3717-05d8-eb78866b4e79")
	issue := json.RawMessage(`{
	  "number":42,"title":"Repair delivery path",
	  "body":"Blocked by #7\nCloses other/repo#9\nSee CHAOS-42",
	  "state":"closed","created_at":"2026-08-01T08:00:00Z",
	  "updated_at":"2026-08-03T09:30:00Z","closed_at":"2026-08-03T09:30:00Z",
	  "labels":[{"name":"doing"}],"assignees":[],"user":{"login":"reporter"}
	}`)
	events := []json.RawMessage{
		json.RawMessage(`{"event":"reopened","created_at":"2026-08-03T08:00:00Z","actor":{"login":"maintainer"}}`),
		json.RawMessage(`{"event":"closed","created_at":"2026-08-02T08:00:00Z"}`),
		json.RawMessage(`{"event":"labeled","created_at":"2026-08-01T10:00:00Z","label":{"name":"doing"}}`),
	}
	comments := []json.RawMessage{
		json.RawMessage(`{"id":99,"body":"Looks good 👋","created_at":"2026-08-03T08:30:00Z","user":{"login":"reviewer"}}`),
		json.RawMessage(`{"id":0,"body":"missing id","created_at":"2026-08-03T08:31:00Z"}`),
	}

	rows, err := normalizeGitHubIssueBundle(
		claim, "acme/api", repoID, issue, events, comments, nil, normalizedAt,
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows.WorkItems) != 1 || len(rows.StatusTransitions) != 3 ||
		len(rows.ReopenEvents) != 1 || len(rows.Interactions) != 1 ||
		len(rows.Dependencies) != 3 {
		t.Fatalf("rows=%+v", rows)
	}
	item := rows.WorkItems[0]
	if item.StartedAt == nil || !item.StartedAt.Equal(time.Date(2026, 8, 1, 10, 0, 0, 0, time.UTC)) ||
		item.CompletedAt == nil || !item.CompletedAt.Equal(time.Date(2026, 8, 2, 8, 0, 0, 0, time.UTC)) {
		t.Fatalf("item transition timestamps=%+v", item)
	}
	gotTransitions := make([]string, 0, len(rows.StatusTransitions))
	for _, transition := range rows.StatusTransitions {
		gotTransitions = append(gotTransitions, transition.FromStatus+"->"+transition.ToStatus)
		if transition.OrgID != claim.OrgID {
			t.Fatalf("transition tenant=%+v", transition)
		}
	}
	if want := []string{"unknown->in_progress", "in_progress->done", "done->todo"}; !reflect.DeepEqual(gotTransitions, want) {
		t.Fatalf("transitions=%v want=%v", gotTransitions, want)
	}
	if rows.ReopenEvents[0].Actor == nil || *rows.ReopenEvents[0].Actor != "github:maintainer" ||
		!rows.ReopenEvents[0].LastSynced.Equal(normalizedAt) || rows.ReopenEvents[0].OrgID != claim.OrgID {
		t.Fatalf("reopen=%+v", rows.ReopenEvents[0])
	}
	if rows.Interactions[0].BodyLength != 12 || rows.Interactions[0].Actor == nil ||
		*rows.Interactions[0].Actor != "github:reviewer" || rows.Interactions[0].OrgID != claim.OrgID {
		t.Fatalf("interaction=%+v", rows.Interactions[0])
	}
	gotDependencies := make([]string, 0, len(rows.Dependencies))
	for _, dependency := range rows.Dependencies {
		gotDependencies = append(gotDependencies,
			dependency.SourceWorkItemID+"|"+dependency.TargetWorkItemID+"|"+dependency.RelationshipType,
		)
		if dependency.OrgID != claim.OrgID || !dependency.LastSynced.Equal(normalizedAt) ||
			dependency.RelationshipSemanticsVersion != "canonical-blocks.v2" {
			t.Fatalf("dependency=%+v", dependency)
		}
	}
	wantDependencies := []string{
		"gh:acme/api#7|gh:acme/api#42|blocks",
		"gh:acme/api#42|gh:other/repo#9|relates_to",
		"gh:acme/api#42|extkey:CHAOS-42|relates_to",
	}
	if !reflect.DeepEqual(gotDependencies, wantDependencies) {
		t.Fatalf("dependencies=%v want=%v", gotDependencies, wantDependencies)
	}
}

func TestNormalizeGitHubPullRequestBundlePreservesPythonPRSemantics(t *testing.T) {
	t.Parallel()
	claim := githubWorkItemOracleClaim()
	claim.OrgID = "77777777-7777-4777-8777-777777777777"
	normalizedAt := time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC)
	repoID := uuid.MustParse("c7198fbc-1945-3717-05d8-eb78866b4e79")
	pr := json.RawMessage(`{
	  "number":17,"title":"Codex generated repair","body":"AI-Assisted-By: Codex\nCloses CHAOS-17",
	  "state":"closed","merged":true,"draft":false,
	  "created_at":"2026-08-01T08:00:00Z","updated_at":"2026-08-03T09:30:00Z",
	  "closed_at":"2026-08-03T09:30:00Z","merged_at":"2026-08-03T09:29:00Z",
	  "labels":[{"name":"codex"},{"name":"p1"}],"assignees":[],
	  "user":{"login":"chatgpt-codex[bot]","type":"Bot"},
	  "head":{"ref":"codex/chaos-17-repair"},"html_url":"https://github.com/acme/api/pull/17"
	}`)
	events := []json.RawMessage{
		json.RawMessage(`{"event":"closed","created_at":"2026-08-03T09:30:00Z"}`),
		json.RawMessage(`{"event":"merged","created_at":"2026-08-03T09:29:00Z"}`),
		json.RawMessage(`{"event":"reopened","created_at":"2026-08-02T09:00:00Z","actor":{"login":"maintainer"}}`),
	}
	comments := []json.RawMessage{
		json.RawMessage(`{"id":101,"body":"Linked: https://linear.app/fullchaos/issue/CHAOS-99/task","created_at":"2026-08-03T08:00:00Z","user":{"login":"linear[bot]"}}`),
		json.RawMessage(`{"id":102,"body":"https://linear.app/fullchaos/issue/CHAOS-100/task","created_at":"2026-08-03T08:01:00Z","user":{"login":"other[bot]"}}`),
	}

	rows, err := normalizeGitHubPullRequestBundle(
		claim, "acme/api", repoID, pr, events, comments, nil, normalizedAt,
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows.WorkItems) != 1 || len(rows.StatusTransitions) != 3 ||
		len(rows.ReopenEvents) != 1 || len(rows.Interactions) != 2 ||
		len(rows.Dependencies) != 2 || len(rows.AIAttributions) != 4 {
		t.Fatalf("rows=%+v", rows)
	}
	item := rows.WorkItems[0]
	if item.WorkItemID != "ghpr:acme/api#17" || item.Type != "pr" || item.Status != "done" ||
		item.StatusRaw == nil || *item.StatusRaw != "merged" || item.StartedAt == nil ||
		!item.StartedAt.Equal(item.CreatedAt) || item.CompletedAt == nil ||
		!item.CompletedAt.Equal(time.Date(2026, 8, 3, 9, 29, 0, 0, time.UTC)) {
		t.Fatalf("item=%+v", item)
	}
	gotTransitions := make([]string, 0, len(rows.StatusTransitions))
	for _, transition := range rows.StatusTransitions {
		gotTransitions = append(gotTransitions, transition.FromStatus+"->"+transition.ToStatus)
	}
	if want := []string{"in_progress->in_progress", "in_progress->done", "done->done"}; !reflect.DeepEqual(gotTransitions, want) {
		t.Fatalf("transitions=%v want=%v", gotTransitions, want)
	}
	gotTargets := make(map[string]string, len(rows.Dependencies))
	for _, dependency := range rows.Dependencies {
		gotTargets[dependency.TargetWorkItemID] = dependency.RelationshipType
	}
	if gotTargets["extkey:CHAOS-17"] != "relates_to" ||
		gotTargets["extkey:CHAOS-99"] != "relates_to" || len(gotTargets) != 2 {
		t.Fatalf("dependency targets=%v", gotTargets)
	}
	wantSources := []string{"pr_label", "bot_author", "commit_trailer", "branch_name"}
	gotSources := make([]string, 0, len(rows.AIAttributions))
	for _, attribution := range rows.AIAttributions {
		gotSources = append(gotSources, attribution.Source)
		if attribution.SubjectID != "17" || attribution.RepoID == nil || *attribution.RepoID != repoID ||
			attribution.OrgID != uuid.MustParse(claim.OrgID) || attribution.RecordID == uuid.Nil ||
			!attribution.IngestedAt.Equal(normalizedAt) {
			t.Fatalf("attribution=%+v", attribution)
		}
	}
	if !reflect.DeepEqual(gotSources, wantSources) {
		t.Fatalf("sources=%v want=%v", gotSources, wantSources)
	}
	second, err := normalizeGitHubPullRequestBundle(
		claim, "acme/api", repoID, pr, events, comments, nil, normalizedAt,
	)
	if err != nil {
		t.Fatal(err)
	}
	for index := range rows.AIAttributions {
		if rows.AIAttributions[index].RecordID != second.AIAttributions[index].RecordID {
			t.Fatalf("record id changed across retry: %s != %s", rows.AIAttributions[index].RecordID, second.AIAttributions[index].RecordID)
		}
	}
}

func TestNormalizeGitHubPullRequestStatusPartitionsMatchPython(t *testing.T) {
	t.Parallel()
	claim := githubWorkItemOracleClaim()
	claim.OrgID = "77777777-7777-4777-8777-777777777777"
	normalizedAt := time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC)
	repoID := uuid.MustParse("c7198fbc-1945-3717-05d8-eb78866b4e79")
	tests := []struct {
		name          string
		extra         string
		wantStatus    string
		wantRaw       string
		wantCompleted bool
	}{
		{name: "merged boolean without timestamp", extra: `"state":"closed","merged":true`, wantStatus: "done", wantRaw: "merged"},
		{name: "merged timestamp without boolean", extra: `"state":"closed","merged":false,"merged_at":"2026-08-03T09:29:00Z"`, wantStatus: "done", wantRaw: "merged", wantCompleted: true},
		{name: "closed unmerged", extra: `"state":"closed","merged":false,"closed_at":"2026-08-03T09:30:00Z"`, wantStatus: "canceled", wantRaw: "closed", wantCompleted: true},
		{name: "open draft", extra: `"state":"open","draft":true`, wantStatus: "todo", wantRaw: "open"},
		{name: "open ready", extra: `"state":"open","draft":false`, wantStatus: "in_progress", wantRaw: "open"},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			raw := json.RawMessage(`{"number":17,"title":"Partition","created_at":"2026-08-01T08:00:00Z",` + test.extra + `}`)
			rows, err := normalizeGitHubPullRequestBundle(
				claim, "acme/api", repoID, raw, nil, nil, nil, normalizedAt,
			)
			if err != nil {
				t.Fatal(err)
			}
			item := rows.WorkItems[0]
			if item.Status != test.wantStatus || item.StatusRaw == nil || *item.StatusRaw != test.wantRaw {
				t.Fatalf("status=%q raw=%v want status=%q raw=%q", item.Status, item.StatusRaw, test.wantStatus, test.wantRaw)
			}
			if (item.CompletedAt != nil) != test.wantCompleted {
				t.Fatalf("completed_at=%v want_present=%v", item.CompletedAt, test.wantCompleted)
			}
		})
	}
}
