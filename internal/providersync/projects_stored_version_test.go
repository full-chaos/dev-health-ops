package providersync

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
)

func utcDay(year int, month time.Month, day int) *time.Time {
	value := time.Date(year, month, day, 0, 0, 0, 0, time.UTC)
	return &value
}

func sameDay(left, right *time.Time) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return left.Equal(*right)
}

var targetFetchedAt = time.Date(2026, 10, 14, 12, 0, 0, 0, time.UTC)

func TestPickProjectTargetIsTheEarliestOpenDatedCandidate(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name       string
		candidates []projectTargetCandidate
		want       *time.Time
		wantRef    string
		wantName   string
		wantOthers uint32
	}{
		{"an overdue open candidate stays the target over a later one", []projectTargetCandidate{
			{Ref: "a", Name: "2.0", Date: "2026-12-15", Open: true}, {Ref: "b", Name: "1.9", Date: "2026-09-30", Open: true},
		}, utcDay(2026, 9, 30), "b", "1.9", 1},
		{"closed candidates are not targets", []projectTargetCandidate{
			{Ref: "a", Date: "2026-08-01"}, {Ref: "b", Name: "open", Date: "2026-12-15", Open: true},
		}, utcDay(2026, 12, 15), "b", "open", 0},
		{"an undated candidate is no target and no other", []projectTargetCandidate{
			{Ref: "a", Open: true}, {Ref: "b", Date: "", Open: true}, {Ref: "c", Date: "soon", Open: true},
			{Ref: "d", Name: "dated", Date: "2026-11-01", Open: true},
		}, utcDay(2026, 11, 1), "d", "dated", 0},
		{"equal dates pick the lower id as a number, not as text", []projectTargetCandidate{
			{Ref: "10", Date: "2026-11-01", Open: true}, {Ref: "9", Date: "2026-11-01", Open: true},
		}, utcDay(2026, 11, 1), "9", "", 1},
		{"equal dates pick the same one in either listing order", []projectTargetCandidate{
			{Ref: "9", Date: "2026-11-01", Open: true}, {Ref: "10", Date: "2026-11-01", Open: true},
		}, utcDay(2026, 11, 1), "9", "", 1},
		{"a date-time value is its UTC day", []projectTargetCandidate{
			{Ref: "a", Date: "2026-10-31T23:30:00-05:00", Open: true},
		}, utcDay(2026, 11, 1), "a", "", 0},
		{"every candidate closed leaves no target", []projectTargetCandidate{{Ref: "a", Date: "2026-08-01"}}, nil, "", "", 0},
		{"no candidate", nil, nil, "", "", 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			candidates := append([]projectTargetCandidate(nil), tc.candidates...)
			for index := range candidates {
				candidates[index].ID = candidates[index].Ref
			}
			got, facts := pickProjectTarget("k", "x.y.z", targetFetchedAt, candidates)
			if !sameDay(got, tc.want) {
				t.Fatalf("target=%v, want %v", got, tc.want)
			}
			if tc.want == nil {
				if facts.TargetState != projectTargetNoneDate || facts.TargetKind != "" || facts.TargetSource != "" || facts.TargetRef != "" ||
					facts.TargetRefName != "" || facts.TargetOtherOpen != 0 || facts.TargetFetchedAt == nil || !facts.TargetFetchedAt.Equal(targetFetchedAt) {
					t.Fatalf("facts=%+v, want none_dated with only the fetch time", facts)
				}
				return
			}
			want := projectTargetFacts{
				TargetState: projectTargetStated, TargetKind: "k", TargetSource: "x.y.z", TargetRef: tc.wantRef, TargetRefName: tc.wantName,
				TargetOtherOpen: tc.wantOthers, TargetFetchedAt: &targetFetchedAt,
			}
			if !reflect.DeepEqual(facts, want) {
				t.Fatalf("facts=%+v, want %+v", facts, want)
			}
		})
	}
}

// collectJiraProjectTargets runs the catalog walk over one project search and
// one project body per key; a key with no body answers 500.
func collectJiraProjectTargets(t *testing.T, keys []string, bodies map[string]string) JiraTeamCatalogBatch {
	t.Helper()
	search := `{"values":[`
	byURI := map[string]jiraTeamCatalogFixtureResponse{}
	for index, key := range keys {
		if index > 0 {
			search += ","
		}
		search += `{"id":"` + string(rune('1'+index)) + `0001","key":"` + key + `","name":"Project ` + key + `"}`
		if body, ok := bodies[key]; ok {
			byURI["/rest/api/3/project/"+key] = jiraTeamCatalogFixtureResponse{body: body}
		} else {
			byURI["/rest/api/3/project/"+key] = jiraTeamCatalogFixtureResponse{status: http.StatusInternalServerError, body: `{}`}
		}
		byURI["/rest/agile/1.0/board?maxResults=100&projectKeyOrId="+key+"&startAt=0"] = jiraTeamCatalogFixtureResponse{body: `{"values":[],"isLast":true}`}
	}
	search += `],"isLast":true}`
	byURI[jiraTeamCatalogProjectSearchURI] = jiraTeamCatalogFixtureResponse{body: search}
	doer := &jiraTeamCatalogFixtureDoer{t: t, byURI: byURI}
	batch, err := JiraTeamCatalogRouteHandler{}.CollectTeamCatalog(context.Background(),
		TeamCatalogReference{OrgID: "org-1", SyncRunID: "run-1"}, providerfoundation.Credential{Provider: "jira"},
		jiraTeamCatalogTestClient(t, fakehttp.Client(doer)),
		TeamCatalogSelections{Projects: true}, targetFetchedAt)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	return batch
}

func jiraProjectRowByKey(t *testing.T, batch JiraTeamCatalogBatch, key string) jiraTeamCatalogProjectRow {
	t.Helper()
	for _, row := range batch.Rows.Projects {
		if row.ProjectKey != nil && *row.ProjectKey == key {
			return row
		}
	}
	t.Fatalf("no project row for %s in %+v", key, batch.Rows.Projects)
	return jiraTeamCatalogProjectRow{}
}

func TestJiraTeamCatalogStatesTheProjectTargetFromItsVersions(t *testing.T) {
	t.Parallel()
	batch := collectJiraProjectTargets(t, []string{"OPS", "DONE", "EMPTY", "NOLIST"}, map[string]string{
		"OPS": `{"projectTypeKey":"software","versions":[
			{"id":"101","name":"2.0","released":false,"archived":false,"releaseDate":"2026-12-15"},
			{"id":"102","name":"1.9","released":false,"archived":false,"releaseDate":"2026-09-30"},
			{"id":"103","name":"1.8","released":true,"archived":false,"releaseDate":"2026-08-01"},
			{"id":"104","name":"1.7","released":false,"archived":true,"releaseDate":"2026-07-01"},
			{"id":"105","name":"3.0","released":false,"archived":false}]}`,
		"DONE":   `{"projectTypeKey":"business","versions":[{"id":"1","released":true,"releaseDate":"2026-08-01"}]}`,
		"EMPTY":  `{"projectTypeKey":"business","versions":[]}`,
		"NOLIST": `{"projectTypeKey":"business"}`,
	})
	ops := jiraProjectRowByKey(t, batch, "OPS")
	want := projectTargetFacts{
		TargetState: projectTargetStated, TargetKind: "version", TargetSource: "jira.version.releaseDate", TargetRef: "102", TargetRefName: "1.9",
		TargetOtherOpen: 1, TargetFetchedAt: &targetFetchedAt,
	}
	if !sameDay(ops.TargetDate, utcDay(2026, 9, 30)) || !reflect.DeepEqual(ops.projectTargetFacts, want) {
		t.Fatalf("OPS target=%v facts=%+v, want the overdue open 2026-09-30 (version 102) with %+v", ops.TargetDate, ops.projectTargetFacts, want)
	}
	for _, key := range []string{"DONE", "EMPTY"} {
		row := jiraProjectRowByKey(t, batch, key)
		if row.TargetState != projectTargetNoneDate || row.TargetDate != nil {
			t.Fatalf("%s target=%v state=%q, want none_dated with no date (clears a held value)", key, row.TargetDate, row.TargetState)
		}
	}
	noList := jiraProjectRowByKey(t, batch, "NOLIST")
	if noList.read() || noList.TargetDate != nil {
		t.Fatalf("a body with no version list states nothing: target=%v facts=%+v", noList.TargetDate, noList.projectTargetFacts)
	}
}

func TestJiraTeamCatalogLeavesTheTargetUnreadForAProjectBodyThatFailed(t *testing.T) {
	t.Parallel()
	batch := collectJiraProjectTargets(t, []string{"A", "B"}, map[string]string{
		"A": `{"projectTypeKey":"business","versions":[{"id":"1","releaseDate":"2026-11-05"}]}`,
	})
	a := jiraProjectRowByKey(t, batch, "A")
	if !a.read() || !sameDay(a.TargetDate, utcDay(2026, 11, 5)) {
		t.Fatalf("A target=%v facts=%+v, want 2026-11-05 read: the walk read its body before B failed", a.TargetDate, a.projectTargetFacts)
	}
	if b := jiraProjectRowByKey(t, batch, "B"); b.read() || b.TargetDate != nil {
		t.Fatalf("B target=%v facts=%+v, want unread: its body was not read", b.TargetDate, b.projectTargetFacts)
	}
}

func TestArchivedJiraProjectRowStatesNoTarget(t *testing.T) {
	t.Parallel()
	row := normalizeJiraArchivedProjectRow("org-1", testPID("20001"), "OLD", "Old", time.Now())
	if row.read() || row.TargetDate != nil {
		t.Fatalf("archived row target=%v facts=%+v, want unread: no body of it is read", row.TargetDate, row.projectTargetFacts)
	}
}

// gitlabMilestoneFixture serves the catalog walk of one group with the given
// projects, and the open milestones of each project from milestones (a
// project absent from it answers 404).
func gitlabMilestoneFixture(t *testing.T, projects map[string]string, milestones map[string]string) (*httptest.Server, *[]string) {
	t.Helper()
	var requests []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests = append(requests, r.URL.RequestURI())
		w.Header().Set("Content-Type", "application/json")
		path := r.URL.EscapedPath()
		switch {
		case path == "/api/v4/groups/org":
			_, _ = w.Write([]byte(`{"id":1,"full_path":"org","name":"Org"}`))
		case path == "/api/v4/groups/org/subgroups":
			_, _ = w.Write([]byte(`[]`))
		case path == "/api/v4/groups/org/projects":
			entries := make([]string, 0, len(projects))
			for id, fullPath := range projects {
				archived := "false"
				if strings.HasSuffix(fullPath, "/archived") {
					archived = "true"
				}
				entries = append(entries, `{"id":`+id+`,"path_with_namespace":"`+fullPath+`","name":"n","archived":`+archived+`,"web_url":"u"}`)
			}
			_, _ = w.Write([]byte("[" + strings.Join(entries, ",") + "]"))
		case strings.HasPrefix(path, "/api/v4/projects/") && strings.HasSuffix(path, "/milestones"):
			id := strings.TrimSuffix(strings.TrimPrefix(path, "/api/v4/projects/"), "/milestones")
			body, ok := milestones[id]
			if !ok || r.URL.Query().Get("state") != "active" {
				http.NotFound(w, r)
				return
			}
			if r.URL.Query().Get("include_parent_milestones") != "true" {
				// GitLab lists the ancestor groups' milestones (they carry group_id)
				// only when asked.
				var all, own []json.RawMessage
				if json.Unmarshal([]byte(body), &all) == nil {
					for _, raw := range all {
						if !strings.Contains(string(raw), `"group_id"`) {
							own = append(own, raw)
						}
					}
					filtered, _ := json.Marshal(own)
					body = string(filtered)
					if own == nil {
						body = `[]`
					}
				}
			}
			_, _ = w.Write([]byte(body))
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(server.Close)
	return server, &requests
}

func collectGitLabProjectTargets(t *testing.T, projects, milestones map[string]string) (GitLabTeamCatalogBatch, []string) {
	t.Helper()
	server, requests := gitlabMilestoneFixture(t, projects, milestones)
	batch, err := (GitLabTeamCatalogRouteHandler{}).CollectTeamCatalog(context.Background(),
		TeamCatalogReference{OrgID: "org-1", SyncRunID: "run-1"},
		providerfoundation.Credential{Provider: "gitlab", Config: map[string]string{"group_path": "org"}},
		gitlabTeamCatalogTestClient(t, server.URL), TeamCatalogSelections{Projects: true}, targetFetchedAt)
	if err != nil {
		t.Fatalf("collect: %v", err)
	}
	return batch, *requests
}

func gitlabProjectRowByKey(t *testing.T, batch GitLabTeamCatalogBatch, path string) gitlabTeamCatalogProjectRow {
	t.Helper()
	for _, row := range batch.Rows.Projects {
		if row.ProjectKey != nil && *row.ProjectKey == path {
			return row
		}
	}
	t.Fatalf("no project row for %s in %+v", path, batch.Rows.Projects)
	return gitlabTeamCatalogProjectRow{}
}

func TestGitLabTeamCatalogStatesTheProjectTargetFromItsOpenMilestones(t *testing.T) {
	t.Parallel()
	batch, requests := collectGitLabProjectTargets(t,
		map[string]string{"100": "org/api", "101": "org/none", "102": "org/failed", "103": "org/archived"},
		map[string]string{
			"100": `[{"id":7,"title":"Q4","state":"active","start_date":"2026-10-01","due_date":"2026-12-31"},
				{"id":6,"title":"Q3","state":"active","start_date":"2026-07-01","due_date":"2026-09-30"},
				{"id":5,"title":"Undated","state":"active","due_date":null},
				{"id":4,"title":"Closed","state":"closed","due_date":"2026-01-31"}]`,
			"101": `[{"id":9,"title":"Undated","state":"active","due_date":null}]`,
			"103": `[{"id":10,"title":"Never read","state":"active","due_date":"2026-11-30"}]`,
		})
	api := gitlabProjectRowByKey(t, batch, "org/api")
	want := projectTargetFacts{
		TargetState: projectTargetStated, TargetKind: "milestone", TargetSource: "gitlab.milestone.due_date",
		TargetRef: gitlabMilestoneSprintID("org/api", "6"), TargetRefName: "Q3", TargetOtherOpen: 1, TargetFetchedAt: &targetFetchedAt,
	}
	if !sameDay(api.TargetDate, utcDay(2026, 9, 30)) || !reflect.DeepEqual(api.projectTargetFacts, want) {
		t.Fatalf("api target=%v facts=%+v, want the overdue open 2026-09-30 (milestone 6) with %+v", api.TargetDate, api.projectTargetFacts, want)
	}
	if none := gitlabProjectRowByKey(t, batch, "org/none"); none.TargetState != projectTargetNoneDate || none.TargetDate != nil {
		t.Fatalf("none target=%v state=%q, want none_dated", none.TargetDate, none.TargetState)
	}
	if failed := gitlabProjectRowByKey(t, batch, "org/failed"); failed.read() || failed.TargetDate != nil {
		t.Fatalf("failed target=%v facts=%+v, want unread: its milestone read answered 404", failed.TargetDate, failed.projectTargetFacts)
	}
	if archived := gitlabProjectRowByKey(t, batch, "org/archived"); archived.read() {
		t.Fatalf("archived facts=%+v, want unread", archived.projectTargetFacts)
	}
	for _, request := range requests {
		if strings.Contains(request, "/projects/103/") {
			t.Fatalf("an archived project's milestones were read: %v", requests)
		}
	}
}

// The milestone that supplies a project's target is named by the key of its
// sprints row, so a reader joins by key and not by name or date.
func TestGitLabProjectTargetRefIsTheSprintsRowKeyOfTheSameMilestone(t *testing.T) {
	t.Parallel()
	const milestone = `{"id":7,"title":"Q4","state":"active","start_date":"2026-10-01","due_date":"2026-12-31"}`
	batch, _ := collectGitLabProjectTargets(t, map[string]string{"100": "org/team-a/api"}, map[string]string{"100": `[` + milestone + `]`})
	row := gitlabProjectRowByKey(t, batch, "org/team-a/api")

	var payload gitlabIssueMilestonePayload
	if err := json.Unmarshal([]byte(milestone), &payload); err != nil {
		t.Fatal(err)
	}
	claim := nativeTestClaim("gitlab", "work-items")
	sprint, err := normalizeGitLabSprint(claim, "org/team-a/api", payload, targetFetchedAt)
	if err != nil {
		t.Fatal(err)
	}
	if row.TargetRef == "" || row.TargetRef != sprint.SprintID {
		t.Fatalf("target_ref=%q, sprints.sprint_id=%q, want equal by value", row.TargetRef, sprint.SprintID)
	}
	if sprint.EndedAt == nil || !sameDay(row.TargetDate, sprint.EndedAt) {
		t.Fatalf("target_date=%v, sprint ended_at=%v, want the same day", row.TargetDate, sprint.EndedAt)
	}
}

func isNilProjectValue(value any) bool {
	if value == nil {
		return true
	}
	reflected := reflect.ValueOf(value)
	return reflected.Kind() == reflect.Pointer && reflected.IsNil()
}

func projectValuesOf(id string, target *time.Time, facts projectTargetFacts) projectStoredRow {
	at := time.Date(2026, 10, 14, 12, 0, 0, 0, time.UTC)
	key, lead := "OPS", "lead"
	return newProjectStoredRow(id, "org-1", "jira", &key, "Ops", 1, "started", "jira.project.status=live", target, facts,
		"https://x", []string{"t1"}, []string{"ENG"}, &lead, &lead, &lead, at, at)
}

type heldProject struct {
	present bool
	target  *time.Time
	facts   projectTargetFacts
}

func (held heldProject) lookup([]any) (map[string]any, bool) {
	if !held.present {
		return nil, false
	}
	state := held.facts.TargetState
	if state == "" {
		state = projectTargetUnknown
	}
	version := map[string]any{
		"target_date": nil, "target_state": state, "target_kind": held.facts.TargetKind, "target_source": held.facts.TargetSource,
		"target_ref": held.facts.TargetRef, "target_ref_name": held.facts.TargetRefName, "target_other_open": nil, "target_fetched_at": nil,
	}
	if held.target != nil {
		version["target_date"] = *held.target
	}
	if state == projectTargetStated {
		version["target_other_open"] = held.facts.TargetOtherOpen
	}
	if held.facts.TargetFetchedAt != nil {
		version["target_fetched_at"] = *held.facts.TargetFetchedAt
	}
	return version, true
}

func foldProject(t *testing.T, row projectStoredRow, held heldProject) []any {
	t.Helper()
	rows := []projectStoredRow{row}
	stored := projectStoredVersionRows(rows)
	if _, err := projectsContract.Fold(projectsCatalogInsert, stored, held.lookup); err != nil {
		t.Fatal(err)
	}
	markHeldTargets(rows, stored)
	return stored[0].Values
}

func TestProjectsContractKeepsTheHeldTargetOnlyWhenTheRowIsUnread(t *testing.T) {
	t.Parallel()
	oldFetch := time.Date(2026, 10, 1, 8, 0, 0, 0, time.UTC)
	heldFacts := projectTargetFacts{
		TargetState: projectTargetStated, TargetKind: "version", TargetSource: "jira.version.releaseDate", TargetRef: "55", TargetRefName: "5.5",
		TargetOtherOpen: 3, TargetFetchedAt: &oldFetch,
	}
	held := heldProject{present: true, target: utcDay(2026, 12, 15), facts: heldFacts}
	newFacts := projectTargetFacts{
		TargetState: projectTargetStated, TargetKind: "version", TargetSource: "jira.version.releaseDate", TargetRef: "7", TargetRefName: "7.0",
		TargetOtherOpen: 0, TargetFetchedAt: &targetFetchedAt,
	}
	none := projectTargetFacts{TargetState: projectTargetNoneDate, TargetFetchedAt: &targetFetchedAt}
	positions := projectsInsertPositions

	// A read project states its own target: a date replaces the held one, none clears it.
	got := foldProject(t, projectValuesOf("10001", utcDay(2026, 9, 30), newFacts), held)
	if state := got[positions["target_state"]]; state != projectTargetStated || got[positions["target_ref"]] != "7" {
		t.Fatalf("stated row = %v", got)
	}
	got = foldProject(t, projectValuesOf("10001", nil, none), held)
	if !isNilProjectValue(got[positions["target_date"]]) || got[positions["target_state"]] != projectTargetNoneDate ||
		got[positions["target_ref"]] != "" || got[positions["target_source"]] != "" || got[positions["target_kind"]] != "" ||
		!isNilProjectValue(got[positions["target_other_open"]]) {
		t.Fatalf("none_dated row over a held date = %v, want the date cleared and no count", got)
	}

	// An unread project keeps the held target and its fetch time and writes held.
	got = foldProject(t, projectValuesOf("10001", nil, projectTargetFacts{}), held)
	date, err := projectTargetDate(got)
	if err != nil || !sameDay(date, utcDay(2026, 12, 15)) {
		t.Fatalf("unread row date = %v err=%v, want the held 2026-12-15", date, err)
	}
	if got[positions["target_state"]] != projectTargetHeld || got[positions["target_kind"]] != "version" ||
		got[positions["target_source"]] != "jira.version.releaseDate" || got[positions["target_ref"]] != "55" ||
		got[positions["target_ref_name"]] != "5.5" || got[positions["target_other_open"]] != uint32(3) ||
		got[positions["target_fetched_at"]] != oldFetch {
		t.Fatalf("unread row = %v, want the held values kept and state held", got)
	}

	// Held stays held on the next failed run; a held none_dated stays a held absence.
	heldAgain := heldProject{present: true, target: utcDay(2026, 12, 15), facts: projectTargetFacts{
		TargetState: projectTargetHeld, TargetKind: "version", TargetSource: "jira.version.releaseDate", TargetRef: "55", TargetFetchedAt: &oldFetch,
	}}
	if got = foldProject(t, projectValuesOf("10001", nil, projectTargetFacts{}), heldAgain); got[positions["target_state"]] != projectTargetHeld {
		t.Fatalf("a second failed run reads %v, want held", got[positions["target_state"]])
	}
	heldNone := heldProject{present: true, facts: projectTargetFacts{TargetState: projectTargetNoneDate, TargetFetchedAt: &oldFetch}}
	if got = foldProject(t, projectValuesOf("10001", nil, projectTargetFacts{}), heldNone); got[positions["target_state"]] != projectTargetHeld ||
		!isNilProjectValue(got[positions["target_date"]]) || got[positions["target_fetched_at"]] != oldFetch {
		t.Fatalf("a failed run over a measured absence = %v, want held, no date, the old fetch time", got)
	}

	// An unread project nothing was ever read for is unknown: no date, no count, no fetch time.
	for _, heldCase := range []heldProject{{}, {present: true}} {
		got = foldProject(t, projectValuesOf("10001", nil, projectTargetFacts{}), heldCase)
		if got[positions["target_state"]] != projectTargetUnknown || !isNilProjectValue(got[positions["target_other_open"]]) ||
			!isNilProjectValue(got[positions["target_fetched_at"]]) {
			t.Fatalf("unread row with held %+v = %v, want unknown, no count, no fetch time", heldCase, got)
		}
	}
}

// The contract also covers Linear's writer, which states every project's
// target. A Linear row is by value the same before and after the contract,
// whatever the key holds.
func TestProjectsContractLeavesEveryLinearRowAsItWas(t *testing.T) {
	t.Parallel()
	oldFetch := time.Date(2026, 10, 1, 8, 0, 0, 0, time.UTC)
	for _, own := range []*time.Time{nil, utcDay(2026, 9, 30)} {
		for _, held := range []heldProject{
			{},
			{present: true},
			{present: true, target: utcDay(2026, 12, 15), facts: projectTargetFacts{TargetState: projectTargetStated, TargetKind: "project", TargetSource: "x", TargetRef: "r", TargetOtherOpen: 2, TargetFetchedAt: &oldFetch}},
		} {
			before := projectValuesOf("project-1", own, linearProjectTargetFacts(own, targetFetchedAt))
			beforeValues := append([]any(nil), before.Values...)
			after := foldProject(t, projectValuesOf("project-1", own, linearProjectTargetFacts(own, targetFetchedAt)), held)
			if !reflect.DeepEqual(beforeValues, after) {
				t.Fatalf("own=%v held=%+v: row after the contract %v differs from before %v", own, held, after, beforeValues)
			}
		}
	}
}

func TestLinearProjectTargetFactsStateTheNativeDate(t *testing.T) {
	t.Parallel()
	if facts := linearProjectTargetFacts(utcDay(2026, 9, 30), targetFetchedAt); facts.TargetState != projectTargetStated ||
		facts.TargetKind != "project" || facts.TargetSource != "linear.project.targetDate" || facts.TargetRef != "" || facts.TargetRefName != "" ||
		facts.TargetOtherOpen != 0 || facts.TargetFetchedAt == nil || !facts.TargetFetchedAt.Equal(targetFetchedAt) {
		t.Fatalf("facts = %+v", facts)
	}
	if facts := linearProjectTargetFacts(nil, targetFetchedAt); facts.TargetState != projectTargetNoneDate || facts.TargetSource != "" {
		t.Fatalf("facts = %+v, want none_dated", facts)
	}
}

func TestProviderDateOnlyKeepsTheDay(t *testing.T) {
	t.Parallel()
	for text, want := range map[string]*time.Time{
		"2026-10-31":                utcDay(2026, 10, 31),
		" 2026-02-28 ":              utcDay(2026, 2, 28),
		"2026-10-31T00:00:00Z":      utcDay(2026, 10, 31),
		"2026-10-31T00:00:00+14:00": utcDay(2026, 10, 30),
		"":                          nil,
		"31/10/2026":                nil,
	} {
		got, ok := providerDateOnly(text)
		if want == nil {
			if ok {
				t.Fatalf("%q read as %v, want no date", text, got)
			}
			continue
		}
		if !ok || !got.Equal(*want) {
			t.Fatalf("%q read as %v ok=%v, want %v", text, got, ok, want)
		}
	}
}

// A sprint key that changed would orphan the sprints rows already stored, so
// the shared key function must give, byte for byte, the key the normalizer
// built before it existed: "gitlab:" + the project's full path + ":milestone:"
// + the milestone's id. Checked on the milestones of the existing route
// fixture and oracle cases, and on paths of every depth.
func TestGitLabSprintKeysAreByteIdenticalToTheKeysStoredBeforeTheSharedFunction(t *testing.T) {
	t.Parallel()
	legacy := func(fullName, id string) string { return "gitlab:" + fullName + ":milestone:" + id }
	claim := nativeTestClaim("gitlab", "work-items")
	cases := []struct {
		fullName string
		payload  string
	}{
		{"acme/api", `{"id":7,"title":"July","state":"closed","start_date":"2026-07-01","due_date":"2026-07-31"}`},
		{"acme/api", `{"id":7,"title":"August","state":"closed","start_date":"2026-08-01","due_date":"2026-08-31"}`},
		{"acme/api", `{"id":8,"title":"September","state":"active","start_date":"2026-09-01","due_date":null}`},
		{"org/team-a/sub group/svc", `{"id":123456789,"title":"x","state":"active"}`},
		{"solo", `{"id":1,"title":"x","state":"active"}`},
	}
	for _, tc := range cases {
		var payload gitlabIssueMilestonePayload
		if err := json.Unmarshal([]byte(tc.payload), &payload); err != nil {
			t.Fatal(err)
		}
		row, err := normalizeGitLabSprint(claim, tc.fullName, payload, targetFetchedAt)
		if err != nil {
			t.Fatal(err)
		}
		if want := legacy(tc.fullName, payload.ID.String()); row.SprintID != want {
			t.Fatalf("sprint key = %q, want the stored form %q", row.SprintID, want)
		}
	}
	for _, response := range gitLabWorkItemResponses() {
		for _, body := range response {
			var milestones []gitlabIssueMilestonePayload
			if json.Unmarshal([]byte(body), &milestones) != nil {
				continue
			}
			for _, payload := range milestones {
				if payload.ID.String() == "" || payload.Title == "" {
					continue
				}
				row, err := normalizeGitLabSprint(claim, "acme/api", payload, targetFetchedAt)
				if err != nil {
					continue
				}
				if want := legacy("acme/api", payload.ID.String()); row.SprintID != want {
					t.Fatalf("fixture sprint key = %q, want the stored form %q", row.SprintID, want)
				}
			}
		}
	}
}

// CHAOS-9702: a project whose only open dated milestone belongs to an ancestor
// group states that date, with provenance naming the group, from the same one
// call per project (D6009).
func TestGitLabProjectTargetCountsGroupMilestones(t *testing.T) {
	t.Parallel()
	batch, requests := collectGitLabProjectTargets(t,
		map[string]string{"100": "org/only-group", "101": "org/group-earlier", "102": "org/project-earlier"},
		map[string]string{
			"100": `[{"id":31,"title":"Group Q4","state":"active","due_date":"2026-12-31","group_id":1,"web_url":"https://gitlab.example/groups/org/-/milestones/2"},
				{"id":5,"title":"Undated","state":"active","due_date":null}]`,
			"101": `[{"id":32,"title":"Group Q3","state":"active","due_date":"2026-09-30","group_id":1,"web_url":"https://gitlab.example/groups/org/-/milestones/3"},
				{"id":8,"title":"Project Q4","state":"active","due_date":"2026-12-31"}]`,
			"102": `[{"id":33,"title":"Group Q4","state":"active","due_date":"2026-12-31","group_id":1,"web_url":"https://gitlab.example/groups/org/-/milestones/4"},
				{"id":9,"title":"Project Q3","state":"active","due_date":"2026-09-30"}]`,
		})
	onlyGroup := gitlabProjectRowByKey(t, batch, "org/only-group")
	want := projectTargetFacts{
		TargetState: projectTargetStated, TargetKind: "milestone", TargetSource: "gitlab.group_milestone.due_date",
		TargetRef: gitlabMilestoneSprintID("org", "31"), TargetRefName: "Group Q4", TargetFetchedAt: &targetFetchedAt,
	}
	if !sameDay(onlyGroup.TargetDate, utcDay(2026, 12, 31)) || !reflect.DeepEqual(onlyGroup.projectTargetFacts, want) {
		t.Fatalf("only-group target=%v facts=%+v, want the group milestone 2026-12-31 with %+v", onlyGroup.TargetDate, onlyGroup.projectTargetFacts, want)
	}
	groupEarlier := gitlabProjectRowByKey(t, batch, "org/group-earlier")
	if !sameDay(groupEarlier.TargetDate, utcDay(2026, 9, 30)) || groupEarlier.TargetSource != "gitlab.group_milestone.due_date" ||
		groupEarlier.TargetRef != gitlabMilestoneSprintID("org", "32") || groupEarlier.TargetOtherOpen != 1 {
		t.Fatalf("group-earlier target=%v facts=%+v, want the earlier group milestone, one other open", groupEarlier.TargetDate, groupEarlier.projectTargetFacts)
	}
	projectEarlier := gitlabProjectRowByKey(t, batch, "org/project-earlier")
	if !sameDay(projectEarlier.TargetDate, utcDay(2026, 9, 30)) || projectEarlier.TargetSource != "gitlab.milestone.due_date" ||
		projectEarlier.TargetRef != gitlabMilestoneSprintID("org/project-earlier", "9") {
		t.Fatalf("project-earlier target=%v facts=%+v, want the earlier project milestone and project provenance", projectEarlier.TargetDate, projectEarlier.projectTargetFacts)
	}
	calls := 0
	for _, request := range requests {
		if strings.Contains(request, "/projects/100/milestones") {
			calls++
			if !strings.Contains(request, "include_parent_milestones=true") {
				t.Fatalf("milestone read %q does not ask for the parent groups' milestones", request)
			}
		}
	}
	if calls != 1 {
		t.Fatalf("project 100 milestone calls = %d, want 1", calls)
	}
}

func TestGitLabGroupMilestoneOwnerNamesTheGroup(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct{ webURL, groupID, want string }{
		{"https://gitlab.example/groups/org/sub/-/milestones/4", "7", "org/sub"},
		{"", "7", "group:7"},
		{"https://gitlab.example/org/api/-/milestones/4", "7", "group:7"},
	} {
		if got := gitlabGroupMilestoneOwner(gitlabIssueMilestonePayload{GroupID: json.Number(tc.groupID), WebURL: tc.webURL}); got != tc.want {
			t.Errorf("owner(%q, %s) = %q, want %q", tc.webURL, tc.groupID, got, tc.want)
		}
	}
}
