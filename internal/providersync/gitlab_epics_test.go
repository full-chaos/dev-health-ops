package providersync

import (
	"context"
	"log/slog"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/fakehttp"
)

func gitLabEpicResponses() map[string][]string {
	responses := gitLabWorkItemResponses()
	responses["/api/v4/groups/acme/epics?page=1"] = []string{`[
		{"iid":4,"title":"Platform epic","description":"all of it","state":"opened","created_at":"2026-07-01T08:00:00Z","updated_at":"2026-07-10T08:00:00Z",
		 "start_date":"2026-07-01","due_date":"2026-09-30","labels":["priority::high","in progress"],"author":{"username":"bob","name":"Bob"},
		 "web_url":"https://gitlab.example/groups/acme/-/epics/4","parent_id":null,"parent_iid":null},
		{"iid":5,"title":"Child epic","state":"closed","created_at":"2026-07-02T08:00:00Z","updated_at":"2026-07-12T08:00:00Z","closed_at":"2026-07-12T08:00:00Z",
		 "labels":[],"author":{"username":"alice"},"parent_id":4}]`, `[]`}
	responses["/api/v4/groups/acme/epics/4/resource_state_events?page=1"] = []string{`[
		{"state":"closed","created_at":"2026-07-05T08:00:00Z","user":{"username":"bob","name":"Bob"}},
		{"state":"reopened","created_at":"2026-07-06T08:00:00Z","user":{"username":"bob","name":"Bob"}}]`, `[]`}
	responses["/api/v4/groups/acme/epics/5/resource_state_events?page=1"] = []string{`[]`}
	responses["/api/v4/groups/acme/epics/4/notes?page=1"] = []string{`[{"id":901,"system":false,"body":"epic note","created_at":"2026-07-03T08:00:00Z","author":{"username":"alice"}}]`, `[]`}
	responses["/api/v4/groups/acme/epics/5/notes?page=1"] = []string{`[]`}
	return responses
}

func collectGitLabEpicUnit(t *testing.T, doer *gitLabWorkItemsDoer) (CompleteRouteBatch, error) {
	t.Helper()
	claim := nativeTestClaim("gitlab", "work-items")
	handler := GitLabWorkItemsRouteHandler{StatusMapping: loadRealStatusMapping(t), PerPage: 2, MaxPages: 10, NestedMaxPages: 10}
	return handler.Collect(context.Background(), claim, providerfoundation.Credential{Provider: "gitlab", ID: claim.CredentialID},
		gitLabWorkItemsClient(t, fakehttp.Client(doer)), time.Date(2026, 8, 3, 12, 0, 0, 0, time.UTC))
}

func epicRowsOf(t *testing.T, batch CompleteRouteBatch) map[string]gitlabWorkItemRow {
	t.Helper()
	out := map[string]gitlabWorkItemRow{}
	for _, effect := range batch.Effects {
		if effect.Destination != "work_items" {
			continue
		}
		for _, raw := range effect.Rows {
			var row gitlabWorkItemRow
			if err := jsonUnmarshalEffect(raw, &row); err != nil {
				t.Fatal(err)
			}
			if row.Type == "epic" {
				out[row.WorkItemID] = row
			}
		}
	}
	return out
}

// CHAOS-9857: the group's epics are collected as work items, with the parent
// epic in epic_id, transitions from the state events, the reopen, and the notes,
// and both window bounds on the epics request.
func TestGitLabWorkItemsRouteCollectsGroupEpicsAsWorkItems(t *testing.T) {
	doer := &gitLabWorkItemsDoer{responses: gitLabEpicResponses()}
	batch, err := collectGitLabEpicUnit(t, doer)
	if err != nil {
		t.Fatal(err)
	}
	epics := epicRowsOf(t, batch)
	parent, child := epics["gitlab:acme:epic:4"], epics["gitlab:acme:epic:5"]
	if len(epics) != 2 || parent.WorkItemID == "" || child.WorkItemID == "" {
		t.Fatalf("epic rows = %v", epics)
	}
	if parent.ProjectID == nil || *parent.ProjectID != "acme" || parent.RepoID != nil || parent.ProjectKey != nil ||
		parent.EpicID != nil || parent.Status == "" || parent.Status == "done" {
		t.Fatalf("parent epic = %+v", parent)
	}
	if child.EpicID == nil || *child.EpicID != "gitlab:acme:epic:4" || child.Status != "done" {
		t.Fatalf("child epic = %+v", child)
	}
	if parent.DueAt == nil || parent.StartedAt == nil || parent.PriorityRaw == nil {
		t.Fatalf("dates/priority of the parent epic lost: %+v", parent)
	}
	summary := batch.Result["gitlab_work_items"].(GitLabWorkItemsResult)
	if summary.EpicsSynced != 2 || batch.Result["epics_synced"] != 2 {
		t.Fatalf("epics_synced = %d / %v", summary.EpicsSynced, batch.Result["epics_synced"])
	}
	var transitions, reopens, interactions int
	for _, effect := range batch.Effects {
		for _, raw := range effect.Rows {
			if !strings.Contains(string(raw), "gitlab:acme:epic:4") {
				continue
			}
			switch effect.Destination {
			case "work_item_transitions":
				transitions++
			case "work_item_reopen_events":
				reopens++
			case "work_item_interactions":
				interactions++
			}
		}
	}
	if transitions != 2 || reopens != 1 || interactions != 1 {
		t.Fatalf("epic 4 transitions=%d reopens=%d interactions=%d, want 2/1/1", transitions, reopens, interactions)
	}
	if batch.Watermark == nil {
		t.Fatal("a clean epic fetch held the watermark")
	}
	sawEpicList := false
	for _, request := range doer.requests {
		if request.URL.Path == "/api/v4/groups/acme/epics" {
			sawEpicList = true
			query := request.URL.Query()
			if query.Get("state") != "all" || query.Get("updated_after") != "2026-07-01T00:00:00Z" || query.Get("updated_before") != "2026-07-31T23:59:59Z" {
				t.Fatalf("epics query = %s", request.URL.RawQuery)
			}
		}
	}
	if !sawEpicList {
		t.Fatal("the epics were not requested")
	}
}

// Epics are a Premium/Ultimate feature: 403 and 404 are the free-tier answer, a
// clean no-op; the unit goes on and its watermark advances.
func TestGitLabWorkItemsRouteTreatsAnEpicsRefusalAsTheFreeTier(t *testing.T) {
	for _, status := range []int{http.StatusForbidden, http.StatusNotFound} {
		responses := gitLabWorkItemResponses()
		delete(responses, "/api/v4/groups/acme/epics?page=1")
		doer := &gitLabWorkItemsDoer{responses: responses, statusByPath: map[string]int{"/api/v4/groups/acme/epics": status}}
		batch, err := collectGitLabEpicUnit(t, doer)
		if err != nil {
			t.Fatalf("status %d: %v", status, err)
		}
		if len(epicRowsOf(t, batch)) != 0 || batch.Watermark == nil || batch.Result["incomplete"] != nil {
			t.Fatalf("status %d: epics=%v watermark=%v incomplete=%v", status, epicRowsOf(t, batch), batch.Watermark, batch.Result["incomplete"])
		}
	}
}

// Any other failure is a lost answer, not zero epics: the part is reported
// incomplete and the watermark is held, so the next run asks again.
func TestGitLabWorkItemsRouteHoldsTheWatermarkWhenTheEpicsFetchFails(t *testing.T) {
	responses := gitLabWorkItemResponses()
	delete(responses, "/api/v4/groups/acme/epics?page=1")
	doer := &gitLabWorkItemsDoer{responses: responses, statusByPath: map[string]int{"/api/v4/groups/acme/epics": http.StatusInternalServerError}}
	batch, err := collectGitLabEpicUnit(t, doer)
	if err != nil {
		t.Fatal(err)
	}
	incomplete, _ := batch.Result["incomplete"].([]string)
	if batch.Watermark != nil || len(incomplete) != 1 || incomplete[0] != "epics" {
		t.Fatalf("watermark=%v incomplete=%v, want a held watermark and [epics]", batch.Watermark, batch.Result["incomplete"])
	}
	if len(batch.Effects) == 0 {
		t.Fatal("the raw rows of the issues and merge requests were dropped with the epics")
	}
}

// A failed state-event or note read of one epic holds the watermark too, and the
// epic row is still written.
func TestGitLabWorkItemsRouteHoldsTheWatermarkWhenAnEpicSubFetchFails(t *testing.T) {
	responses := gitLabEpicResponses()
	delete(responses, "/api/v4/groups/acme/epics/4/notes?page=1")
	doer := &gitLabWorkItemsDoer{responses: responses, statusByPath: map[string]int{"/api/v4/groups/acme/epics/4/notes": http.StatusBadGateway}}
	batch, err := collectGitLabEpicUnit(t, doer)
	if err != nil {
		t.Fatal(err)
	}
	incomplete, _ := batch.Result["incomplete"].([]string)
	if batch.Watermark != nil || len(incomplete) != 1 || incomplete[0] != "epic_notes:4" || len(epicRowsOf(t, batch)) != 2 {
		t.Fatalf("watermark=%v incomplete=%v epics=%d", batch.Watermark, batch.Result["incomplete"], len(epicRowsOf(t, batch)))
	}
}

// FetchEpics off (GITLAB_FETCH_EPICS=false) collects no epic.
func TestGitLabWorkItemsRouteCollectsNoEpicsWhenFetchEpicsIsOff(t *testing.T) {
	doer := &gitLabWorkItemsDoer{responses: gitLabEpicResponses()}
	off := false
	claim := nativeTestClaim("gitlab", "work-items")
	handler := GitLabWorkItemsRouteHandler{StatusMapping: loadRealStatusMapping(t), PerPage: 2, MaxPages: 10, NestedMaxPages: 10, FetchEpics: &off}
	batch, err := handler.Collect(context.Background(), claim, providerfoundation.Credential{Provider: "gitlab", ID: claim.CredentialID},
		gitLabWorkItemsClient(t, fakehttp.Client(doer)), time.Date(2026, 8, 3, 12, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	if len(epicRowsOf(t, batch)) != 0 {
		t.Fatal("epics were collected with FetchEpics off")
	}
	for _, request := range doer.requests {
		if strings.Contains(request.URL.Path, "/groups/") {
			t.Fatalf("a group request was made with FetchEpics off: %s", request.URL)
		}
	}
}

// Every epic outcome names itself at Info, so a free-tier group (no epics), a
// project outside a group and a collected group are told apart in the logs.
func TestGitLabWorkItemsRouteLogsTheEpicOutcomeAtInfo(t *testing.T) {
	var logged lockedLogBuffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logged, &slog.HandlerOptions{Level: slog.LevelInfo})))
	t.Cleanup(func() { slog.SetDefault(previous) })

	if _, err := collectGitLabEpicUnit(t, &gitLabWorkItemsDoer{responses: gitLabEpicResponses()}); err != nil {
		t.Fatal(err)
	}
	responses := gitLabWorkItemResponses()
	delete(responses, "/api/v4/groups/acme/epics?page=1")
	if _, err := collectGitLabEpicUnit(t, &gitLabWorkItemsDoer{responses: responses, statusByPath: map[string]int{"/api/v4/groups/acme/epics": http.StatusForbidden}}); err != nil {
		t.Fatal(err)
	}
	failing := gitLabWorkItemResponses()
	delete(failing, "/api/v4/groups/acme/epics?page=1")
	if _, err := collectGitLabEpicUnit(t, &gitLabWorkItemsDoer{responses: failing, statusByPath: map[string]int{"/api/v4/groups/acme/epics": http.StatusInternalServerError}}); err != nil {
		t.Fatal(err)
	}
	subFailing := gitLabEpicResponses()
	delete(subFailing, "/api/v4/groups/acme/epics/4/notes?page=1")
	if _, err := collectGitLabEpicUnit(t, &gitLabWorkItemsDoer{responses: subFailing, statusByPath: map[string]int{"/api/v4/groups/acme/epics/4/notes": http.StatusBadGateway}}); err != nil {
		t.Fatal(err)
	}
	line := logged.String()
	for _, want := range []string{"outcome=ok", "epics_written=2", "outcome=unavailable", "outcome=incomplete epics_listed=0 epics_written=0 epics_skipped=0 incomplete=1", "outcome=incomplete epics_listed=2 epics_written=2 epics_skipped=0 incomplete=1"} {
		if !strings.Contains(line, want) {
			t.Fatalf("epic outcomes not told apart at Info: missing %q in %q", want, line)
		}
	}
}

// The declared divergence from Python (an epic with no usable iid or no title
// is skipped, where Python writes epic:0 or an empty title) is visible: one Warn
// per skip names the group, the iid and the reason, and the unit result counts
// the skips. The valid epic of the same list is still written and the watermark
// is not held.
func TestGitLabWorkItemsRouteSkipsAnUnusableEpicVisibly(t *testing.T) {
	var logged lockedLogBuffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logged, &slog.HandlerOptions{Level: slog.LevelInfo})))
	t.Cleanup(func() { slog.SetDefault(previous) })

	responses := gitLabWorkItemResponses()
	responses["/api/v4/groups/acme/epics?page=1"] = []string{`[
		{"iid":4,"title":"Good epic","state":"opened","created_at":"2026-07-01T08:00:00Z","labels":[]},
		{"iid":0,"title":"Zero iid","state":"opened","created_at":"2026-07-01T08:00:00Z","labels":[]},
		{"iid":6,"title":"","state":"opened","created_at":"2026-07-01T08:00:00Z","labels":[]}]`, `[]`}
	for _, iid := range []string{"4", "6"} {
		responses["/api/v4/groups/acme/epics/"+iid+"/resource_state_events?page=1"] = []string{`[]`}
		responses["/api/v4/groups/acme/epics/"+iid+"/notes?page=1"] = []string{`[]`}
	}
	responses["/api/v4/groups/acme/epics/0/resource_state_events?page=1"] = []string{`[]`}
	responses["/api/v4/groups/acme/epics/0/notes?page=1"] = []string{`[]`}
	batch, err := collectGitLabEpicUnit(t, &gitLabWorkItemsDoer{responses: responses})
	if err != nil {
		t.Fatal(err)
	}
	epics := epicRowsOf(t, batch)
	if len(epics) != 1 || epics["gitlab:acme:epic:4"].WorkItemID == "" {
		t.Fatalf("epics written = %v, want only epic 4", epics)
	}
	if batch.Result["epics_synced"] != 1 || batch.Result["epics_skipped"] != 2 || batch.Watermark == nil {
		t.Fatalf("epics_synced=%v epics_skipped=%v watermark=%v", batch.Result["epics_synced"], batch.Result["epics_skipped"], batch.Watermark)
	}
	line := logged.String()
	for _, want := range []string{"level=WARN msg=gitlab_work_items_epic_skipped", "group=acme", "iid=0", "reason=no_usable_iid", "iid=6", "reason=empty_title", "epics_skipped=2"} {
		if !strings.Contains(line, want) {
			t.Fatalf("log lacks %q: %q", want, line)
		}
	}
}

// The group epics list is bounded by an inclusive updated_before, like the issue and merge-request lists: an
// epic updated outside [since, before) belongs to another window and is not written by this one.
func TestGitLabWorkItemsRouteKeepsOnlyTheEpicsInTheHalfOpenWindow(t *testing.T) {
	responses := gitLabEpicResponses()
	responses["/api/v4/groups/acme/epics?page=1"] = []string{`[
		{"iid":4,"title":"inside","state":"opened","created_at":"2026-06-01T08:00:00Z","updated_at":"2026-07-10T08:00:00Z","labels":[]},
		{"iid":6,"title":"at the start","state":"opened","created_at":"2026-06-01T08:00:00Z","updated_at":"2026-07-01T00:00:00Z","labels":[]},
		{"iid":7,"title":"at the end","state":"opened","created_at":"2026-06-01T08:00:00Z","updated_at":"2026-07-31T23:59:59Z","labels":[]},
		{"iid":8,"title":"before the start","state":"opened","created_at":"2026-06-01T08:00:00Z","updated_at":"2026-06-30T23:00:00Z","labels":[]},
		{"iid":9,"title":"after the end","state":"opened","created_at":"2026-06-01T08:00:00Z","updated_at":"2026-08-01T01:00:00Z","labels":[]}]`, `[]`}
	for _, iid := range []string{"6", "7", "8", "9"} {
		responses["/api/v4/groups/acme/epics/"+iid+"/resource_state_events?page=1"] = []string{`[]`}
		responses["/api/v4/groups/acme/epics/"+iid+"/notes?page=1"] = []string{`[]`}
	}
	batch, err := collectGitLabEpicUnit(t, &gitLabWorkItemsDoer{responses: responses})
	if err != nil {
		t.Fatal(err)
	}
	epics := epicRowsOf(t, batch)
	for iid, want := range map[string]bool{"4": true, "6": true, "7": false, "8": false, "9": false} {
		if _, written := epics["gitlab:acme:epic:"+iid]; written != want {
			t.Errorf("epic %s written = %v, want %v (rows: %v)", iid, written, want, len(epics))
		}
	}
}
