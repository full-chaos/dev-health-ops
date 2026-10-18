package providersync

import (
	"context"
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
