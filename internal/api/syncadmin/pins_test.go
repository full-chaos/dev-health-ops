package syncadmin

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/licensing"
	"github.com/full-chaos/dev-health-ops/internal/api/policy"
)

// fixedFeatures decides every feature as allowed; called counts calls.
type fixedFeatures struct {
	allowed bool
	called  *int
}

func (f fixedFeatures) Decide(context.Context, string, string) (licensing.Decision, error) {
	if f.called != nil {
		*f.called++
	}
	return licensing.Decision{Allowed: f.allowed}, nil
}

func TestSyncTargetsFilterFollowsTheDecision(t *testing.T) {
	open := `{"github":["git","prs","blame","cicd","deployments","security","tests","work-items"],` +
		`"gitlab":["git","prs","blame","cicd","deployments","incidents","security","tests","work-items","feature-flags"],` +
		`"jira":["work-items","operational"],"linear":["work-items"],"launchdarkly":["feature-flags"],"pagerduty":["operational"]}`
	closed := `{"github":["git","prs","blame","cicd","deployments","security","tests","work-items"],` +
		`"gitlab":["git","prs","blame","cicd","deployments","security","tests","work-items","feature-flags"],` +
		`"jira":["work-items"],"linear":["work-items"],"launchdarkly":["feature-flags"],"pagerduty":[]}`
	for _, tc := range []struct {
		allowed bool
		org     string
		want    string
		calls   int
	}{
		{true, uuid.NewString(), open, 1},
		{false, uuid.NewString(), closed, 1},
		// uuid.UUID() refuses the org id: closed, and no decision is read.
		{true, "not-a-uuid", closed, 0},
	} {
		calls := 0
		h := &handlers{store: &faultReader{}, features: fixedFeatures{allowed: tc.allowed, called: &calls}, logger: quiet()}
		request := newRequest(t, "/x", tc.org)
		recorder := record(h.syncTargets, request)
		if recorder.Code != http.StatusOK || recorder.Body.String() != tc.want || calls != tc.calls {
			t.Errorf("allowed=%v org=%s: %d %s (%d decisions)", tc.allowed, tc.org, recorder.Code, recorder.Body.String(), calls)
		}
	}
}

// listReader serves three configs: a parent with an integration, its
// child, and a config carrying a source_id; childrenCounts counts one
// child for every id it is asked about.
type listReader struct{ faultReader }

func (listReader) listConfigs(context.Context, string, bool) ([]*syncConfig, error) {
	targets, options := `[]`, `{}`
	parent, integration := uuid.MustParse("00000000-0000-0000-0000-000000000001"), uuid.New()
	source := uuid.New()
	return []*syncConfig{
		{ID: parent, Name: "parent", SyncTargets: &targets, SyncOptions: &options, IntegrationID: &integration},
		{ID: uuid.MustParse("00000000-0000-0000-0000-000000000002"), Name: "child", SyncTargets: &targets, SyncOptions: &options, ParentID: &parent},
		{ID: uuid.MustParse("00000000-0000-0000-0000-000000000003"), Name: "sourced", SyncTargets: &targets, SyncOptions: &options, SourceID: &source},
	}, nil
}

func (listReader) childrenCounts(_ context.Context, ids []uuid.UUID) (map[uuid.UUID]int64, error) {
	counts := map[uuid.UUID]int64{}
	for _, id := range ids {
		counts[id] = 1
	}
	return counts, nil
}

func TestListFiltersAndChildrenCounts(t *testing.T) {
	for _, tc := range []struct {
		hide, query string
		want        []string
	}{
		{"", "", []string{`"name":"parent"` + `.*"children_count":1`, `"name":"child"` + `.*"children_count":null`, `"name":"sourced"` + `.*"children_count":1`}},
		{"", "?parent_only=true", []string{`"name":"parent"`, `"name":"sourced"`}},
		{"on", "", []string{`"name":"parent"`}},
		{"on", "?include_migrated=true", []string{`"name":"parent"`, `"name":"child"`, `"name":"sourced"`}},
		{"on", "?include_migrated=true&parent_only=true", []string{`"name":"parent"`, `"name":"sourced"`}},
	} {
		hide := tc.hide
		h := &handlers{store: &listReader{}, features: fixedFeatures{}, logger: quiet(),
			lookupEnv: func(string) (string, bool) { return hide, true }}
		body := record(h.listSyncConfigs, newRequest(t, "/x"+tc.query, uuid.NewString())).Body.String()
		if got := strings.Count(body, `"name":`); got != len(tc.want) {
			t.Errorf("hide=%q %s: %d configs in %s", tc.hide, tc.query, got, body)
			continue
		}
		for index, item := range strings.Split(strings.TrimSuffix(strings.TrimPrefix(body, "[{"), "}]"), "},{") {
			if !matchesAll(item, tc.want[index]) {
				t.Errorf("hide=%q %s item %d: %s, want %s", tc.hide, tc.query, index, item, tc.want[index])
			}
		}
	}
	h := &handlers{store: &listReader{}, features: fixedFeatures{}, logger: quiet(), lookupEnv: func(string) (string, bool) { return "", false }}
	if recorder := record(h.listSyncConfigs, newRequest(t, "/x?active_only=maybe&parent_only=2", uuid.NewString())); recorder.Code != http.StatusUnprocessableEntity ||
		strings.Count(recorder.Body.String(), `"bool_parsing"`) != 2 {
		t.Errorf("bad bools: %d %s", recorder.Code, recorder.Body.String())
	}
}

func matchesAll(text, pattern string) bool {
	rest := text
	for _, part := range strings.Split(pattern, ".*") {
		index := strings.Index(rest, part)
		if index < 0 {
			return false
		}
		rest = rest[index+len(part):]
	}
	return true
}

// TestUnparsableIDsReadNothing pins that an id uuid.UUID() refuses answers
// the route's 404 without a read (the failing store would make any read a
// 500), and that a backfill job whose sync_run marker is not a UUID reads
// no sync run.
func TestUnparsableIDsReadNothing(t *testing.T) {
	for _, tc := range []struct {
		name    string
		handler func(*handlers) http.HandlerFunc
		key     string
		detail  string
	}{
		{"get", func(h *handlers) http.HandlerFunc { return h.getSyncConfig }, "config_id", "Sync configuration not found"},
		{"repositories", func(h *handlers) http.HandlerFunc { return h.getRepositories }, "config_id", "Sync configuration not found"},
		{"jobs", func(h *handlers) http.HandlerFunc { return h.listJobs }, "config_id", "Sync configuration not found"},
		{"sync run", func(h *handlers) http.HandlerFunc { return h.getSyncRun }, "run_id", "Sync run not found"},
	} {
		reader := &faultReader{fail: "*"}
		h := &handlers{store: failEverything{reader}, features: fixedFeatures{}, logger: quiet()}
		request := newRequest(t, "/x", uuid.NewString())
		request.SetPathValue(tc.key, "not-a-uuid")
		recorder := record(tc.handler(h), request)
		if recorder.Code != http.StatusNotFound || recorder.Body.String() != `{"detail":"`+tc.detail+`"}` {
			t.Errorf("%s: %d %s", tc.name, recorder.Code, recorder.Body.String())
		}
	}
	reader := &markerReader{faultReader: faultReader{fail: "syncRunByID"}, task: "sync_run:not-a-uuid"}
	h := &handlers{store: reader, features: fixedFeatures{}, logger: quiet()}
	if recorder := record(h.listBackfillJobs, newRequest(t, "/x", uuid.NewString())); recorder.Code != http.StatusOK || reader.calls["syncRunByID"] != 0 {
		t.Errorf("backfill with a non-UUID marker: %d, %d sync run reads", recorder.Code, reader.calls["syncRunByID"])
	}
}

// failEverything fails every read.
type failEverything struct{ *faultReader }

func (f failEverything) configByID(context.Context, string, uuid.UUID) (*syncConfig, error) {
	return nil, errInjected
}
func (f failEverything) syncRunByID(context.Context, string, uuid.UUID) (*syncRun, error) {
	return nil, errInjected
}

// markerReader is faultReader with one backfill job carrying task.
type markerReader struct {
	faultReader
	task string
}

func (m *markerReader) backfillJobs(context.Context, string, int64, int64) ([]backfillJob, error) {
	return []backfillJob{{ID: uuid.New(), CeleryTaskID: &m.task}}, nil
}

func TestLatestIsTheLaterOfTwoNullableInstants(t *testing.T) {
	early, late := time.Unix(100, 0), time.Unix(200, 0)
	for _, tc := range []struct{ first, second, want *time.Time }{
		{nil, nil, nil}, {&early, nil, &early}, {nil, &late, &late}, {&early, &late, &late}, {&late, &early, &late},
	} {
		got := latest(tc.first, tc.second)
		if (got == nil) != (tc.want == nil) || (got != nil && !got.Equal(*tc.want)) {
			t.Errorf("latest(%v, %v) = %v, want %v", tc.first, tc.second, got, tc.want)
		}
	}
}

func TestSyncRunResponseWritesNoneForAMissingIntegration(t *testing.T) {
	body, err := syncRunResponse(&syncRun{ID: uuid.Nil})
	if err != nil || !strings.Contains(marshal(t, body), `"integration_id":"None"`) {
		t.Fatalf("%v %s", err, marshal(t, body))
	}
}

func TestJobRunResponseWithoutARollupOrADictRunResult(t *testing.T) {
	listResult := `[1]`
	planner := &syncRun{ID: uuid.Nil, Mode: "m", TriggeredBy: "t", Status: "planned", TotalUnits: 2, Result: &listResult}
	body, err := jobRunResponse(&jobRun{ID: uuid.Nil, JobID: uuid.Nil, TriggeredBy: "t"}, decode(t, `{"sync_run_id":"x"}`), planner, nil)
	if err != nil {
		t.Fatal(err)
	}
	got := marshal(t, body)
	for _, want := range []string{`"status":"pending"`, `"result":{"sync_run_id":"x","sync_run_status":"planned","total_units":2,"completed_units":0,"failed_units":0}`,
		`"requested_range":null,"covered_range":null`} {
		if !strings.Contains(got, want) {
			t.Errorf("%s lacks %s", got, want)
		}
	}
}

func record(handler http.HandlerFunc, request *http.Request) *httptest.ResponseRecorder {
	recorder := httptest.NewRecorder()
	handler(recorder, request)
	return recorder
}

func newRequest(t *testing.T, target, org string) *http.Request {
	t.Helper()
	request, err := http.NewRequest(http.MethodGet, target, nil)
	if err != nil {
		t.Fatal(err)
	}
	return request.WithContext(policy.WithUser(request.Context(), &policy.User{OrgID: org, Role: "admin"}))
}
