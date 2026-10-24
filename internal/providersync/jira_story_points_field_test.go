package providersync

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

const jiraFieldListBoth = `[
 {"id":"summary","name":"Summary","custom":false,"schema":{"type":"string"}},
 {"id":"customfield_10016","name":"Story point estimate","custom":true,"schema":{"type":"number"}},
 {"id":"customfield_10042","name":"Story Points","custom":true,"schema":{"type":"number"}},
 {"id":"customfield_10099","name":"Story Points","custom":true,"schema":{"type":"string"}}
]`

func decodeJiraFieldList(t *testing.T, raw string) []jiraFieldMetadata {
	t.Helper()
	var fields []jiraFieldMetadata
	if err := json.Unmarshal([]byte(raw), &fields); err != nil {
		t.Fatal(err)
	}
	return fields
}

func TestPickJiraStoryPointsFieldsOrdersByNameAndRequiresANumberField(t *testing.T) {
	got := pickJiraStoryPointsFields(context.Background(), decodeJiraFieldList(t, jiraFieldListBoth))
	if strings.Join(got, ",") != "customfield_10042,customfield_10016" {
		t.Fatalf("picked=%v want the classic Story Points number field, then the estimate field; the string-typed namesake is never used", got)
	}
	only := `[{"id":"customfield_10016","name":"Story point estimate","custom":true,"schema":{"type":"number"}}]`
	if got := pickJiraStoryPointsFields(context.Background(), decodeJiraFieldList(t, only)); strings.Join(got, ",") != "customfield_10016" {
		t.Fatalf("picked=%v", got)
	}
	none := `[{"id":"customfield_1","name":"Story Points","custom":false,"schema":{"type":"number"}},{"id":"customfield_2","name":"Points","custom":true,"schema":{"type":"number"}}]`
	if got := pickJiraStoryPointsFields(context.Background(), decodeJiraFieldList(t, none)); len(got) != 0 {
		t.Fatalf("picked=%v want none", got)
	}
}

func jiraStoryPointsOf(t *testing.T, options map[string]any, discovered []string, extra map[string]any) *float64 {
	t.Helper()
	input := jiraWorkItemOraclePrepCases()[0].Input
	raw := input["raw_issue"].(map[string]any)
	fields := map[string]any{}
	for key, value := range raw["fields"].(map[string]any) {
		fields[key] = value
	}
	for key, value := range extra {
		fields[key] = value
	}
	claim := nativeTestClaim("jira", "work-items")
	claim = jiraFixtureWindow(claim)
	claim.OrgID = input["org_id"].(string)
	claim.DatasetOptions = options
	row, _, err := normalizeJiraWorkItem(
		claim, jiraWorkItemFixtureInput{Raw: map[string]any{"key": raw["key"], "self": raw["self"], "fields": fields}, PointsFields: discovered},
		loadRealStatusMapping(t), jiraOracleIdentity, time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC),
	)
	if err != nil {
		t.Fatal(err)
	}
	return row.StoryPoints
}

func TestJiraWorkItemStoryPointsSelection(t *testing.T) {
	both := []string{"customfield_10042", "customfield_10016"}
	cases := []struct {
		name       string
		options    map[string]any
		discovered []string
		extra      map[string]any
		want       *float64
	}{
		{"configured field wins over discovery", map[string]any{"story_points_field": "customfield_555"}, both,
			map[string]any{"customfield_555": 8.0, "customfield_10042": 3.0}, jiraPointsFloatPtr(8)},
		{"configured field is the only one read", map[string]any{"story_points_field": "customfield_555"}, both,
			map[string]any{"customfield_10042": 3.0}, nil},
		{"unset, Story Points present", nil, both, map[string]any{"customfield_10042": 5.0}, jiraPointsFloatPtr(5)},
		{"unset, only the estimate field exists", nil, []string{"customfield_10016"}, map[string]any{"customfield_10016": 2.0}, jiraPointsFloatPtr(2)},
		{"both exist, value only in the estimate", nil, both, map[string]any{"customfield_10016": 13.0}, jiraPointsFloatPtr(13)},
		{"both hold a value, Story Points first", nil, both, map[string]any{"customfield_10042": 5.0, "customfield_10016": 13.0}, jiraPointsFloatPtr(5)},
		{"malformed classic value falls back to the estimate", nil, both, map[string]any{"customfield_10042": "n/a", "customfield_10016": 8.0}, jiraPointsFloatPtr(8)},
		{"malformed value alone stays unsized", nil, both, map[string]any{"customfield_10042": "n/a", "customfield_10016": nil}, nil},
		{"both exist, both null", nil, both, map[string]any{"customfield_10042": nil, "customfield_10016": nil}, nil},
		{"neither field exists", nil, nil, map[string]any{"customfield_10042": 5.0}, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := jiraStoryPointsOf(t, tc.options, tc.discovered, tc.extra)
			if (got == nil) != (tc.want == nil) || (got != nil && *got != *tc.want) {
				t.Fatalf("story_points=%v want %v", derefFloat(got), derefFloat(tc.want))
			}
		})
	}
}

func jiraPointsFloatPtr(v float64) *float64 { return &v }

func derefFloat(v *float64) any {
	if v == nil {
		return nil
	}
	return *v
}

type jiraPointsRoute struct {
	mu         sync.Mutex
	fieldCalls int
	fieldBody  string
	fieldCode  int
}

func (route *jiraPointsRoute) Do(request *http.Request) (*http.Response, error) {
	respond := func(code int, body string) (*http.Response, error) {
		return &http.Response{StatusCode: code, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(strings.NewReader(body)), Request: request}, nil
	}
	switch {
	case request.URL.Path == "/rest/api/3/field":
		route.mu.Lock()
		route.fieldCalls++
		route.mu.Unlock()
		return respond(route.fieldCode, route.fieldBody)
	case request.URL.Path == "/rest/api/3/search/jql":
		return respond(http.StatusOK, `{"issues":[`+
			`{"id":"1","key":"OPS-1","self":"https://acme.atlassian.net/rest/api/3/issue/OPS-1","fields":{"project":{"key":"OPS"},"summary":"sized","status":{"name":"Open","statusCategory":{"key":"new"}},"issuetype":{"name":"Task"},"labels":[],"created":"2026-08-01T00:00:00Z","updated":"2026-08-01T00:00:00Z","customfield_10042":5,"customfield_10016":null},"changelog":{"histories":[]}},`+
			`{"id":"2","key":"OPS-2","self":"https://acme.atlassian.net/rest/api/3/issue/OPS-2","fields":{"project":{"key":"OPS"},"summary":"estimate only","status":{"name":"Open","statusCategory":{"key":"new"}},"issuetype":{"name":"Task"},"labels":[],"created":"2026-08-01T00:00:00Z","updated":"2026-08-01T00:00:00Z","customfield_10042":null,"customfield_10016":8},"changelog":{"histories":[]}},`+
			`{"id":"3","key":"OPS-3","self":"https://acme.atlassian.net/rest/api/3/issue/OPS-3","fields":{"project":{"key":"OPS"},"summary":"unsized","status":{"name":"Open","statusCategory":{"key":"new"}},"issuetype":{"name":"Task"},"labels":[],"created":"2026-08-01T00:00:00Z","updated":"2026-08-01T00:00:00Z"},"changelog":{"histories":[]}}],"isLast":true}`)
	case strings.HasSuffix(request.URL.Path, "/changelog"):
		return respond(http.StatusOK, `{"values":[],"total":0,"isLast":true}`)
	}
	return respond(http.StatusNotFound, `{}`)
}

func jiraPointsByKey(t *testing.T, batch CompleteRouteBatch) map[string]*float64 {
	t.Helper()
	out := map[string]*float64{}
	for _, effect := range batch.Effects {
		if effect.Destination != "work_items" {
			continue
		}
		for _, raw := range effect.Rows {
			var row jiraWorkItemRow
			if err := json.Unmarshal(raw, &row); err != nil {
				t.Fatal(err)
			}
			out[row.WorkItemID] = row.StoryPoints
		}
	}
	return out
}

func collectJiraPoints(t *testing.T, live bool, options map[string]any, route *jiraPointsRoute) CompleteRouteBatch {
	t.Helper()
	batch, err := tryCollectJiraPoints(t, live, options, route)
	if err != nil {
		t.Fatal(err)
	}
	return batch
}

func tryCollectJiraPoints(t *testing.T, live bool, options map[string]any, route *jiraPointsRoute) (CompleteRouteBatch, error) {
	t.Helper()
	claim := nativeTestClaim("jira", "work-items")
	claim = jiraFixtureWindow(claim)
	claim.SourceExternalID = "OPS"
	claim.DatasetOptions = options
	client := jiraWorkItemsTestClient(t, route, providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }))
	now := time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC)
	var batch CompleteRouteBatch
	var err error
	if live {
		batch, err = jiraAtlassianCompleteHandler(t).Collect(context.Background(), claim, providerfoundation.Credential{}, client, now)
	} else {
		batch, err = (JiraWorkItemsRouteHandler{StatusMapping: loadRealStatusMapping(t)}).Collect(context.Background(), claim, providerfoundation.Credential{}, client, now)
	}
	return batch, err
}

func TestJiraRoutesDiscoverTheStoryPointsFieldWhenUnset(t *testing.T) {
	for _, live := range []bool{true, false} {
		name := "work-items route"
		if live {
			name = "atlassian route"
		}
		t.Run(name, func(t *testing.T) {
			route := &jiraPointsRoute{fieldCode: http.StatusOK, fieldBody: jiraFieldListBoth}
			batch := collectJiraPoints(t, live, map[string]any{"fetch_comments": false}, route)
			got := jiraPointsByKey(t, batch)
			if route.fieldCalls != 1 {
				t.Fatalf("field list requests=%d want one per run", route.fieldCalls)
			}
			if got["jira:OPS-1"] == nil || *got["jira:OPS-1"] != 5 || got["jira:OPS-2"] == nil || *got["jira:OPS-2"] != 8 || got["jira:OPS-3"] != nil {
				t.Fatalf("points=%v want OPS-1=5 (Story Points), OPS-2=8 (estimate fallback), OPS-3=NULL", got)
			}
			if batch.Watermark == nil {
				t.Fatal("a clean discovery must not hold the watermark")
			}
		})
	}
}

func TestJiraRoutesSkipDiscoveryWhenTheFieldIsConfigured(t *testing.T) {
	for _, live := range []bool{true, false} {
		route := &jiraPointsRoute{fieldCode: http.StatusOK, fieldBody: jiraFieldListBoth}
		batch := collectJiraPoints(t, live, map[string]any{"fetch_comments": false, "story_points_field": "customfield_10016"}, route)
		got := jiraPointsByKey(t, batch)
		if route.fieldCalls != 0 {
			t.Fatalf("live=%v field list requested %d times with story_points_field set", live, route.fieldCalls)
		}
		if got["jira:OPS-1"] != nil || got["jira:OPS-2"] == nil || *got["jira:OPS-2"] != 8 {
			t.Fatalf("live=%v points=%v want only the configured field read", live, got)
		}
	}
}

func TestJiraRoutesStopTheRunWhenTheFieldListFails(t *testing.T) {
	type response struct {
		code int
		body string
	}
	bodies := map[string]response{
		"forbidden":  {http.StatusForbidden, `{}`},
		"null body":  {http.StatusOK, `null`},
		"empty list": {http.StatusOK, `[]`},
		"not a list": {http.StatusOK, `{"fields":[]}`},
	}
	for name, seed := range bodies {
		for _, live := range []bool{true, false} {
			route := &jiraPointsRoute{fieldCode: seed.code, fieldBody: seed.body}
			batch, err := tryCollectJiraPoints(t, live, map[string]any{"fetch_comments": false}, route)
			if err == nil || batch.Watermark != nil || len(batch.Effects) != 0 {
				t.Fatalf("%s live=%v err=%v watermark=%v effects=%d want the run stopped with no rows (a NULL row would overwrite stored points)", name, live, err, batch.Watermark, len(batch.Effects))
			}
		}
	}
}

func TestJiraRoutesLeaveUnsizedNullWhenNeitherFieldExists(t *testing.T) {
	route := &jiraPointsRoute{fieldCode: http.StatusOK, fieldBody: `[{"id":"summary","name":"Summary","custom":false,"schema":{"type":"string"}}]`}
	batch := collectJiraPoints(t, true, map[string]any{"fetch_comments": false}, route)
	for key, points := range jiraPointsByKey(t, batch) {
		if points != nil {
			t.Fatalf("%s points=%v want NULL", key, *points)
		}
	}
	if batch.Watermark == nil {
		t.Fatal("no points field on the site is not an incomplete run")
	}
}

func TestJiraRoutesIgnoreANonNumberStoryPointsField(t *testing.T) {
	for _, live := range []bool{true, false} {
		route := &jiraPointsRoute{fieldCode: http.StatusOK, fieldBody: `[{"id":"customfield_10042","name":"Story Points","custom":true,"schema":{"type":"string"}},{"id":"customfield_10016","name":"Story point estimate","custom":true,"schema":{"type":"option"}}]`}
		batch := collectJiraPoints(t, live, map[string]any{"fetch_comments": false}, route)
		got := jiraPointsByKey(t, batch)
		if len(got) != 3 {
			t.Fatalf("live=%v rows=%d want 3", live, len(got))
		}
		for key, points := range got {
			if points != nil {
				t.Fatalf("live=%v %s sized %v from a non-number field", live, key, *points)
			}
		}
	}
}
