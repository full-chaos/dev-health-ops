package providersync

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

type jiraIncidentDoer struct {
	t        *testing.T
	requests []string
}

type jiraIncidentDoerFunc func(*http.Request) (*http.Response, error)

func (doer jiraIncidentDoerFunc) Do(request *http.Request) (*http.Response, error) {
	return doer(request)
}

type jiraIncidentEntitlementFunc func(context.Context, string) error

func (require jiraIncidentEntitlementFunc) Require(ctx context.Context, orgID string) error {
	return require(ctx, orgID)
}

var allowJiraIncidentEntitlement = jiraIncidentEntitlementFunc(
	func(context.Context, string) error { return nil },
)

func (doer *jiraIncidentDoer) Do(request *http.Request) (*http.Response, error) {
	doer.t.Helper()
	body := ""
	switch request.URL.String() {
	case "https://acme.atlassian.net/_edge/tenant_info":
		body = `{"cloudId":"cloud-123"}`
	case "https://acme.atlassian.net/rest/servicedeskapi/servicedesk?limit=100&start=0":
		body = `{"values":[{"projectKey":"JSM"}],"isLastPage":true}`
	case "https://acme.atlassian.net/rest/api/3/search/jql":
		if request.Method != http.MethodPost || request.Header.Get("Content-Type") != "application/json" {
			doer.t.Fatalf("JQL request method=%s content-type=%q", request.Method, request.Header.Get("Content-Type"))
		}
		var payload map[string]any
		if err := json.NewDecoder(request.Body).Decode(&payload); err != nil {
			doer.t.Fatal(err)
		}
		if !strings.Contains(payload["jql"].(string), `project in (JSM)`) {
			doer.t.Fatalf("JQL=%q", payload["jql"])
		}
		body = `{"issues":[{"id":"10001","key":"JSM-1","fields":{"summary":"API down","created":"2026-07-22T10:00:00Z","updated":"2026-07-22T10:05:00Z","resolutiondate":null,"status":{"name":"Investigating","statusCategory":{"key":"indeterminate"}},"priority":{"name":"Highest"}}},{"id":"10002","key":"JSM-2","fields":{"summary":"Ordinary request","created":"2026-07-22T11:00:00Z","updated":"2026-07-22T11:05:00Z","resolutiondate":null,"status":{"name":"Open","statusCategory":{"key":"new"}},"priority":null}}],"isLast":true}`
	case "https://api.atlassian.com/jsm/incidents/cloudId/cloud-123/v1/incident/10001":
		body = `{}`
	case "https://api.atlassian.com/jsm/incidents/cloudId/cloud-123/v1/incident/10002":
		doer.requests = append(doer.requests, request.URL.String())
		return &http.Response{
			StatusCode: http.StatusNotFound,
			Header:     http.Header{"Content-Type": []string{"application/json"}},
			Body:       io.NopCloser(strings.NewReader(`{"error":"not found"}`)),
			Request:    request,
		}, nil
	default:
		doer.t.Fatalf("unexpected Jira request %s", request.URL.String())
	}
	doer.requests = append(doer.requests, request.URL.String())
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       io.NopCloser(strings.NewReader(body)),
		Request:    request,
	}, nil
}

func TestJiraIncidentsRouteCollectsOnlyNativelyAdmittedJSMIncidents(t *testing.T) {
	t.Parallel()
	normalizedAt := time.Date(2026, 7, 23, 12, 30, 0, 0, time.UTC)
	claim := nativeTestClaim("jira", "incidents")
	claim.SourceExternalID = "JSM"
	doer := &jiraIncidentDoer{t: t}
	client, err := providerfoundation.NewHTTPClient(
		"jira", "https://acme.atlassian.net", doer,
		func(request *http.Request) error {
			request.Header.Set("Accept", "application/json")
			request.Header.Set("Content-Type", "application/json")
			return nil
		},
		providerfoundation.RetryPolicy{MaxAttempts: 1, InitialWait: time.Millisecond, MaxWait: time.Millisecond},
		providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }),
	)
	if err != nil {
		t.Fatal(err)
	}

	batch, err := (JiraIncidentRouteHandler{Entitlement: allowJiraIncidentEntitlement}).Collect(
		context.Background(), claim, providerfoundation.Credential{}, client, normalizedAt,
	)
	if err != nil {
		t.Fatal(err)
	}
	descriptor, ok := (CompleteRouteSwitches{}).Descriptor("jira", "incidents")
	if !ok || !descriptor.RouteReady || descriptor.Executor != ExecutorNativeGo {
		t.Fatalf("descriptor=%+v ok=%v", descriptor, ok)
	}
	if err := batch.validate(descriptor); err != nil {
		t.Fatal(err)
	}
	if batch.Watermark == nil || claim.BeforeAt == nil || !batch.Watermark.Equal(*claim.BeforeAt) {
		t.Fatalf("watermark=%v want=%v", batch.Watermark, claim.BeforeAt)
	}
	if len(batch.Effects) != 1 || batch.Effects[0].Destination != "operational_incidents" ||
		batch.Effects[0].Recovery != EffectReadbackRequired || len(batch.Effects[0].Rows) != 1 {
		t.Fatalf("effects=%+v", batch.Effects)
	}
	var row jiraIncidentRow
	if err := json.Unmarshal(batch.Effects[0].Rows[0], &row); err != nil {
		t.Fatal(err)
	}
	if row.OrgID != claim.OrgID || row.Provider != "jira" || row.ProviderInstanceID != "cloud-123" ||
		row.ExternalID != "10001" || row.SourceEventID == nil || *row.SourceEventID != "JSM-1" ||
		row.NormalizedStatus == nil || *row.NormalizedStatus != "active" || row.Title != "API down" {
		t.Fatalf("row=%+v", row)
	}
	if got := len(doer.requests); got != 5 {
		t.Fatalf("requests=%d want=5: %v", got, doer.requests)
	}
}

func TestJiraIncidentsRouteFailsClosedBeforeWatermarkOnIncompleteTraversal(t *testing.T) {
	t.Parallel()
	claim := nativeTestClaim("jira", "incidents")
	claim.SourceExternalID = "JSM"
	doer := jiraIncidentDoerFunc(func(request *http.Request) (*http.Response, error) {
		body := `{"cloudId":"cloud-123"}`
		if request.URL.Path == "/rest/servicedeskapi/servicedesk" {
			body = `{"values":[{"projectKey":"JSM"}],"isLastPage":false}`
		}
		return &http.Response{StatusCode: http.StatusOK, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(body)), Request: request}, nil
	})
	client, err := providerfoundation.NewHTTPClient(
		"jira", "https://acme.atlassian.net", doer, func(*http.Request) error { return nil },
		providerfoundation.RetryPolicy{MaxAttempts: 1, InitialWait: time.Millisecond, MaxWait: time.Millisecond},
		providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }),
	)
	if err != nil {
		t.Fatal(err)
	}
	batch, err := (JiraIncidentRouteHandler{
		Entitlement: allowJiraIncidentEntitlement, MaxPages: 1,
	}).Collect(
		context.Background(), claim, providerfoundation.Credential{}, client,
		time.Date(2026, 7, 23, 12, 30, 0, 0, time.UTC),
	)
	if err == nil || batch.Watermark != nil || len(batch.Effects) != 0 {
		t.Fatalf("batch=%+v err=%v", batch, err)
	}
}

func TestJiraIncidentsRouteRejectsNullIssueInventory(t *testing.T) {
	t.Parallel()
	claim := nativeTestClaim("jira", "incidents")
	claim.SourceExternalID = "JSM"
	doer := jiraIncidentDoerFunc(func(request *http.Request) (*http.Response, error) {
		body := `{"cloudId":"cloud-123"}`
		switch request.URL.Path {
		case "/rest/servicedeskapi/servicedesk":
			body = `{"values":[{"projectKey":"JSM"}],"isLastPage":true}`
		case "/rest/api/3/search/jql":
			body = `{"issues":null,"isLast":true}`
		}
		return &http.Response{
			StatusCode: http.StatusOK, Header: http.Header{},
			Body: io.NopCloser(strings.NewReader(body)), Request: request,
		}, nil
	})
	client, err := providerfoundation.NewHTTPClient(
		"jira", "https://acme.atlassian.net", doer, func(*http.Request) error { return nil },
		providerfoundation.RetryPolicy{MaxAttempts: 1, InitialWait: time.Millisecond, MaxWait: time.Millisecond},
		providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }),
	)
	if err != nil {
		t.Fatal(err)
	}
	batch, err := (JiraIncidentRouteHandler{Entitlement: allowJiraIncidentEntitlement}).Collect(
		context.Background(), claim, providerfoundation.Credential{}, client,
		time.Date(2026, 7, 23, 12, 30, 0, 0, time.UTC),
	)
	if err == nil || batch.Watermark != nil || len(batch.Effects) != 0 {
		t.Fatalf("batch=%+v err=%v", batch, err)
	}
}

func TestJiraIncidentsRouteRejectsDisabledEntitlementBeforeProviderFetch(t *testing.T) {
	t.Parallel()
	claim := nativeTestClaim("jira", "incidents")
	claim.SourceExternalID = "JSM"
	requests := 0
	doer := jiraIncidentDoerFunc(func(*http.Request) (*http.Response, error) {
		requests++
		return nil, errors.New("provider fetch must not run")
	})
	client, err := providerfoundation.NewHTTPClient(
		"jira", "https://acme.atlassian.net", doer, func(*http.Request) error { return nil },
		providerfoundation.RetryPolicy{MaxAttempts: 1, InitialWait: time.Millisecond, MaxWait: time.Millisecond},
		providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }),
	)
	if err != nil {
		t.Fatal(err)
	}
	batch, err := (JiraIncidentRouteHandler{
		Entitlement: jiraIncidentEntitlementFunc(func(context.Context, string) error {
			return ErrJiraIncidentEntitlementDisabled
		}),
	}).Collect(
		context.Background(), claim, providerfoundation.Credential{}, client,
		time.Date(2026, 7, 23, 12, 30, 0, 0, time.UTC),
	)
	if !errors.Is(err, ErrJiraIncidentEntitlementDisabled) || requests != 0 ||
		batch.Watermark != nil || len(batch.Effects) != 0 {
		t.Fatalf("requests=%d batch=%+v err=%v", requests, batch, err)
	}
}

func TestJiraIncidentsRouteRechecksRevokedEntitlementBeforePersistenceHandoff(t *testing.T) {
	t.Parallel()
	claim := nativeTestClaim("jira", "incidents")
	claim.SourceExternalID = "JSM"
	doer := &jiraIncidentDoer{t: t}
	client, err := providerfoundation.NewHTTPClient(
		"jira", "https://acme.atlassian.net", doer,
		func(request *http.Request) error {
			request.Header.Set("Accept", "application/json")
			request.Header.Set("Content-Type", "application/json")
			return nil
		},
		providerfoundation.RetryPolicy{MaxAttempts: 1, InitialWait: time.Millisecond, MaxWait: time.Millisecond},
		providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }),
	)
	if err != nil {
		t.Fatal(err)
	}
	checks := 0
	entitlement := jiraIncidentEntitlementFunc(func(context.Context, string) error {
		checks++
		if checks > 1 {
			return ErrJiraIncidentEntitlementDisabled
		}
		return nil
	})
	batch, err := (JiraIncidentRouteHandler{Entitlement: entitlement}).Collect(
		context.Background(), claim, providerfoundation.Credential{}, client,
		time.Date(2026, 7, 23, 12, 30, 0, 0, time.UTC),
	)
	if !errors.Is(err, ErrJiraIncidentEntitlementDisabled) || checks != 2 ||
		len(batch.Effects) != 0 || batch.Watermark != nil {
		t.Fatalf("entitlement checks=%d batch=%+v err=%v", checks, batch, err)
	}
}
