package providersync

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

type jiraWorkItemsDoer struct {
	t        *testing.T
	mu       sync.Mutex
	paths    []string
	search   int
	comments int
}

type jiraWorkItemsDoerFunc func(*http.Request) (*http.Response, error)

func (doer jiraWorkItemsDoerFunc) Do(request *http.Request) (*http.Response, error) {
	return doer(request)
}

func (doer *jiraWorkItemsDoer) Do(request *http.Request) (*http.Response, error) {
	doer.t.Helper()
	doer.mu.Lock()
	doer.paths = append(doer.paths, request.URL.String())
	doer.mu.Unlock()
	var body string
	switch {
	case request.URL.Path == "/rest/api/3/search/jql":
		doer.search++
		body = `{"issues":[{"key":"OPS-101","self":"https://acme.atlassian.net/rest/api/3/issue/OPS-101","fields":{"project":{"key":"OPS","id":"10001","name":"Operations"},"summary":"Repair the delivery path","description":"Customer-visible repair","status":{"name":"Done","statusCategory":{"key":"done"}},"issuetype":{"name":"Bug"},"labels":["bug"],"priority":{"name":"Highest"},"created":"2026-07-20T08:00:00Z","updated":"2026-07-21T09:30:00Z","resolutiondate":"2026-07-21T09:00:00Z","customfield_10020":[{"id":"9001","name":"July support"}],"issuelinks":[{"type":{"outward":"blocks","inward":"is blocked by"},"outwardIssue":{"key":"OPS-102"}}]},"changelog":{"histories":[{"created":"2026-07-20T09:00:00Z","author":{"accountId":"jira-account-1"},"items":[{"field":"status","fromString":"To Do","toString":"Done"}]},{"created":"2026-07-21T10:00:00Z","author":{"accountId":"jira-account-1"},"items":[{"field":"status","fromString":"Done","toString":"To Do"}]}]}}],"isLast":true}`
	case strings.HasPrefix(request.URL.Path, "/rest/api/3/issue/OPS-101/comment"):
		doer.comments++
		body = `{"comments":[{"created":"2026-07-21T11:00:00Z","author":{"accountId":"jira-commenter"},"body":"verified"}],"isLast":true}`
	case request.URL.Path == "/rest/agile/1.0/sprint/9001":
		body = `{"id":9001,"name":"July support","state":"closed","startDate":"2026-07-01T00:00:00Z","endDate":"2026-07-15T00:00:00Z","completeDate":"2026-07-16T00:00:00Z"}`
	default:
		doer.t.Fatalf("unexpected Jira request %s", request.URL.String())
	}
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       io.NopCloser(strings.NewReader(body)), Request: request,
	}, nil
}

func jiraRouteIdentity(email, accountID, displayName string) string {
	if email != "" {
		return strings.ToLower(email)
	}
	if accountID != "" {
		return "jira:accountid:" + accountID
	}
	return displayName
}

func jiraWorkItemsTestClient(t *testing.T, doer providerfoundation.HTTPDoer, lease providerfoundation.LeaseGuard) *providerfoundation.HTTPClient {
	t.Helper()
	client, err := providerfoundation.NewHTTPClient(
		"jira", "https://acme.atlassian.net", doer,
		func(request *http.Request) error {
			request.SetBasicAuth("jira@example.com", "token")
			request.Header.Set("Accept", "application/json")
			return nil
		}, providerfoundation.RetryPolicy{MaxAttempts: 1, InitialWait: time.Millisecond, MaxWait: time.Millisecond}, lease,
	)
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func TestJiraWorkItemsRouteCollectsCanonicalFamilyAndWithholdsWatermarkOnOptionalFailure(t *testing.T) {
	claim := nativeTestClaim("jira", "work-items")
	claim.SourceExternalID = "OPS"
	claim.DatasetOptions = map[string]any{
		"fetch_comments": true,
		"comments_limit": 10,
		"sprint_field":   "customfield_10020",
	}
	leaseChecks := 0
	doer := &jiraWorkItemsDoer{t: t}
	client := jiraWorkItemsTestClient(t, doer, providerfoundation.LeaseGuardFunc(func(context.Context) error {
		leaseChecks++
		return nil
	}))
	batch, err := (JiraWorkItemsRouteHandler{
		StatusMapping: loadRealStatusMapping(t), Identity: jiraRouteIdentity,
	}).Collect(context.Background(), claim, providerfoundation.Credential{}, client, time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	if batch.Watermark == nil || !batch.Watermark.Equal(*claim.BeforeAt) {
		t.Fatalf("watermark=%v want=%v", batch.Watermark, claim.BeforeAt)
	}
	if len(batch.Effects) != 6 {
		t.Fatalf("effects=%v", batch.Effects)
	}
	if batch.Evidence.Records != 1 || batch.Evidence.Pages != 1 || batch.Evidence.Requests != 3 {
		t.Fatalf("evidence=%+v", batch.Evidence)
	}
	foundSearch := false
	for _, rawPath := range doer.paths {
		parsed, parseErr := url.Parse(rawPath)
		if parseErr != nil || parsed.Path != "/rest/api/3/search/jql" {
			continue
		}
		foundSearch = true
		wantJQL := "project = 'OPS' AND (updated >= '2026-07-01' OR (statusCategory != Done AND created <= '2026-07-31')) ORDER BY updated DESC"
		if got := parsed.Query().Get("jql"); got != wantJQL {
			t.Fatalf("JQL=%q want=%q", got, wantJQL)
		}
	}
	if !foundSearch {
		t.Fatalf("search request not observed: %v", doer.paths)
	}
	if leaseChecks < 3 {
		t.Fatalf("lease checks=%d, provider request chain was not fenced", leaseChecks)
	}
	if got := batch.Result["interactions_synced"]; got != 1 {
		t.Fatalf("result=%v", batch.Result)
	}
	var workItem jiraWorkItemRow
	var transition jiraWorkItemTransitionRow
	var dependency jiraWorkItemDependencyRow
	var reopen jiraWorkItemReopenRow
	var interaction jiraWorkItemInteractionRow
	var sprint jiraSprintRow
	for _, effect := range batch.Effects {
		if len(effect.Rows) == 0 {
			continue
		}
		switch effect.Destination {
		case "work_items":
			_ = json.Unmarshal(effect.Rows[0], &workItem)
		case "work_item_transitions":
			_ = json.Unmarshal(effect.Rows[0], &transition)
		case "work_item_dependencies":
			_ = json.Unmarshal(effect.Rows[0], &dependency)
		case "work_item_reopen_events":
			_ = json.Unmarshal(effect.Rows[0], &reopen)
		case "work_item_interactions":
			_ = json.Unmarshal(effect.Rows[0], &interaction)
		case "sprints":
			_ = json.Unmarshal(effect.Rows[0], &sprint)
		}
	}
	if workItem.WorkItemID != "jira:OPS-101" || workItem.Status != "done" || workItem.ServiceClass == nil || *workItem.ServiceClass != "expedite" {
		t.Fatalf("work item=%+v", workItem)
	}
	if transition.ToStatus != "done" || dependency.RelationshipType != "blocks" || dependency.SourceWorkItemID != "jira:OPS-101" {
		t.Fatalf("transition=%+v dependency=%+v", transition, dependency)
	}
	if reopen.FromStatus != "done" || reopen.ToStatus != "todo" || interaction.BodyLength != len("verified") {
		t.Fatalf("reopen=%+v interaction=%+v", reopen, interaction)
	}
	if sprint.SprintID != "9001" || sprint.State == nil || *sprint.State != "closed" {
		t.Fatalf("sprint=%+v", sprint)
	}
}

func TestJiraWorkItemsRouteOptionalCommentFailureIsTypedAndDoesNotAdvance(t *testing.T) {
	claim := nativeTestClaim("jira", "work-items")
	claim.SourceExternalID = "OPS"
	claim.DatasetOptions = map[string]any{"fetch_comments": true}
	doer := jiraWorkItemsDoerFunc(func(request *http.Request) (*http.Response, error) {
		if request.URL.Path == "/rest/api/3/search/jql" {
			return &http.Response{StatusCode: http.StatusOK, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(`{"issues":[{"key":"OPS-103","self":"https://acme.atlassian.net/rest/api/3/issue/OPS-103","fields":{"project":{"key":"OPS"},"summary":"Optional child failure","status":{"name":"Open","statusCategory":{"key":"new"}},"issuetype":{"name":"Task"},"labels":[],"created":"2026-07-20T00:00:00Z","updated":"2026-07-21T00:00:00Z"},"changelog":{"histories":[]}}],"isLast":true}`)), Request: request}, nil
		}
		return nil, errors.New("comments endpoint unavailable")
	})
	client := jiraWorkItemsTestClient(t, doer, providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }))
	batch, err := (JiraWorkItemsRouteHandler{StatusMapping: loadRealStatusMapping(t)}).Collect(
		context.Background(), claim, providerfoundation.Credential{}, client, time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC),
	)
	if err != nil {
		t.Fatal(err)
	}
	if batch.Watermark != nil || len(batch.Effects) != 6 {
		t.Fatalf("batch=%+v", batch)
	}
	incomplete, ok := batch.Result["incomplete"].([]string)
	if !ok || len(incomplete) != 1 || incomplete[0] != "comments:jira:OPS-103" {
		t.Fatalf("typed incomplete=%#v", batch.Result["incomplete"])
	}
}

func TestJiraWorkItemsRouteFailsClosedWhenJiraDoesNotProvideNextPageToken(t *testing.T) {
	claim := nativeTestClaim("jira", "work-items")
	claim.SourceExternalID = "OPS"
	doer := jiraWorkItemsDoerFunc(func(request *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK, Header: http.Header{},
			Body: io.NopCloser(strings.NewReader(`{"issues":[],"isLast":false}`)), Request: request,
		}, nil
	})
	client := jiraWorkItemsTestClient(t, doer, providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }))
	batch, err := (JiraWorkItemsRouteHandler{StatusMapping: loadRealStatusMapping(t)}).Collect(
		context.Background(), claim, providerfoundation.Credential{}, client, time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC),
	)
	if err != ErrPaginationCapExceeded || batch.Watermark != nil || len(batch.Effects) != 0 {
		t.Fatalf("batch=%+v err=%v", batch, err)
	}
}

type jiraRecordingEffectAdapter struct {
	writeCalls   int
	inspectCalls int
	lastIdentity GitHubWorkItemEffectIdentity
}

func (adapter *jiraRecordingEffectAdapter) WriteGitHubWorkItemEffect(_ context.Context, identity GitHubWorkItemEffectIdentity, effect EffectBatch) error {
	adapter.writeCalls++
	adapter.lastIdentity = identity
	if effect.Recovery != EffectReadbackRequired || identity.Provider != "jira" || identity.OrgID == "" {
		return ErrInvalidConfiguration
	}
	return nil
}

func (adapter *jiraRecordingEffectAdapter) InspectGitHubWorkItemEffect(_ context.Context, identity GitHubWorkItemEffectIdentity, effect EffectBatch) (EffectInspection, error) {
	adapter.inspectCalls++
	adapter.lastIdentity = identity
	if effect.Recovery != EffectReadbackRequired || identity.Provider != "jira" || identity.OrgID == "" {
		return EffectConflict, ErrInvalidConfiguration
	}
	return EffectExact, nil
}

func TestJiraWorkItemsEffectsFenceTenantLeaseAndReadbackRecovery(t *testing.T) {
	claim := nativeTestClaim("jira", "work-items")
	rows := jiraWorkItemRows{WorkItems: []jiraWorkItemRow{{WorkItemID: "jira:OPS-1", Provider: "jira", Title: "x", Type: "task", Status: "todo", Assignees: []string{}, Labels: []string{}, CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC(), OrgID: claim.OrgID, LastSynced: time.Now().UTC()}}}
	effects, err := BuildJiraWorkItemEffects(rows)
	if err != nil {
		t.Fatal(err)
	}
	adapter := &jiraRecordingEffectAdapter{}
	leaseCalls := 0
	sink := JiraWorkItemClickHouseEffects{
		Lease:   providerfoundation.LeaseGuardFunc(func(context.Context) error { leaseCalls++; return nil }),
		Sprints: adapter, Dependencies: adapter, Interactions: adapter, Reopens: adapter,
		Transitions: adapter, WorkItems: adapter,
	}
	for _, effect := range effects {
		if err := sink.WriteEffect(context.Background(), claim, effect); err != nil {
			t.Fatal(err)
		}
		inspection, err := sink.InspectEffect(context.Background(), claim, effect)
		if err != nil || inspection != EffectExact {
			t.Fatalf("destination=%s inspection=%v err=%v", effect.Destination, inspection, err)
		}
	}
	if adapter.writeCalls != 6 || adapter.inspectCalls != 6 || leaseCalls != 18 || adapter.lastIdentity.Provider != "jira" || adapter.lastIdentity.OrgID != claim.OrgID {
		t.Fatalf("adapter=%+v leaseCalls=%d", adapter, leaseCalls)
	}
	foreign := claim
	foreign.OrgID = "other-org"
	if err := sink.WriteEffect(context.Background(), foreign, effects[len(effects)-1]); err == nil {
		t.Fatal("foreign tenant write unexpectedly accepted")
	}
}
