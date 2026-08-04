package providersync

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

func TestGitHubProjectV2TargetsComeOnlyFromClaimIntegrationConfig(t *testing.T) {
	claim := githubWorkItemOracleClaim()
	claim.IntegrationConfig = map[string]any{"github_projects_v2": []any{
		map[string]any{"org_login": " acme ", "project_number": 3},
		map[string]any{"org_login": "labs", "project_number": json.Number("12")},
	}}
	t.Setenv("GITHUB_PROJECTS_V2", "ignored:99")
	targets, err := githubProjectV2Targets(claim)
	if err != nil {
		t.Fatal(err)
	}
	want := []GitHubProjectV2Target{{OrgLogin: "acme", ProjectNumber: 3}, {OrgLogin: "labs", ProjectNumber: 12}}
	if !reflect.DeepEqual(targets, want) {
		t.Fatalf("targets=%+v want=%+v", targets, want)
	}
}

func TestGitHubProjectV2ActivationDecisionRemainsPending(t *testing.T) {
	if got := ProviderExecutor("github", "work-items"); got != ExecutorNone {
		t.Fatalf("Projects v2 foundation was registered before the Linear activation decision: %s", got)
	}
	descriptor, known := (CompleteRouteSwitches{}).Descriptor("github", "work-items")
	if !known {
		t.Fatal("github/work-items capability disappeared")
	}
	if descriptor.RouteReady || descriptor.RouteEnabled || len(descriptor.Destinations) != 0 {
		t.Fatalf("unapproved Projects v2 route became active: %+v", descriptor)
	}
}

func TestGitHubProjectV2TargetsFailClosedOnMalformedDurableConfig(t *testing.T) {
	for _, value := range []any{
		[]any{map[string]any{"org_login": "", "project_number": 1}},
		[]any{map[string]any{"org_login": "acme", "project_number": 0}},
		[]any{map[string]any{"org_login": "acme", "project_number": 1, "token": "forbidden"}},
		"acme:1",
	} {
		claim := githubWorkItemOracleClaim()
		claim.IntegrationConfig = map[string]any{"github_projects_v2": value}
		if _, err := githubProjectV2Targets(claim); !errors.Is(err, ErrInvalidConfiguration) {
			t.Fatalf("value=%#v error=%v", value, err)
		}
	}
}

type gitHubProjectV2Doer struct {
	t        *testing.T
	replies  []string
	statuses []int
	bodies   []map[string]any
}

func (doer *gitHubProjectV2Doer) Do(request *http.Request) (*http.Response, error) {
	doer.t.Helper()
	if request.URL.Path != "/graphql" {
		doer.t.Fatalf("unexpected path %s", request.URL.Path)
	}
	var body map[string]any
	decoder := json.NewDecoder(request.Body)
	decoder.UseNumber()
	if err := decoder.Decode(&body); err != nil {
		doer.t.Fatal(err)
	}
	doer.bodies = append(doer.bodies, body)
	if len(doer.bodies) > len(doer.replies) {
		doer.t.Fatalf("unexpected request %d", len(doer.bodies))
	}
	index := len(doer.bodies) - 1
	status := http.StatusOK
	if index < len(doer.statuses) && doer.statuses[index] != 0 {
		status = doer.statuses[index]
	}
	return &http.Response{
		StatusCode: status,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       io.NopCloser(strings.NewReader(doer.replies[index])),
		Request:    request,
	}, nil
}

func TestGitHubProjectV2FetcherCompletesOuterAndNestedPagination(t *testing.T) {
	doer := &gitHubProjectV2Doer{t: t, replies: []string{
		`{"data":{"organization":{"projectV2":{"items":{"nodes":[{"id":"PVTI_1","content":{"__typename":"Issue","number":7,"title":"Ship it","state":"OPEN","createdAt":"2026-08-01T08:00:00Z","updatedAt":"2026-08-02T08:00:00Z","repository":{"nameWithOwner":"acme/api"},"labels":{"nodes":[]},"assignees":{"nodes":[]}},"fieldValues":{"nodes":[]},"changes":{"nodes":[{"field":{"name":"Status"},"previousValue":{"name":"Todo"},"newValue":{"name":"Doing"},"createdAt":"2026-08-01T09:00:00Z","actor":{"login":"octocat"}}],"pageInfo":{"hasNextPage":true,"endCursor":"change-1"}}}],"pageInfo":{"hasNextPage":true,"endCursor":"item-1"}}}}}}`,
		`{"data":{"node":{"changes":{"nodes":[{"field":{"name":"Status"},"previousValue":{"name":"Doing"},"newValue":{"name":"Done"},"createdAt":"2026-08-02T09:00:00Z","actor":{"login":"octocat"}}],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}}`,
		`{"data":{"organization":{"projectV2":{"items":{"nodes":[{"id":"PVTI_2","content":{"__typename":"PullRequest","number":8,"title":"not a work item"},"fieldValues":{"nodes":[]},"changes":{"nodes":[],"pageInfo":{"hasNextPage":false,"endCursor":null}}}],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}}}`,
	}}
	client := githubProjectV2TestClient(t, doer)
	claim := githubWorkItemOracleClaim()
	claim.IntegrationConfig = map[string]any{"github_projects_v2": []any{map[string]any{"org_login": "acme", "project_number": 3}}}
	credential := providerfoundation.Credential{Provider: "github", ID: claim.CredentialID}
	result, err := (GitHubProjectV2Fetcher{}).Fetch(
		context.Background(), claim, credential, client,
		time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC), nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Rows.WorkItems) != 1 || len(result.Rows.StatusTransitions) != 2 {
		t.Fatalf("rows=%+v", result.Rows)
	}
	if result.Evidence.Pages != 3 || result.Evidence.Requests != 3 || result.Evidence.Records != 3 {
		t.Fatalf("evidence=%+v", result.Evidence)
	}
	if got := result.Rows.WorkItems[0]; got.WorkItemID != "gh:acme/api#7" || got.RepoID != nil || got.ProjectID == nil || *got.ProjectID != "ghprojv2:acme#3" {
		t.Fatalf("work item=%+v", got)
	}
	if len(doer.bodies) != 3 || doer.bodies[1]["query"] == doer.bodies[0]["query"] {
		t.Fatalf("requests=%+v", doer.bodies)
	}
	outerQuery := doer.bodies[0]["query"].(string)
	for _, leaf := range []string{"items(first: $first", "labels(first: 50)", "assignees(first: 10)", "fieldValues(first: 20)", "changes(first: 100"} {
		if !strings.Contains(outerQuery, leaf) {
			t.Errorf("query missing documented leaf bound %q", leaf)
		}
	}
}

func TestGitHubProjectV2FetcherFailsClosedOnUnusableCursors(t *testing.T) {
	for _, replies := range [][]string{
		{
			`{"data":{"organization":{"projectV2":{"items":{"nodes":[],"pageInfo":{"hasNextPage":true,"endCursor":null}}}}}}`,
			`{"data":{"organization":{"projectV2":{"items":{"nodes":[],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}}}`,
		},
		{
			`{"data":{"organization":{"projectV2":{"items":{"nodes":[{"id":"PVTI_1","content":{"__typename":"DraftIssue","title":"Draft","createdAt":"2026-08-01T08:00:00Z","updatedAt":"2026-08-01T08:00:00Z"},"fieldValues":{"nodes":[]},"changes":{"nodes":[],"pageInfo":{"hasNextPage":true,"endCursor":null}}}],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}}}`,
			`{"data":{"node":{"changes":{"nodes":[],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}}`,
		},
	} {
		doer := &gitHubProjectV2Doer{t: t, replies: replies}
		claim := githubWorkItemOracleClaim()
		claim.IntegrationConfig = map[string]any{"github_projects_v2": []any{map[string]any{"org_login": "acme", "project_number": 3}}}
		_, err := (GitHubProjectV2Fetcher{}).Fetch(context.Background(), claim,
			providerfoundation.Credential{Provider: "github", ID: claim.CredentialID},
			githubProjectV2TestClient(t, doer), time.Now().UTC(), nil)
		if !errors.Is(err, providerfoundation.ErrPaginationInvalid) {
			t.Fatalf("replies=%v error=%v", replies, err)
		}
	}
}

func TestGitHubProjectV2FetcherRequiresClaimResolvedCredentialAndClient(t *testing.T) {
	claim := githubWorkItemOracleClaim()
	claim.IntegrationConfig = map[string]any{"github_projects_v2": []any{map[string]any{"org_login": "acme", "project_number": 3}}}
	client := githubProjectV2TestClient(t, &gitHubProjectV2Doer{t: t, replies: []string{
		`{"data":{"organization":{"projectV2":{"items":{"nodes":[],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}}}`,
	}})
	for _, credential := range []providerfoundation.Credential{
		{Provider: "github", ID: "77777777-7777-4777-8777-777777777777"},
		{Provider: "gitlab", ID: claim.CredentialID},
	} {
		if _, err := (GitHubProjectV2Fetcher{}).Fetch(context.Background(), claim, credential, client, time.Now().UTC(), nil); !errors.Is(err, ErrInvalidConfiguration) {
			t.Fatalf("credential=%+v error=%v", credential.SafeAttributes(), err)
		}
	}
}

func TestGitHubProjectV2FetcherCountsPhysicalRetriesButReservesOnce(t *testing.T) {
	doer := &gitHubProjectV2Doer{
		t: t, statuses: []int{http.StatusServiceUnavailable, http.StatusOK},
		replies: []string{
			`{"message":"unavailable"}`,
			`{"data":{"organization":{"projectV2":{"items":{"nodes":[],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}}}`,
		},
	}
	client := githubProjectV2TestClient(t, doer)
	client.Retry.MaxAttempts = 2
	budget := &gitHubProjectV2Budget{}
	client.Budget = budget
	client.BudgetKey = providerfoundation.BudgetKey{
		Provider: "github", OrgID: "org-acme", Host: "api.github.com",
		CostClass: "medium", Limit: 1, TTL: time.Minute,
	}
	claim := githubWorkItemOracleClaim()
	claim.IntegrationConfig = map[string]any{"github_projects_v2": []any{
		map[string]any{"org_login": "acme", "project_number": 3},
	}}
	result, err := (GitHubProjectV2Fetcher{}).Fetch(
		context.Background(), claim,
		providerfoundation.Credential{Provider: "github", ID: claim.CredentialID},
		client, time.Now().UTC(), nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	if result.Evidence.Requests != 2 || result.Evidence.Pages != 1 ||
		result.Usage != (GitHubProjectV2Usage{
			Transport: "graphql", RouteFamily: "work_item_prs",
			Dimension: BudgetGraphQLCost, RequestCount: 2,
		}) || budget.acquires != 1 || budget.releases != 1 {
		t.Fatalf("result=%+v budget=%+v", result, budget)
	}
}

func TestGitHubProjectV2FetcherRetainsPhysicalUsageOnTerminalError(t *testing.T) {
	doer := &gitHubProjectV2Doer{
		t: t, statuses: []int{http.StatusServiceUnavailable, http.StatusServiceUnavailable},
		replies: []string{`{"message":"unavailable"}`, `{"message":"still unavailable"}`},
	}
	client := githubProjectV2TestClient(t, doer)
	client.Retry.MaxAttempts = 2
	budget := &gitHubProjectV2Budget{}
	client.Budget = budget
	client.BudgetKey = providerfoundation.BudgetKey{
		Provider: "github", OrgID: "org-acme", Host: "api.github.com",
		CostClass: "medium", Limit: 1, TTL: time.Minute,
	}
	claim := githubWorkItemOracleClaim()
	claim.IntegrationConfig = map[string]any{"github_projects_v2": []any{
		map[string]any{"org_login": "acme", "project_number": 3},
	}}
	result, err := (GitHubProjectV2Fetcher{}).Fetch(
		context.Background(), claim,
		providerfoundation.Credential{Provider: "github", ID: claim.CredentialID},
		client, time.Now().UTC(), nil,
	)
	var providerErr *providerfoundation.ProviderError
	if !errors.As(err, &providerErr) || providerErr.Class != providerfoundation.ErrorTransient {
		t.Fatalf("error=%v", err)
	}
	if result.Targets != 1 || result.Evidence.Requests != 2 || result.Evidence.Pages != 0 ||
		result.Usage.RequestCount != 2 || budget.acquires != 1 || budget.releases != 1 {
		t.Fatalf("result=%+v budget=%+v", result, budget)
	}
}

func TestGitHubProjectV2FetcherPreservesTemporaryPerClaimFanout(t *testing.T) {
	doer := &gitHubProjectV2Doer{t: t, replies: []string{
		`{"data":{"organization":{"projectV2":{"items":{"nodes":[],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}}}`,
		`{"data":{"organization":{"projectV2":{"items":{"nodes":[],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}}}`,
	}}
	client := githubProjectV2TestClient(t, doer)
	for _, source := range []string{"acme/api", "acme/web"} {
		claim := githubWorkItemOracleClaim()
		claim.SourceExternalID = source
		claim.IntegrationConfig = map[string]any{"github_projects_v2": []any{
			map[string]any{"org_login": "acme", "project_number": 3},
		}}
		result, err := (GitHubProjectV2Fetcher{}).Fetch(
			context.Background(), claim,
			providerfoundation.Credential{Provider: "github", ID: claim.CredentialID},
			client, time.Now().UTC(), nil,
		)
		if err != nil || result.Targets != 1 {
			t.Fatalf("source=%s result=%+v error=%v", source, result, err)
		}
	}
	if len(doer.bodies) != 2 {
		t.Fatalf("requests=%d want one target traversal per source claim", len(doer.bodies))
	}
}

func TestMergeGitHubProjectV2RowsPreservesPythonLastWinsAndTransitionAppend(t *testing.T) {
	repository := githubWorkItemRows{
		WorkItems:         []githubWorkItemRow{{WorkItemID: "same", Title: "repository"}, {WorkItemID: "repo-only", Title: "repo"}},
		StatusTransitions: []githubWorkItemTransitionRow{{WorkItemID: "same", ToStatus: "todo"}},
		Dependencies:      []githubWorkItemDependencyRow{{SourceWorkItemID: "same"}},
	}
	project := githubWorkItemRows{
		WorkItems:         []githubWorkItemRow{{WorkItemID: "same", Title: "project"}, {WorkItemID: "project-only", Title: "project"}},
		StatusTransitions: []githubWorkItemTransitionRow{{WorkItemID: "same", ToStatus: "done"}},
	}
	got := mergeGitHubProjectV2Rows(repository, project)
	if titles := []string{got.WorkItems[0].Title, got.WorkItems[1].Title, got.WorkItems[2].Title}; !reflect.DeepEqual(titles, []string{"project", "repo", "project"}) {
		t.Fatalf("titles=%v", titles)
	}
	if len(got.StatusTransitions) != 2 || !reflect.DeepEqual(got.Dependencies, repository.Dependencies) {
		t.Fatalf("rows=%+v", got)
	}
}

func githubProjectV2TestClient(t *testing.T, doer providerfoundation.HTTPDoer) *providerfoundation.HTTPClient {
	t.Helper()
	client, err := providerfoundation.NewHTTPClient(
		"github", "https://api.github.com", doer, func(*http.Request) error { return nil },
		providerfoundation.RetryPolicy{MaxAttempts: 1, InitialWait: time.Nanosecond, MaxWait: time.Nanosecond},
		providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }),
	)
	if err != nil {
		t.Fatal(err)
	}
	return client
}

type gitHubProjectV2Budget struct{ acquires, releases int }

func (budget *gitHubProjectV2Budget) Acquire(
	context.Context, providerfoundation.BudgetKey,
) (providerfoundation.Reservation, error) {
	budget.acquires++
	return gitHubProjectV2Reservation{budget: budget}, nil
}

type gitHubProjectV2Reservation struct{ budget *gitHubProjectV2Budget }

func (reservation gitHubProjectV2Reservation) Release(context.Context) error {
	reservation.budget.releases++
	return nil
}
