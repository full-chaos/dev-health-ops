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

// D18 ratified the Projects v2 COLLECTOR contract. It did not activate the
// route, and the two are easy to conflate now that nothing in the source says
// "pending" any more. This pins the distinction: the collector is decided, the
// five-alias family is still off, and activation remains the composite
// all-or-nothing layer's job.
func TestGitHubProjectV2RatificationIsNotActivation(t *testing.T) {
	if got := ProviderExecutor("github", "work-items"); got != ExecutorNone {
		t.Fatalf("ratifying the Projects v2 collector must not register the route: %s", got)
	}
	descriptor, known := (CompleteRouteSwitches{}).Descriptor("github", "work-items")
	if !known {
		t.Fatal("github/work-items capability disappeared")
	}
	if descriptor.RouteReady || descriptor.RouteEnabled || len(descriptor.Destinations) != 0 {
		t.Fatalf("ratifying the Projects v2 collector activated the route: %+v", descriptor)
	}
}

// D18 puts the environment outside the Go route. The positive half of that
// ("durable config wins") is pinned above; this is the negative half, and it
// asserts the REQUEST COUNTER rather than the row set. An assertion on empty
// rows would pass just as happily with the whole collector deleted, and would
// also pass if Go quietly adopted the env targets and the fixture returned
// nothing — the only observation that separates "ignored the environment" from
// "used the environment and found nothing" is that no request was ever issued.
func TestGitHubProjectV2EnvironmentTargetsAreNeverAFallback(t *testing.T) {
	t.Setenv("GITHUB_PROJECTS_V2", "acme:3,labs:12")
	t.Setenv("GITHUB_TOKEN", "ghp_environment_token_that_must_never_be_used")
	claim := githubWorkItemOracleClaim()
	claim.IntegrationConfig = map[string]any{}

	targets, err := githubProjectV2Targets(claim)
	if err != nil || len(targets) != 0 {
		t.Fatalf("environment targets leaked into durable config: targets=%+v error=%v", targets, err)
	}

	// The whole fetch, not just the parser: a target list is only half the
	// path, and a collector that re-read the environment further down would
	// still be caught here.
	doer := &gitHubProjectV2Doer{t: t}
	result, err := (GitHubProjectV2Fetcher{}).Fetch(
		context.Background(), claim,
		providerfoundation.Credential{Provider: "github", ID: claim.CredentialID},
		githubProjectV2TestClient(t, doer), time.Now().UTC(), nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(doer.bodies) != 0 {
		t.Fatalf("environment configuration issued %d GraphQL request(s); D18 puts the "+
			"environment outside the Go route entirely", len(doer.bodies))
	}
	if result.Targets != 0 || result.Evidence.Requests != 0 || result.Usage.RequestCount != 0 {
		t.Fatalf("environment configuration produced request accounting: %+v", result)
	}
}

// The credential half of the same clause. An environment token is present and
// the claim's resolved credential is not usable; the collector must refuse
// rather than reach for the one lying around in the process. Each rejected
// credential shape is asserted to issue ZERO requests, so "refused" cannot be
// satisfied by fetching first and erroring afterwards.
func TestGitHubProjectV2RefusesEnvironmentTokenWhenClaimCredentialIsUnusable(t *testing.T) {
	t.Setenv("GITHUB_TOKEN", "ghp_environment_token_that_must_never_be_used")
	t.Setenv("GITHUB_PROJECTS_V2", "acme:3")
	claim := githubWorkItemOracleClaim()
	claim.IntegrationConfig = map[string]any{"github_projects_v2": []any{
		map[string]any{"org_login": "acme", "project_number": 3},
	}}
	for _, test := range []struct {
		name       string
		credential providerfoundation.Credential
	}{
		{"absent", providerfoundation.Credential{}},
		{"unresolved id", providerfoundation.Credential{Provider: "github"}},
		{"other tenant's credential", providerfoundation.Credential{
			Provider: "github", ID: "77777777-7777-4777-8777-777777777777",
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			doer := &gitHubProjectV2Doer{t: t}
			_, err := (GitHubProjectV2Fetcher{}).Fetch(
				context.Background(), claim, test.credential,
				githubProjectV2TestClient(t, doer), time.Now().UTC(), nil,
			)
			if !errors.Is(err, ErrInvalidConfiguration) {
				t.Fatalf("error=%v want ErrInvalidConfiguration", err)
			}
			if len(doer.bodies) != 0 {
				t.Fatalf("refused credential still issued %d request(s)", len(doer.bodies))
			}
		})
	}
}

// The strongest expression of D18's no-environment-credentials clause lives one
// layer above this collector: Unit.Validate refuses a claim whose AuthSource is
// "environment" outright, so a unit whose credentials would come from process
// state can never reach any Go collector at all.
//
// That fence had NO test anywhere in the package and no mutation covering it.
// It was found by a SURVIVING mutation on this file's own `credential.ID == ""`
// clause — which survived precisely BECAUSE this fence already guarantees a
// resolved, non-empty credential id upstream. The redundant clause is gone; the
// property it was pretending to cover is now asserted where it actually lives.
//
// This is the literal "environment configured, no integration credential"
// scenario: it must fail, not fall back.
func TestGitHubProjectV2RefusesClaimsAuthoredFromTheEnvironment(t *testing.T) {
	t.Setenv("GITHUB_TOKEN", "ghp_environment_token_that_must_never_be_used")
	t.Setenv("GITHUB_PROJECTS_V2", "acme:3")
	claim := githubWorkItemOracleClaim()
	claim.IntegrationConfig = map[string]any{"github_projects_v2": []any{
		map[string]any{"org_login": "acme", "project_number": 3},
	}}
	credential := providerfoundation.Credential{Provider: "github", ID: claim.CredentialID}

	// Baseline: this exact claim is otherwise usable, so the refusal below is
	// attributable to AuthSource alone and not to some other invalid field.
	if err := claim.Validate(); err != nil {
		t.Fatalf("baseline claim is not valid, so the AuthSource case proves nothing: %v", err)
	}

	claim.AuthSource = "environment"
	if err := claim.Validate(); !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("an environment-authored claim validated: %v", err)
	}
	doer := &gitHubProjectV2Doer{t: t}
	if _, err := (GitHubProjectV2Fetcher{}).Fetch(
		context.Background(), claim, credential,
		githubProjectV2TestClient(t, doer), time.Now().UTC(), nil,
	); !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("error=%v want ErrInvalidConfiguration", err)
	}
	if len(doer.bodies) != 0 {
		t.Fatalf("environment-authored claim issued %d request(s)", len(doer.bodies))
	}
}

func TestGitHubProjectV2TargetsFailClosedOnMalformedDurableConfig(t *testing.T) {
	for _, value := range []any{
		[]any{map[string]any{"org_login": "", "project_number": 1}},
		[]any{map[string]any{"org_login": "acme", "project_number": 0}},
		[]any{map[string]any{"org_login": "acme", "project_number": 1, "token": "forbidden"}},
		"acme:1",
		// A present key holding a typed nil slice decodes cleanly to a nil
		// target list. Without the nil check this reads as "configured, with
		// nothing in it" -- indistinguishable from a genuinely empty list, so
		// a config the operator wrote would be silently ignored rather than
		// rejected. The key being present at all is the signal that they meant
		// something by it.
		[]any(nil),
		[]any{nil},
		map[string]any{"org_login": "acme", "project_number": 1},
		[]any{map[string]any{"org_login": "acme", "project_number": 3.7}},
		[]any{map[string]any{"org_login": "   ", "project_number": 1}},
		[]any{map[string]any{"org_login": "acme", "project_number": -1}},
	} {
		claim := githubWorkItemOracleClaim()
		claim.IntegrationConfig = map[string]any{"github_projects_v2": value}
		if _, err := githubProjectV2Targets(claim); !errors.Is(err, ErrInvalidConfiguration) {
			t.Fatalf("value=%#v error=%v", value, err)
		}
	}
}

// Every test above builds IntegrationConfig as a Go literal, with `int` or
// json.Number project numbers. PRODUCTION NEVER PRODUCES EITHER: the claim's
// integration_config arrives as a Postgres JSONB column decoded by a plain
// json.Unmarshal into map[string]any (repository_postgres.go), so every number
// is a float64. A parser that happened to accept only int/json.Number would
// pass this package's whole suite and reject every real tenant's configuration.
//
// This builds the config the way the repository does -- from JSON bytes -- so
// the representation under test is the one production hands us.
func TestGitHubProjectV2TargetsAcceptThePostgresJSONRepresentation(t *testing.T) {
	var integrationConfig map[string]any
	if err := json.Unmarshal([]byte(
		`{"github_projects_v2":[{"org_login":"acme","project_number":3},`+
			`{"org_login":"labs","project_number":12}]}`,
	), &integrationConfig); err != nil {
		t.Fatal(err)
	}
	// Guard the guard: if this stops being float64, the gap this test exists to
	// close has moved and the test would otherwise keep passing for the wrong
	// representation.
	first := integrationConfig["github_projects_v2"].([]any)[0].(map[string]any)
	if _, isFloat := first["project_number"].(float64); !isFloat {
		t.Fatalf("project_number decoded as %T, not float64 -- this test is no "+
			"longer exercising the production representation", first["project_number"])
	}

	claim := githubWorkItemOracleClaim()
	claim.IntegrationConfig = integrationConfig
	targets, err := githubProjectV2Targets(claim)
	if err != nil {
		t.Fatalf("the production JSONB representation was rejected: %v", err)
	}
	want := []GitHubProjectV2Target{{OrgLogin: "acme", ProjectNumber: 3}, {OrgLogin: "labs", ProjectNumber: 12}}
	if !reflect.DeepEqual(targets, want) {
		t.Fatalf("targets=%+v want=%+v", targets, want)
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
