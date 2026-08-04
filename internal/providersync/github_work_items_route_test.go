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

type githubWorkItemsRouteDoer struct {
	t              *testing.T
	rest           *githubWorkItemsRESTDoer
	graphqlReplies []string
	graphqlCalls   int
}

func (doer *githubWorkItemsRouteDoer) Do(request *http.Request) (*http.Response, error) {
	doer.t.Helper()
	if request.URL.Path != "/graphql" {
		return doer.rest.Do(request)
	}
	doer.graphqlCalls++
	if doer.graphqlCalls > len(doer.graphqlReplies) {
		doer.t.Fatalf("unexpected GraphQL request %d", doer.graphqlCalls)
	}
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body: io.NopCloser(strings.NewReader(
			doer.graphqlReplies[doer.graphqlCalls-1],
		)),
		Request: request,
	}, nil
}

type githubWorkItemsRouteProjectPolicy struct {
	result GitHubProjectV2FetchResult
	err    error
	calls  int
}

func (policy *githubWorkItemsRouteProjectPolicy) Fetch(
	context.Context,
	Claim,
	providerfoundation.Credential,
	*providerfoundation.HTTPClient,
	time.Time,
	githubIdentityResolver,
) (GitHubProjectV2FetchResult, error) {
	policy.calls++
	return policy.result, policy.err
}

type githubWorkItemsRouteDeriver struct {
	rows  map[string][]json.RawMessage
	got   githubWorkItemRows
	err   error
	calls int
}

func (deriver *githubWorkItemsRouteDeriver) Derive(
	_ context.Context,
	_ Claim,
	rows githubWorkItemRows,
	_ time.Time,
) (map[string][]json.RawMessage, error) {
	deriver.calls++
	deriver.got = rows
	return deriver.rows, deriver.err
}

func githubWorkItemsRouteDerivedRows(t *testing.T) map[string][]json.RawMessage {
	t.Helper()
	rows := make(map[string][]json.RawMessage, len(githubWorkItemDerivedDestinations))
	for _, destination := range githubWorkItemDerivedDestinations {
		encoded, err := json.Marshal(map[string]any{
			"destination": destination,
			"value":       1,
		})
		if err != nil {
			t.Fatal(err)
		}
		rows[destination] = []json.RawMessage{encoded}
	}
	return rows
}

func TestGitHubWorkItemsRouteComposesRESTSocialProjectsDerivedRowsAndUsage(t *testing.T) {
	normalizedAt := time.Date(2026, 8, 4, 12, 0, 0, 123456000, time.UTC)
	claim := githubWorkItemsRESTClaim()
	claim.DatasetOptions["fetch_milestones"] = false
	claim.IntegrationConfig = map[string]any{"github_projects_v2": []any{
		map[string]any{"org_login": "acme", "project_number": 3},
	}}
	fixtures := githubWorkItemsRESTFixtures()
	delete(fixtures, "/repos/acme/api/milestones")
	doer := &githubWorkItemsRouteDoer{
		t:              t,
		rest:           &githubWorkItemsRESTDoer{t: t, replies: fixtures},
		graphqlReplies: []string{`{"data":{"repository":{"pr0":{"number":52,"comments":{"nodes":[{"databaseId":9007199254740993,"body":"social","createdAt":"2026-07-23T00:00:00Z","author":{"login":"reviewer"}}],"pageInfo":{"hasNextPage":false,"endCursor":null}},"timelineItems":{"nodes":[{"__typename":"ClosedEvent","createdAt":"2026-07-24T00:00:00Z","actor":{"login":"closer"}}],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}}}`},
	}
	client := gitHubPullRequestClient(t, doer, "https://api.github.com")
	projectRow := githubWorkItemRow{
		WorkItemID: "gh:Acme/API-Renamed#42", Provider: "github",
		Title: "project wins", Type: "issue", Status: "done",
		ProjectID: stringPointer("ghprojv2:acme#3"),
		Assignees: []string{}, Labels: []string{}, OrgID: claim.OrgID,
		CreatedAt: normalizedAt, UpdatedAt: normalizedAt,
	}
	projects := &githubWorkItemsRouteProjectPolicy{result: GitHubProjectV2FetchResult{
		Rows: githubWorkItemRows{
			WorkItems: []githubWorkItemRow{projectRow},
			StatusTransitions: []githubWorkItemTransitionRow{{
				WorkItemID: projectRow.WorkItemID, Provider: "github",
				OccurredAt: normalizedAt, FromStatus: "todo", ToStatus: "done",
				OrgID: claim.OrgID,
			}},
		},
		Evidence: FetchEvidence{Provider: "github", Dataset: "projects-v2", Requests: 3, Pages: 2, Records: 2},
		Usage: GitHubProjectV2Usage{
			Transport: "graphql", RouteFamily: "work_item_prs",
			Dimension: BudgetGraphQLCost, RequestCount: 3,
		},
		Targets: 1,
	}}
	deriver := &githubWorkItemsRouteDeriver{rows: githubWorkItemsRouteDerivedRows(t)}
	handler := GitHubWorkItemsRouteHandler{Projects: projects, Deriver: deriver}

	batch, err := handler.Collect(
		context.Background(), claim,
		providerfoundation.Credential{Provider: "github", ID: claim.CredentialID},
		client, normalizedAt,
	)
	if err != nil {
		t.Fatal(err)
	}
	if batch.Watermark != nil {
		t.Fatalf("unregistered route advanced watermark: %v", batch.Watermark)
	}
	if projects.calls != 1 || doer.graphqlCalls != 1 {
		t.Fatalf("project calls=%d social GraphQL calls=%d", projects.calls, doer.graphqlCalls)
	}
	if len(deriver.got.WorkItems) != 2 || deriver.got.WorkItems[0].Title != "project wins" ||
		deriver.got.WorkItems[1].WorkItemID != "ghpr:Acme/API-Renamed#52" {
		t.Fatalf("composed work items=%+v", deriver.got.WorkItems)
	}
	if len(deriver.got.StatusTransitions) != 3 ||
		deriver.got.StatusTransitions[1].WorkItemID != "ghpr:Acme/API-Renamed#52" ||
		deriver.got.StatusTransitions[1].ToStatus != "canceled" {
		t.Fatalf("PR-social events did not become transitions: %+v", deriver.got.StatusTransitions)
	}
	if got := deriver.got.StatusTransitions[len(deriver.got.StatusTransitions)-1]; got.WorkItemID != projectRow.WorkItemID || got.ToStatus != "done" {
		t.Fatalf("Projects v2 transition was not appended last: %+v", deriver.got.StatusTransitions)
	}
	if len(deriver.got.Interactions) != 2 || len(deriver.got.Dependencies) != 2 {
		t.Fatalf("composed rows=%+v", deriver.got)
	}

	wantDestinations := workItemRouteDestinations()
	gotDestinations := make([]string, 0, len(batch.Effects))
	for _, effect := range batch.Effects {
		gotDestinations = append(gotDestinations, effect.Destination)
		if effect.Recovery != EffectReadbackRequired {
			t.Errorf("effect %s recovery=%s, want readback-required", effect.Destination, effect.Recovery)
		}
	}
	if !reflect.DeepEqual(gotDestinations, wantDestinations) {
		t.Fatalf("destinations=%v want=%v", gotDestinations, wantDestinations)
	}
	workItemsEffect := githubWorkItemsRouteEffect(t, batch, "work_items")
	if len(workItemsEffect.Rows) != 2 {
		t.Fatalf("work_items rows=%s", workItemsEffect.Rows)
	}
	for _, destination := range githubWorkItemDerivedDestinations {
		if rows := githubWorkItemsRouteEffect(t, batch, destination).Rows; len(rows) != 1 {
			t.Fatalf("derived %s rows=%s", destination, rows)
		}
	}

	usage := githubWorkItemsRouteUsage(t, batch)
	wantUsage := []GitHubWorkItemsRequestUsage{
		{Transport: "graphql", RouteFamily: "work_item_prs", Dimension: BudgetGraphQLCost, RequestCount: 4},
		{Transport: "rest", RouteFamily: "work_items", Dimension: BudgetRESTCore, RequestCount: 6},
	}
	if !reflect.DeepEqual(usage, wantUsage) {
		t.Fatalf("usage=%+v want=%+v", usage, wantUsage)
	}
	if batch.Evidence.Requests != 10 || batch.Evidence.Pages != 7 || batch.Evidence.CapReached {
		t.Fatalf("evidence=%+v", batch.Evidence)
	}
	if incomplete := githubWorkItemsRouteIncomplete(t, batch); len(incomplete) != 0 {
		t.Fatalf("incomplete=%+v", incomplete)
	}
}

func TestGitHubWorkItemsRouteDefaultsProjectsToTypedPendingWithoutFetching(t *testing.T) {
	claim := githubWorkItemsRESTClaim()
	claim.DatasetOptions = map[string]any{
		"include_issues": false, "include_pull_requests": false,
		"fetch_comments": false, "fetch_milestones": false,
	}
	claim.IntegrationConfig = map[string]any{"github_projects_v2": []any{
		map[string]any{"org_login": "acme", "project_number": 3},
	}}
	doer := &githubWorkItemsRouteDoer{
		t: t,
		rest: &githubWorkItemsRESTDoer{t: t, replies: map[string][]githubWorkItemsRESTReply{
			"/repos/acme/api": {{body: `{"id":4567,"full_name":"Acme/API"}`}},
		}},
	}
	deriver := &githubWorkItemsRouteDeriver{rows: githubWorkItemsRouteDerivedRows(t)}
	batch, err := (GitHubWorkItemsRouteHandler{
		Deriver: deriver,
	}).Collect(
		context.Background(), claim,
		providerfoundation.Credential{Provider: "github", ID: claim.CredentialID},
		gitHubPullRequestClient(t, doer, "https://api.github.com"), time.Now().UTC(),
	)
	if !errors.Is(err, ErrGitHubWorkItemsIncomplete) {
		t.Fatalf("error=%v", err)
	}
	if !reflect.DeepEqual(batch, CompleteRouteBatch{}) || deriver.calls != 0 {
		t.Fatalf("incomplete route returned batch=%+v or derived %d times", batch, deriver.calls)
	}
	if doer.graphqlCalls != 0 {
		t.Fatalf("pending default fetched Projects v2 %d times", doer.graphqlCalls)
	}
	routeErr := githubWorkItemsIncompleteError(t, err)
	incomplete := routeErr.Incomplete
	if !reflect.DeepEqual(incomplete, []GitHubWorkItemsIncomplete{{
		Component: "projects_v2", Cause: "policy_pending",
	}}) {
		t.Fatalf("incomplete=%+v", incomplete)
	}
	if usage := routeErr.Usage; !reflect.DeepEqual(usage, []GitHubWorkItemsRequestUsage{{
		Transport: "rest", RouteFamily: "work_items", Dimension: BudgetRESTCore, RequestCount: 1,
	}}) {
		t.Fatalf("usage=%+v", usage)
	}
}

func TestGitHubWorkItemsRoutePreservesOptionalSocialFailureAndPhysicalUsage(t *testing.T) {
	claim := githubWorkItemsRESTClaim()
	claim.DatasetOptions = map[string]any{
		"include_issues": false, "include_pull_requests": true,
		"fetch_comments": true, "fetch_milestones": false, "comments_limit": 500,
	}
	fixtures := githubWorkItemsRESTFixtures()
	for path := range fixtures {
		if path != "/repos/acme/api" && path != "/repos/acme/api/pulls" && path != "/repos/acme/api/pulls/52" {
			delete(fixtures, path)
		}
	}
	doer := &githubWorkItemsRouteDoer{
		t:              t,
		rest:           &githubWorkItemsRESTDoer{t: t, replies: fixtures},
		graphqlReplies: []string{`{"errors":[{"message":"unavailable"}],"data":{"repository":null}}`},
	}
	deriver := &githubWorkItemsRouteDeriver{rows: githubWorkItemsRouteDerivedRows(t)}
	batch, err := (GitHubWorkItemsRouteHandler{
		Deriver: deriver,
	}).Collect(
		context.Background(), claim,
		providerfoundation.Credential{Provider: "github", ID: claim.CredentialID},
		gitHubPullRequestClient(t, doer, "https://api.github.com"), time.Now().UTC(),
	)
	if !errors.Is(err, ErrGitHubWorkItemsIncomplete) {
		t.Fatalf("error=%v", err)
	}
	if !reflect.DeepEqual(batch, CompleteRouteBatch{}) || deriver.calls != 0 {
		t.Fatalf("incomplete route returned batch=%+v or derived %d times", batch, deriver.calls)
	}
	routeErr := githubWorkItemsIncompleteError(t, err)
	if incomplete := routeErr.Incomplete; !reflect.DeepEqual(incomplete, []GitHubWorkItemsIncomplete{{
		Component: "pr_social", Cause: "invalid_response",
	}}) {
		t.Fatalf("incomplete=%+v", incomplete)
	}
	if usage := routeErr.Usage; !reflect.DeepEqual(usage, []GitHubWorkItemsRequestUsage{
		{Transport: "graphql", RouteFamily: "work_item_prs", Dimension: BudgetGraphQLCost, RequestCount: 1},
		{Transport: "rest", RouteFamily: "work_items", Dimension: BudgetRESTCore, RequestCount: 3},
	}) {
		t.Fatalf("usage=%+v", usage)
	}
}

func TestGitHubWorkItemsRouteRejectsRESTIncompleteBeforeDerivationAndEffects(t *testing.T) {
	claim := githubWorkItemsRESTClaim()
	claim.DatasetOptions["include_pull_requests"] = false
	doer := &githubWorkItemsRouteDoer{
		t: t,
		rest: &githubWorkItemsRESTDoer{t: t, replies: map[string][]githubWorkItemsRESTReply{
			"/repos/acme/api":                    {{body: `{"id":4567,"full_name":"Acme/API"}`}},
			"/repos/acme/api/milestones":         {{status: http.StatusInternalServerError, body: `{"message":"down"}`}},
			"/repos/acme/api/issues":             {{body: `[{"number":42,"title":"Issue","state":"open","created_at":"2026-07-02T00:00:00Z","updated_at":"2026-07-20T00:00:00Z"}]`}},
			"/repos/acme/api/issues/42/events":   {{body: `[]`}},
			"/repos/acme/api/issues/42/comments": {{status: http.StatusBadGateway, body: `{"message":"down"}`}},
		}},
	}
	deriver := &githubWorkItemsRouteDeriver{rows: githubWorkItemsRouteDerivedRows(t)}
	batch, err := (GitHubWorkItemsRouteHandler{Deriver: deriver}).Collect(
		context.Background(), claim,
		providerfoundation.Credential{Provider: "github", ID: claim.CredentialID},
		gitHubPullRequestClient(t, doer, "https://api.github.com"), time.Now().UTC(),
	)
	if !errors.Is(err, ErrGitHubWorkItemsIncomplete) {
		t.Fatalf("error=%v", err)
	}
	if !reflect.DeepEqual(batch, CompleteRouteBatch{}) || deriver.calls != 0 {
		t.Fatalf("incomplete route returned batch=%+v or derived %d times", batch, deriver.calls)
	}
	routeErr := githubWorkItemsIncompleteError(t, err)
	wantIncomplete := []GitHubWorkItemsIncomplete{
		{Component: "milestones", Cause: "transient"},
		{Component: "issue_comments", SubjectID: "42", Cause: "transient"},
	}
	if !reflect.DeepEqual(routeErr.Incomplete, wantIncomplete) ||
		routeErr.Evidence.Requests != 5 || routeErr.Evidence.Records != 1 ||
		!reflect.DeepEqual(routeErr.Usage, []GitHubWorkItemsRequestUsage{{
			Transport: "rest", RouteFamily: "work_items", Dimension: BudgetRESTCore, RequestCount: 5,
		}}) {
		t.Fatalf("route error=%+v", routeErr)
	}
}

func TestGitHubWorkItemsRouteFailsBeforeFetchWithoutDerivedImplementation(t *testing.T) {
	claim := githubWorkItemsRESTClaim()
	doer := &githubWorkItemsRouteDoer{
		t: t, rest: &githubWorkItemsRESTDoer{t: t, replies: githubWorkItemsRESTFixtures()},
	}
	_, err := (GitHubWorkItemsRouteHandler{}).Collect(
		context.Background(), claim,
		providerfoundation.Credential{Provider: "github", ID: claim.CredentialID},
		gitHubPullRequestClient(t, doer, "https://api.github.com"), time.Now().UTC(),
	)
	if !errors.Is(err, ErrGitHubWorkItemsDerivationsUnavailable) {
		t.Fatalf("error=%v", err)
	}
	if len(doer.rest.requests) != 0 || doer.graphqlCalls != 0 {
		t.Fatalf("requests happened before completeness preflight: REST=%v GraphQL=%d", doer.rest.requests, doer.graphqlCalls)
	}
}

func TestGitHubWorkItemsRouteFailsBeforeFetchOnMalformedProjectsConfiguration(t *testing.T) {
	claim := githubWorkItemsRESTClaim()
	claim.IntegrationConfig = map[string]any{"github_projects_v2": "acme:3"}
	doer := &githubWorkItemsRouteDoer{
		t: t, rest: &githubWorkItemsRESTDoer{t: t, replies: githubWorkItemsRESTFixtures()},
	}
	_, err := (GitHubWorkItemsRouteHandler{
		Deriver: &githubWorkItemsRouteDeriver{rows: githubWorkItemsRouteDerivedRows(t)},
	}).Collect(
		context.Background(), claim,
		providerfoundation.Credential{Provider: "github", ID: claim.CredentialID},
		gitHubPullRequestClient(t, doer, "https://api.github.com"), time.Now().UTC(),
	)
	if !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("error=%v", err)
	}
	if len(doer.rest.requests) != 0 || doer.graphqlCalls != 0 {
		t.Fatalf("requests happened before projects config validation: REST=%v GraphQL=%d", doer.rest.requests, doer.graphqlCalls)
	}
}

func TestGitHubWorkItemsRouteErrorRetainsRequiredPhasePhysicalUsage(t *testing.T) {
	claim := githubWorkItemsRESTClaim()
	claim.DatasetOptions = map[string]any{
		"include_issues": false, "include_pull_requests": false,
		"fetch_comments": false, "fetch_milestones": false,
	}
	claim.IntegrationConfig = map[string]any{"github_projects_v2": []any{
		map[string]any{"org_login": "acme", "project_number": 3},
	}}
	doer := &githubWorkItemsRouteDoer{
		t: t,
		rest: &githubWorkItemsRESTDoer{t: t, replies: map[string][]githubWorkItemsRESTReply{
			"/repos/acme/api": {{body: `{"id":4567,"full_name":"Acme/API"}`}},
		}},
	}
	cause := providerfoundation.ErrPaginationInvalid
	projects := &githubWorkItemsRouteProjectPolicy{
		result: GitHubProjectV2FetchResult{
			Rows: emptyGitHubWorkItemRows(),
			Evidence: FetchEvidence{
				Provider: "github", Dataset: "projects-v2", Requests: 2, Pages: 1,
			},
			Usage: GitHubProjectV2Usage{
				Transport: "graphql", RouteFamily: "work_item_prs",
				Dimension: BudgetGraphQLCost, RequestCount: 2,
			},
			Targets: 1,
		},
		err: cause,
	}
	_, err := (GitHubWorkItemsRouteHandler{
		Projects: projects,
		Deriver:  &githubWorkItemsRouteDeriver{rows: githubWorkItemsRouteDerivedRows(t)},
	}).Collect(
		context.Background(), claim,
		providerfoundation.Credential{Provider: "github", ID: claim.CredentialID},
		gitHubPullRequestClient(t, doer, "https://api.github.com"), time.Now().UTC(),
	)
	if !errors.Is(err, cause) {
		t.Fatalf("error=%v", err)
	}
	var routeErr *GitHubWorkItemsRouteError
	if !errors.As(err, &routeErr) {
		t.Fatalf("error type=%T", err)
	}
	want := []GitHubWorkItemsRequestUsage{
		{Transport: "graphql", RouteFamily: "work_item_prs", Dimension: BudgetGraphQLCost, RequestCount: 2},
		{Transport: "rest", RouteFamily: "work_items", Dimension: BudgetRESTCore, RequestCount: 1},
	}
	if !reflect.DeepEqual(routeErr.Usage, want) {
		t.Fatalf("usage=%+v want=%+v", routeErr.Usage, want)
	}
}

func TestGitHubWorkItemsRouteRejectsIncompleteDerivedDestinationSet(t *testing.T) {
	complete := githubWorkItemsRouteDerivedRows(t)
	missing := githubWorkItemsRouteDerivedRows(t)
	delete(missing, "work_item_team_attributions")
	extra := githubWorkItemsRouteDerivedRows(t)
	extra["not_a_destination"] = []json.RawMessage{json.RawMessage(`{"value":1}`)}
	for name, rows := range map[string]map[string][]json.RawMessage{
		"missing": missing,
		"extra":   extra,
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := buildGitHubWorkItemsRouteEffects(emptyGitHubWorkItemRows(), rows); !errors.Is(err, ErrGitHubWorkItemsDerivationsUnavailable) {
				t.Fatalf("error=%v", err)
			}
		})
	}
	if effects, err := buildGitHubWorkItemsRouteEffects(emptyGitHubWorkItemRows(), complete); err != nil || len(effects) != len(workItemRouteDestinations()) {
		t.Fatalf("complete effects=%d error=%v", len(effects), err)
	}
}

func githubWorkItemsRouteEffect(t *testing.T, batch CompleteRouteBatch, destination string) EffectBatch {
	t.Helper()
	for _, effect := range batch.Effects {
		if effect.Destination == destination {
			return effect
		}
	}
	t.Fatalf("effect %s missing from %+v", destination, batch.Effects)
	return EffectBatch{}
}

func githubWorkItemsRouteUsage(t *testing.T, batch CompleteRouteBatch) []GitHubWorkItemsRequestUsage {
	t.Helper()
	observations, ok := batch.Result["observations"].(map[string]any)
	if !ok {
		t.Fatalf("observations=%#v", batch.Result["observations"])
	}
	usage, ok := observations["provider_usage"].([]GitHubWorkItemsRequestUsage)
	if !ok {
		t.Fatalf("provider_usage=%#v", observations["provider_usage"])
	}
	return usage
}

func githubWorkItemsRouteIncomplete(t *testing.T, batch CompleteRouteBatch) []GitHubWorkItemsIncomplete {
	t.Helper()
	incomplete, ok := batch.Result["incomplete"].([]GitHubWorkItemsIncomplete)
	if !ok {
		t.Fatalf("incomplete=%#v", batch.Result["incomplete"])
	}
	return incomplete
}

func githubWorkItemsIncompleteError(t *testing.T, err error) *GitHubWorkItemsRouteError {
	t.Helper()
	var routeErr *GitHubWorkItemsRouteError
	if !errors.As(err, &routeErr) {
		t.Fatalf("error type=%T", err)
	}
	return routeErr
}
