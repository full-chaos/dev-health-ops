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

// githubProjectV2NoopSnapshotDiffReader is the ProjectMembershipSnapshotDiff
// double for every route test not specifically exercising CHAOS-4193(d)'s
// diff: it answers "nothing was previously active", so the diff step is a
// well-formed no-op and these tests keep proving what they always proved.
// Snapshot-diff-specific behavior has its own dedicated tests instead of
// asserting through this stand-in.
type githubProjectV2NoopSnapshotDiffReader struct{}

func (githubProjectV2NoopSnapshotDiffReader) PriorActiveSubjects(
	context.Context, string, string,
) ([]githubProjectV2SnapshotSubject, error) {
	return nil, nil
}

type githubWorkItemsRouteDoer struct {
	t              *testing.T
	rest           *githubWorkItemsRESTDoer
	graphqlReplies []string
	graphqlStatus  []int
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
	index := doer.graphqlCalls - 1
	status := http.StatusOK
	if index < len(doer.graphqlStatus) && doer.graphqlStatus[index] != 0 {
		status = doer.graphqlStatus[index]
	}
	header := http.Header{"Content-Type": []string{"application/json"}}
	if status == http.StatusForbidden {
		header.Set("X-RateLimit-Remaining", "0")
	}
	return &http.Response{
		StatusCode: status,
		Header:     header,
		Body: io.NopCloser(strings.NewReader(
			doer.graphqlReplies[index],
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
		CreatedAt: normalizedAt, UpdatedAt: normalizedAt, LastSynced: normalizedAt,
	}
	projects := &githubWorkItemsRouteProjectPolicy{result: GitHubProjectV2FetchResult{
		Rows: githubWorkItemRows{
			WorkItems: []githubWorkItemRow{projectRow},
			StatusTransitions: []githubWorkItemTransitionRow{{
				WorkItemID: projectRow.WorkItemID, Provider: "github",
				OccurredAt: normalizedAt, FromStatus: "todo", ToStatus: "done",
				OrgID: claim.OrgID, LastSynced: normalizedAt,
			}},
		},
		Evidence: FetchEvidence{Provider: "github", Dataset: "projects-v2", Requests: 3, Pages: 2, Records: 2},
		Usage: GitHubProjectV2Usage{
			Transport: "graphql", RouteFamily: "work_item_prs",
			Dimension: BudgetGraphQLCost, RequestCount: 3,
		},
		Targets:   1,
		Snapshots: []githubProjectV2BoardSnapshot{{ProjectScopeID: "ghprojv2:acme#3"}},
	}}
	deriver := &githubWorkItemsRouteDeriver{rows: githubWorkItemsRouteDerivedRows(t)}
	handler := GitHubWorkItemsRouteHandler{Projects: projects, ProjectMembershipSnapshotDiff: githubProjectV2NoopSnapshotDiffReader{}, Deriver: deriver}

	batch, err := handler.Collect(
		context.Background(), claim,
		providerfoundation.Credential{Provider: "github", ID: claim.CredentialID},
		client, normalizedAt,
	)
	if err != nil {
		t.Fatal(err)
	}
	if batch.Watermark == nil || claim.BeforeAt == nil || !batch.Watermark.Equal(*claim.BeforeAt) {
		t.Fatalf("complete route watermark=%v want claim.BeforeAt=%v", batch.Watermark, claim.BeforeAt)
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

	wantDestinations := githubWorkItemRouteDestinations()
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

// D18 replaced the policy_pending degradation with a construction guard. A
// handler built without a Projects collector is a WIRING DEFECT, not a batch
// that came back incomplete, and the two must not share a vocabulary: an
// incomplete entry says the provider declined data, which would send whoever
// reads it looking at GitHub instead of at the handler.
//
// The guard is at Collect entry, so it also spends nothing: the old
// policy_pending path had already issued the repo-metadata REST call before
// discovering the route could not run.
func TestGitHubWorkItemsRouteRefusesAnUnwiredProjectsCollector(t *testing.T) {
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
	batch, err := (GitHubWorkItemsRouteHandler{ProjectMembershipSnapshotDiff: githubProjectV2NoopSnapshotDiffReader{}, Deriver: deriver}).Collect(
		context.Background(), claim,
		providerfoundation.Credential{Provider: "github", ID: claim.CredentialID},
		gitHubPullRequestClient(t, doer, "https://api.github.com"), time.Now().UTC(),
	)
	if !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("error=%v want ErrInvalidConfiguration", err)
	}
	if errors.Is(err, ErrGitHubWorkItemsIncomplete) {
		t.Fatal("an unwired collector was reported as provider incompleteness")
	}
	if !reflect.DeepEqual(batch, CompleteRouteBatch{}) || deriver.calls != 0 {
		t.Fatalf("misconstructed route returned batch=%+v or derived %d times", batch, deriver.calls)
	}
	if doer.graphqlCalls != 0 || len(doer.rest.requests) != 0 {
		t.Fatalf("misconstructed route spent requests: graphql=%d rest=%d",
			doer.graphqlCalls, len(doer.rest.requests))
	}
}

// The guard does not wait for a tenant that happens to have configured a
// project. A handler missing its collector is misconstructed for EVERY claim,
// and gating the refusal on target presence would make a global build defect
// surface as a tenant-specific data problem on whichever org configured
// Projects v2 first.
func TestGitHubWorkItemsRouteRefusesAnUnwiredProjectsCollectorWithoutTargets(t *testing.T) {
	claim := githubWorkItemsRESTClaim()
	claim.IntegrationConfig = map[string]any{}
	doer := &githubWorkItemsRouteDoer{
		t:    t,
		rest: &githubWorkItemsRESTDoer{t: t, replies: githubWorkItemsRESTFixtures()},
	}
	deriver := &githubWorkItemsRouteDeriver{rows: githubWorkItemsRouteDerivedRows(t)}
	_, err := (GitHubWorkItemsRouteHandler{ProjectMembershipSnapshotDiff: githubProjectV2NoopSnapshotDiffReader{}, Deriver: deriver}).Collect(
		context.Background(), claim,
		providerfoundation.Credential{Provider: "github", ID: claim.CredentialID},
		gitHubPullRequestClient(t, doer, "https://api.github.com"), time.Now().UTC(),
	)
	if !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("error=%v want ErrInvalidConfiguration even with no targets configured", err)
	}
	if deriver.calls != 0 || len(doer.rest.requests) != 0 {
		t.Fatalf("misconstructed route ran: derived=%d rest=%d", deriver.calls, len(doer.rest.requests))
	}
}

// Route-level half of the D18 environment clause. The process has both
// GITHUB_PROJECTS_V2 and GITHUB_TOKEN set and the integration has no durable
// targets: the batch must succeed and report "disabled".
//
// WHAT ACTUALLY HOLDS THE LINE, measured rather than assumed: `projects.calls`
// and `validateGitHubWorkItemsProjectResult` (route.go:370). Re-applying an
// env-adopting mutant kills this test THERE — the collector gets invoked with
// targets the claim never configured, and its result fails validation against
// len(projectTargets).
//
// The GraphQL counter below is NOT what fails. This case substitutes a stub
// Projects policy, which issues no HTTP at all, so doer.graphqlCalls cannot
// rise however the route behaves. It is kept as a backstop for a future
// arrangement where a real collector reaches the wire, and is documented as
// such rather than presented as the assertion doing the work — a counter that
// cannot be tripped reads like stronger evidence than it is.
func TestGitHubWorkItemsRouteTreatsEnvironmentProjectsAsNoConfiguration(t *testing.T) {
	t.Setenv("GITHUB_PROJECTS_V2", "acme:3,labs:12")
	t.Setenv("GITHUB_TOKEN", "ghp_environment_token_that_must_never_be_used")
	claim := githubWorkItemsRESTClaim()
	// Pull requests off, so the social phase issues no GraphQL of its own and
	// any GraphQL request at all is unambiguously a Projects v2 traversal.
	claim.DatasetOptions = map[string]any{
		"include_issues": false, "include_pull_requests": false,
		"fetch_comments": false, "fetch_milestones": false,
	}
	claim.IntegrationConfig = map[string]any{}
	doer := &githubWorkItemsRouteDoer{
		t: t,
		rest: &githubWorkItemsRESTDoer{t: t, replies: map[string][]githubWorkItemsRESTReply{
			"/repos/acme/api": {{body: `{"id":4567,"full_name":"Acme/API"}`}},
		}},
	}
	projects := &githubWorkItemsRouteProjectPolicy{}
	deriver := &githubWorkItemsRouteDeriver{rows: githubWorkItemsRouteDerivedRows(t)}
	batch, err := (GitHubWorkItemsRouteHandler{Projects: projects, ProjectMembershipSnapshotDiff: githubProjectV2NoopSnapshotDiffReader{}, Deriver: deriver}).Collect(
		context.Background(), claim,
		providerfoundation.Credential{Provider: "github", ID: claim.CredentialID},
		gitHubPullRequestClient(t, doer, "https://api.github.com"), time.Now().UTC(),
	)
	if err != nil {
		t.Fatal(err)
	}
	if projects.calls != 0 {
		t.Fatalf("environment configuration invoked the collector %d time(s)", projects.calls)
	}
	if doer.graphqlCalls != 0 {
		t.Fatalf("environment configuration issued %d GraphQL request(s)", doer.graphqlCalls)
	}
	if state := batch.Result["projects_v2"]; state != "disabled" {
		t.Fatalf("projects_v2=%v want disabled", state)
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
		Projects: GitHubProjectV2Fetcher{}, ProjectMembershipSnapshotDiff: githubProjectV2NoopSnapshotDiffReader{}, Deriver: deriver,
	}).Collect(
		context.Background(), claim,
		providerfoundation.Credential{Provider: "github", ID: claim.CredentialID},
		gitHubPullRequestClient(t, doer, "https://api.github.com"), time.Now().UTC(),
	)
	// provider.py:369-402 logs the failed social batch and keeps normalizing
	// every pull request with empty events/comments, so the required rows still
	// land. Executed Python evidence for the identical scenario: no exception,
	// 2 work items returned. Before this mirrored the route returned a zeroed
	// CompleteRouteBatch and never called the deriver.
	if err != nil {
		t.Fatalf("optional social failure zeroed the batch: %v", err)
	}
	if deriver.calls != 1 {
		t.Fatalf("deriver calls=%d", deriver.calls)
	}
	if len(deriver.got.WorkItems) != 1 || deriver.got.WorkItems[0].WorkItemID != "ghpr:Acme/API-Renamed#52" {
		t.Fatalf("pull request rows dropped on optional social failure: %+v", deriver.got.WorkItems)
	}
	// The enrichment itself is genuinely absent, not fabricated.
	if len(deriver.got.Interactions) != 0 {
		t.Fatalf("interactions=%+v", deriver.got.Interactions)
	}
	workItemsEffect := githubWorkItemsRouteEffect(t, batch, "work_items")
	if len(workItemsEffect.Rows) != 1 {
		t.Fatalf("work_items effect rows=%s", workItemsEffect.Rows)
	}
	if batch.Watermark != nil {
		t.Fatalf("degraded run advanced watermark: %v", batch.Watermark)
	}
	// The degradation stays queryable so the activation layer can withhold
	// every alias watermark for this run.
	if incomplete := githubWorkItemsRouteIncomplete(t, batch); !reflect.DeepEqual(
		incomplete, []GitHubWorkItemsIncomplete{{Component: "pr_social", Cause: "invalid_response"}},
	) {
		t.Fatalf("incomplete=%+v", incomplete)
	}
	if usage := githubWorkItemsRouteUsage(t, batch); !reflect.DeepEqual(usage, []GitHubWorkItemsRequestUsage{
		{Transport: "graphql", RouteFamily: "work_item_prs", Dimension: BudgetGraphQLCost, RequestCount: 1},
		{Transport: "rest", RouteFamily: "work_items", Dimension: BudgetRESTCore, RequestCount: 3},
	}) {
		t.Fatalf("usage=%+v", usage)
	}
}

func TestGitHubWorkItemsRouteContinuesPastOptionalRESTFailuresAndLandsEffects(t *testing.T) {
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
	batch, err := (GitHubWorkItemsRouteHandler{Projects: GitHubProjectV2Fetcher{}, ProjectMembershipSnapshotDiff: githubProjectV2NoopSnapshotDiffReader{}, Deriver: deriver}).Collect(
		context.Background(), claim,
		providerfoundation.Credential{Provider: "github", ID: claim.CredentialID},
		gitHubPullRequestClient(t, doer, "https://api.github.com"), time.Now().UTC(),
	)
	// provider.py:202-217 (milestones) and provider.py:293-301 (issue comments)
	// both log and continue. Executed Python evidence: a milestone failure
	// degrades sprints 1 -> 0 and a comment failure drops interactions, but the
	// issue work items still land and nothing is raised. Before this mirrored
	// the route zeroed the whole batch over the same two failures.
	if err != nil {
		t.Fatalf("optional REST failures zeroed the batch: %v", err)
	}
	if deriver.calls != 1 || len(deriver.got.WorkItems) != 1 ||
		deriver.got.WorkItems[0].WorkItemID != "gh:Acme/API#42" {
		t.Fatalf("issue rows dropped on optional failures: calls=%d rows=%+v", deriver.calls, deriver.got.WorkItems)
	}
	if len(deriver.got.Sprints) != 0 || len(deriver.got.Interactions) != 0 {
		t.Fatalf("optional rows fabricated: %+v", deriver.got)
	}
	if len(githubWorkItemsRouteEffect(t, batch, "work_items").Rows) != 1 {
		t.Fatalf("work_items effect rows=%s", githubWorkItemsRouteEffect(t, batch, "work_items").Rows)
	}
	if batch.Watermark != nil {
		t.Fatalf("degraded run advanced watermark: %v", batch.Watermark)
	}
	wantIncomplete := []GitHubWorkItemsIncomplete{
		{Component: "milestones", Cause: "transient"},
		{Component: "issue_comments", SubjectID: "42", Cause: "transient"},
	}
	if !reflect.DeepEqual(githubWorkItemsRouteIncomplete(t, batch), wantIncomplete) ||
		batch.Evidence.Requests != 5 ||
		!reflect.DeepEqual(githubWorkItemsRouteUsage(t, batch), []GitHubWorkItemsRequestUsage{{
			Transport: "rest", RouteFamily: "work_items", Dimension: BudgetRESTCore, RequestCount: 5,
		}}) {
		t.Fatalf("batch=%+v incomplete=%+v", batch, githubWorkItemsRouteIncomplete(t, batch))
	}
}

// provider.py:503-507 wraps the WHOLE per-PR processing loop in a
// warn-and-continue, so a pull request we cannot turn into rows costs us that
// pull request -- not the issues, sprints and dependencies already collected,
// and not the unit.
func TestGitHubWorkItemsRouteContinuesPastUnprocessablePullRequest(t *testing.T) {
	claim := githubWorkItemsRESTClaim()
	claim.DatasetOptions["fetch_milestones"] = false
	fixtures := githubWorkItemsRESTFixtures()
	delete(fixtures, "/repos/acme/api/milestones")
	doer := &githubWorkItemsRouteDoer{
		t:    t,
		rest: &githubWorkItemsRESTDoer{t: t, replies: fixtures},
		// createdAt is typed *string in the adapter's node contract; an object
		// there fails the decode that adaptGitHubWorkItemPRSocialPayload runs.
		graphqlReplies: []string{`{"data":{"repository":{"pr0":{"number":52,"comments":{"nodes":[{"databaseId":1,"body":"c","createdAt":{"bad":true},"author":{"login":"reviewer"}}],"pageInfo":{"hasNextPage":false,"endCursor":null}},"timelineItems":{"nodes":[],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}}}`},
	}
	deriver := &githubWorkItemsRouteDeriver{rows: githubWorkItemsRouteDerivedRows(t)}
	batch, err := (GitHubWorkItemsRouteHandler{Projects: GitHubProjectV2Fetcher{}, ProjectMembershipSnapshotDiff: githubProjectV2NoopSnapshotDiffReader{}, Deriver: deriver}).Collect(
		context.Background(), claim,
		providerfoundation.Credential{Provider: "github", ID: claim.CredentialID},
		gitHubPullRequestClient(t, doer, "https://api.github.com"), time.Now().UTC(),
	)
	if err != nil {
		t.Fatalf("an unprocessable pull request zeroed the batch: %v", err)
	}
	if deriver.calls != 1 {
		t.Fatalf("deriver calls=%d", deriver.calls)
	}
	for _, item := range deriver.got.WorkItems {
		if strings.HasPrefix(item.WorkItemID, "ghpr:") {
			t.Fatalf("unprocessable pull request became a row: %+v", item)
		}
	}
	if len(deriver.got.WorkItems) == 0 {
		t.Fatal("issues collected before the failing pull request were discarded")
	}
	if incomplete := githubWorkItemsRouteIncomplete(t, batch); !reflect.DeepEqual(
		incomplete, []GitHubWorkItemsIncomplete{
			{Component: "pull_request_processing", SubjectID: "52", Cause: "invalid_response"},
		},
	) {
		t.Fatalf("incomplete=%+v", incomplete)
	}
	if batch.Watermark != nil {
		t.Fatalf("degraded run advanced watermark: %v", batch.Watermark)
	}
}

// A batch can mix optional degradation with a blocking entry, and the blocking
// entry is appended LATER than the optional one -- the worst ordering for a
// classification loop that stops early. Without this case a check-first loop
// reads clean on exactly the ordering production produces.
//
// This case used to get its blocking entry from projects_v2 policy_pending,
// which D18 removed. The property is not about Projects v2, so it keeps its
// coverage from the pairing that still exists in production: REST milestones
// record an OPTIONAL entry first, and the social phase appends a BLOCKING
// pagination cause after it. Deleting the milestones failure (leaving only the
// blocking entry) makes this pass without testing ordering at all -- which is
// why the assertion pins both entries and their order, not just the outcome.
func TestGitHubWorkItemsRouteFailsClosedOnMixedOptionalAndBlockingIncomplete(t *testing.T) {
	claim := githubWorkItemsRESTClaim()
	claim.DatasetOptions = map[string]any{
		"include_issues": true, "include_pull_requests": true,
		"fetch_comments": true, "fetch_milestones": true, "comments_limit": 500,
	}
	fixtures := githubWorkItemsRESTFixtures()
	fixtures["/repos/acme/api/milestones"] = []githubWorkItemsRESTReply{
		{status: http.StatusInternalServerError, body: `{"message":"down"}`},
	}
	// A comments cursor that claims a next page and supplies no cursor: the
	// invalid_pagination class, blocking on the optional pr_social component.
	stalled := `{"data":{"repository":{"pr0":{"number":52,"comments":{"nodes":[],"pageInfo":{"hasNextPage":true,"endCursor":null}},"timelineItems":{"nodes":[],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}}}`
	doer := &githubWorkItemsRouteDoer{
		t:              t,
		rest:           &githubWorkItemsRESTDoer{t: t, replies: fixtures},
		graphqlReplies: []string{stalled, stalled},
	}
	deriver := &githubWorkItemsRouteDeriver{rows: githubWorkItemsRouteDerivedRows(t)}
	batch, err := (GitHubWorkItemsRouteHandler{Projects: GitHubProjectV2Fetcher{}, ProjectMembershipSnapshotDiff: githubProjectV2NoopSnapshotDiffReader{}, Deriver: deriver}).Collect(
		context.Background(), claim,
		providerfoundation.Credential{Provider: "github", ID: claim.CredentialID},
		gitHubPullRequestClient(t, doer, "https://api.github.com"), time.Now().UTC(),
	)
	if !errors.Is(err, ErrGitHubWorkItemsIncomplete) {
		t.Fatalf("blocking entry after an optional one did not fail the unit: %v", err)
	}
	if !reflect.DeepEqual(batch, CompleteRouteBatch{}) || deriver.calls != 0 {
		t.Fatalf("mixed-incomplete route returned batch=%+v or derived %d times", batch, deriver.calls)
	}
	routeErr := githubWorkItemsIncompleteError(t, err)
	if !reflect.DeepEqual(routeErr.Incomplete, []GitHubWorkItemsIncomplete{
		{Component: "milestones", Cause: "transient"},
		{Component: "pr_social", Cause: "invalid_pagination"},
	}) {
		t.Fatalf("incomplete=%+v (the optional entry must precede the blocking one)", routeErr.Incomplete)
	}
}

// Classification cannot key on Component alone. pagination_cap and
// invalid_pagination arrive on the OPTIONAL pr_social component but are not the
// provider declining optional data: one is a deterministically truncated
// traversal, the other a defect in our own paging. Both must block -- which is
// what the REST side already does by returning ErrPaginationCapExceeded.
func TestGitHubWorkItemsRouteFailsClosedOnBlockingSocialCauses(t *testing.T) {
	for _, test := range []struct {
		name      string
		fetcher   GitHubWorkItemPRSocialFetcher
		reply     string
		wantCause string
	}{
		{
			name:      "invalid_pagination",
			reply:     `{"data":{"repository":{"pr0":{"number":52,"comments":{"nodes":[],"pageInfo":{"hasNextPage":true,"endCursor":null}},"timelineItems":{"nodes":[],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}}}`,
			wantCause: "invalid_pagination",
		},
		{
			name:      "pagination_cap",
			fetcher:   GitHubWorkItemPRSocialFetcher{MaxRequests: 1},
			reply:     `{"data":{"repository":{"pr0":{"number":52,"comments":{"nodes":[],"pageInfo":{"hasNextPage":true,"endCursor":"c1"}},"timelineItems":{"nodes":[],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}}}`,
			wantCause: "pagination_cap",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
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
				graphqlReplies: []string{test.reply, test.reply},
			}
			deriver := &githubWorkItemsRouteDeriver{rows: githubWorkItemsRouteDerivedRows(t)}
			batch, err := (GitHubWorkItemsRouteHandler{
				Projects: GitHubProjectV2Fetcher{}, Social: test.fetcher, ProjectMembershipSnapshotDiff: githubProjectV2NoopSnapshotDiffReader{}, Deriver: deriver,
			}).Collect(
				context.Background(), claim,
				providerfoundation.Credential{Provider: "github", ID: claim.CredentialID},
				gitHubPullRequestClient(t, doer, "https://api.github.com"), time.Now().UTC(),
			)
			if !errors.Is(err, ErrGitHubWorkItemsIncomplete) {
				t.Fatalf("%s landed a batch instead of failing the unit: %v", test.wantCause, err)
			}
			if !reflect.DeepEqual(batch, CompleteRouteBatch{}) || deriver.calls != 0 {
				t.Fatalf("blocking cause returned batch=%+v or derived %d times", batch, deriver.calls)
			}
			routeErr := githubWorkItemsIncompleteError(t, err)
			if !reflect.DeepEqual(routeErr.Incomplete, []GitHubWorkItemsIncomplete{
				{Component: "pr_social", Cause: test.wantCause},
			}) {
				t.Fatalf("incomplete=%+v", routeErr.Incomplete)
			}
		})
	}
}

// The optional-data contract is scoped to NON-rate-limit failures. A rate limit
// is not a degradation to record and continue past: continuing would keep
// spending the exhausted budget and would bank a batch built from a window the
// provider refused to serve. These two tests are the boundary clause of the
// same ruling that makes the tests above continue, and they fail if the
// classification ever widens to swallow a rate limit.
func TestGitHubWorkItemsRouteFailsClosedOnRateLimitedSocialFetch(t *testing.T) {
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
		graphqlReplies: []string{`{"message":"API rate limit exceeded"}`},
		graphqlStatus:  []int{http.StatusForbidden},
	}
	deriver := &githubWorkItemsRouteDeriver{rows: githubWorkItemsRouteDerivedRows(t)}
	batch, err := (GitHubWorkItemsRouteHandler{Projects: GitHubProjectV2Fetcher{}, ProjectMembershipSnapshotDiff: githubProjectV2NoopSnapshotDiffReader{}, Deriver: deriver}).Collect(
		context.Background(), claim,
		providerfoundation.Credential{Provider: "github", ID: claim.CredentialID},
		gitHubPullRequestClient(t, doer, "https://api.github.com"), time.Now().UTC(),
	)
	var providerErr *providerfoundation.ProviderError
	if !errors.As(err, &providerErr) || providerErr.Class != providerfoundation.ErrorRateLimited {
		t.Fatalf("rate-limited social fetch did not abort the unit: error=%v", err)
	}
	if !reflect.DeepEqual(batch, CompleteRouteBatch{}) || deriver.calls != 0 {
		t.Fatalf("rate-limited route returned batch=%+v or derived %d times", batch, deriver.calls)
	}
}

func TestGitHubWorkItemsRouteFailsClosedOnRateLimitedIssueComments(t *testing.T) {
	claim := githubWorkItemsRESTClaim()
	claim.DatasetOptions["include_pull_requests"] = false
	doer := &githubWorkItemsRouteDoer{
		t: t,
		rest: &githubWorkItemsRESTDoer{t: t, replies: map[string][]githubWorkItemsRESTReply{
			"/repos/acme/api":                    {{body: `{"id":4567,"full_name":"Acme/API"}`}},
			"/repos/acme/api/milestones":         {{body: `[]`}},
			"/repos/acme/api/issues":             {{body: `[{"number":42,"title":"Issue","state":"open","created_at":"2026-07-02T00:00:00Z","updated_at":"2026-07-20T00:00:00Z"}]`}},
			"/repos/acme/api/issues/42/events":   {{body: `[]`}},
			"/repos/acme/api/issues/42/comments": {{status: http.StatusForbidden, body: `{"message":"API rate limit exceeded"}`}},
		}},
	}
	deriver := &githubWorkItemsRouteDeriver{rows: githubWorkItemsRouteDerivedRows(t)}
	batch, err := (GitHubWorkItemsRouteHandler{Projects: GitHubProjectV2Fetcher{}, ProjectMembershipSnapshotDiff: githubProjectV2NoopSnapshotDiffReader{}, Deriver: deriver}).Collect(
		context.Background(), claim,
		providerfoundation.Credential{Provider: "github", ID: claim.CredentialID},
		gitHubPullRequestClient(t, doer, "https://api.github.com"), time.Now().UTC(),
	)
	var providerErr *providerfoundation.ProviderError
	if !errors.As(err, &providerErr) || providerErr.Class != providerfoundation.ErrorRateLimited {
		t.Fatalf("rate-limited comment fetch did not abort the unit: error=%v", err)
	}
	if !reflect.DeepEqual(batch, CompleteRouteBatch{}) || deriver.calls != 0 {
		t.Fatalf("rate-limited route returned batch=%+v or derived %d times", batch, deriver.calls)
	}
}

// "Never a silent omission" is only satisfied if the evidence survives the
// encoding the durable write actually performs. PostgresRepository.Complete
// runs the result through workItemAliasCompletionMetadata and json.Marshal
// before the unit row is written, so this drives that exact pair rather than
// inspecting the in-memory map -- an incomplete entry that cannot round-trip
// would leave a degraded run indistinguishable from a clean one on disk.
func TestGitHubWorkItemsRouteIncompletenessSurvivesDurableCompletionEncoding(t *testing.T) {
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
	batch, err := (GitHubWorkItemsRouteHandler{
		Projects:                      GitHubProjectV2Fetcher{},
		ProjectMembershipSnapshotDiff: githubProjectV2NoopSnapshotDiffReader{},
		Deriver:                       &githubWorkItemsRouteDeriver{rows: githubWorkItemsRouteDerivedRows(t)},
	}).Collect(
		context.Background(), claim,
		providerfoundation.Credential{Provider: "github", ID: claim.CredentialID},
		gitHubPullRequestClient(t, doer, "https://api.github.com"), time.Now().UTC(),
	)
	if err != nil {
		t.Fatal(err)
	}
	_, audited, err := workItemAliasCompletionMetadata(
		claim.Provider, claim.Dataset, claim.ProcessorFlags, batch.Result,
	)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(audited)
	if err != nil {
		t.Fatal(err)
	}
	var durable struct {
		Incomplete []GitHubWorkItemsIncomplete `json:"incomplete"`
	}
	if err := json.Unmarshal(encoded, &durable); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(durable.Incomplete, []GitHubWorkItemsIncomplete{
		{Component: "milestones", Cause: "transient"},
		{Component: "issue_comments", SubjectID: "42", Cause: "transient"},
	}) {
		t.Fatalf("durable incompleteness evidence=%s", encoded)
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
		Projects:                      GitHubProjectV2Fetcher{},
		ProjectMembershipSnapshotDiff: githubProjectV2NoopSnapshotDiffReader{},
		Deriver:                       &githubWorkItemsRouteDeriver{rows: githubWorkItemsRouteDerivedRows(t)},
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
		Projects:                      projects,
		ProjectMembershipSnapshotDiff: githubProjectV2NoopSnapshotDiffReader{},
		Deriver:                       &githubWorkItemsRouteDeriver{rows: githubWorkItemsRouteDerivedRows(t)},
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
	if effects, err := buildGitHubWorkItemsRouteEffects(emptyGitHubWorkItemRows(), complete); err != nil || len(effects) != len(githubWorkItemRouteDestinations()) {
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

// The route must truncate normalizedAt to the precision its destination columns
// can hold. REST.Collect truncates only its own by-value copy, so without the
// route-entry truncation the PR-bundle and projects-v2 paths carry a wall-clock
// instant's sub-millisecond component into DateTime64(3) columns; the stored
// value then never equals the expectation, the readback answers Absent for a
// row that landed, and the committer rewrites it on every recovery pass.
//
// Asserted on the EFFECT PAYLOAD rather than on a stored row, because that is
// what the recovery snapshot persists and replays: an untruncated payload is
// unsatisfiable no matter what the adapter later does.
func TestGitHubWorkItemsRouteTruncatesTimestampsToTheColumnPrecision(t *testing.T) {
	// Deliberately sub-millisecond: 123.456789ms. A whole-millisecond fixture
	// would pass with or without the truncation.
	normalizedAt := time.Date(2026, 8, 4, 12, 0, 0, 123456789, time.UTC)
	batch := githubWorkItemsRouteCollectForTruncation(t, normalizedAt)

	for _, destination := range []string{"work_items", "work_item_transitions"} {
		effect := githubWorkItemsRouteEffect(t, batch, destination)
		if len(effect.Rows) == 0 {
			t.Fatalf("%s produced no rows, so this test would assert nothing", destination)
		}
		for index, raw := range effect.Rows {
			var fields map[string]any
			if err := json.Unmarshal(raw, &fields); err != nil {
				t.Fatal(err)
			}
			for name, value := range fields {
				text, isText := value.(string)
				if !isText {
					continue
				}
				stamp, err := time.Parse(time.RFC3339Nano, text)
				if err != nil {
					continue // not a timestamp field
				}
				if stamp.Truncate(time.Millisecond) != stamp {
					t.Fatalf("%s row %d field %q carries sub-millisecond precision (%s) "+
						"that DateTime64(3) cannot store", destination, index, name,
						stamp.Format(time.RFC3339Nano))
				}
			}
		}
	}
}

// githubWorkItemsRouteCollectForTruncation drives the real route with the same
// fixtures as the composition test, parameterised on normalizedAt.
func githubWorkItemsRouteCollectForTruncation(t *testing.T, normalizedAt time.Time) CompleteRouteBatch {
	t.Helper()
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
	// The Projects policy is a stub, so these rows bypass the real normalizer
	// (which stamps the route's already-truncated normalizedAt). Stamping them
	// at millisecond precision here mirrors what normalizeGitHubProjectV2Item
	// would produce, leaving the route's OWN path as the only thing this test
	// can be measuring.
	stamped := normalizedAt.UTC().Truncate(time.Millisecond)
	projectRow := githubWorkItemRow{
		WorkItemID: "gh:Acme/API-Renamed#42", Provider: "github",
		Title: "project wins", Type: "issue", Status: "done",
		ProjectID: stringPointer("ghprojv2:acme#3"),
		Assignees: []string{}, Labels: []string{}, OrgID: claim.OrgID,
		CreatedAt: stamped, UpdatedAt: stamped, LastSynced: stamped,
	}
	projects := &githubWorkItemsRouteProjectPolicy{result: GitHubProjectV2FetchResult{
		Rows: githubWorkItemRows{
			WorkItems: []githubWorkItemRow{projectRow},
			StatusTransitions: []githubWorkItemTransitionRow{{
				WorkItemID: projectRow.WorkItemID, Provider: "github",
				OccurredAt: stamped, FromStatus: "todo", ToStatus: "done",
				OrgID: claim.OrgID, LastSynced: stamped,
			}},
		},
		Evidence: FetchEvidence{Provider: "github", Dataset: "projects-v2", Requests: 3, Pages: 2, Records: 2},
		Usage: GitHubProjectV2Usage{
			Transport: "graphql", RouteFamily: "work_item_prs",
			Dimension: BudgetGraphQLCost, RequestCount: 3,
		},
		Targets:   1,
		Snapshots: []githubProjectV2BoardSnapshot{{ProjectScopeID: "ghprojv2:acme#3"}},
	}}
	deriver := &githubWorkItemsRouteDeriver{rows: githubWorkItemsRouteDerivedRows(t)}
	handler := GitHubWorkItemsRouteHandler{Projects: projects, ProjectMembershipSnapshotDiff: githubProjectV2NoopSnapshotDiffReader{}, Deriver: deriver}
	batch, err := handler.Collect(
		context.Background(), claim,
		providerfoundation.Credential{Provider: "github", ID: claim.CredentialID},
		client, normalizedAt,
	)
	if err != nil {
		t.Fatal(err)
	}
	return batch
}
