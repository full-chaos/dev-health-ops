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

	"github.com/full-chaos/dev-health-ops/internal/projectmembership"
	"github.com/google/uuid"

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

// D18 ratified the Projects v2 collector and CHAOS-3606 activates the complete
// five-alias family. The canonical route is native, matrix-ready, and
// plannable unconditionally (CHAOS-4054: capability is always on in the
// binary; there is no route enablement switch left to gate it). D18 also
// retired the old policy_pending degradation: a missing collector is a
// fail-closed construction error (covered by
// TestGitHubWorkItemsRouteRefusesAnUnwiredProjectsCollector), never an
// incomplete provider result.
func TestGitHubProjectV2RatificationCompletesActivatedRouteContract(t *testing.T) {
	if got := ProviderExecutor("github", "work-items"); got != ExecutorNativeGo {
		t.Fatalf("github/work-items executor=%s want native_go", got)
	}
	descriptor, known := Descriptor("github", "work-items")
	if !known {
		t.Fatal("github/work-items capability disappeared")
	}
	if !descriptor.RouteReady || !descriptor.Plannable || !descriptor.PreparedManifestRecovery ||
		!reflect.DeepEqual(descriptor.Destinations, githubWorkItemRouteDestinations()) {
		t.Fatalf("github/work-items descriptor=%+v", descriptor)
	}
	if _, policyPending := githubWorkItemsOptionalIncompleteComponents["projects_v2"]; policyPending {
		t.Fatal("projects_v2 still has a policy_pending-style incomplete fallback")
	}
}

// D18 puts the environment outside the Go route for CREDENTIALS AND TARGETS.
// The positive half ("durable config wins") is pinned above; this is the
// negative half.
//
// What actually holds the line here is the `len(targets) != 0` assertion a few
// lines down: with no durable config the collector never enters its fetch loop,
// so the request counter cannot rise no matter what the environment says. The
// counter assertion is a BACKSTOP against a future collector that reads the
// environment further down the path, not the thing failing today — worth
// stating, because a counter that cannot currently be tripped reads like
// stronger evidence than it is.
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
		t.Fatalf("environment configuration issued %d GraphQL request(s); D18 keeps "+
			"credentials and targets out of the Go route", len(doer.bodies))
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

// F4: an empty CredentialID is rejected by uuid.Parse, which is now the ONLY
// clause enforcing it -- the separate `unit.CredentialID == ""` test was
// removed as the unkillable twin of the fetcher clause removed earlier.
// Asserting the property explicitly here means the removal cannot quietly
// become a hole: if uuid.Parse were ever relaxed or reordered, this fails.
func TestUnitValidateRejectsEmptyCredentialIDViaUUIDParse(t *testing.T) {
	if _, err := uuid.Parse(""); err == nil {
		t.Fatal("uuid.Parse accepts the empty string, so the clause removed in " +
			"F4 was load-bearing after all and must be restored")
	}
	claim := githubWorkItemOracleClaim()
	if err := claim.Validate(); err != nil {
		t.Fatalf("baseline claim invalid, so the case below proves nothing: %v", err)
	}
	for _, credentialID := range []string{"", "   ", "not-a-uuid"} {
		candidate := claim
		candidate.CredentialID = credentialID
		if err := candidate.Validate(); !errors.Is(err, ErrInvalidConfiguration) {
			t.Fatalf("credential_id=%q validated: %v", credentialID, err)
		}
	}
}

func TestGitHubProjectV2TargetsFailClosedOnMalformedDurableConfig(t *testing.T) {
	for _, value := range []any{
		[]any{map[string]any{"org_login": "", "project_number": 1}},
		[]any{map[string]any{"org_login": "acme", "project_number": 0}},
		[]any{map[string]any{"org_login": "acme", "project_number": 1, "token": "forbidden"}},
		"acme:1",
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

// The two sides of the configured-but-empty boundary, both built FROM JSON
// BYTES because that is the only way to get the shapes production emits.
//
// A Go-literal `[]any(nil)` is a TYPED nil and takes a different path through
// json.Marshal than a JSONB null, which decodes to an UNTYPED nil interface.
// Testing only the typed one is how the null path stayed fail-open while
// looking covered -- the same mistake as testing int project numbers when
// production only ever sends float64, one clause over.
//
//   - JSON `null` under a present key: the operator wrote the key, so silently
//     returning "no projects configured" drops their intent. Must REFUSE.
//   - JSON `[]`: an explicitly empty list is a valid way to say "configured,
//     currently none". Must be accepted as validly empty, NOT refused.
//
// Both are pinned, because a fix for one that broke the other would otherwise
// pass.
func TestGitHubProjectV2TargetsSeparateNullConfigFromEmptyConfig(t *testing.T) {
	decode := func(t *testing.T, raw string) map[string]any {
		t.Helper()
		var integrationConfig map[string]any
		if err := json.Unmarshal([]byte(raw), &integrationConfig); err != nil {
			t.Fatal(err)
		}
		return integrationConfig
	}

	t.Run("json null under a present key is refused", func(t *testing.T) {
		integrationConfig := decode(t, `{"github_projects_v2":null}`)
		value, configured := integrationConfig["github_projects_v2"]
		// Guard the guard: if this ever stops being a present key holding an
		// untyped nil, this test is no longer exercising the production shape.
		if !configured || value != nil {
			t.Fatalf("configured=%t value=%#v -- not the production JSONB null shape",
				configured, value)
		}
		claim := githubWorkItemOracleClaim()
		claim.IntegrationConfig = integrationConfig
		targets, err := githubProjectV2Targets(claim)
		if !errors.Is(err, ErrInvalidConfiguration) {
			t.Fatalf("a configured JSON null returned targets=%+v err=%v; until "+
				"CHAOS-3123 this path returned empty targets and a nil error, "+
				"silently discarding configuration the operator wrote", targets, err)
		}
	})

	t.Run("json empty list is validly empty", func(t *testing.T) {
		claim := githubWorkItemOracleClaim()
		claim.IntegrationConfig = decode(t, `{"github_projects_v2":[]}`)
		targets, err := githubProjectV2Targets(claim)
		if err != nil {
			t.Fatalf("an explicitly empty list was refused: %v", err)
		}
		if len(targets) != 0 {
			t.Fatalf("targets=%+v want none", targets)
		}
	})
}

// Every test above builds IntegrationConfig as a Go literal, with `int` or
// json.Number project numbers. PRODUCTION NEVER PRODUCES EITHER: the claim's
// integration_config arrives as a Postgres JSONB column decoded by a plain
// json.Unmarshal into map[string]any (repository_postgres.go), so every number
// is a float64.
//
// This closes a COVERAGE gap, not a behavior gap: the parser has always
// accepted float64, so nothing here changes what production does. What was
// missing was any test that would notice if that stopped being true -- a
// parser tightened to accept only int/json.Number would have passed this
// package's entire suite while rejecting every real tenant's configuration.
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
	// Records is 4, not 3: one work item, two status transitions, and the
	// `projects` catalogue row for the configured board (CHAOS-4194). The PR
	// item in this fixture carries no repository and no createdAt, so it
	// produces NO membership row -- and is counted rather than dropped
	// silently, which is the behaviour the ticket exists to end.
	if result.Evidence.Pages != 3 || result.Evidence.Requests != 3 || result.Evidence.Records != 4 {
		t.Fatalf("evidence=%+v", result.Evidence)
	}
	if len(result.Rows.Projects) != 1 || result.Rows.Projects[0].ID != "ghprojv2:acme#3" ||
		result.Rows.Projects[0].Provider != "github" || result.Rows.Projects[0].OrgID != claim.OrgID {
		t.Fatalf("projects=%+v", result.Rows.Projects)
	}
	if len(result.Rows.ProjectMemberships) != 0 {
		t.Fatalf("an unidentifiable PR produced a membership row: %+v", result.Rows.ProjectMemberships)
	}
	if result.MembershipSkips["pull_request_incomplete"] != 1 {
		t.Fatalf("the incomplete PR was not counted: %+v", result.MembershipSkips)
	}
	// The issue is no longer a skip at all (CHAOS-4193): it is positively
	// identified for the snapshot-diff pass instead.
	if _, counted := result.MembershipSkips["issue_deferred_to_snapshot_diff"]; counted {
		t.Fatalf("the retired label was still emitted: %+v", result.MembershipSkips)
	}
	if len(result.Snapshots) != 1 || len(result.Snapshots[0].Subjects) != 1 {
		t.Fatalf("snapshots=%+v, want one project with the identified issue subject", result.Snapshots)
	}
	if got := result.Snapshots[0]; got.ProjectScopeID != "ghprojv2:acme#3" ||
		got.Subjects[0].SubjectKind != "work_item" || got.Subjects[0].SubjectID != "gh:acme/api#7" {
		t.Fatalf("snapshot=%+v", got)
	}
	// The unidentifiable PR in this fixture is a real board item this sync
	// simply could not name -- the snapshot must say so, or the snapshot-diff
	// pass would read its absence from a future complete sync's board as a
	// removal that never happened (codex round 1 finding, CHAOS-4193d).
	if result.Snapshots[0].Complete {
		t.Fatalf("snapshot=%+v, want Complete=false: it contains an unidentifiable PR", result.Snapshots[0])
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

	// Null organization and projectV2 responses are inaccessible boards, not
	// empty boards. A genuinely empty, non-null board remains authoritative and
	// must still retire prior memberships.
	{
		claim := githubWorkItemOracleClaim()
		claim.IntegrationConfig = map[string]any{"github_projects_v2": []any{
			map[string]any{"org_login": "acme", "project_number": 3},
		}}
		prior := []githubProjectV2SnapshotSubject{{
			SubjectKind: projectmembership.SubjectWorkItem,
			SubjectID:   "gh:acme/api#7",
			RepoID:      githubProjectV2TestRepoID(t),
		}}
		normalizedAt := time.Date(2026, 8, 25, 12, 0, 0, 0, time.UTC)
		for _, test := range []struct {
			name         string
			reply        string
			wantComplete bool
			wantRemovals int
			wantCause    string
		}{
			{
				name:         "null organization",
				reply:        `{"data":{"organization":null}}`,
				wantComplete: false,
				wantRemovals: 0,
				wantCause:    githubProjectsV2NullOrganization,
			},
			{
				name:         "null project",
				reply:        `{"data":{"organization":{"projectV2":null}}}`,
				wantComplete: false,
				wantRemovals: 0,
				wantCause:    githubProjectsV2NullProject,
			},
			{
				name:         "missing page info",
				reply:        `{"data":{"organization":{"projectV2":{"items":{"nodes":[]}}}}}`,
				wantComplete: false,
				wantRemovals: 0,
				wantCause:    githubProjectsV2StructuralDegraded,
			},
			{
				name:         "page info missing hasNextPage",
				reply:        `{"data":{"organization":{"projectV2":{"items":{"nodes":[],"pageInfo":{"endCursor":null}}}}}}`,
				wantComplete: false,
				wantRemovals: 0,
				wantCause:    githubProjectsV2StructuralDegraded,
			},
			{
				// codex adversarial review, CHAOS-4289 round 2: the outer
				// `items` connection already refuses a null/omitted `nodes`
				// (the "missing page info" case above), but the nested
				// per-item `changes` connection is the same GraphQL shape and
				// had no equivalent check -- an item reporting
				// hasNextPage:false with `nodes` entirely omitted silently
				// dropped its status-transition history while the board
				// still reported Complete. The item itself (Issue #7 in
				// acme/api) is otherwise fully identifiable, isolating this
				// case to the pagination-completeness gate rather than
				// item-identification (boardIncomplete).
				name: "nested changes nodes missing",
				reply: `{"data":{"organization":{"projectV2":{"items":{"nodes":[` +
					`{"id":"PVTI_1","content":{"__typename":"Issue","number":7,"repository":{"nameWithOwner":"acme/api"}},"fieldValues":{"nodes":[]},"changes":{"pageInfo":{"hasNextPage":false,"endCursor":null}}}` +
					`],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}}}`,
				wantComplete: false,
				wantRemovals: 0,
				wantCause:    githubProjectsV2StructuralDegraded,
			},
			{
				name:         "genuinely empty board",
				reply:        `{"data":{"organization":{"projectV2":{"items":{"nodes":[],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}}}`,
				wantComplete: true,
				wantRemovals: 1,
			},
		} {
			t.Run(test.name, func(t *testing.T) {
				doer := &gitHubProjectV2Doer{t: t, replies: []string{test.reply}}
				result, err := (GitHubProjectV2Fetcher{}).Fetch(
					context.Background(), claim,
					providerfoundation.Credential{Provider: "github", ID: claim.CredentialID},
					githubProjectV2TestClient(t, doer), normalizedAt, nil,
				)
				if err != nil {
					t.Fatal(err)
				}
				if len(result.Snapshots) != 1 || result.Snapshots[0].Complete != test.wantComplete {
					t.Fatalf("snapshot=%+v, want Complete=%t", result.Snapshots, test.wantComplete)
				}
				if test.wantCause == "" {
					if len(result.Incomplete) != 0 {
						t.Fatalf("incomplete=%+v, want authoritative snapshot", result.Incomplete)
					}
				} else if len(result.Incomplete) != 1 || result.Incomplete[0].Component != githubProjectsV2IncompleteComponent ||
					result.Incomplete[0].Cause != test.wantCause {
					t.Fatalf("incomplete=%+v, want projects_v2/%s", result.Incomplete, test.wantCause)
				}
				rows, counts := diffGitHubProjectV2Snapshot(
					claim, "ghprojv2:acme#3", result.Snapshots[0].Subjects, prior,
					result.Snapshots[0].Complete, normalizedAt,
				)
				if counts.Removals != test.wantRemovals || len(rows) != test.wantRemovals {
					t.Fatalf("rows=%+v counts=%+v, want %d removals", rows, counts, test.wantRemovals)
				}
			})
		}
	}
}

// TestGitHubProjectV2FetcherContinuationPageMissingNodesIsIncomplete is codex
// adversarial review CHAOS-4289 round 3's finding: the initial item.Changes
// payload's null/omitted `nodes` is refused (see
// TestGitHubProjectV2FetcherCompletesOuterAndNestedPagination's "nested
// changes nodes missing" case), but a CONTINUATION page -- fetched only when
// the initial page claims hasNextPage:true -- is the identical GraphQL shape
// and needs the identical check. Without it, a continuation page reporting
// hasNextPage:false with `nodes` entirely omitted silently truncated this
// item's status-transition history while the board still reported Complete.
//
// The item (Issue #7 in acme/api) is otherwise fully identifiable, isolating
// this to the pagination-completeness gate rather than item-identification
// (boardIncomplete).
func TestGitHubProjectV2FetcherContinuationPageMissingNodesIsIncomplete(t *testing.T) {
	doer := &gitHubProjectV2Doer{t: t, replies: []string{
		`{"data":{"organization":{"projectV2":{"items":{"nodes":[` +
			`{"id":"PVTI_1","content":{"__typename":"Issue","number":7,"repository":{"nameWithOwner":"acme/api"}},"fieldValues":{"nodes":[]},"changes":{"nodes":[` +
			`{"field":{"name":"Status"},"previousValue":{"name":"Todo"},"newValue":{"name":"Doing"},"createdAt":"2026-08-01T09:00:00Z","actor":{"login":"octocat"}}` +
			`],"pageInfo":{"hasNextPage":true,"endCursor":"change-1"}}}` +
			`],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}}}`,
		// The continuation reply: valid, explicit hasNextPage:false, but
		// `nodes` is entirely omitted -- the same malformed shape the initial
		// page's own guard refuses.
		`{"data":{"node":{"changes":{"pageInfo":{"hasNextPage":false,"endCursor":null}}}}}`,
	}}
	claim := githubWorkItemOracleClaim()
	claim.IntegrationConfig = map[string]any{"github_projects_v2": []any{map[string]any{"org_login": "acme", "project_number": 3}}}
	result, err := (GitHubProjectV2Fetcher{}).Fetch(
		context.Background(), claim,
		providerfoundation.Credential{Provider: "github", ID: claim.CredentialID},
		githubProjectV2TestClient(t, doer), time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC), nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Snapshots) != 1 || result.Snapshots[0].Complete {
		t.Fatalf("snapshot=%+v, want Complete=false", result.Snapshots)
	}
	if len(result.Snapshots[0].Subjects) != 1 || result.Snapshots[0].Subjects[0].SubjectID != "gh:acme/api#7" {
		t.Fatalf("snapshot=%+v, want the Issue still positively identified", result.Snapshots)
	}
	if len(result.Incomplete) != 1 || result.Incomplete[0].Component != githubProjectsV2IncompleteComponent ||
		result.Incomplete[0].Cause != githubProjectsV2StructuralDegraded {
		t.Fatalf("incomplete=%+v, want exactly one %s/%s entry",
			result.Incomplete, githubProjectsV2IncompleteComponent, githubProjectsV2StructuralDegraded)
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

// TestGitHubProjectV2FetcherEmitsPullRequestBoardMembership is CHAOS-4194's
// producer-side acceptance claim, and the direct reversal of the drop this
// ticket was filed for.
//
// Before this change a fully hydrated PullRequest board item was fetched and
// then discarded by the normalizer with no counter and no log -- the GraphQL
// query already selected the `... on PullRequest` fragment, so nothing was
// missing but the decision to keep it. The assertions below are the three
// things that make the resulting row joinable rather than merely present:
// subject_id is the PR number verbatim, repo_id is the SAME uuid
// repositoryIdentity derives for that repository (so it joins to
// git_pull_requests), and occurred_at is the item's own createdAt rather than
// the sync clock -- which is what makes the content-determined event_id stable
// across re-syncs.
func TestGitHubProjectV2FetcherEmitsPullRequestBoardMembership(t *testing.T) {
	doer := &gitHubProjectV2Doer{t: t, replies: []string{
		`{"data":{"organization":{"projectV2":{"items":{"nodes":[{"id":"PVTI_PR","createdAt":"2026-08-01T08:00:00Z","content":{"__typename":"PullRequest","number":42,"title":"A PR","repository":{"nameWithOwner":"acme/api"}},"fieldValues":{"nodes":[]},"changes":{"nodes":[],"pageInfo":{"hasNextPage":false,"endCursor":null}}}],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}}}`,
	}}
	client := githubProjectV2TestClient(t, doer)
	claim := githubWorkItemOracleClaim()
	claim.IntegrationConfig = map[string]any{"github_projects_v2": []any{map[string]any{"org_login": "acme", "project_number": 3}}}
	credential := providerfoundation.Credential{Provider: "github", ID: claim.CredentialID}
	normalizedAt := time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC)
	result, err := (GitHubProjectV2Fetcher{}).Fetch(
		context.Background(), claim, credential, client, normalizedAt, nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	// A pull request is NOT a work item and must not become one. The fix is
	// that its board membership stops being discarded, not that PRs start
	// appearing in `work_items`.
	if len(result.Rows.WorkItems) != 0 {
		t.Fatalf("a pull request was normalized into a work item: %+v", result.Rows.WorkItems)
	}
	if len(result.Rows.ProjectMemberships) != 1 {
		t.Fatalf("memberships=%+v", result.Rows.ProjectMemberships)
	}
	row := result.Rows.ProjectMemberships[0]
	identity, err := repositoryIdentity("acme/api")
	if err != nil {
		t.Fatal(err)
	}
	if row.SubjectKind != "pull_request" || row.SubjectID != "42" || row.RepoID.String() != identity {
		t.Fatalf("subject identity = %s / %s / %s, want the (repo_id, number) pair",
			row.SubjectKind, row.SubjectID, row.RepoID)
	}
	if row.ToProjectID != "ghprojv2:acme#3" || row.FromProjectID != "" || row.ToProjectKey != "" {
		t.Fatalf("destination = %+v", row)
	}
	if !row.OccurredAt.Equal(time.Date(2026, 8, 1, 8, 0, 0, 0, time.UTC)) {
		t.Fatalf("occurred_at = %s, want the item's own createdAt", row.OccurredAt)
	}
	if row.LastSynced != normalizedAt {
		t.Fatalf("last_synced = %s, want the unit's normalizedAt", row.LastSynced)
	}
	if row.EventID == "" || len(row.EventID) != 32 {
		t.Fatalf("event_id = %q, want the 32-char content hash", row.EventID)
	}
	if result.MembershipSkips["pull_request_incomplete"] != 0 {
		t.Fatalf("an emitted PR was also counted as skipped: %+v", result.MembershipSkips)
	}
	// A fully identifiable board must produce a Complete snapshot, or the
	// snapshot-diff pass would refuse to ever compute a removal for it.
	if len(result.Snapshots) != 1 || !result.Snapshots[0].Complete {
		t.Fatalf("snapshots=%+v, want exactly one Complete snapshot", result.Snapshots)
	}
	if len(result.Snapshots[0].Subjects) != 1 || result.Snapshots[0].Subjects[0].SubjectKind != "pull_request" ||
		result.Snapshots[0].Subjects[0].SubjectID != "42" {
		t.Fatalf("snapshot subjects=%+v", result.Snapshots[0].Subjects)
	}
}

// TestGitHubProjectV2MembershipEventIDIsStableAcrossResyncs is the property the
// whole event_id formula exists for, and the one a single-fetch test cannot
// state.
//
// event_id is a sorting-key member, so if it varied with observation time a
// re-sync of ONE unchanged membership would mint a new key and
// ReplacingMergeTree would keep both rows -- the table would grow one row per
// sync of a board that never changed. The two fetches below differ only in
// normalizedAt, which is exactly the difference a re-sync makes.
func TestGitHubProjectV2MembershipEventIDIsStableAcrossResyncs(t *testing.T) {
	reply := `{"data":{"organization":{"projectV2":{"items":{"nodes":[{"id":"PVTI_PR","createdAt":"2026-08-01T08:00:00Z","content":{"__typename":"PullRequest","number":42,"title":"A PR","repository":{"nameWithOwner":"acme/api"}},"fieldValues":{"nodes":[]},"changes":{"nodes":[],"pageInfo":{"hasNextPage":false,"endCursor":null}}}],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}}}`
	claim := githubWorkItemOracleClaim()
	claim.IntegrationConfig = map[string]any{"github_projects_v2": []any{map[string]any{"org_login": "acme", "project_number": 3}}}
	credential := providerfoundation.Credential{Provider: "github", ID: claim.CredentialID}
	eventIDs := []string{}
	for _, normalizedAt := range []time.Time{
		time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC),
		time.Date(2026, 8, 5, 12, 0, 0, 0, time.UTC),
	} {
		doer := &gitHubProjectV2Doer{t: t, replies: []string{reply}}
		result, err := (GitHubProjectV2Fetcher{}).Fetch(
			context.Background(), claim, credential, githubProjectV2TestClient(t, doer), normalizedAt, nil,
		)
		if err != nil {
			t.Fatal(err)
		}
		if len(result.Rows.ProjectMemberships) != 1 {
			t.Fatalf("memberships=%+v", result.Rows.ProjectMemberships)
		}
		eventIDs = append(eventIDs, result.Rows.ProjectMemberships[0].EventID)
	}
	if eventIDs[0] != eventIDs[1] {
		t.Fatalf("event_id changed across re-sync: %q then %q -- the table would keep both rows",
			eventIDs[0], eventIDs[1])
	}
}

// TestGitHubProjectV2SnapshotCompleteAcrossEveryIdentificationOutcome is the
// coverage codex round 2 asked for directly: one Fetch-level case per way a
// board's Complete flag can land, since round 1's fix (a false removal for a
// still-present but unidentifiable subject) is only as good as every path
// into it being covered, not just the mixed-fixture case the original
// pagination test happened to exercise.
func TestGitHubProjectV2SnapshotCompleteAcrossEveryIdentificationOutcome(t *testing.T) {
	claim := githubWorkItemOracleClaim()
	claim.IntegrationConfig = map[string]any{"github_projects_v2": []any{map[string]any{"org_login": "acme", "project_number": 3}}}
	credential := providerfoundation.Credential{Provider: "github", ID: claim.CredentialID}
	normalizedAt := time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC)

	fetch := func(t *testing.T, reply string) GitHubProjectV2FetchResult {
		t.Helper()
		doer := &gitHubProjectV2Doer{t: t, replies: []string{reply}}
		result, err := (GitHubProjectV2Fetcher{}).Fetch(
			context.Background(), claim, credential, githubProjectV2TestClient(t, doer), normalizedAt, nil,
		)
		if err != nil {
			t.Fatal(err)
		}
		if len(result.Snapshots) != 1 {
			t.Fatalf("snapshots=%+v, want exactly one project", result.Snapshots)
		}
		return result
	}

	t.Run("issue missing repository is incomplete", func(t *testing.T) {
		result := fetch(t, `{"data":{"organization":{"projectV2":{"items":{"nodes":[`+
			`{"id":"PVTI_1","content":{"__typename":"Issue","number":7,"title":"no repo"},"fieldValues":{"nodes":[]},"changes":{"nodes":[],"pageInfo":{"hasNextPage":false,"endCursor":null}}}`+
			`],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}}}`)
		if result.Snapshots[0].Complete || len(result.Snapshots[0].Subjects) != 0 {
			t.Fatalf("snapshot=%+v, want Complete=false and no identified subjects", result.Snapshots[0])
		}
		// Codex adversarial review, CHAOS-4289 round 1: suppressing this
		// board's removals is not enough on its own -- the route's watermark
		// gate keys off Fetch's durable Incomplete evidence, not the
		// snapshot's Complete flag, so a still-unidentified item must show up
		// here too or a later sync could advance past it unretried.
		if len(result.Incomplete) != 1 || result.Incomplete[0].Component != githubProjectsV2IncompleteComponent ||
			result.Incomplete[0].Cause != githubProjectsV2UnidentifiedItem {
			t.Fatalf("incomplete=%+v, want exactly one %s/%s entry",
				result.Incomplete, githubProjectsV2IncompleteComponent, githubProjectsV2UnidentifiedItem)
		}
	})

	t.Run("a board of only draft issues is complete", func(t *testing.T) {
		result := fetch(t, `{"data":{"organization":{"projectV2":{"items":{"nodes":[`+
			`{"id":"PVTI_1","content":{"__typename":"DraftIssue","title":"idea"},"fieldValues":{"nodes":[]},"changes":{"nodes":[],"pageInfo":{"hasNextPage":false,"endCursor":null}}}`+
			`],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}}}`)
		if !result.Snapshots[0].Complete || len(result.Snapshots[0].Subjects) != 0 {
			t.Fatalf("snapshot=%+v, want Complete=true (a draft issue names no subject at all, which is complete information) and no subjects", result.Snapshots[0])
		}
		if len(result.Incomplete) != 0 {
			t.Fatalf("incomplete=%+v, want none: a draft-only board is genuinely complete", result.Incomplete)
		}
	})

	t.Run("an unrecognised content typename is incomplete", func(t *testing.T) {
		result := fetch(t, `{"data":{"organization":{"projectV2":{"items":{"nodes":[`+
			`{"id":"PVTI_1","content":{"__typename":"SomeFutureContentType"},"fieldValues":{"nodes":[]},"changes":{"nodes":[],"pageInfo":{"hasNextPage":false,"endCursor":null}}}`+
			`],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}}}`)
		if result.Snapshots[0].Complete || len(result.Snapshots[0].Subjects) != 0 {
			t.Fatalf("snapshot=%+v, want Complete=false: GitHub added a content kind this code has never seen", result.Snapshots[0])
		}
		if len(result.Incomplete) != 1 || result.Incomplete[0].Component != githubProjectsV2IncompleteComponent ||
			result.Incomplete[0].Cause != githubProjectsV2UnidentifiedItem {
			t.Fatalf("incomplete=%+v, want exactly one %s/%s entry",
				result.Incomplete, githubProjectsV2IncompleteComponent, githubProjectsV2UnidentifiedItem)
		}
	})

	t.Run("a fully identified mixed board is complete", func(t *testing.T) {
		result := fetch(t, `{"data":{"organization":{"projectV2":{"items":{"nodes":[`+
			`{"id":"PVTI_1","content":{"__typename":"Issue","number":7,"title":"ok","repository":{"nameWithOwner":"acme/api"},"labels":{"nodes":[]},"assignees":{"nodes":[]}},"fieldValues":{"nodes":[]},"changes":{"nodes":[],"pageInfo":{"hasNextPage":false,"endCursor":null}}},`+
			`{"id":"PVTI_2","createdAt":"2026-08-01T08:00:00Z","content":{"__typename":"PullRequest","number":42,"title":"ok","repository":{"nameWithOwner":"acme/api"}},"fieldValues":{"nodes":[]},"changes":{"nodes":[],"pageInfo":{"hasNextPage":false,"endCursor":null}}},`+
			`{"id":"PVTI_3","content":{"__typename":"DraftIssue","title":"idea"},"fieldValues":{"nodes":[]},"changes":{"nodes":[],"pageInfo":{"hasNextPage":false,"endCursor":null}}}`+
			`],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}}}`)
		if !result.Snapshots[0].Complete || len(result.Snapshots[0].Subjects) != 2 {
			t.Fatalf("snapshot=%+v, want Complete=true with 2 identified subjects", result.Snapshots[0])
		}
		if len(result.Incomplete) != 0 {
			t.Fatalf("incomplete=%+v, want none: every board item was positively identified", result.Incomplete)
		}
	})
}
