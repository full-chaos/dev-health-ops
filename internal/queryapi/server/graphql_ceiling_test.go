package server

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/99designs/gqlgen/graphql"
	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"
	"github.com/full-chaos/dev-health-go/pep"

	"github.com/full-chaos/dev-health-ops/internal/authzenforce"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/authctx"
	gqlpkg "github.com/full-chaos/dev-health-ops/internal/queryapi/graph"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/pdp"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/pdp/pdptest"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/pepgate"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/teamscope"
)

// CHAOS-13161 (6.11b PR-2a): the eight GraphQL roots whose every read is keyed by repository serve an allowlist
// decision as a repository ceiling (cells R2-R4 of PLAN-10865 on GraphQL, the REST form of CHAOS-13090).

// The two repositories of the organization; ids unlike every org and user id of the fixtures.
const (
	gqlCeilingRepoA = "aaaaaaaa-0000-4000-8000-00000000000a"
	gqlCeilingRepoB = "bbbbbbbb-0000-4000-8000-00000000000b"
)

// ceilingRoot is one N root and a document that reaches its reads.
type ceilingRoot struct {
	key, doc string
}

func ceilingRoots() []ceilingRoot {
	org := ecOrg.String()
	window := `sinceUtc: "2026-01-01T00:00:00Z", untilUtc: "2026-01-15T00:00:00Z"`
	return []ceilingRoot{
		{"Query.complexityTimeseries", `query Q { complexityTimeseries(input: {orgId: "` + org + `", ` + window + `, granularity: DAY, scope: REPO}) { __typename } }`},
		{"Query.featureFlags", `query Q { featureFlags(orgId: "` + org + `") { __typename } }`},
		{"Query.featureFlagEvents", `query Q { featureFlagEvents(orgId: "` + org + `") { __typename } }`},
		{"Query.hotspots", `query Q { hotspots(input: {orgId: "` + org + `", ` + window + `}) { __typename } }`},
		{"Query.pr", `query Q { pr(orgId: "` + org + `", id: "` + gqlCeilingRepoA + `#12") { __typename } }`},
		{"Query.securityAlerts", `query Q { securityAlerts(orgId: "` + org + `") { __typename } }`},
		{"Query.securityOverview", `query Q { securityOverview(orgId: "` + org + `") { __typename } }`},
		{"Query.testopsRisk", `query Q { testopsRisk(orgId: "` + org + `", input: {startDate: "2026-01-01", endDate: "2026-01-15"}) { __typename } }`},
	}
}

// ceilingTables are the repository-keyed tables the eight roots read (PR-0's census of their reads).
var ceilingTables = []string{"feature_flag", "file_complexity_snapshots", "file_hotspot_daily", "git_pull_request_reviews",
	"git_pull_requests", "git_commits", "repo_complexity_daily", "security_alerts", "testops_", "work_graph_pr_commit"}

func isCeilingData(statement string, bindings []dhclickhouse.Binding) bool {
	if hasBinding(bindings, teamscope.BindingCeilingKeys) {
		return false
	}
	for _, table := range ceilingTables {
		if strings.Contains(statement, "FROM "+table) || strings.Contains(statement, "JOIN "+table) {
			return true
		}
	}
	return false
}

// ceilingGraphQLRun is one operation's answer and reads.
type ceilingGraphQLRun struct {
	body       string
	statements []string
	bindings   [][]dhclickhouse.Binding
	roots      int
}

// runCeilingRoot runs doc on the /graphql server as a user caller of ecOrg, with every cell of the roots' namespaces
// on and the decision point answering obligations (nil = an organization-wide allow); the matcher answers acme/a and
// acme/b with their repository ids.
func runCeilingRoot(t *testing.T, doc string, obligations map[string]any) ceilingGraphQLRun {
	t.Helper()
	srv := pdptest.New(t)
	if obligations != nil {
		srv.Obligations(obligations)
	}
	gate := pdp.New(pdp.Config{Mode: pdp.ModeEnforce, Decider: pepOf(t, srv.URL), Switch: authzenforce.Parse(authzenforce.QueryAPI, "analytics,metrics,git"),
		ExtraShadowRows: graphQLShadowRows(), Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	return runCeilingRootWith(t, gate, doc)
}

func runCeilingRootWith(t *testing.T, gate *pdp.Gate, doc string) ceilingGraphQLRun {
	t.Helper()
	client := &teamScopeReachClient{matcher: map[string][]string{"acme/a": {gqlCeilingRepoA}, "acme/b": {gqlCeilingRepoB}}}
	server := newGraphQLServerWithLimits(&gqlpkg.Resolver{ClickHouse: client}, 1000, 10)
	roots := 0
	server.AroundRootFields(func(ctx context.Context, next graphql.RootResolver) graphql.Marshaler {
		roots++
		return next(ctx)
	})
	req := httptest.NewRequest(http.MethodPost, "/graphql", strings.NewReader(`{"query":`+jsonQuote(t, doc)+`}`))
	req.Header.Set("Content-Type", "application/json")
	claims := authctx.Claims{OrgID: ecOrg.String(), Role: "admin", Scope: pepgate.Legacy(ecOrg.String())}
	ctx := gqlpkg.WithShadowSubject(authctx.WithClaims(req.Context(), claims), graphQLShadowSubject(newAccessMinter(t).mint(ecUser, nil, nil)))
	rec := httptest.NewRecorder()
	server.ServeHTTP(rec, req.WithContext(pdp.WithGate(ctx, gate)))
	gate.Wait()
	client.mu.Lock()
	defer client.mu.Unlock()
	return ceilingGraphQLRun{body: rec.Body.String(), statements: append([]string(nil), client.statements...),
		bindings: append([][]dhclickhouse.Binding(nil), client.bindings...), roots: roots}
}

func allowlistOf(keys ...string) map[string]any {
	return map[string]any{"tenant_scope": ecOrg.String(), "repository_allowlist": keys}
}

// R4: an allowlist decision on an N root is served; one matcher statement, bound to the decided organization; every
// repository-keyed read holds the ceiling of the allowed repository, bound as its id (a pull request's reads are keyed
// to its one repository, held to the ceiling before any read); the other repository is bound nowhere.
func TestGraphQLCeilingAllowlistServesOnlyTheAllowedRepository(t *testing.T) {
	for _, root := range ceilingRoots() {
		t.Run(root.key, func(t *testing.T) {
			got := runCeilingRoot(t, root.doc, allowlistOf("acme/a"))
			if strings.Contains(got.body, `"code":"SERVICE_UNAVAILABLE"`) || strings.Contains(got.body, `"code":"FORBIDDEN"`) || got.roots != 1 {
				t.Fatalf("answered %s with %d root fields run: an allowlist caller is served on an N root", got.body, got.roots)
			}
			matchers := 0
			for i := range got.statements {
				if hasBinding(got.bindings[i], teamscope.BindingCeilingKeys) {
					matchers++
					if !mentionsValue(got.bindings[i], ecOrg.String()) {
						t.Fatalf("the matcher is not bound to the decided organization:\n%s", got.statements[i])
					}
				}
			}
			if matchers != 1 {
				t.Fatalf("%d matcher statements, want exactly 1 per root field", matchers)
			}
			data := 0
			for i, statement := range got.statements {
				if mentionsValue(got.bindings[i], gqlCeilingRepoB) || strings.Contains(statement, gqlCeilingRepoB) {
					t.Fatalf("the other repository reached a statement:\n%s", statement)
				}
				if !isCeilingData(statement, got.bindings[i]) {
					continue
				}
				data++
				if root.key == "Query.pr" {
					// Every pull request read is keyed to the one repository of the id; the resolver holds that
					// repository to the ceiling before any read.
					if ids := bindingStringValues(got.bindings[i], "repo_id"); !reflect.DeepEqual(ids, []string{gqlCeilingRepoA}) {
						t.Fatalf("a pull request read is bound to %v, want the allowed repository [%s]:\n%s", ids, gqlCeilingRepoA, statement)
					}
					continue
				}
				if !strings.Contains(statement, "{"+teamscope.BindingCeilingIDs+":Array(String)}") {
					t.Fatalf("a repository-keyed read has no ceiling term:\n%s", statement)
				}
				if ids := bindingStringValues(got.bindings[i], teamscope.BindingCeilingIDs); !reflect.DeepEqual(ids, []string{gqlCeilingRepoA}) {
					t.Fatalf("ceiling_ids = %v, want [%s]", ids, gqlCeilingRepoA)
				}
			}
			if data == 0 {
				t.Fatalf("no repository-keyed read ran:\n%s", strings.Join(got.statements, "\n----\n"))
			}
		})
	}
}

// R3: an allowlist that names no repository the organization has (and an empty one) serves nothing: every
// repository-keyed read is held to no repository.
func TestGraphQLCeilingNothingMatchedServesNothing(t *testing.T) {
	for name, keys := range map[string][]string{"unknown key": {"acme/none"}, "empty": {}} {
		for _, root := range ceilingRoots() {
			t.Run(name+"/"+root.key, func(t *testing.T) {
				got := runCeilingRoot(t, root.doc, allowlistOf(keys...))
				if strings.Contains(got.body, `"code":"SERVICE_UNAVAILABLE"`) || got.roots != 1 {
					t.Fatalf("answered %s: an allowlist caller is served on an N root", got.body)
				}
				for i, statement := range got.statements {
					if isCeilingData(statement, got.bindings[i]) && !strings.Contains(statement, "1 = 0") {
						t.Fatalf("a repository-keyed read is not held to no repository:\n%s", statement)
					}
				}
			})
		}
	}
}

// R2: an organization-wide allow reads exactly today's statements: no matcher, no ceiling term or binding.
func TestGraphQLCeilingOrganizationScopeReadsAsToday(t *testing.T) {
	for _, root := range ceilingRoots() {
		t.Run(root.key, func(t *testing.T) {
			legacy := runCeilingRootWith(t, nil, root.doc)
			got := runCeilingRoot(t, root.doc, nil)
			if !reflect.DeepEqual(got.statements, legacy.statements) || got.body != legacy.body {
				t.Fatalf("an organization-wide allow changed the reads or the answer:\n got %q\nwant %q", got.statements, legacy.statements)
			}
		})
	}
}

// A pull request of a repository outside the ceiling is answered as one that does not exist, with no read of its rows.
func TestGraphQLCeilingPullRequestOutsideTheCeilingIsNotFound(t *testing.T) {
	doc := `query Q { pr(orgId: "` + ecOrg.String() + `", id: "` + gqlCeilingRepoB + `#12") { __typename } }`
	got := runCeilingRoot(t, doc, allowlistOf("acme/a"))
	if !strings.Contains(got.body, `"pr":null`) || strings.Contains(got.body, `"errors"`) {
		t.Fatalf("answered %s, want pr null and no error", got.body)
	}
	for i, statement := range got.statements {
		if isCeilingData(statement, got.bindings[i]) {
			t.Fatalf("a pull request outside the ceiling was read:\n%s", statement)
		}
	}
}

// An operation with an N root and a root that does not carry the ceiling (an M root) under an allowlist is refused
// whole, no root field runs.
func TestGraphQLCeilingMixedOperationIsRefused(t *testing.T) {
	doc := `query Q { securityOverview(orgId: "` + ecOrg.String() + `") { __typename } improveOpportunities(limit: 5, windowDays: 30) { __typename } }`
	got := runCeilingRoot(t, doc, allowlistOf("acme/a"))
	if !strings.Contains(got.body, `"reason":"`+string(pdp.ReasonNotCarriedOut)+`"`) || got.roots != 0 || len(got.statements) != 0 {
		t.Fatalf("answered %s with %d root fields and %d statements, want the whole operation refused", got.body, got.roots, len(got.statements))
	}
}

// The operation's merged allowlist must be carried by every decided root: an N root allowed with an allowlist and a
// root not in the registry allowed organization-wide (each action decided on its own) is refused whole, no root
// field runs; the organization-wide allow alone does not let the unregistered root read past the allowlist.
func TestGraphQLCeilingMergedAllowlistMustBeCarriedByEveryRoot(t *testing.T) {
	allowlisted, orgWide := pdptest.New(t), pdptest.New(t)
	allowlisted.Obligations(allowlistOf("acme/a"))
	byAction := funcDecider{func(ctx context.Context, req pep.Request) pep.Decision {
		if req.Action == "metrics.read" {
			return pepOf(t, orgWide.URL).Decide(ctx, req)
		}
		return pepOf(t, allowlisted.URL).Decide(ctx, req)
	}}
	gate := pdp.New(pdp.Config{Mode: pdp.ModeEnforce, Decider: byAction, Switch: authzenforce.Parse(authzenforce.QueryAPI, "analytics,metrics"),
		ExtraShadowRows: graphQLShadowRows(), Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	doc := `query Q { securityOverview(orgId: "` + ecOrg.String() + `") { __typename } busFactor(orgId: "` + ecOrg.String() + `") { __typename } }`
	got := runCeilingRootWith(t, gate, doc)
	if !strings.Contains(got.body, `"reason":"`+string(pdp.ReasonNotCarriedOut)+`"`) || got.roots != 0 || len(got.statements) != 0 {
		t.Fatalf("answered %s with %d root fields and %d statements, want the whole operation refused", got.body, got.roots, len(got.statements))
	}
}
