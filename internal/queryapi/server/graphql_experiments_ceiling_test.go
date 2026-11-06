package server

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"regexp"
	"slices"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/pdp"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/pdp/pdptest"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/pepgate"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/routeswitch"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/teamscope"
)

// CHAOS-13162 (6.11b PR-2b): Query.experiments serves an allowlist decision under the repository ceiling (it is the
// REST opportunities build, which carries the ceiling since CHAOS-13086); Query.home stays out of the GraphQL registry
// and an allowlist decision on it is refused with allowlist_route_unsupported before any read (D6743 option B). A
// caller with no ceiling reads today's statements and gets today's bytes. Every cell drives the real /graphql pipeline
// (edge chain, org scope, dispatch, gqlgen server with the enforce interceptor) over the recording ClickHouse client.

var (
	experimentsDoc        = `query Q { experiments(orgId: "` + ecOrg.String() + `") { items { id metric } derivedFromOpportunities } }`
	homeDoc               = `query Q { home(orgId: "` + ecOrg.String() + `") { healthState { status } } }`
	experimentsAndHomeDoc = `query Q { experiments(orgId: "` + ecOrg.String() + `") { derivedFromOpportunities } home(orgId: "` + ecOrg.String() + `") { healthState { status } } }`
	// The two repositories of the cells. Neither is the organization id of the edge cells (ecOrg), so a bound
	// organization never reads as a repository.
	experimentsRepoA       = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
	experimentsRepoB       = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
	experimentsCeilingKeys = map[string][]string{"acme/a": {experimentsRepoA}, "acme/b": {experimentsRepoB}}

	// experimentsAggregateTables hold no item and no repository: an allowlist caller never reads them.
	experimentsAggregateTables = []string{"work_item_metrics_daily", "work_item_user_metrics_daily", "work_item_state_durations_daily"}
	experimentsTableRef        = regexp.MustCompile(`(?:FROM|JOIN)\s+([a-z_]+)`)
)

func experimentsReadsAny(statement string, tables []string) bool {
	for _, match := range experimentsTableRef.FindAllStringSubmatch(statement, -1) {
		if slices.Contains(tables, match[1]) {
			return true
		}
	}
	return false
}

// ceilingRunGQL is what one operation answered and every statement it read, with its bindings.
type ceilingRunGQL struct {
	code       int
	body       string
	statements []string
	bindings   [][]dhclickhouse.Binding
}

// homePostgres serves no Postgres row: home reads its sync-run state from Postgres.
type homePostgres struct{}

func (homePostgres) Query(context.Context, string, ...any) (pgx.Rows, error) {
	return nil, errors.New("homePostgres: no rows")
}
func (homePostgres) QueryRow(context.Context, string, ...any) pgx.Row { return homePostgresRow{} }

type homePostgresRow struct{}

func (homePostgresRow) Scan(...any) error { return pgx.ErrNoRows }

// runGraphQLCeiling is runGraphQLEnforce for a user caller, over a recording client that answers the allowlist key
// matcher, with Postgres for home; it returns every statement and its bindings.
func runGraphQLCeiling(t *testing.T, gate *pdp.Gate, document string) ceilingRunGQL {
	t.Helper()
	quietStdLog(t)
	store := &fakeEdgeStore{
		states:  map[uuid.UUID]policy.UserState{ecUser: {IsActive: true, TokenVersion: 5}},
		found:   map[uuid.UUID]bool{ecUser: true},
		members: map[[2]uuid.UUID]bool{{ecUser, ecOrg}: true},
		roles:   map[[2]uuid.UUID]string{{ecUser, ecOrg}: "admin"},
	}
	client := &teamScopeReachClient{matcher: experimentsCeilingKeys}
	server := newGraphQLServerWithLimits(&graph.Resolver{ClickHouse: client, Postgres: homePostgres{}}, 1000, 10)
	subject := graphQLShadowSubject(newAccessMinter(t).mint(ecUser, nil, nil))
	mux := routeswitch.NewMux(routeswitch.StaticSwitch{"gql": true})
	mux.Register("gql", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := graph.WithShadowSubject(pdp.WithGate(r.Context(), gate), subject)
		server.ServeHTTP(w, r.WithContext(ctx))
	}))
	verifier, _ := iaVerifier(t)
	auth := ecEdgeAuth(t, store)
	chain := graphQLEdgeChain(newDocumentDispatchHandler(os.Getenv, mux, map[string]string{digestHex(document): "gql"}, verifier, auth, store, "", nil),
		graphQLEdgeDeps{auth: auth, maxBytes: defaultGraphQLMaxQueryBytes})
	token := ecMintEdgeToken(t, ecEdgeClaims{sub: ecUser.String(), orgID: ecOrg.String(), role: "admin", tokenVersion: 5})
	request := httptest.NewRequest(http.MethodPost, "/graphql", strings.NewReader(`{"query": `+jsonQuote(t, document)+`}`))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Authorization", "Bearer "+token)
	recorder := httptest.NewRecorder()
	chain.ServeHTTP(recorder, request)
	gate.Wait()
	client.mu.Lock()
	defer client.mu.Unlock()
	return ceilingRunGQL{code: recorder.Code, body: recorder.Body.String(),
		statements: append([]string(nil), client.statements...), bindings: append([][]dhclickhouse.Binding(nil), client.bindings...)}
}

// fingerprint is the status, the body and the sorted statements with their bindings, time values masked.
func (r ceilingRunGQL) fingerprint() string {
	blocks := make([]string, 0, len(r.statements))
	for i, statement := range r.statements {
		var b strings.Builder
		fmt.Fprintf(&b, "%s\n", statement)
		for _, binding := range r.bindings[i] {
			if _, isTime := binding.Value.(time.Time); isTime {
				fmt.Fprintf(&b, "  %s=<time>\n", binding.Name)
				continue
			}
			fmt.Fprintf(&b, "  %s=%v\n", binding.Name, binding.Value)
		}
		blocks = append(blocks, b.String())
	}
	sort.Strings(blocks)
	return fmt.Sprintf("%d %s\n%s", r.code, r.body, strings.Join(blocks, ""))
}

// allowlistGate is an enforcing gate with the analytics cell on over a PDP that allows with obligations.
func allowlistGate(t *testing.T, obligations map[string]any) *pdp.Gate {
	t.Helper()
	srv := pdptest.New(t)
	srv.Obligations(obligations)
	gate, _ := enforceGate(t, pdp.ModeEnforce, "analytics", pepOf(t, srv.URL))
	return gate
}

func allowlistOf(keys ...string) map[string]any {
	return map[string]any{"tenant_scope": ecOrg.String(), "repository_allowlist": keys}
}

// An allowlist decision on Query.experiments is served under the ceiling: one key matcher read, no read of a daily
// table that holds no repository, each repository-keyed read held to the ceiling of the allowed repository, and the
// other repository bound nowhere. (The work-item reads are those of the REST build: the parity cell below.)
func TestGraphQLExperimentsUnderAnAllowlistIsHeldToTheCeiling(t *testing.T) {
	got := runGraphQLCeiling(t, allowlistGate(t, allowlistOf("acme/a")), experimentsDoc)
	if got.code != http.StatusOK || strings.Contains(got.body, `"errors"`) || !strings.Contains(got.body, `"derivedFromOpportunities":true`) {
		t.Fatalf("answered %d %s, want the experiments derived from the opportunity cards", got.code, got.body)
	}
	matchers, repoReads := 0, 0
	for i, statement := range got.statements {
		bindings := got.bindings[i]
		if hasBinding(bindings, teamscope.BindingCeilingKeys) {
			matchers++
			continue
		}
		if experimentsReadsAny(statement, experimentsAggregateTables) && !strings.Contains(statement, "1 = 0") {
			t.Fatalf("an allowlist caller reads a daily table with no repository:\n%s", statement)
		}
		if isRepoKeyedRead(statement) {
			repoReads++
			if !reflect.DeepEqual(bindingStringValues(bindings, teamscope.BindingCeilingIDs), []string{experimentsRepoA}) {
				t.Fatalf("a repository-keyed read has no ceiling over [%s]:\n%s", experimentsRepoA, statement)
			}
		}
		if mentionsValue(bindings, experimentsRepoB) || strings.Contains(statement, experimentsRepoB) {
			t.Fatalf("the other repository reached a statement (bindings %v):\n%s", bindings, statement)
		}
	}
	if matchers != 1 || repoReads == 0 {
		t.Fatalf("%d key matcher reads and %d repository-keyed reads, want 1 and some:\n%s", matchers, repoReads, got.fingerprint())
	}
}

// Parity (PLAN-10865 Q5): the same allowlist decision on REST GET /api/v1/opportunities and on Query.experiments, with
// the same window and scope, runs the same statements with the same bindings: one build, one ceiling.
func TestGraphQLExperimentsAndRESTOpportunitiesReadTheSameUnderTheCeiling(t *testing.T) {
	gql := runGraphQLCeiling(t, allowlistGate(t, allowlistOf("acme/a")), experimentsDoc)
	var rest restOrgCase
	for _, tc := range restOrgCases() {
		if tc.class == "opportunities" {
			rest = tc
		}
	}
	rest.target = "/api/v1/opportunities?range_days=30&compare_days=30"
	restRun, _ := ceilingRun(t, rest, pepgate.Allowlist(restTestScopeOrg, []string{"acme/a"}), experimentsCeilingKeys)
	if restRun.status != http.StatusOK {
		t.Fatalf("REST answered %d %s", restRun.status, restRun.body)
	}
	norm := func(statements []string, bindings [][]dhclickhouse.Binding, org string) []string {
		var out []string
		for i, statement := range statements {
			var b strings.Builder
			b.WriteString(statement)
			for _, binding := range bindings[i] {
				value := fmt.Sprint(binding.Value)
				if _, isTime := binding.Value.(time.Time); isTime {
					value = "<time>"
				}
				fmt.Fprintf(&b, "\n  %s=%s", binding.Name, strings.ReplaceAll(value, org, "<org>"))
			}
			out = append(out, b.String())
		}
		sort.Strings(out)
		return out
	}
	gqlReads := norm(gql.statements, gql.bindings, ecOrg.String())
	restReads := norm(restRun.statements, restRun.bindings, restTestScopeOrg)
	if !reflect.DeepEqual(gqlReads, restReads) {
		t.Fatalf("GraphQL experiments and REST opportunities read differently under one ceiling:\nGraphQL (%d):\n%s\n\nREST (%d):\n%s",
			len(gqlReads), strings.Join(gqlReads, "\n---\n"), len(restReads), strings.Join(restReads, "\n---\n"))
	}
}

// D6743 option B: an allowlist decision on Query.home, alone or beside Query.experiments, is refused with
// allowlist_route_unsupported before any resolver runs (0 statements); an empty allowlist included.
func TestGraphQLHomeUnderAnAllowlistIsRefusedAsUnsupported(t *testing.T) {
	for _, document := range []string{homeDoc, experimentsAndHomeDoc} {
		for name, keys := range map[string][]string{"one": {"acme/a"}, "empty": {}} {
			t.Run(name, func(t *testing.T) {
				got := runGraphQLCeiling(t, allowlistGate(t, allowlistOf(keys...)), document)
				run := enforceRun{code: got.code, body: got.body, statements: len(got.statements)}
				run.code503(t, string(pdp.ReasonAllowlistRouteUnsupported))
			})
		}
	}
}

// A caller with no ceiling is today's answer: the gate off, an Organization allow, and an allowlist holding "*" read
// the statements of the path with no gate and answer its bytes, on experiments, on home, and on both together.
func TestGraphQLExperimentsAndHomeWithoutACeilingAreTodaysAnswer(t *testing.T) {
	for _, document := range []string{experimentsDoc, homeDoc, experimentsAndHomeDoc} {
		legacy := runGraphQLCeiling(t, nil, document)
		if legacy.code != http.StatusOK || len(legacy.statements) == 0 {
			t.Fatalf("today's path answered %d with %d statements: %s", legacy.code, len(legacy.statements), legacy.body)
		}
		for name, gate := range map[string]*pdp.Gate{
			"gate off":     pdp.New(pdp.Config{Mode: pdp.ModeOff}),
			"organization": allowlistGate(t, map[string]any{"tenant_scope": ecOrg.String(), "repository_scope": "organization"}),
			"star":         allowlistGate(t, allowlistOf("*")),
		} {
			if got := runGraphQLCeiling(t, gate, document); got.fingerprint() != legacy.fingerprint() {
				t.Fatalf("%s differs from today's path for %s:\n got %s\nwant %s", name, document, got.fingerprint(), legacy.fingerprint())
			}
		}
	}
}
