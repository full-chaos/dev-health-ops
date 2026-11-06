package server

import (
	"net/http"
	"reflect"
	"strings"
	"testing"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/authctx"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/heatmap"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/pdp"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/pepgate"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/teamscope"
)

// CHAOS-13090 (PR-2b of CHAOS-10864): the eight N handlers read only the repositories of an allowlist decision.
// Every cell drives the real handler over a recording ClickHouse client; the matcher statement is answered with the
// (key, repository ids) rows a ClickHouse would return.

const (
	ceilingRepoA = "11111111-1111-1111-1111-111111111111"
	ceilingRepoB = "22222222-2222-2222-2222-222222222222"
)

func ceilingMatcherRows() map[string][]string {
	return map[string][]string{"acme/a": {ceilingRepoA}, "acme/b": {ceilingRepoB}}
}

// ceilingCases are the N rows: the GET and POST cases of the eight N classes, plus the developer heatmap.
func ceilingCases() []restOrgCase {
	var cases []restOrgCase
	for _, tc := range append(restOrgCases(), restOrgPostCases()...) {
		base := strings.TrimSuffix(tc.class, "_post")
		if kind, _ := pdp.ClassKind(base); kind != pdp.KindNarrowable || isLinkScopeClass(tc.class) || isUserScopeClass(tc.class) || isStateScopeClass(tc.class) {
			continue
		}
		cases = append(cases, tc)
	}
	cases = append(cases, restOrgCase{"heatmap_individual", http.MethodGet,
		"/api/v1/heatmap?type=individual&metric=active_hours&scope_type=developer&scope_id=abc123&range_days=30", "", nil,
		func(_ *testing.T, c *teamScopeReachClient) http.HandlerFunc {
			return newHeatmapWorkHandler(heatmap.QueryClient(c))
		}})
	return cases
}

func ceilingDrilldown(query string) restOrgCase {
	for _, tc := range restOrgCases() {
		if tc.class == "drilldown_prs" {
			tc.target = "/api/v1/drilldown/prs?start_date=2026-01-01&end_date=2026-01-15&" + query
			return tc
		}
	}
	panic("no drilldown_prs case")
}

func ceilingRun(t *testing.T, tc restOrgCase, scope pepgate.Scope, matcher map[string][]string) (restOrgRun, *teamScopeReachClient) {
	t.Helper()
	client := &teamScopeReachClient{matcher: matcher, person: "alice@example.com"}
	return tc.runWith(t, client, authctx.Claims{OrgID: restScopeClaimsOrg, Scope: scope}), client
}

// isRepoData reports a statement that reads repository-keyed rows. The two observer reads of the investment routes
// (membership state, argMax guard) are org-wide and feed metrics only (acp Q8: file:line proof in the PR body).
func isRepoData(statement string) bool {
	if strings.Contains(statement, "FROM system.") || strings.Contains(statement, "multi_generation_units") ||
		strings.Contains(statement, "latest_investment_computed_at") {
		return false
	}
	for _, table := range []string{"FROM git_pull_requests", "FROM git_commits", "FROM file_metrics_daily", "work_unit_investments"} {
		if strings.Contains(statement, table) {
			return true
		}
	}
	return false
}

func (r restOrgRun) dataStatements() []int {
	var out []int
	for i, statement := range r.statements {
		if isRepoData(statement) {
			out = append(out, i)
		}
	}
	return out
}

func (r restOrgRun) matcherStatements() []int {
	var out []int
	for i := range r.statements {
		if hasBinding(r.bindings[i], teamscope.BindingCeilingKeys) {
			out = append(out, i)
		}
	}
	return out
}

func mentionsValue(bindings []dhclickhouse.Binding, want string) bool {
	for _, binding := range bindings {
		switch value := binding.Value.(type) {
		case string:
			if value == want {
				return true
			}
		case []string:
			for _, v := range value {
				if v == want {
					return true
				}
			}
		}
	}
	return false
}

// R4 per handler: one matcher statement, org-scoped; every repository-keyed statement holds the ceiling of the
// allowed repository, bound as ids; the other repository is bound nowhere.
func TestCeiling_AllowlistServesOnlyTheAllowedRepository(t *testing.T) {
	for _, tc := range ceilingCases() {
		t.Run(tc.class, func(t *testing.T) {
			got, _ := ceilingRun(t, tc, pepgate.Allowlist(restTestScopeOrg, []string{"acme/a"}), ceilingMatcherRows())
			if got.status == http.StatusServiceUnavailable || got.status >= 500 {
				t.Fatalf("answered %d %q: an allowlist caller is served on an N route", got.status, got.body)
			}
			matchers := got.matcherStatements()
			if len(matchers) != 1 {
				t.Fatalf("%d matcher statements, want exactly 1 per request", len(matchers))
			}
			if !mentionsValue(got.bindings[matchers[0]], restTestScopeOrg) || mentionsValue(got.bindings[matchers[0]], restScopeClaimsOrg) {
				t.Fatalf("the matcher is not bound to the scope's organization")
			}
			data := got.dataStatements()
			if len(data) == 0 {
				t.Fatalf("no repository-keyed statement ran:\n%s", got.fingerprint())
			}
			for _, i := range data {
				if !strings.Contains(got.statements[i], "{"+teamscope.BindingCeilingIDs+":Array(String)}") {
					t.Fatalf("a repository-keyed statement has no ceiling term:\n%s", got.statements[i])
				}
				if ids := bindingStringValues(got.bindings[i], teamscope.BindingCeilingIDs); !reflect.DeepEqual(ids, []string{ceilingRepoA}) {
					t.Fatalf("ceiling_ids = %v, want [%s]", ids, ceilingRepoA)
				}
			}
			for i := range got.statements {
				if mentionsValue(got.bindings[i], ceilingRepoB) || strings.Contains(got.statements[i], ceilingRepoB) {
					t.Fatalf("the other repository reached a statement:\n%s", got.statements[i])
				}
			}
		})
	}
}

// R2: an Organization scope and Legacy read today's statements; no ceiling term, binding or matcher appears.
func TestCeiling_OrganizationAndLegacyScopesCarryNoCeiling(t *testing.T) {
	for _, tc := range ceilingCases() {
		for name, scope := range map[string]pepgate.Scope{"organization": pepgate.Organization(restTestScopeOrg), "legacy": pepgate.Legacy(restTestScopeOrg)} {
			t.Run(tc.class+"/"+name, func(t *testing.T) {
				got, _ := ceilingRun(t, tc, scope, ceilingMatcherRows())
				if len(got.statements) == 0 {
					t.Fatalf("no statement ran (status %d)", got.status)
				}
				for i, statement := range got.statements {
					if strings.Contains(statement, "ceiling_") || strings.Contains(statement, "identity_path") && strings.Contains(statement, "lower(identity_path)") {
						t.Fatalf("a ceiling reached an unrestricted statement:\n%s", statement)
					}
					if hasBinding(got.bindings[i], teamscope.BindingCeilingIDs) || hasBinding(got.bindings[i], teamscope.BindingCeilingKeys) {
						t.Fatalf("a ceiling binding reached an unrestricted statement:\n%s", statement)
					}
				}
			})
		}
	}
}

// R3, matcher 0 ids, matcher 2+ ids: nothing is served. No ceiling_ids binding exists; each repository-keyed
// statement holds "1 = 0" (or no such statement runs).
func TestCeiling_NothingMatchedServesNothing(t *testing.T) {
	cases := map[string]struct {
		keys    []string
		matcher map[string][]string
		matched bool
	}{
		"empty allowlist":     {nil, ceilingMatcherRows(), false},
		"key with no repo":    {[]string{"acme/none"}, ceilingMatcherRows(), true},
		"key with two ids":    {[]string{"acme/a"}, map[string][]string{"acme/a": {ceilingRepoA, ceilingRepoB}}, true},
		"two ids, one single": {[]string{"acme/a", "acme/c"}, map[string][]string{"acme/a": {ceilingRepoA, ceilingRepoB}, "acme/c": {}}, true},
	}
	for _, tc := range ceilingCases() {
		for name, c := range cases {
			t.Run(tc.class+"/"+name, func(t *testing.T) {
				got, _ := ceilingRun(t, tc, pepgate.Allowlist(restTestScopeOrg, c.keys), c.matcher)
				if want := map[bool]int{false: 0, true: 1}[c.matched]; len(got.matcherStatements()) != want {
					t.Fatalf("%d matcher statements, want %d", len(got.matcherStatements()), want)
				}
				for _, i := range got.dataStatements() {
					if !strings.Contains(got.statements[i], "1 = 0") || hasBinding(got.bindings[i], teamscope.BindingCeilingIDs) {
						t.Fatalf("a repository-keyed statement can read a repository:\n%s", got.statements[i])
					}
				}
				if got.status >= 500 {
					t.Fatalf("answered %d %q", got.status, got.body)
				}
			})
		}
	}
}

// R6: a request that names a repository outside the ceiling reads the repositories that are both named and allowed:
// two AND-ed membership terms, never the named one alone.
func TestCeiling_NamedRepositoryOutsideTheCeilingIsAnded(t *testing.T) {
	tc := ceilingDrilldown("scope_type=repo&scope_id=acme/b")
	client := &teamScopeReachClient{matcher: ceilingMatcherRows(), knownRepo: ceilingRepoB}
	got := tc.runWith(t, client, authctx.Claims{OrgID: restScopeClaimsOrg, Scope: pepgate.Allowlist(restTestScopeOrg, []string{"acme/a"})})
	data := got.dataStatements()
	if len(data) != 1 {
		t.Fatalf("%d repository-keyed statements:\n%s", len(data), got.fingerprint())
	}
	statement := got.statements[data[0]]
	if !strings.Contains(statement, "pr.repo_id IN {scope_ids:Array(String)}") || !strings.Contains(statement, "pr.repo_id IN {ceiling_ids:Array(String)}") {
		t.Fatalf("named and ceiling terms are not both present:\n%s", statement)
	}
	if strings.Contains(statement, " OR ") {
		t.Fatalf("the terms are not AND-ed:\n%s", statement)
	}
	if !reflect.DeepEqual(bindingStringValues(got.bindings[data[0]], teamscope.BindingCeilingIDs), []string{ceilingRepoA}) ||
		!reflect.DeepEqual(bindingStringValues(got.bindings[data[0]], "scope_ids"), []string{ceilingRepoB}) {
		t.Fatalf("bindings: %v", got.bindings[data[0]])
	}
}

// R5: the ceiling ANDs with a team scope; the team scope is never replaced or OR-ed.
func TestCeiling_AndsWithTheTeamScope(t *testing.T) {
	tc := ceilingDrilldown("scope_type=team&scope_id=T1")
	got, _ := ceilingRun(t, tc, pepgate.Allowlist(restTestScopeOrg, []string{"acme/a"}), ceilingMatcherRows())
	data := got.dataStatements()
	if len(data) != 1 {
		t.Fatalf("%d repository-keyed statements:\n%s", len(data), got.fingerprint())
	}
	statement := got.statements[data[0]]
	if !strings.Contains(statement, teamscope.Marker) || !strings.Contains(statement, "{ceiling_ids:Array(String)} AND ") ||
		strings.Contains(statement, "{ceiling_ids:Array(String)} OR ") || !strings.Contains(statement, "toUUIDOrNull") {
		t.Fatalf("ceiling and team scope are not AND-ed over UUIDs (the String-vs-UUID rule of teamscope):\n%s", statement)
	}
}

// Q4 (acp): the person lookups stay org-wide (existence only), and a person with no row in an allowed repository
// answers the SAME as a person that does not exist.
func TestCeiling_HiddenOnlyPersonAnswersAsAnUnknownPerson(t *testing.T) {
	for _, tc := range ceilingCases() {
		if tc.class != "people_drilldown_prs" && tc.class != "heatmap_individual" {
			continue
		}
		t.Run(tc.class, func(t *testing.T) {
			scope := pepgate.Allowlist(restTestScopeOrg, []string{"acme/a"})
			hidden := tc.runWith(t, &teamScopeReachClient{matcher: ceilingMatcherRows(), person: "alice@example.com"}, authctx.Claims{OrgID: restScopeClaimsOrg, Scope: scope})
			unknown := tc.runWith(t, &teamScopeReachClient{matcher: ceilingMatcherRows()}, authctx.Claims{OrgID: restScopeClaimsOrg, Scope: scope})
			if hidden.status != unknown.status || hidden.body != unknown.body {
				t.Fatalf("hidden-only person: %d %q; unknown person: %d %q", hidden.status, hidden.body, unknown.status, unknown.body)
			}
			// R2 (acp): the one answer is a 404 with a body; a known person with no allowed activity is not told apart.
			if hidden.status != http.StatusNotFound || hidden.body == "" {
				t.Fatalf("known person without allowed activity: %d %q, want the unknown person's 404", hidden.status, hidden.body)
			}
		})
	}
}

// Q8: the observer reads of #7 and #8 stay org-wide; they carry no ceiling, and the response is read through the
// ceiling alone.
func TestCeiling_ObserverReadsStayOrgWide(t *testing.T) {
	for _, tc := range ceilingCases() {
		if !strings.HasPrefix(tc.class, "investment") {
			continue
		}
		t.Run(tc.class, func(t *testing.T) {
			got, _ := ceilingRun(t, tc, pepgate.Allowlist(restTestScopeOrg, []string{"acme/a"}), ceilingMatcherRows())
			for i, statement := range got.statements {
				observer := strings.Contains(statement, "multi_generation_units") || strings.Contains(statement, "latest_investment_computed_at")
				if observer && (hasBinding(got.bindings[i], teamscope.BindingCeilingIDs) || strings.Contains(statement, "ceiling_")) {
					t.Fatalf("an observer read carries a ceiling:\n%s", statement)
				}
			}
		})
	}
}

// #9: an allowlist caller reads no data-version aggregate (it is org-wide) and no cache.
func TestCeiling_InvestmentExplainRunsNoOrgWideVersionRead(t *testing.T) {
	for _, tc := range ceilingCases() {
		if tc.class != "investment_explain" {
			continue
		}
		got, _ := ceilingRun(t, tc, pepgate.Allowlist(restTestScopeOrg, []string{"acme/a"}), ceilingMatcherRows())
		for _, statement := range got.statements {
			if strings.Contains(statement, "toString(max(computed_at))") || strings.Contains(statement, "FROM investment_explanations") {
				t.Fatalf("an allowlist caller read an org-wide aggregate or the cache:\n%s", statement)
			}
		}
	}
}

// #13 has no explanation cache: no scope kind reads or writes one.
func TestCeiling_WorkUnitExplainHasNoCache(t *testing.T) {
	for _, tc := range ceilingCases() {
		if tc.class != "work_unit_explain" {
			continue
		}
		for _, scope := range []pepgate.Scope{pepgate.Legacy(restTestScopeOrg), pepgate.Organization(restTestScopeOrg), pepgate.Allowlist(restTestScopeOrg, []string{"acme/a"})} {
			got, _ := ceilingRun(t, tc, scope, ceilingMatcherRows())
			for _, statement := range got.statements {
				if strings.Contains(statement, "investment_explanations") || strings.Contains(statement, "toString(max(computed_at))") {
					t.Fatalf("work-unit explain touched the investment cache:\n%s", statement)
				}
			}
		}
	}
}
