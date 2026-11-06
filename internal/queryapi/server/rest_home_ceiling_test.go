package server

import (
	"net/http"
	"reflect"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/authctx"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/pdp"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/pepgate"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/teamscope"
)

// CHAOS-13086 (PR-B of CHAOS-13083): home and opportunities read only the repositories of an allowlist decision. The
// repository-keyed reads carry the ceiling; the work-item reads go through the link of an item to an allowed
// repository; the panels that carry no repository are omitted with a stated reason.

func homeCeilingCases() []restOrgCase {
	var cases []restOrgCase
	for _, tc := range append(restOrgCases(), restOrgPostCases()...) {
		switch tc.class {
		case "home", "home_post", "opportunities", "opportunities_post":
			cases = append(cases, tc)
		}
	}
	return cases
}

func homeTeamCase() restOrgCase {
	for _, tc := range restOrgCases() {
		if tc.class == "home" {
			tc.target += "&scope_type=team&scope_id=T1"
			return tc
		}
	}
	panic("no home case")
}

// repoKeyedTables are the tables of a home read keyed by a repository column.
var repoKeyedTables = []string{"FROM repo_metrics_daily", "FROM repos", "FROM ci_pipeline_runs", "FROM deploy_metrics_daily",
	"FROM cicd_metrics_daily", "FROM work_unit_investments"}

func isRepoKeyedRead(statement string) bool {
	if strings.Contains(statement, "{"+teamscope.BindingCeilingKeys+":") {
		return false
	}
	for _, table := range repoKeyedTables {
		if strings.Contains(statement, table) {
			return true
		}
	}
	return false
}

// R4: every repository-keyed statement carries the ceiling, bound as the ids of the allowed repository; the other
// repository reaches no statement.
func TestHomeCeiling_RepositoryKeyedReadsCarryTheCeiling(t *testing.T) {
	for _, tc := range homeCeilingCases() {
		t.Run(tc.class, func(t *testing.T) {
			got, _ := ceilingRun(t, tc, pepgate.Allowlist(restTestScopeOrg, []string{"acme/a"}), ceilingMatcherRows())
			if got.status >= 500 {
				t.Fatalf("answered %d %q", got.status, got.body)
			}
			seen := 0
			for i, statement := range got.statements {
				if !isRepoKeyedRead(statement) {
					continue
				}
				seen++
				if !strings.Contains(statement, "{"+teamscope.BindingCeilingIDs+":Array(String)}") ||
					!reflect.DeepEqual(bindingStringValues(got.bindings[i], teamscope.BindingCeilingIDs), []string{ceilingRepoA}) {
					t.Fatalf("a repository-keyed statement has no ceiling over [%s]:\n%s", ceilingRepoA, statement)
				}
			}
			if seen == 0 {
				t.Fatalf("no repository-keyed statement ran:\n%s", got.fingerprint())
			}
			for i := range got.statements {
				if mentionsValue(got.bindings[i], ceilingRepoB) || strings.Contains(got.statements[i], ceilingRepoB) {
					t.Fatalf("the other repository reached a statement:\n%s", got.statements[i])
				}
			}
		})
	}
}

// R2: an Organization scope and Legacy read today's statements and body: no ceiling, no link, no omitted panel.
func TestHomeCeiling_OrganizationAndLegacyAreTodaysAnswer(t *testing.T) {
	for _, tc := range homeCeilingCases() {
		for name, scope := range map[string]pepgate.Scope{"organization": pepgate.Organization(restTestScopeOrg), "legacy": pepgate.Legacy(restTestScopeOrg)} {
			t.Run(tc.class+"/"+name, func(t *testing.T) {
				got, _ := ceilingRun(t, tc, scope, ceilingMatcherRows())
				if len(got.statements) == 0 {
					t.Fatalf("no statement ran (status %d)", got.status)
				}
				for i, statement := range got.statements {
					if strings.Contains(statement, "ceiling_") || hasBinding(got.bindings[i], teamscope.BindingCeilingIDs) ||
						hasBinding(got.bindings[i], linkBinding) || hasBinding(got.bindings[i], teamscope.BindingCeilingKeys) {
						t.Fatalf("a ceiling or link term reached an unrestricted statement:\n%s", statement)
					}
				}
				if strings.Contains(got.body, "omitted_signals") || strings.Contains(got.body, "narrowed_counts") {
					t.Fatalf("an unrestricted answer carries omitted_signals or narrowed_counts: %s", got.body)
				}
			})
		}
	}
}

// R3: nothing matched reads nothing: no repository-keyed statement can read a repository.
func TestHomeCeiling_NothingMatchedReadsNothing(t *testing.T) {
	for _, tc := range homeCeilingCases() {
		for name, keys := range map[string][]string{"empty allowlist": nil, "key with no repo": {"acme/none"}} {
			t.Run(tc.class+"/"+name, func(t *testing.T) {
				got, _ := ceilingRun(t, tc, pepgate.Allowlist(restTestScopeOrg, keys), ceilingMatcherRows())
				if got.status >= 500 {
					t.Fatalf("answered %d %q", got.status, got.body)
				}
				for i, statement := range got.statements {
					if (isRepoKeyedRead(statement) || isWorkItemRead(statement)) &&
						(!strings.Contains(statement, "1 = 0") || hasBinding(got.bindings[i], teamscope.BindingCeilingIDs) || hasBinding(got.bindings[i], linkBinding)) {
						t.Fatalf("a statement can read a repository or an item:\n%s", statement)
					}
				}
			})
		}
	}
}

// acp (a): recommendations are not served to an allowlist caller, and the answer says why. A bare empty list is not
// the answer: the reason is present.
func TestHomeCeiling_RecommendationsAreOmittedWithAReason(t *testing.T) {
	got, _ := ceilingRun(t, homeTeamCase(), pepgate.Allowlist(restTestScopeOrg, []string{"acme/a"}), ceilingMatcherRows())
	if got.status != http.StatusOK {
		t.Fatalf("answered %d %q", got.status, got.body)
	}
	for _, statement := range got.statements {
		if strings.Contains(statement, "FROM recommendations_daily") {
			t.Fatalf("recommendations were read for an allowlist caller:\n%s", statement)
		}
	}
	for _, want := range []string{`"omitted_signals"`, `"kind":"recommendations"`, `"reason":"not scoped to repositories"`,
		`"narrowed_counts"`, `"scope":"repository ceiling"`} {
		if !strings.Contains(got.body, want) {
			t.Fatalf("body has no %s: %s", want, got.body)
		}
	}
}

// acp (b): compounding risk is read for scope repo inside the ceiling only; a team scope request reads none.
func TestHomeCeiling_CompoundingRiskServesRepoRowsInsideTheCeilingOnly(t *testing.T) {
	var orgCase restOrgCase
	for _, tc := range restOrgCases() {
		if tc.class == "home" {
			orgCase = tc
		}
	}
	scope := pepgate.Allowlist(restTestScopeOrg, []string{"acme/a"})
	for name, tc := range map[string]restOrgCase{"org level": orgCase, "team level": homeTeamCase()} {
		got, _ := ceilingRun(t, tc, scope, ceilingMatcherRows())
		var risk []int
		for i, statement := range got.statements {
			if strings.Contains(statement, "FROM compounding_risk_daily") {
				risk = append(risk, i)
			}
		}
		if name == "team level" {
			if len(risk) != 0 {
				t.Fatalf("%s: a team request read compounding_risk_daily for an allowlist caller", name)
			}
			continue
		}
		if len(risk) == 0 {
			continue
		}
		for _, i := range risk {
			statement := got.statements[i]
			if !strings.Contains(statement, "scope = 'repo' AND scope_id IN {"+teamscope.BindingCeilingIDs+":Array(String)}") ||
				!reflect.DeepEqual(bindingStringValues(got.bindings[i], teamscope.BindingCeilingIDs), []string{ceilingRepoA}) {
				t.Fatalf("%s: a compounding risk read is not held to repository rows of the ceiling:\n%s", name, statement)
			}
		}
	}
}

// A named repository outside the ceiling is never read: the link ids are the named repositories the ceiling holds.
func TestHomeCeiling_NamedRepositoryOutsideTheCeilingIsNeverLinked(t *testing.T) {
	var tc restOrgCase
	for _, c := range restOrgCases() {
		if c.class == "home" {
			c.target += "&scope_type=repo&scope_id=" + ceilingRepoB
			tc = c
		}
	}
	client := &teamScopeReachClient{matcher: ceilingMatcherRows(), knownRepo: ceilingRepoB}
	got := tc.runWith(t, client, authctx.Claims{OrgID: restScopeClaimsOrg, Scope: pepgate.Allowlist(restTestScopeOrg, []string{"acme/a"})})
	for i, statement := range got.statements {
		for _, name := range []string{linkBinding, teamscope.BindingCeilingIDs} {
			if slicesContain(bindingStringValues(got.bindings[i], name), ceilingRepoB) {
				t.Fatalf("%s binds the repository outside the ceiling:\n%s", name, statement)
			}
		}
	}
}

func slicesContain(values []string, want string) bool {
	for _, v := range values {
		if v == want {
			return true
		}
	}
	return false
}

// C5: home and opportunities are carried now: their classes are narrowed.
func TestHomeCeiling_TheHomeClassesAreNarrowed(t *testing.T) {
	for _, class := range []string{"home", "opportunities"} {
		if kind, _ := pdp.ClassKind(class); kind != pdp.KindNarrowable || !pdp.AllowlistNarrowed(class) {
			t.Errorf("class %s kind %q narrowed %t, want N and narrowed", class, kind, pdp.AllowlistNarrowed(class))
		}
	}
}
