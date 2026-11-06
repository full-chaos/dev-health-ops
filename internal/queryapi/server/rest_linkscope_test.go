package server

import (
	"net/http"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/authctx"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/pdp"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/pepgate"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/teamscope"
)

// CHAOS-13085 (PR-A of CHAOS-13083): the three item-grain routes narrow every work-item read to the items linked to an
// allowed repository through work_graph_issue_pr (never work_items.repo_id). Every cell drives the real handler over
// the recording client of the ceiling cells.

const linkBinding = "link_repo_ids"

var linkScopeClasses = []string{"drilldown_issues", "drilldown_issues_post", "people_drilldown_issues", "flame"}

func isLinkScopeClass(class string) bool { return slices.Contains(linkScopeClasses, class) }

// linkItemCases are the handlers whose work-item reads are item-grain: drilldown issues (GET, POST, POST blocked), the
// person's issues and the flame of an issue.
func linkItemCases() []restOrgCase {
	var cases []restOrgCase
	for _, tc := range append(restOrgCases(), restOrgPostCases()...) {
		if isLinkScopeClass(tc.class) {
			cases = append(cases, tc)
		}
	}
	for _, tc := range restOrgPostCases() {
		if tc.class == "drilldown_issues_post" {
			tc.class, tc.body = "drilldown_issues_post_blocked", `{"filters":{"how":{"blocked":true}}}`
			cases = append(cases, tc)
		}
	}
	return cases
}

// linkFlameCases are the flame entities that are keyed by repository: a pull request and a deployment.
func linkFlameCases() []restOrgCase {
	var cases []restOrgCase
	for _, tc := range restOrgCases() {
		if tc.class != "flame" {
			continue
		}
		for _, entity := range []string{"pr&entity_id=" + ceilingRepoB + ":5", "deployment&entity_id=" + ceilingRepoB + ":d1"} {
			tc.target = "/api/v1/flame?entity_type=" + entity
			tc.class = "flame_" + strings.SplitN(entity, "&", 2)[0]
			cases = append(cases, tc)
		}
	}
	return cases
}

func isWorkItemRead(statement string) bool {
	for _, table := range []string{"FROM work_item_cycle_times", "FROM work_item_blocked_durations_daily"} {
		if strings.Contains(statement, table) {
			return true
		}
	}
	return false
}

func (r restOrgRun) workItemReads() []int {
	var out []int
	for i, statement := range r.statements {
		if isWorkItemRead(statement) {
			out = append(out, i)
		}
	}
	return out
}

// C1/C2/C10: an allowlist caller reads the items linked to the allowed repository: the link subquery is in the same
// statement, org-bound, over the ceiling ids; the other repository reaches no statement.
func TestLinkScope_AllowlistBindsTheCeilingToTheLink(t *testing.T) {
	for _, tc := range linkItemCases() {
		t.Run(tc.class, func(t *testing.T) {
			got, _ := ceilingRun(t, tc, pepgate.Allowlist(restTestScopeOrg, []string{"acme/a"}), ceilingMatcherRows())
			if got.status >= 500 {
				t.Fatalf("answered %d %q", got.status, got.body)
			}
			reads := got.workItemReads()
			if len(reads) == 0 {
				t.Fatalf("no work-item read ran:\n%s", got.fingerprint())
			}
			for _, i := range reads {
				statement := got.statements[i]
				if !strings.Contains(statement, "FROM work_graph_issue_pr FINAL") || !strings.Contains(statement, "{"+linkBinding+":Array(UUID)}") {
					t.Fatalf("a work-item read is not narrowed by the link:\n%s", statement)
				}
				if ids := bindingStringValues(got.bindings[i], linkBinding); !reflect.DeepEqual(ids, []string{ceilingRepoA}) {
					t.Fatalf("%s = %v, want [%s]", linkBinding, ids, ceilingRepoA)
				}
				if !mentionsValue(got.bindings[i], restTestScopeOrg) {
					t.Fatalf("the link read is not bound to the scope's organization")
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

// C4: an Organization scope and Legacy read today's statements.
func TestLinkScope_OrganizationAndLegacyCarryNoLink(t *testing.T) {
	cases := append(linkItemCases(), linkFlameCases()...)
	for _, tc := range cases {
		for name, scope := range map[string]pepgate.Scope{"organization": pepgate.Organization(restTestScopeOrg), "legacy": pepgate.Legacy(restTestScopeOrg)} {
			t.Run(tc.class+"/"+name, func(t *testing.T) {
				got, _ := ceilingRun(t, tc, scope, ceilingMatcherRows())
				if len(got.statements) == 0 {
					t.Fatalf("no statement ran (status %d)", got.status)
				}
				for i, statement := range got.statements {
					if hasBinding(got.bindings[i], linkBinding) || hasBinding(got.bindings[i], teamscope.BindingCeilingIDs) ||
						hasBinding(got.bindings[i], teamscope.BindingCeilingKeys) || strings.Contains(statement, "ceiling_") {
						t.Fatalf("a link or ceiling term reached an unrestricted statement:\n%s", statement)
					}
					if isWorkItemRead(statement) && (strings.Contains(statement, "work_graph_issue_pr") || strings.Contains(statement, "1 = 0")) {
						t.Fatalf("a work-item read joins the link table in an unrestricted scope:\n%s", statement)
					}
				}
			})
		}
	}
}

// C6: nothing matched reads nothing: each work-item read holds "1 = 0" and binds no repository.
func TestLinkScope_NothingMatchedReadsNothing(t *testing.T) {
	for _, tc := range linkItemCases() {
		for name, keys := range map[string][]string{"empty allowlist": nil, "key with no repo": {"acme/none"}} {
			t.Run(tc.class+"/"+name, func(t *testing.T) {
				got, _ := ceilingRun(t, tc, pepgate.Allowlist(restTestScopeOrg, keys), ceilingMatcherRows())
				if got.status >= 500 {
					t.Fatalf("answered %d %q", got.status, got.body)
				}
				for _, i := range got.workItemReads() {
					if !strings.Contains(got.statements[i], "1 = 0") || hasBinding(got.bindings[i], linkBinding) {
						t.Fatalf("a work-item read can read an item:\n%s", got.statements[i])
					}
				}
			})
		}
	}
}

// C3/Q4: a person with no item in an allowed repository answers as a person that does not exist.
func TestLinkScope_HiddenOnlyPersonAnswersAsAnUnknownPerson(t *testing.T) {
	for _, tc := range linkItemCases() {
		if tc.class != "people_drilldown_issues" {
			continue
		}
		scope := pepgate.Allowlist(restTestScopeOrg, []string{"acme/a"})
		hidden := tc.runWith(t, &teamScopeReachClient{matcher: ceilingMatcherRows(), person: "alice@example.com"}, authctx.Claims{OrgID: restScopeClaimsOrg, Scope: scope})
		unknown := tc.runWith(t, &teamScopeReachClient{matcher: ceilingMatcherRows()}, authctx.Claims{OrgID: restScopeClaimsOrg, Scope: scope})
		if hidden.status != unknown.status || hidden.body != unknown.body {
			t.Fatalf("hidden-only person: %d %q; unknown person: %d %q", hidden.status, hidden.body, unknown.status, unknown.body)
		}
		if hidden.status != http.StatusNotFound || hidden.body == "" {
			t.Fatalf("known person without allowed items: %d %q, want the unknown person's 404", hidden.status, hidden.body)
		}
	}
}

// F-6: the flame of a pull request or a deployment is read through the ceiling on the entity's repository.
func TestLinkScope_FlameRepositoryEntitiesAreHeldToTheCeiling(t *testing.T) {
	for _, tc := range linkFlameCases() {
		t.Run(tc.class, func(t *testing.T) {
			got, _ := ceilingRun(t, tc, pepgate.Allowlist(restTestScopeOrg, []string{"acme/a"}), ceilingMatcherRows())
			var entityReads int
			for i, statement := range got.statements {
				if !strings.Contains(statement, "FROM git_pull_requests") && !strings.Contains(statement, "FROM deployments") {
					continue
				}
				entityReads++
				if !strings.Contains(statement, "{"+teamscope.BindingCeilingIDs+":Array(String)}") ||
					!reflect.DeepEqual(bindingStringValues(got.bindings[i], teamscope.BindingCeilingIDs), []string{ceilingRepoA}) {
					t.Fatalf("an entity read has no ceiling over [%s]:\n%s", ceilingRepoA, statement)
				}
			}
			if entityReads == 0 {
				t.Fatalf("no entity read ran:\n%s", got.fingerprint())
			}
			if got.status != http.StatusNotFound {
				t.Fatalf("an entity of a repository outside the ceiling answered %d %q, want 404", got.status, got.body)
			}
		})
	}
}

// F-6/K2: for an allowlist caller the flame of an issue never reads a repository from work_items.
func TestLinkScope_FlameIssueIdentityNeverReadsWorkItemsRepo(t *testing.T) {
	for _, tc := range restOrgCases() {
		if tc.class != "flame" {
			continue
		}
		got, _ := ceilingRun(t, tc, pepgate.Allowlist(restTestScopeOrg, []string{"acme/a"}), ceilingMatcherRows())
		for _, statement := range got.statements {
			if strings.Contains(statement, "FROM work_items") && strings.Contains(statement, "repo_id") {
				t.Fatalf("an allowlist read takes a repository from work_items:\n%s", statement)
			}
		}
	}
}

// C5: the three classes are carried now, so an allowlist decision reaches the handler.
func TestLinkScope_TheItemGrainClassesAreNarrowed(t *testing.T) {
	for _, class := range []string{"drilldown_issues", "people_drilldown_issues", "flame"} {
		if kind, _ := pdp.ClassKind(class); kind != pdp.KindNarrowable || !pdp.AllowlistNarrowed(class) {
			t.Errorf("class %s kind %q narrowed %t, want N and narrowed", class, kind, pdp.AllowlistNarrowed(class))
		}
	}
}
