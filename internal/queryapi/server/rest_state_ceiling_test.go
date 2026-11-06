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

// CHAOS-13088 (PR-D of CHAOS-13083): sankey and filter options read only the repositories of an allowlist decision.
// Repository-keyed reads carry the ceiling; item reads go through the link of an item to an allowed repository; the
// daily aggregates that hold no item (work_item_metrics_daily, work_item_state_durations_daily,
// work_item_user_metrics_daily) are never read: their rows, and the status list, are recomputed from the linked items.
// Every cell drives the real handler over the recording client of the ceiling cells.

var stateScopeClasses = []string{"sankey", "filteroptions"}

func isStateScopeClass(class string) bool {
	return slices.Contains(stateScopeClasses, strings.TrimSuffix(class, "_post"))
}

var sankeyModes = []string{"investment", "expense", "state", "hotspot"}

// stateCeilingCases are the handlers of PR-D: sankey GET and POST in each mode, and filter options.
func stateCeilingCases() []restOrgCase {
	base := map[string]restOrgCase{}
	for _, tc := range append(restOrgCases(), restOrgPostCases()...) {
		base[tc.class] = tc
	}
	var out []restOrgCase
	for _, mode := range sankeyModes {
		get := base["sankey"]
		get.class, get.target = "sankey_"+mode, "/api/v1/sankey?mode="+mode+"&start_date=2026-01-01&end_date=2026-01-15"
		post := base["sankey_post"]
		post.class, post.body = "sankey_post_"+mode, `{"mode":"`+mode+`","filters":{"time":{"start_date":"2026-01-01","end_date":"2026-01-15"}}}`
		out = append(out, get, post)
	}
	return append(out, base["filteroptions"])
}

func stateCase(t *testing.T, class string) restOrgCase {
	t.Helper()
	for _, tc := range stateCeilingCases() {
		if tc.class == class {
			return tc
		}
	}
	t.Fatalf("no case %s", class)
	return restOrgCase{}
}

var (
	// stateItemTables are keyed by a work item: a read goes through the link.
	stateItemTables = []string{"work_items", "work_item_cycle_times", "work_item_team_attributions", "work_item_transitions"}
	// stateRepoTables are keyed by a repository column.
	stateRepoTables = []string{"work_unit_investments", "file_metrics_daily", "user_metrics_daily"}
)

// isMatcher reports the one statement that resolves the allowlist keys (it reads repos by name, with no ceiling).
func isMatcher(run restOrgRun, i int) bool {
	return hasBinding(run.bindings[i], teamscope.BindingCeilingKeys)
}

// R4: an allowlist caller reads no aggregate table; each item read is in the same statement as the link over the
// allowed repository; each repository-keyed read, and each read of the repository list, carries the ceiling of the
// allowed repository; the other repository reaches no statement.
func TestStateCeiling_ReadsAreHeldToTheAllowedRepository(t *testing.T) {
	for _, tc := range stateCeilingCases() {
		t.Run(tc.class, func(t *testing.T) {
			got, _ := ceilingRun(t, tc, pepgate.Allowlist(restTestScopeOrg, []string{"acme/a"}), ceilingMatcherRows())
			if got.status != http.StatusOK {
				t.Fatalf("answered %d %q", got.status, got.body)
			}
			reads := 0
			for i, statement := range got.statements {
				if readsAny(statement, aggregateTables) {
					t.Fatalf("an allowlist caller reads an aggregate daily table:\n%s", statement)
				}
				if readsAny(statement, stateItemTables) {
					reads++
					if !strings.Contains(statement, "FROM work_graph_issue_pr FINAL") || !strings.Contains(statement, "{"+linkBinding+":Array(UUID)}") ||
						!reflect.DeepEqual(boundIDs(got, i, linkBinding), []string{ceilingRepoA}) || !mentionsValue(got.bindings[i], restTestScopeOrg) {
						t.Fatalf("an item read is not held to the link of [%s]:\n%s", ceilingRepoA, statement)
					}
				}
				if readsAny(statement, stateRepoTables) || (readsAny(statement, []string{"repos"}) && !isMatcher(got, i)) {
					reads++
					if !reflect.DeepEqual(boundIDs(got, i, teamscope.BindingCeilingIDs), []string{ceilingRepoA}) {
						t.Fatalf("a repository-keyed read has no ceiling over [%s]:\n%s", ceilingRepoA, statement)
					}
				}
			}
			if reads == 0 {
				t.Fatalf("no item or repository read ran:\n%s", got.fingerprint())
			}
			for i := range got.statements {
				if mentionsValue(got.bindings[i], ceilingRepoB) || strings.Contains(got.statements[i], ceilingRepoB) {
					t.Fatalf("the other repository reached a statement:\n%s", got.statements[i])
				}
			}
		})
	}
}

// C4: an Organization scope and Legacy read today's statements and body: no ceiling, no link, no omission, no mark.
func TestStateCeiling_OrganizationAndLegacyAreTodaysAnswer(t *testing.T) {
	for _, tc := range stateCeilingCases() {
		for name, scope := range map[string]pepgate.Scope{"organization": pepgate.Organization(restTestScopeOrg), "legacy": pepgate.Legacy(restTestScopeOrg)} {
			t.Run(tc.class+"/"+name, func(t *testing.T) {
				got, _ := ceilingRun(t, tc, scope, ceilingMatcherRows())
				if len(got.statements) == 0 || got.status != http.StatusOK {
					t.Fatalf("no statement ran (status %d)", got.status)
				}
				for i, statement := range got.statements {
					if strings.Contains(statement, "ceiling_") || strings.Contains(statement, "work_graph_issue_pr") ||
						hasBinding(got.bindings[i], teamscope.BindingCeilingIDs) || hasBinding(got.bindings[i], linkBinding) ||
						hasBinding(got.bindings[i], teamscope.BindingCeilingKeys) {
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

// C6: nothing matched reads nothing: no aggregate, and each item or repository read holds "1 = 0" and binds no
// repository.
func TestStateCeiling_NothingMatchedReadsNothing(t *testing.T) {
	for _, tc := range stateCeilingCases() {
		for name, keys := range map[string][]string{"empty allowlist": nil, "key with no repo": {"acme/none"}} {
			t.Run(tc.class+"/"+name, func(t *testing.T) {
				got, _ := ceilingRun(t, tc, pepgate.Allowlist(restTestScopeOrg, keys), ceilingMatcherRows())
				if got.status != http.StatusOK {
					t.Fatalf("answered %d %q", got.status, got.body)
				}
				for i, statement := range got.statements {
					if readsAny(statement, aggregateTables) {
						t.Fatalf("an aggregate daily table is read:\n%s", statement)
					}
					if isMatcher(got, i) {
						continue
					}
					if (readsAny(statement, stateItemTables) || readsAny(statement, stateRepoTables) || readsAny(statement, []string{"repos"})) &&
						(!strings.Contains(statement, "1 = 0") || hasBinding(got.bindings[i], teamscope.BindingCeilingIDs) || hasBinding(got.bindings[i], linkBinding)) {
						t.Fatalf("a statement can read a repository or an item:\n%s", statement)
					}
				}
			})
		}
	}
}

// C10: a repository the request names outside the ceiling is never read: no link or ceiling binds it, and the
// work-item flows read no item for it.
func TestStateCeiling_NamedRepositoryOutsideTheCeilingReadsNothing(t *testing.T) {
	for _, mode := range sankeyModes {
		t.Run(mode, func(t *testing.T) {
			tc := stateCase(t, "sankey_"+mode)
			tc.target += "&scope_type=repo&scope_id=" + ceilingRepoB
			client := &teamScopeReachClient{matcher: ceilingMatcherRows(), knownRepo: ceilingRepoB}
			got := tc.runWith(t, client, authctx.Claims{OrgID: restScopeClaimsOrg, Scope: pepgate.Allowlist(restTestScopeOrg, []string{"acme/a"})})
			if got.status != http.StatusOK {
				t.Fatalf("answered %d %q", got.status, got.body)
			}
			for i, statement := range got.statements {
				for _, binding := range []string{linkBinding, teamscope.BindingCeilingIDs} {
					if slicesContain(bindingStringValues(got.bindings[i], binding), ceilingRepoB) {
						t.Fatalf("%s binds the repository outside the ceiling:\n%s", binding, statement)
					}
				}
				if readsAny(statement, stateItemTables) && !strings.Contains(statement, "1 = 0") {
					t.Fatalf("an item read ran for a named repository outside the ceiling (link %v):\n%s", boundIDs(got, i, linkBinding), statement)
				}
				if readsAny(statement, stateRepoTables) && !reflect.DeepEqual(boundIDs(got, i, teamscope.BindingCeilingIDs), []string{ceilingRepoA}) {
					t.Fatalf("a repository-keyed read is not held to the ceiling:\n%s", statement)
				}
			}
		})
	}
}

// acp: the answer says what it holds. A work-item flow counts the items linked to the allowed repositories and is
// marked so; a repository-keyed flow needs no mark; filter options list the lists they do not serve, with the reason.
func TestStateCeiling_TheAnswerSaysWhatItHolds(t *testing.T) {
	for class, want := range map[string][]string{
		"sankey_expense":      {`"narrowed_counts":[{"kind":"expense","scope":"repository ceiling"}]`},
		"sankey_post_state":   {`"narrowed_counts":[{"kind":"state","scope":"repository ceiling"}]`},
		"sankey_investment":   nil,
		"sankey_post_hotspot": nil,
		"filteroptions": {
			`"omitted_signals":[{"kind":"issue_type","reason":"not scoped to repositories"},{"kind":"team_catalog","reason":"not scoped to repositories"}]`,
			`"narrowed_counts":[{"kind":"filter_options","scope":"repository ceiling"}]`,
		},
	} {
		t.Run(class, func(t *testing.T) {
			got, _ := ceilingRun(t, stateCase(t, class), pepgate.Allowlist(restTestScopeOrg, []string{"acme/a"}), ceilingMatcherRows())
			if want == nil {
				if strings.Contains(got.body, "narrowed_counts") || strings.Contains(got.body, "omitted_signals") {
					t.Fatalf("a repository-keyed answer carries a mark it does not need: %s", got.body)
				}
				return
			}
			for _, w := range want {
				if !strings.Contains(got.body, w) {
					t.Fatalf("body has no %s: %s", w, got.body)
				}
			}
		})
	}
}

// C5: the two classes are carried now, each by its own rows; explain and the aggregated flame are still M, and an
// allowlist on them is still refused.
func TestStateCeiling_SankeyAndFilterOptionsAreNarrowed(t *testing.T) {
	for _, class := range stateScopeClasses {
		if kind, _ := pdp.ClassKind(class); kind != pdp.KindNarrowable || !pdp.AllowlistNarrowed(class) {
			t.Errorf("class %s kind %q narrowed %t, want N and narrowed", class, kind, pdp.AllowlistNarrowed(class))
		}
	}
	for _, class := range []string{"explain", "flame_aggregated"} {
		if kind, _ := pdp.ClassKind(class); kind != pdp.KindMixed || pdp.AllowlistNarrowed(class) {
			t.Errorf("class %s kind %q narrowed %t, want M and not narrowed (PR-D2)", class, kind, pdp.AllowlistNarrowed(class))
		}
	}
}
