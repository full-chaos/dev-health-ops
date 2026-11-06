package server

import (
	"reflect"
	"regexp"
	"slices"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/authctx"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/pdp"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/pepgate"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/teamscope"
)

// CHAOS-13087 (PR-C of CHAOS-13083): quadrant, people, people summary and people metric read only the repositories of an
// allowlist decision. Repository-keyed reads carry the ceiling; item reads go through the link of an item to an allowed
// repository; the two daily aggregates that hold no item (work_item_metrics_daily, work_item_user_metrics_daily) are
// never read: their rows are recomputed from the linked items. Every cell drives the real handler over the recording
// client of the ceiling cells.

var userScopeClasses = []string{"quadrant", "people", "people_summary", "people_metric"}

func isUserScopeClass(class string) bool { return slices.Contains(userScopeClasses, class) }

// userCeilingCases are the handlers of PR-C, each with the variants that reach a different read set.
func userCeilingCases() []restOrgCase {
	base := map[string]restOrgCase{}
	for _, tc := range restOrgCases() {
		base[tc.class] = tc
	}
	const dates = "start_date=2026-01-01&end_date=2026-01-15"
	var out []restOrgCase
	add := func(class, name, target string) {
		tc := base[class]
		tc.class, tc.target = name, target
		out = append(out, tc)
	}
	for _, q := range []struct{ name, query string }{
		{"quadrant_wip_team", "type=wip_throughput"},
		{"quadrant_cycle_team", "type=cycle_throughput"},
		{"quadrant_review_team", "type=review_load_latency"},
		{"quadrant_churn", "type=churn_throughput"},
		{"quadrant_wip_repo", "type=wip_throughput&scope_type=repo"},
		{"quadrant_review_repo", "type=review_load_latency&scope_type=repo"},
		{"quadrant_wip_person", "type=wip_throughput&scope_type=person&scope_id=abc"},
		{"quadrant_cycle_person", "type=cycle_throughput&scope_type=person&scope_id=abc"},
		{"quadrant_churn_person", "type=churn_throughput&scope_type=person&scope_id=abc"},
		{"quadrant_review_person", "type=review_load_latency&scope_type=person&scope_id=abc"},
	} {
		add("quadrant", q.name, "/api/v1/quadrant?"+q.query+"&"+dates)
	}
	add("people", "people", "/api/v1/people?q=ali")
	add("people_summary", "people_summary", "/api/v1/people/person-1/summary?"+dates)
	for _, metric := range []string{"churn", "throughput", "cycle_time", "wip_overlap", "blocked_work", "review_latency"} {
		add("people_metric", "people_metric_"+metric, "/api/v1/people/person-1/metric?metric="+metric+"&"+dates)
	}
	return out
}

var statementTable = regexp.MustCompile(`(?:FROM|JOIN)\s+([a-z_]+)`)

func tablesOf(statement string) map[string]bool {
	out := map[string]bool{}
	for _, match := range statementTable.FindAllStringSubmatch(statement, -1) {
		out[match[1]] = true
	}
	return out
}

var (
	// aggregateTables hold no item and no repository: a restricted caller never reads them.
	aggregateTables = []string{"work_item_metrics_daily", "work_item_user_metrics_daily", "work_item_state_durations_daily"}
	// userItemTables are keyed by a work item: a read goes through the link.
	userItemTables = []string{"work_items", "work_item_cycle_times", "work_item_team_attributions", "work_item_blocked_durations_daily"}
	// userRepoTables are keyed by a repository column.
	userRepoTables = []string{"user_metrics_daily", "repo_metrics_daily", "git_pull_requests"}
)

func readsAny(statement string, tables []string) bool {
	found := tablesOf(statement)
	for _, table := range tables {
		if found[table] {
			return true
		}
	}
	return false
}

func boundIDs(run restOrgRun, i int, name string) []string {
	return bindingStringValues(run.bindings[i], name)
}

// R4: an allowlist caller reads no aggregate table; each item read is in the same statement as the link over the
// allowed repository; each repository-keyed read carries the ceiling or the held ids of the allowed repository; the
// other repository reaches no statement.
func TestUserCeiling_ReadsAreHeldToTheAllowedRepository(t *testing.T) {
	for _, tc := range userCeilingCases() {
		t.Run(tc.class, func(t *testing.T) {
			got, _ := ceilingRun(t, tc, pepgate.Allowlist(restTestScopeOrg, []string{"acme/a"}), ceilingMatcherRows())
			if got.status >= 500 {
				t.Fatalf("answered %d %q", got.status, got.body)
			}
			reads := 0
			for i, statement := range got.statements {
				if readsAny(statement, aggregateTables) {
					t.Fatalf("an allowlist caller reads an aggregate daily table:\n%s", statement)
				}
				if readsAny(statement, userItemTables) {
					reads++
					if !strings.Contains(statement, "FROM work_graph_issue_pr FINAL") || !strings.Contains(statement, "{"+linkBinding+":Array(UUID)}") ||
						!reflect.DeepEqual(boundIDs(got, i, linkBinding), []string{ceilingRepoA}) || !mentionsValue(got.bindings[i], restTestScopeOrg) {
						t.Fatalf("an item read is not held to the link of [%s]:\n%s", ceilingRepoA, statement)
					}
				}
				if readsAny(statement, userRepoTables) {
					reads++
					if !reflect.DeepEqual(boundIDs(got, i, teamscope.BindingCeilingIDs), []string{ceilingRepoA}) &&
						!reflect.DeepEqual(boundIDs(got, i, "scope_repo_ids"), []string{ceilingRepoA}) {
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

// C4: an Organization scope and Legacy read today's statements and body: no ceiling, no link, no omitted signal.
func TestUserCeiling_OrganizationAndLegacyAreTodaysAnswer(t *testing.T) {
	for _, tc := range userCeilingCases() {
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

// C6: nothing matched reads nothing: each item or repository read holds "1 = 0" and binds no repository.
func TestUserCeiling_NothingMatchedReadsNothing(t *testing.T) {
	for _, tc := range userCeilingCases() {
		for name, keys := range map[string][]string{"empty allowlist": nil, "key with no repo": {"acme/none"}} {
			t.Run(tc.class+"/"+name, func(t *testing.T) {
				got, _ := ceilingRun(t, tc, pepgate.Allowlist(restTestScopeOrg, keys), ceilingMatcherRows())
				if got.status >= 500 {
					t.Fatalf("answered %d %q", got.status, got.body)
				}
				for i, statement := range got.statements {
					if readsAny(statement, aggregateTables) {
						t.Fatalf("an aggregate daily table is read:\n%s", statement)
					}
					if (readsAny(statement, userItemTables) || readsAny(statement, userRepoTables)) &&
						(!strings.Contains(statement, "1 = 0") || hasBinding(got.bindings[i], teamscope.BindingCeilingIDs) || hasBinding(got.bindings[i], linkBinding)) {
						t.Fatalf("a statement can read a repository or an item:\n%s", statement)
					}
				}
			})
		}
	}
}

// The identity of a person is listed or resolved only from an allowed repository: the work arm reads the assignees of
// the linked items, never work_item_user_metrics_daily.
func TestUserCeiling_PeopleSearchListsOnlyPeopleOfAllowedRepositories(t *testing.T) {
	var tc restOrgCase
	for _, c := range userCeilingCases() {
		if c.class == "people" {
			tc = c
		}
	}
	got, _ := ceilingRun(t, tc, pepgate.Allowlist(restTestScopeOrg, []string{"acme/a"}), ceilingMatcherRows())
	found := false
	for i, statement := range got.statements {
		if !strings.Contains(statement, "identities") || !strings.Contains(statement, "LIKE") {
			continue
		}
		found = true
		if strings.Contains(statement, "work_item_user_metrics_daily") {
			t.Fatalf("the work arm of the people list reads the user daily table:\n%s", statement)
		}
		if !strings.Contains(statement, "FROM work_item_cycle_times") || !strings.Contains(statement, "assignee") ||
			!reflect.DeepEqual(boundIDs(got, i, linkBinding), []string{ceilingRepoA}) {
			t.Fatalf("the work arm is not the assignees of the linked items of [%s]:\n%s", ceilingRepoA, statement)
		}
	}
	if !found {
		t.Fatalf("no people list statement ran:\n%s", got.fingerprint())
	}
}

// A person known only by work in a hidden repository answers like an unknown id.
func TestUserCeiling_PersonResolutionUsesTheLinkedAssignees(t *testing.T) {
	for _, class := range []string{"people_summary", "people_metric_churn", "quadrant_wip_person"} {
		var tc restOrgCase
		for _, c := range userCeilingCases() {
			if c.class == class {
				tc = c
			}
		}
		got, _ := ceilingRun(t, tc, pepgate.Allowlist(restTestScopeOrg, []string{"acme/a"}), ceilingMatcherRows())
		found := false
		for i, statement := range got.statements {
			if !strings.Contains(statement, "lower(hex(MD5(identity)))") {
				continue
			}
			found = true
			if strings.Contains(statement, "work_item_user_metrics_daily") || !strings.Contains(statement, "FROM work_item_cycle_times") ||
				!reflect.DeepEqual(boundIDs(got, i, linkBinding), []string{ceilingRepoA}) ||
				!reflect.DeepEqual(boundIDs(got, i, teamscope.BindingCeilingIDs), []string{ceilingRepoA}) {
				t.Fatalf("%s: person resolution is not held to [%s]:\n%s", class, ceilingRepoA, statement)
			}
		}
		if !found {
			t.Fatalf("%s: no person resolution ran:\n%s", class, got.fingerprint())
		}
	}
}

// acp: anything that is not served under a ceiling is listed with the reason. The repository wip of the quadrant reads
// a work scope, not a link; the idle-team points name teams that carry no repository.
func TestUserCeiling_QuadrantOmissionsAreListed(t *testing.T) {
	for class, want := range map[string][]string{
		"quadrant_wip_repo":   {`"omitted_signals"`, `"kind":"wip"`, `"reason":"not scoped to repositories"`},
		"quadrant_wip_team":   {`"omitted_signals"`, `"kind":"idle_teams"`, `"reason":"not scoped to repositories"`},
		"quadrant_wip_person": nil,
	} {
		var tc restOrgCase
		for _, c := range userCeilingCases() {
			if c.class == class {
				tc = c
			}
		}
		got, _ := ceilingRun(t, tc, pepgate.Allowlist(restTestScopeOrg, []string{"acme/a"}), ceilingMatcherRows())
		if class == "quadrant_wip_person" {
			if strings.Contains(got.body, "omitted_signals") {
				t.Fatalf("%s lists an omission it does not make: %s", class, got.body)
			}
			continue
		}
		for _, w := range want {
			if !strings.Contains(got.body, w) {
				t.Fatalf("%s: body has no %s: %s", class, w, got.body)
			}
		}
	}
}

// Counts under a ceiling are the narrowed counts, and the answer says so.
func TestUserCeiling_SummaryCountsAreMarkedNarrowed(t *testing.T) {
	var tc restOrgCase
	for _, c := range userCeilingCases() {
		if c.class == "people_summary" {
			tc = c
		}
	}
	got, _ := ceilingRun(t, tc, pepgate.Allowlist(restTestScopeOrg, []string{"acme/a"}), ceilingMatcherRows())
	for _, w := range []string{`"narrowed_counts"`, `"scope":"repository ceiling"`, `"kind":"coverage"`} {
		if !strings.Contains(got.body, w) {
			t.Fatalf("body has no %s: %s", w, got.body)
		}
	}
}

// A named repository outside the ceiling is never read; a team scope never widens the ceiling.
func TestUserCeiling_NamedRepositoryAndTeamNeverWidenTheCeiling(t *testing.T) {
	var tc restOrgCase
	for _, c := range userCeilingCases() {
		if c.class == "quadrant_wip_team" {
			tc = c
		}
	}
	for name, suffix := range map[string]string{
		"named other repository": "&repo_ids=" + ceilingRepoB,
		"team":                   "&team_ids=T1",
		"team and other repo":    "&team_ids=T1&repo_ids=" + ceilingRepoB,
	} {
		t.Run(name, func(t *testing.T) {
			c := tc
			c.target += suffix
			client := &teamScopeReachClient{matcher: ceilingMatcherRows(), knownRepo: ceilingRepoB}
			got := c.runWith(t, client, authctx.Claims{OrgID: restScopeClaimsOrg, Scope: pepgate.Allowlist(restTestScopeOrg, []string{"acme/a"})})
			for i, statement := range got.statements {
				for _, binding := range []string{linkBinding, teamscope.BindingCeilingIDs, "scope_repo_ids"} {
					if slicesContain(bindingStringValues(got.bindings[i], binding), ceilingRepoB) {
						t.Fatalf("%s binds the repository outside the ceiling:\n%s", binding, statement)
					}
				}
			}
			// A named repository outside the ceiling leaves nothing: no item and no repository-keyed read runs for it.
			if strings.Contains(suffix, "repo_ids="+ceilingRepoB) {
				for i, statement := range got.statements {
					if readsAny(statement, userItemTables) || readsAny(statement, userRepoTables) {
						t.Fatalf("a read ran for a named repository outside the ceiling (link %v):\n%s", boundIDs(got, i, linkBinding), statement)
					}
				}
			}
		})
	}
}

// C5: the four classes are carried now, each by its own rows.
func TestUserCeiling_TheUserClassesAreNarrowed(t *testing.T) {
	for _, class := range userScopeClasses {
		if kind, _ := pdp.ClassKind(class); kind != pdp.KindNarrowable || !pdp.AllowlistNarrowed(class) {
			t.Errorf("class %s kind %q narrowed %t, want N and narrowed", class, kind, pdp.AllowlistNarrowed(class))
		}
	}
}
