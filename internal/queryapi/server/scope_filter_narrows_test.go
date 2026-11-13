package server

import (
	"context"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/home"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/opportunities"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/teamscope"
)

// CHAOS-9104 (D5844, D5841, D5855): every consumer that narrows a repo-keyed read by a
// team scope and by named repositories calls the ONE shared function
// (teamscope.NarrowRepoScope). Each drives its real builder with a recording client
// and reads the statements it actually sent.

// routes that cannot name repositories beside a team scope (their request has no
// what.repos and one scope id): the AND needs both.
var routesWithoutNamedReposBesideATeam = map[string]bool{
	"GET /api/v1/work-units":                         true,
	"POST /api/v1/work-units/{work_unit_id}/explain": true,
	"GET /api/v1/heatmap":                            true,
}

var (
	explicitThenOr  = regexp.MustCompile(`IN \{(scope_ids|repo_ids):Array\(String\)\} OR `)
	explicitThenAnd = regexp.MustCompile(`IN \{(scope_ids|repo_ids):Array\(String\)\} AND `)
)

func (c *teamScopeReachClient) statementsWith(substr string) []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []string
	for _, statement := range c.statements {
		if strings.Contains(statement, substr) {
			out = append(out, statement)
		}
	}
	return out
}

// A team and a named repository narrow: the repositories that are both. Never the
// union.
func TestTeamAndNamedRepositoryNarrowOnEverySurface(t *testing.T) {
	const repo = "11111111-1111-4111-8111-111111111111"
	for _, consumer := range teamScopeConsumers() {
		if routesWithoutNamedReposBesideATeam[consumer.route] {
			continue
		}
		t.Run(consumer.route, func(t *testing.T) {
			client := &teamScopeReachClient{knownRepo: repo}
			consumer.drive(t, client, "team", []string{teamScopeReachTeam}, []string{repo})
			withTeam := client.statementsWith(teamscope.Marker)
			if len(withTeam) == 0 {
				t.Fatalf("%s sent no statement with the team condition", consumer.route)
			}
			anded := false
			for _, statement := range withTeam {
				if explicitThenOr.MatchString(statement) {
					t.Errorf("%s widens: the named repository is ORed with the team's:\n%s", consumer.route, statement)
				}
				if explicitThenAnd.MatchString(statement) {
					anded = true
				}
			}
			if !anded {
				t.Errorf("%s: no statement ANDs the named repository with the team condition:\n%v", consumer.route, withTeam)
			}
		})
	}
}

// emptyMatchStatements is how many statements of each consumer's real builder carry
// the empty match when the one repository it names resolves to nothing: every read
// of a repo-keyed fact table the request makes, not only one of them (a
// consumer that drops the match on one of its reads serves part of the answer over
// the whole organization).
var emptyMatchStatements = map[string]int{
	"GET /api/v1/work-units":                         1,
	"POST /api/v1/work-units":                        1,
	"POST /api/v1/investment/explain":                2,
	"POST /api/v1/work-units/{work_unit_id}/explain": 1,
	"GET/POST /api/v1/opportunities":                 23,
	"GET/POST /api/v1/home":                          23,
	"GET/POST /api/v1/explain":                       4,
	"GET /api/v1/heatmap":                            1,
	"GET/POST /api/v1/sankey":                        1,
	"GET/POST /api/v1/drilldown/prs":                 1,
	"GET/POST /api/v1/investment":                    3,
	"GET /api/v1/investment/sunburst":                2,
}

// The override applies to the repo-level UNRESOLVED case only (CHAOS-6119, R335): the one test that uses it drives a repo-level scope whose
// repository resolves to nothing. Opportunities answers before any metric read, and Home skips the theme allocation read
// (FROM work_unit_investments), which carried one empty match before. Every other consumer, and the org-scope what.repos test, is unchanged.
var repoScopeEmptyMatchStatements = map[string]int{
	"GET/POST /api/v1/opportunities": 0,
	"GET/POST /api/v1/home":          22,
}

// readsNoMetricWhenTheScopeIsUnresolved are the consumers that answer an unresolved repo-level scope without any metric read.
var readsNoMetricWhenTheScopeIsUnresolved = map[string]bool{"GET/POST /api/v1/opportunities": true}

// Named repositories that resolve to nothing leave nothing: no data, never the
// unfiltered read. Every consumer, through a repo-level scope.
func TestNamedRepositoryThatResolvesToNothingLeavesNothingOnEverySurface(t *testing.T) {
	for _, consumer := range teamScopeConsumers() {
		t.Run(consumer.route, func(t *testing.T) {
			client := &teamScopeReachClient{}
			consumer.drive(t, client, "repo", []string{"acme/nothing"}, nil)
			want, known := emptyMatchStatements[consumer.route]
			if !known {
				t.Fatalf("%s is not in emptyMatchStatements: a new consumer states how many of its reads carry the empty match", consumer.route)
			}
			if override, ok := repoScopeEmptyMatchStatements[consumer.route]; ok {
				want = override
			}
			if readsNoMetricWhenTheScopeIsUnresolved[consumer.route] {
				if reads := client.statementsWith("_metrics_daily"); len(reads) != 0 {
					t.Fatalf("%s reads a metric for a repo-level scope that resolved to nothing:\n%v", consumer.route, reads)
				}
			}
			if consumer.route == "GET/POST /api/v1/home" {
				if got := len(client.statementsWith("1 = 0")); got != want {
					t.Fatalf("%s: %d statements carry the empty match, want exactly %d (every metric read, the skipped theme allocation read excepted)", consumer.route, got, want)
				}
				for _, table := range []string{"repo_metrics_daily", "work_item_metrics_daily", "deploy_metrics_daily", "work_item_state_durations_daily"} {
					for _, statement := range client.statementsWith(table) {
						// The coverage read (covered repositories) is an organization-level freshness read with no scope filter.
						if !strings.Contains(statement, "GROUP BY day") {
							continue
						}
						if !strings.Contains(statement, "1 = 0") {
							t.Fatalf("%s: a read of %s does not carry the empty match:\n%s", consumer.route, table, statement)
						}
					}
				}
				if skipped := client.statementsWith("FROM work_unit_investments"); len(skipped) != 0 {
					t.Fatalf("%s: the theme allocation read (FROM work_unit_investments) ran for a scope that resolved to nothing:\n%v", consumer.route, skipped)
				}
			}
			if got := len(client.statementsWith("1 = 0")); got < want {
				t.Fatalf("%s: %d statement(s) carry the empty match for a repository that resolved to nothing, want %d (the others read the whole organization):\n%v", consumer.route, got, want, client.statements)
			}
		})
	}
}

// The opportunities answer for a repo-level scope that resolved to nothing is the empty list: no card, not even the steady-flow fallback.
func TestOpportunitiesAnswerAnEmptyListForARepoLevelScopeThatResolvedToNothing(t *testing.T) {
	client := &teamScopeReachClient{}
	filters := home.DefaultFilters()
	filters.Scope.Level = "repo"
	filters.Scope.IDs = []string{"acme/nothing"}
	resp, err := opportunities.BuildResponse(context.Background(), client, teamScopeReachOrgID, filters, time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC))
	if err != nil || resp == nil || resp.Items == nil || len(resp.Items) != 0 {
		t.Fatalf("answer %+v err %v, want an empty non-nil list", resp, err)
	}
}

// An empty string is not a repository name: a list of only empty strings names no
// repository, so there is no filter and no empty match, the same on every surface.
func TestEmptyRepositoryNamesAreNoFilterOnEverySurface(t *testing.T) {
	for _, consumer := range teamScopeConsumers() {
		if routesWithoutNamedReposBesideATeam[consumer.route] {
			continue
		}
		t.Run(consumer.route, func(t *testing.T) {
			client := &teamScopeReachClient{}
			consumer.drive(t, client, "org", nil, []string{""})
			if got := client.statementsWith("1 = 0"); len(got) != 0 {
				t.Fatalf("%s treats an empty string as a named repository (empty match):\n%v", consumer.route, got)
			}
		})
	}
}

// A repository filter narrows under ANY scope level (D5844, D5900): named repositories
// that resolve to nothing leave nothing under an organization scope too, on every
// consumer, with the same number of empty-match reads as under a repo-level scope.
func TestNamedRepositoryThatResolvesToNothingLeavesNothingUnderAnOrganizationScope(t *testing.T) {
	for _, consumer := range teamScopeConsumers() {
		if routesWithoutNamedReposBesideATeam[consumer.route] {
			continue
		}
		t.Run(consumer.route, func(t *testing.T) {
			client := &teamScopeReachClient{}
			consumer.drive(t, client, "org", nil, []string{"acme/nothing"})
			want, known := emptyMatchStatements[consumer.route]
			if !known {
				t.Fatalf("%s is not in emptyMatchStatements", consumer.route)
			}
			if got := len(client.statementsWith("1 = 0")); got < want {
				t.Fatalf("%s: %d statement(s) carry the empty match under an organization scope with unresolved what.repos, want %d:\n%v", consumer.route, got, want, client.statements)
			}
		})
	}
}

// CHAOS-9183 / CHAOS-9187: a statement that holds the team's ownership condition AND an
// explicit repository list on one repository column keeps the column UUID-typed: the
// ownership subquery yields UUIDs (toUUIDOrNull), so ClickHouse 26.7.1.1315 (prod) does not
// fail analysis with code 169 "Cannot compare UUID with String" and the primary key still
// prunes by repo_id. The consumers all build the pair in teamscope.NarrowRepoScope; this
// census drives every one of them and fails for a surface that builds the pair another way.
var (
	explicitRepoList = regexp.MustCompile(`(\S+) IN \{(scope_ids|repo_ids):Array\(String\)\} AND `)
)

func TestTeamAndNamedRepositoryKeepTheColumnUUIDOnEverySurface(t *testing.T) {
	const repo = "11111111-1111-4111-8111-111111111111"
	for _, consumer := range teamScopeConsumers() {
		if routesWithoutNamedReposBesideATeam[consumer.route] {
			continue
		}
		t.Run(consumer.route, func(t *testing.T) {
			client := &teamScopeReachClient{knownRepo: repo}
			consumer.drive(t, client, "team", []string{teamScopeReachTeam}, []string{repo})
			pairs := 0
			for _, statement := range client.statementsWith(teamscope.Marker) {
				list := explicitRepoList.FindStringSubmatch(statement)
				if list == nil || !strings.Contains(statement, "AS owned_repo_id") {
					continue
				}
				pairs++
				if strings.HasPrefix(strings.TrimLeft(list[1], "("), "toString(") {
					// A column the surface already renders as text compares as text on both sides.
					if strings.Contains(statement, "toUUIDOrNull") {
						t.Errorf("%s: a text column beside a UUID subquery:\n%s", consumer.route, statement)
					}
					continue
				}
				if !strings.Contains(statement, "SELECT DISTINCT toUUIDOrNull(coalesce(toString(o.repo_id), toString(r.id))) AS owned_repo_id") {
					t.Errorf("%s: the ownership subquery does not yield UUIDs:\n%s", consumer.route, statement)
				}
			}
			if pairs == 0 {
				t.Fatalf("%s built no statement with a repository list AND the ownership condition: the census compared nothing", consumer.route)
			}
		})
	}
}
