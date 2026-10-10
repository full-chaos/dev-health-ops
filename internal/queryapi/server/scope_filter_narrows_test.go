package server

import (
	"regexp"
	"strings"
	"testing"

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
	"POST /api/v1/investment/flow":                   2,
	"POST /api/v1/investment/flow/repo-team":         1,
	"GET/POST /api/v1/drilldown/prs":                 1,
	"GET/POST /api/v1/investment":                    3,
	"GET /api/v1/investment/sunburst":                2,
}

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
			if got := len(client.statementsWith("1 = 0")); got < want {
				t.Fatalf("%s: %d statement(s) carry the empty match for a repository that resolved to nothing, want %d (the others read the whole organization):\n%v", consumer.route, got, want, client.statements)
			}
		})
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
			// Home and opportunities have ONE read less under an organization scope: the
			// rework-by-theme allocation read (allocationScope) reads what.repos under no
			// scope but a repo-level one. Known, owned by CHAOS-9150 (D5900): remove this
			// allowance there.
			if consumer.route == "GET/POST /api/v1/home" || consumer.route == "GET/POST /api/v1/opportunities" {
				want--
			}
			if got := len(client.statementsWith("1 = 0")); got < want {
				t.Fatalf("%s: %d statement(s) carry the empty match under an organization scope with unresolved what.repos, want %d:\n%v", consumer.route, got, want, client.statements)
			}
		})
	}
}
