package teamattribution

import (
	"fmt"
	"sort"
	"strings"
	"testing"
)

// manualFallbackParityCase is one catalog of the team id that a manual
// fallback names, for an item of one provider, where no ACTIVE team of the
// item's provider and no ACTIVE admin team has the id.
type manualFallbackParityCase struct {
	name     string
	provider string
	ruleID   string
	ruleName string
	teams    []GithubWorkItemDerivationTeamFact
}

// manualFallbackParityServed is what the cascade serves for one item: the
// resolved team and every candidate row it keeps.
type manualFallbackParityServed struct {
	TeamID   string
	TeamName string
	Rows     []string
}

// manualFallbackParityCases enumerates every such catalog for the four
// providers: the item's own team and the admin team are absent or inactive,
// and each team of the three other providers is absent, active or inactive.
// The rule names the id plain or with spaces, with a name or without one.
func manualFallbackParityCases() []manualFallbackParityCase {
	providers := []string{"jira", "gitlab", "github", "linear"}
	states := []string{"absent", "active", "inactive"}
	var result []manualFallbackParityCase
	for _, provider := range providers {
		var others []string
		for _, other := range providers {
			if other != provider {
				others = append(others, other)
			}
		}
		for _, own := range []string{"absent", "inactive"} {
			for _, admin := range []string{"absent", "inactive"} {
				for _, first := range states {
					for _, second := range states {
						for _, third := range states {
							for _, ruleID := range []string{"ENG", " ENG "} {
								for _, ruleName := range []string{"Rule ENG", ""} {
									var teams []GithubWorkItemDerivationTeamFact
									for index, state := range []string{own, admin, first, second, third} {
										teamProvider := provider
										switch index {
										case 1:
											teamProvider = ""
										case 2, 3, 4:
											teamProvider = others[index-2]
										}
										if state == "absent" {
											continue
										}
										teams = append(teams, GithubWorkItemDerivationTeamFact{
											Provider: teamProvider, TeamID: "ENG",
											TeamName: "Eng " + teamProvider, Inactive: state == "inactive",
										})
									}
									sort.SliceStable(teams, func(left, right int) bool {
										return teams[left].Provider < teams[right].Provider
									})
									result = append(result, manualFallbackParityCase{
										name: fmt.Sprintf("%s/own=%s/admin=%s/%s=%s/%s=%s/%s=%s/id=%q/name=%q",
											provider, own, admin, others[0], first, others[1], second, others[2], third, ruleID, ruleName),
										provider: provider, ruleID: ruleID, ruleName: ruleName, teams: teams,
									})
								}
							}
						}
					}
				}
			}
		}
	}
	return result
}

func manualFallbackParityServe(parity manualFallbackParityCase) manualFallbackParityServed {
	derived := NewGitHubWorkItemDerivationContext(GithubWorkItemDerivationFacts{
		Teams: parity.teams,
		ManualFallbacks: []GithubWorkItemDerivationManualFallback{
			{ScopeType: "issue_key_prefix", ScopeID: "ENG", TeamID: parity.ruleID, TeamName: parity.ruleName, Priority: 5},
		},
	})
	teamID, teamName, candidates := derived.Resolve(GithubWorkItemDerivationSubject{
		WorkItemID: parity.provider + ":ENG-3", Provider: parity.provider, OrgID: "org",
	})
	served := manualFallbackParityServed{
		TeamID: GithubWorkItemDerivationStringValue(teamID), TeamName: GithubWorkItemDerivationStringValue(teamName),
	}
	for _, candidate := range candidates {
		served.Rows = append(served.Rows, fmt.Sprintf("%s|%q|%q|%d|%s|%s",
			candidate.Source, GithubWorkItemDerivationStringValue(candidate.TeamID), GithubWorkItemDerivationStringValue(candidate.TeamName),
			candidate.IsPrimary, candidate.Confidence, candidate.Evidence))
	}
	return served
}

// A manual fallback row stores a bare team id and applies to items of every
// provider. When no active team of the item's provider and no active admin
// team has the id, the row is served exactly as before teams were bound by
// provider: with the rule's own id and name, and only when no team of any
// provider with the id is inactive; else the item is unassigned. A team of
// another provider never gives the row its name, also when it is the only
// active team with the id. The expected rows are main's rule; this test passes
// on main's code unchanged.
func TestAManualFallbackThatNoActiveOwnOrAdminTeamHoldsIsServedAsOnMain(t *testing.T) {
	cases := manualFallbackParityCases()
	if len(cases) != 4*2*2*27*2*2 {
		t.Fatalf("cases = %d, want %d", len(cases), 4*2*2*27*2*2)
	}
	kept, dropped := 0, 0
	for _, parity := range cases {
		t.Run(parity.name, func(t *testing.T) {
			anyInactive := false
			for _, team := range parity.teams {
				anyInactive = anyInactive || team.Inactive
			}
			served := manualFallbackParityServe(parity)
			if anyInactive {
				dropped++
				if served.TeamID != "" || served.TeamName != "" || len(served.Rows) != 1 || !strings.HasPrefix(served.Rows[0], "unassigned|") {
					t.Errorf("served %+v, want unassigned: a team with the id is inactive", served)
				}
				return
			}
			kept++
			name := parity.ruleName
			if name == "" {
				name = parity.ruleID
			}
			want := fmt.Sprintf("manual_fallback|%q|%q|%d|manual|manual_fallback:issue_key_prefix=ENG", parity.ruleID, name, AttributionPrimary)
			if served.TeamID != parity.ruleID || served.TeamName != name || len(served.Rows) != 1 || served.Rows[0] != want {
				t.Errorf("served %+v, want team %q named %q in the one row %s", served, parity.ruleID, name, want)
			}
		})
	}
	if kept == 0 || dropped == 0 {
		t.Fatalf("kept %d, dropped %d: the enumeration must reach both outcomes", kept, dropped)
	}
}
