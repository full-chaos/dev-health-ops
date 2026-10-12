package providersync

import (
	"reflect"
	"sort"
	"testing"
)

var retractionGuardProviders = []string{"jira", "gitlab", "github", "linear"}

func retractionGuardRow(teamID, repo string) teamRepoOwnershipActiveRow {
	return teamRepoOwnershipActiveRow{Provider: "github", TeamID: teamID, RepoFullName: repo}
}

func retractedTeams(rows []teamRepoOwnershipActiveRow) []string {
	teams := make([]string, 0, len(rows))
	for _, row := range rows {
		teams = append(teams, row.TeamID)
	}
	sort.Strings(teams)
	return teams
}

// TestRetractionGuardIsPerProvider is CHAOS-9045 for every ordered provider
// pair: provider A has no open project link, provider B has one. The row of
// A's team is not retracted; the row of B's team is.
func TestRetractionGuardIsPerProvider(t *testing.T) {
	for _, a := range retractionGuardProviders {
		for _, b := range retractionGuardProviders {
			if a == b {
				continue
			}
			t.Run(a+"_empty_"+b+"_open", func(t *testing.T) {
				teamA, teamB := a+"-team", b+"-team"
				guard := newTeamRepoOwnershipRetractionGuard(
					[]TeamRepoOwnershipProjectLink{{Provider: b, ProjectID: b + "-proj", TeamID: teamB}},
					[]TeamRepoOwnershipTeamLinkProvider{{Provider: a, TeamID: teamA}, {Provider: b, TeamID: teamB}},
					nil, nil)
				kept, held, heldKinds := guard.retain([]teamRepoOwnershipActiveRow{
					retractionGuardRow(teamA, "acme/a"), retractionGuardRow(teamB, "acme/b"),
				})
				if got := retractedTeams(kept); !reflect.DeepEqual(got, []string{teamB}) {
					t.Fatalf("retracted %v, want only %s (A has no open link, so A's row is not proven gone)", got, teamB)
				}
				want := []teamRepoOwnershipRetractionHeld{{
					Kind: teamRepoOwnershipRetractionKindProjectLinks, Provider: a,
					Reasons: []string{TeamRepoOwnershipNoOpenProjectLinkOfTheProvider}, Rows: 1,
				}}
				if held != 1 || !reflect.DeepEqual(heldKinds, want) {
					t.Fatalf("held=%d %+v, want 1 %+v", held, heldKinds, want)
				}
			})
		}
	}
}

// TestRetractionGuardSingleProviderWithNoOpenLinkKeepsTheRow: the
// single-provider org keeps its row, as before.
func TestRetractionGuardSingleProviderWithNoOpenLinkKeepsTheRow(t *testing.T) {
	for _, a := range retractionGuardProviders {
		t.Run(a, func(t *testing.T) {
			guard := newTeamRepoOwnershipRetractionGuard(nil,
				[]TeamRepoOwnershipTeamLinkProvider{{Provider: a, TeamID: "team-a"}}, nil, nil)
			kept, held, _ := guard.retain([]teamRepoOwnershipActiveRow{retractionGuardRow("team-a", "acme/a")})
			if len(kept) != 0 || held != 1 {
				t.Fatalf("retracted %v held %d, want none retracted and 1 held", retractedTeams(kept), held)
			}
		})
	}
}

// TestRetractionGuardRetractsWhenTheProvidersOwnLinksProveIt: provider A
// still has an open link (of another team), so A's input is in the run and a
// row of A's team that the run does not derive is gone.
func TestRetractionGuardRetractsWhenTheProvidersOwnLinksProveIt(t *testing.T) {
	for _, a := range retractionGuardProviders {
		for _, b := range retractionGuardProviders {
			if a == b {
				continue
			}
			t.Run(a+"_"+b, func(t *testing.T) {
				guard := newTeamRepoOwnershipRetractionGuard(
					[]TeamRepoOwnershipProjectLink{
						{Provider: a, ProjectID: a + "-proj-2", TeamID: "team-a2"},
						{Provider: b, ProjectID: b + "-proj", TeamID: "team-b"},
					},
					[]TeamRepoOwnershipTeamLinkProvider{{Provider: a, TeamID: "team-a"}},
					nil, nil)
				kept, held, heldKinds := guard.retain([]teamRepoOwnershipActiveRow{retractionGuardRow("team-a", "acme/a")})
				if got := retractedTeams(kept); !reflect.DeepEqual(got, []string{"team-a"}) || held != 0 || len(heldKinds) != 0 {
					t.Fatalf("retracted %v held %d %+v, want team-a retracted, nothing held", got, held, heldKinds)
				}
			})
		}
	}
}

// TestRetractionGuardTeamOfTwoProvidersNeedsBoth: a team that links of two
// providers named can have a row of either; it closes only when both are in
// the run.
func TestRetractionGuardTeamOfTwoProvidersNeedsBoth(t *testing.T) {
	history := []TeamRepoOwnershipTeamLinkProvider{{Provider: "jira", TeamID: "team-x"}, {Provider: "gitlab", TeamID: "team-x"}}
	row := []teamRepoOwnershipActiveRow{retractionGuardRow("team-x", "acme/x")}
	onlyJira := newTeamRepoOwnershipRetractionGuard(
		[]TeamRepoOwnershipProjectLink{{Provider: "jira", ProjectID: "10001", TeamID: "team-y"}}, history, nil, nil)
	if kept, held, _ := onlyJira.retain(row); len(kept) != 0 || held != 1 {
		t.Fatalf("only jira open: retracted %v held %d, want the row kept", retractedTeams(kept), held)
	}
	both := newTeamRepoOwnershipRetractionGuard([]TeamRepoOwnershipProjectLink{
		{Provider: "jira", ProjectID: "10001", TeamID: "team-y"},
		{Provider: "gitlab", ProjectID: "g/p", TeamID: "team-z"},
	}, history, nil, nil)
	if kept, held, _ := both.retain(row); len(kept) != 1 || held != 0 {
		t.Fatalf("both open: retracted %v held %d, want the row retracted", retractedTeams(kept), held)
	}
}

// TestRetractionGuardLinearTeamKeyIsItsOwnKind: a Linear team can have a row
// of the linear_team_key arm, whose input is a Linear work item with a known
// team key, not a project link. Open project links of any provider do not
// prove that input.
func TestRetractionGuardLinearTeamKeyIsItsOwnKind(t *testing.T) {
	known := []TeamRepoOwnershipKnownTeam{{Provider: "linear", ID: "linear:CHAOS", NativeTeamKey: "CHAOS"}}
	links := []TeamRepoOwnershipProjectLink{{Provider: "jira", ProjectID: "10001", TeamID: "jira:ops"}}
	row := []teamRepoOwnershipActiveRow{retractionGuardRow("linear:CHAOS", "acme/linear")}

	noKey := newTeamRepoOwnershipRetractionGuard(links, nil, []TeamRepoOwnershipWorkItem{{WorkItemID: "linear:CHAOS-1", Provider: "linear"}}, known)
	kept, held, heldKinds := noKey.retain(row)
	want := []teamRepoOwnershipRetractionHeld{{
		Kind: teamRepoOwnershipRetractionKindLinearTeamKey, Reasons: []string{TeamRepoOwnershipNoKnownLinearTeamKey}, Rows: 1,
	}}
	if len(kept) != 0 || held != 1 || !reflect.DeepEqual(heldKinds, want) {
		t.Fatalf("no resolvable key: retracted %v held %d %+v, want kept by %+v", retractedTeams(kept), held, heldKinds, want)
	}

	withKey := newTeamRepoOwnershipRetractionGuard(links, nil,
		[]TeamRepoOwnershipWorkItem{{WorkItemID: "linear:CHAOS-1", Provider: "linear", NativeTeamKey: "CHAOS"}}, known)
	if kept, held, _ := withKey.retain(row); len(kept) != 1 || held != 0 {
		t.Fatalf("resolvable key: retracted %v held %d, want the row retracted", retractedTeams(kept), held)
	}

	// A Linear team the catalog lists under an id written before team ids
	// carried a prefix is of the kind through the catalog.
	bare := []TeamRepoOwnershipKnownTeam{{Provider: "linear", ID: "CHAOS", NativeTeamKey: "CHAOS"}}
	bareRow := []teamRepoOwnershipActiveRow{retractionGuardRow("CHAOS", "acme/linear")}
	bareNoKey := newTeamRepoOwnershipRetractionGuard(links, nil, nil, bare)
	if kept, held, _ := bareNoKey.retain(bareRow); len(kept) != 0 || held != 1 {
		t.Fatalf("bare listed linear team, no key input: retracted %v held %d, want the row kept", retractedTeams(kept), held)
	}

	// A prefixed Linear team the catalog no longer lists is still of the kind.
	gone := newTeamRepoOwnershipRetractionGuard(links, nil, nil, nil)
	if kept, held, _ := gone.retain(row); len(kept) != 0 || held != 1 {
		t.Fatalf("unlisted linear team, no key input: retracted %v held %d, want the row kept", retractedTeams(kept), held)
	}
}

// TestRetractionGuardRowOfNoKindKeepsTheOldGuard: a team no project link ever
// named and that is no Linear team: no provider's input can derive its row,
// so it is retracted when the run holds an open link of any provider.
func TestRetractionGuardRowOfNoKindKeepsTheOldGuard(t *testing.T) {
	row := []teamRepoOwnershipActiveRow{retractionGuardRow("team-orphan", "acme/o")}
	withLinks := newTeamRepoOwnershipRetractionGuard([]TeamRepoOwnershipProjectLink{{Provider: "gitlab", ProjectID: "g/p", TeamID: "gl:t"}}, nil, nil, nil)
	if kept, held, _ := withLinks.retain(row); len(kept) != 1 || held != 0 {
		t.Fatalf("open links: retracted %v held %d, want retracted", retractedTeams(kept), held)
	}
	noLinks := newTeamRepoOwnershipRetractionGuard(nil, nil, nil, nil)
	kept, held, heldKinds := noLinks.retain(row)
	want := []teamRepoOwnershipRetractionHeld{{
		Kind: teamRepoOwnershipRetractionKindNone, Reasons: []string{TeamRepoOwnershipNoOpenProjectLink}, Rows: 1,
	}}
	if len(kept) != 0 || held != 1 || !reflect.DeepEqual(heldKinds, want) {
		t.Fatalf("no links: retracted %v held %d %+v, want kept by %+v", retractedTeams(kept), held, heldKinds, want)
	}
}

// TestRetractionGuardCountsAClosedLinkInTheHistoryOnly: a closed link of A
// is in the history (it names A's team) and never in the open links, so it
// proves nothing for A.
func TestRetractionGuardCountsAClosedLinkInTheHistoryOnly(t *testing.T) {
	guard := newTeamRepoOwnershipRetractionGuard(
		[]TeamRepoOwnershipProjectLink{{Provider: "linear", ProjectID: "uuid-1", TeamID: "team-b"}},
		[]TeamRepoOwnershipTeamLinkProvider{{Provider: "jira", TeamID: "team-a"}, {Provider: "linear", TeamID: "team-b"}},
		nil, nil)
	if got := guard.kindsOf("team-a"); !reflect.DeepEqual(got, []teamRepoOwnershipRetractionKind{{Kind: teamRepoOwnershipRetractionKindProjectLinks, Provider: "jira"}}) {
		t.Fatalf("kinds of team-a = %+v, want project_links:jira", got)
	}
	if guard.proofs[teamRepoOwnershipRetractionKind{Kind: teamRepoOwnershipRetractionKindProjectLinks, Provider: "jira"}].Proven() {
		t.Fatal("project_links:jira is proven with no open jira link")
	}
	if !guard.proofs[teamRepoOwnershipRetractionKind{Kind: teamRepoOwnershipRetractionKindProjectLinks, Provider: "linear"}].Proven() {
		t.Fatal("project_links:linear is not proven with one open linear link")
	}
}
