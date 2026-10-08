package teamattribution

import (
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"
)

var coOwnerProviders = []string{"jira", "gitlab", "github", "linear"}

type attributedRow struct {
	source    string
	isPrimary int
	evidence  string
}

// rowsByTeam groups an item's resolved candidates by team id.
func rowsByTeam(candidates []GithubWorkItemDerivationCandidate) map[string][]attributedRow {
	result := map[string][]attributedRow{}
	for _, candidate := range candidates {
		team := GithubWorkItemDerivationStringValue(candidate.TeamID)
		result[team] = append(result[team], attributedRow{candidate.Source, candidate.IsPrimary, candidate.Evidence})
	}
	return result
}

// marks returns the non-zero is_primary values a team holds, by source.
func marks(rows []attributedRow) map[string]int {
	result := map[string]int{}
	for _, row := range rows {
		if row.isPrimary != AttributionNotPrimary {
			result[row.source+"="+fmt.Sprint(row.isPrimary)]++
		}
	}
	return result
}

func assertEveryRowHasProvenance(t *testing.T, candidates []GithubWorkItemDerivationCandidate) {
	t.Helper()
	for _, candidate := range candidates {
		if candidate.Source == "" || candidate.Confidence == "" || strings.TrimSpace(candidate.Evidence) == "" {
			t.Errorf("row without provenance: %+v", candidate)
		}
	}
}

// A project held by two active teams and one inactive team: the item is the
// work of BOTH active teams. The first by rank is the ONE primary row (1);
// the other active team has its own co-owner row (2) from the same source,
// with its own evidence; the inactive team has no row at all. Every provider,
// through the catalog key (issue_project).
func TestAProjectOfSeveralTeamsAttributesTheItemToEveryActiveTeam(t *testing.T) {
	for _, provider := range coOwnerProviders {
		t.Run(provider, func(t *testing.T) {
			key := "KEY"
			derived := NewGitHubWorkItemDerivationContext(GithubWorkItemDerivationFacts{
				Teams: []GithubWorkItemDerivationTeamFact{
					{Provider: provider, TeamID: "team-a", TeamName: "A", ProjectKeys: []string{key}},
					{Provider: provider, TeamID: "team-b", TeamName: "B", ProjectKeys: []string{key}},
					{Provider: provider, TeamID: "team-c", TeamName: "C", ProjectKeys: []string{key}, Inactive: true},
				},
				Projects: []GithubWorkItemDerivationProjectFact{
					{Provider: provider, TeamID: "team-a", TeamName: "A", ProjectID: "p1", ProjectKey: &key, IsPrimary: 1, Specificity: 110, Priority: 10},
					{Provider: provider, TeamID: "team-b", TeamName: "B", ProjectID: "p1", ProjectKey: &key, IsPrimary: 1, Specificity: 110, Priority: 10},
					{Provider: provider, TeamID: "team-c", TeamName: "C", ProjectID: "p1", ProjectKey: &key, IsPrimary: 1, Specificity: 110, Priority: 10},
				},
			})
			projectID := "p1"
			teamID, _, candidates := derived.Resolve(GithubWorkItemDerivationSubject{
				WorkItemID: provider + ":KEY-1", Provider: provider, ProjectKey: &key, ProjectID: &projectID, OrgID: "org",
			})
			if GithubWorkItemDerivationStringValue(teamID) != "team-a" {
				t.Fatalf("Resolve team = %q, want team-a (the one primary row)", GithubWorkItemDerivationStringValue(teamID))
			}
			byTeam := rowsByTeam(candidates)
			if got := marks(byTeam["team-a"]); fmt.Sprint(got) != "map[issue_project=1:1]" {
				t.Errorf("team-a marks = %v, want one issue_project primary row", got)
			}
			if got := marks(byTeam["team-b"]); fmt.Sprint(got) != "map[issue_project=2:1]" {
				t.Errorf("team-b marks = %v, want one issue_project co-owner row", got)
			}
			if rows, present := byTeam["team-c"]; present {
				t.Errorf("inactive team-c has rows %v, want none", rows)
			}
			primaries := 0
			for _, candidate := range candidates {
				if candidate.IsPrimary == AttributionPrimary {
					primaries++
				}
			}
			if primaries != 1 {
				t.Errorf("primary rows = %d, want exactly 1 (the org counts the item once)", primaries)
			}
			for _, row := range byTeam["team-b"] {
				if row.isPrimary == AttributionCoOwner && row.evidence != "issue_project_key=KEY" {
					t.Errorf("team-b co-owner evidence = %q", row.evidence)
				}
			}
			assertEveryRowHasProvenance(t, candidates)
		})
	}
}

// Ownership with no catalog key (project_ownership wins): the same rule. Only
// an owner at the SAME rank as the primary is a co-owner; a lower is_primary,
// specificity or priority, an empty team id, or a second fact of a team
// already marked, is not.
func TestOnlyOwnersAtThePrimaryRankAreCoOwners(t *testing.T) {
	for _, provider := range coOwnerProviders {
		t.Run(provider, func(t *testing.T) {
			at := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
			fact := func(team, projectID string, key *string, isPrimary, specificity, priority int, updated time.Time) GithubWorkItemDerivationProjectFact {
				return GithubWorkItemDerivationProjectFact{
					Provider: provider, TeamID: team, TeamName: team, ProjectID: projectID, ProjectKey: key,
					IsPrimary: isPrimary, Specificity: specificity, Priority: priority, UpdatedAt: updated,
				}
			}
			key := "PROJ"
			derived := NewGitHubWorkItemDerivationContext(GithubWorkItemDerivationFacts{
				Projects: []GithubWorkItemDerivationProjectFact{
					fact("team-a", "p1", nil, 1, 110, 10, at.Add(time.Hour)),
					fact("team-a", "", &key, 1, 110, 10, at),
					fact("team-b", "p1", nil, 1, 110, 10, at),
					fact("team-b", "", &key, 1, 110, 10, at),
					fact("", "p1", nil, 1, 110, 10, at),
					fact("team-not-primary", "p1", nil, 0, 110, 10, at),
					fact("team-less-specific", "p1", nil, 1, 100, 10, at),
					fact("team-other-priority", "p1", nil, 1, 110, 20, at),
				},
			})
			projectID := "p1"
			teamID, _, candidates := derived.Resolve(GithubWorkItemDerivationSubject{
				WorkItemID: provider + ":PROJ-1", Provider: provider, ProjectID: &projectID, ProjectKey: &key, OrgID: "org",
			})
			if GithubWorkItemDerivationStringValue(teamID) != "team-a" {
				t.Fatalf("Resolve team = %q, want team-a (newest at the top rank)", GithubWorkItemDerivationStringValue(teamID))
			}
			byTeam := rowsByTeam(candidates)
			want := map[string]string{
				"team-a":              "map[project_ownership=1:1]",
				"team-b":              "map[project_ownership=2:1]",
				"":                    "map[]",
				"team-not-primary":    "map[]",
				"team-less-specific":  "map[]",
				"team-other-priority": "map[]",
			}
			for team, wantMarks := range want {
				if _, present := byTeam[team]; !present {
					t.Errorf("team %q has no row, want a provenance row", team)
					continue
				}
				if got := fmt.Sprint(marks(byTeam[team])); got != wantMarks {
					t.Errorf("team %q marks = %s, want %s", team, got, wantMarks)
				}
			}
			assertEveryRowHasProvenance(t, candidates)
		})
	}
}

// Co-owners come from the item's PROJECT only. A repository owned by two
// teams at the same rank, and a native team key, still give one team.
func TestRepositoryAndNativeTeamHaveNoCoOwners(t *testing.T) {
	for _, provider := range coOwnerProviders {
		t.Run(provider, func(t *testing.T) {
			repoID := "repo-1"
			derived := NewGitHubWorkItemDerivationContext(GithubWorkItemDerivationFacts{
				Teams: []GithubWorkItemDerivationTeamFact{
					{Provider: provider, TeamID: "native-a", ProjectKeys: []string{"NATIVE"}},
					{Provider: provider, TeamID: "native-b", ProjectKeys: []string{"NATIVE"}},
				},
				Repos: []GithubWorkItemDerivationRepoFact{
					{Provider: provider, TeamID: "repo-a", RepoID: &repoID, IsPrimary: 1, Specificity: 80, Priority: 10},
					{Provider: provider, TeamID: "repo-b", RepoID: &repoID, IsPrimary: 1, Specificity: 80, Priority: 10},
				},
			})
			_, _, repoCandidates := derived.Resolve(GithubWorkItemDerivationSubject{
				WorkItemID: provider + ":r-1", Provider: provider, RepoID: &repoID, OrgID: "org",
			})
			native := "NATIVE"
			_, _, nativeCandidates := derived.Resolve(GithubWorkItemDerivationSubject{
				WorkItemID: provider + ":n-1", Provider: provider, NativeTeamKey: &native, OrgID: "org",
			})
			for name, candidates := range map[string][]GithubWorkItemDerivationCandidate{"repo": repoCandidates, "native": nativeCandidates} {
				for _, candidate := range candidates {
					if candidate.IsPrimary == AttributionCoOwner && candidate.Source != "issue_project" {
						t.Errorf("%s: co-owner row from %s: %+v", name, candidate.Source, candidate)
					}
					if candidate.Source == "native_team" && candidate.IsPrimary == AttributionCoOwner {
						t.Errorf("%s: native team has a co-owner: %+v", name, candidate)
					}
				}
			}
			for _, candidate := range repoCandidates {
				if candidate.IsPrimary == AttributionCoOwner {
					t.Errorf("repo: co-owner row %+v", candidate)
				}
			}
		})
	}
}

// A key string held by a team of another provider does not make that team an
// owner of the item's project: it never takes a co-owner row. The facts come
// in catalog order (provider, id), as the loader reads them. When the first
// holder is of the item's provider, the primary is that team and the other
// holders of the item's provider are co-owners. When the first holder is of
// another provider, it stays the primary exactly as before this change (that
// choice is not changed here) and no team takes a co-owner row.
func TestAKeyOfAnotherProvidersTeamMakesNoCoOwner(t *testing.T) {
	for _, provider := range coOwnerProviders {
		t.Run(provider, func(t *testing.T) {
			other := "linear"
			if provider == "linear" {
				other = "jira"
			}
			key := "SHARED"
			teams := []GithubWorkItemDerivationTeamFact{
				{Provider: provider, TeamID: "team-a", ProjectKeys: []string{key}},
				{Provider: provider, TeamID: "team-b", ProjectKeys: []string{key}},
				{Provider: other, TeamID: "team-z-other", ProjectKeys: []string{key}},
			}
			sort.SliceStable(teams, func(left, right int) bool {
				if teams[left].Provider != teams[right].Provider {
					return teams[left].Provider < teams[right].Provider
				}
				return teams[left].TeamID < teams[right].TeamID
			})
			derived := NewGitHubWorkItemDerivationContext(GithubWorkItemDerivationFacts{
				Teams: teams,
				Projects: []GithubWorkItemDerivationProjectFact{
					{Provider: other, TeamID: "team-z-other", ProjectID: "p1", ProjectKey: &key, IsPrimary: 1, Specificity: 110, Priority: 10},
				},
			})
			projectID := "p1"
			teamID, _, candidates := derived.Resolve(GithubWorkItemDerivationSubject{
				WorkItemID: provider + ":SHARED-1", Provider: provider, ProjectKey: &key, ProjectID: &projectID, OrgID: "org",
			})
			byTeam := rowsByTeam(candidates)
			for _, row := range byTeam["team-z-other"] {
				if row.isPrimary == AttributionCoOwner {
					t.Errorf("team of %s holds a co-owner row of a %s item: %+v", other, provider, row)
				}
			}
			first := teams[0].TeamID
			if got := GithubWorkItemDerivationStringValue(teamID); got != first {
				t.Fatalf("Resolve team = %q, want the first holder %q (the primary choice is unchanged)", got, first)
			}
			wantB := "map[]"
			if teams[0].Provider == provider {
				wantB = "map[issue_project=2:1]"
			}
			if got := marks(byTeam["team-b"]); fmt.Sprint(got) != wantB {
				t.Errorf("team-b marks = %v, want %s", got, wantB)
			}
			coOwners := 0
			for _, candidate := range candidates {
				if candidate.IsPrimary == AttributionCoOwner {
					coOwners++
				}
			}
			if teams[0].Provider != provider && coOwners != 0 {
				t.Errorf("first holder of another provider: %d co-owner rows, want 0", coOwners)
			}
		})
	}
}

func TestAttributionRowPreferenceOrdersPrimaryCoOwnerProvenance(t *testing.T) {
	primary := AttributionRowPreference(AttributionPrimary)
	coOwner := AttributionRowPreference(AttributionCoOwner)
	provenance := AttributionRowPreference(AttributionNotPrimary)
	if !(primary > coOwner && coOwner > provenance) {
		t.Fatalf("preference primary=%d co-owner=%d provenance=%d, want primary > co-owner > provenance", primary, coOwner, provenance)
	}
}
