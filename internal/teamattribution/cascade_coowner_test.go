package teamattribution

import (
	"fmt"
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

// Co-owners come from the item's project, and from the main owners of the
// item's repository: a repository with two main owners at the same rank (a
// full ownership tie, D5990) attributes the item to both, the first by rank
// as the one primary row. Two repository facts that are not a main owner's
// (provider_access grants, is_primary = 0) and a native team key still give
// one team.
func TestOnlyMainRepositoryOwnersAndProjectsHaveCoOwners(t *testing.T) {
	for _, provider := range coOwnerProviders {
		t.Run(provider, func(t *testing.T) {
			mainRepo, grantRepo := "repo-main", "repo-grant"
			derived := NewGitHubWorkItemDerivationContext(GithubWorkItemDerivationFacts{
				Teams: []GithubWorkItemDerivationTeamFact{
					{Provider: provider, TeamID: "native-a", ProjectKeys: []string{"NATIVE"}},
					{Provider: provider, TeamID: "native-b", ProjectKeys: []string{"NATIVE"}},
				},
				Repos: []GithubWorkItemDerivationRepoFact{
					{Provider: provider, TeamID: "main-a", RepoID: &mainRepo, IsPrimary: 1, Specificity: 150, Priority: 0},
					{Provider: provider, TeamID: "main-b", RepoID: &mainRepo, IsPrimary: 1, Specificity: 150, Priority: 0},
					{Provider: provider, TeamID: "grant-a", RepoID: &grantRepo, IsPrimary: 0, Specificity: 100, Priority: 0},
					{Provider: provider, TeamID: "grant-b", RepoID: &grantRepo, IsPrimary: 0, Specificity: 100, Priority: 0},
				},
			})
			native := "NATIVE"
			for _, tc := range []struct {
				name    string
				subject GithubWorkItemDerivationSubject
				want    map[string]map[string]int
			}{
				{"two main owners of the repository", GithubWorkItemDerivationSubject{WorkItemID: provider + ":r-1", Provider: provider, RepoID: &mainRepo, OrgID: "org"},
					map[string]map[string]int{"main-a": {"repo_ownership=1": 1}, "main-b": {"repo_ownership=2": 1}}},
				{"two grants of the repository", GithubWorkItemDerivationSubject{WorkItemID: provider + ":g-1", Provider: provider, RepoID: &grantRepo, OrgID: "org"},
					map[string]map[string]int{"grant-a": {"repo_ownership=1": 1}, "grant-b": {}}},
				{"a native team key", GithubWorkItemDerivationSubject{WorkItemID: provider + ":n-1", Provider: provider, NativeTeamKey: &native, OrgID: "org"},
					map[string]map[string]int{"native-a": {"native_team=1": 1}, "native-b": {}}},
			} {
				_, _, candidates := derived.Resolve(tc.subject)
				byTeam := rowsByTeam(candidates)
				for team, want := range tc.want {
					if got := marks(byTeam[team]); fmt.Sprint(got) != fmt.Sprint(want) {
						t.Errorf("%s: %s marks %v, want %v (rows %+v)", tc.name, team, got, want, byTeam)
					}
				}
				assertEveryRowHasProvenance(t, candidates)
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
