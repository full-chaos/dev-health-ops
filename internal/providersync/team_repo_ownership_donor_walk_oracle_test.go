package providersync

import (
	"encoding/json"
	"testing"
	"time"
)

// team_repo_ownership_donor_walk_oracle_test.go is CHAOS-4365 item 1b's
// differential-oracle pair: it proves buildDonorProjectIDResolver's edge
// gating (docs/contribute/architecture/team-attribution.md Sec 0.6) matches
// the recorded answer of production compute_work_items.py::build_linked_issue_team_resolver
// it was built to mirror -- not just this package's own inline unit tests
// (team_repo_ownership_derivation_test.go), which only check Go's internal
// self-consistency. A future drift between the two implementations of this
// shared gating logic fails HERE, not silently.
//
// See testdata/oracle_pairs/team_repo_ownership_donor_walk.py for the Python
// side and its scope note: this pair compares only the shared edge-selection
// surface (relationship-type gating, latest-edge-per-pair, extkey
// resolution, deterministic tie-break) -- team_repo_ownership derivation
// itself has no Python equivalent to compare against.

type teamRepoOwnershipDonorWalkOracleRow struct {
	TeamID *string `json:"team_id"`
}

type teamRepoOwnershipDonorWalkOracleProjectLink struct {
	ProjectID string `json:"project_id"`
	TeamID    string `json:"team_id"`
}

type teamRepoOwnershipDonorWalkOracleWorkItem struct {
	WorkItemID string `json:"work_item_id"`
	ProjectID  string `json:"project_id"`
}

type teamRepoOwnershipDonorWalkOracleDependency struct {
	SourceWorkItemID string `json:"source_work_item_id"`
	TargetWorkItemID string `json:"target_work_item_id"`
	RelationshipType string `json:"relationship_type"`
	LastSynced       string `json:"last_synced"`
}

type teamRepoOwnershipDonorWalkOracleInput struct {
	ProjectOwnership []teamRepoOwnershipDonorWalkOracleProjectLink `json:"project_ownership"`
	WorkItems        []teamRepoOwnershipDonorWalkOracleWorkItem    `json:"work_items"`
	Dependencies     []teamRepoOwnershipDonorWalkOracleDependency  `json:"dependencies"`
	SourceWorkItemID string                                        `json:"source_work_item_id"`
}

func teamRepoOwnershipDonorWalkOracleCases() []oracleCase {
	return []oracleCase{
		{
			ID: "own_project_id_wins_outright",
			Input: map[string]any{
				"project_ownership": []map[string]any{
					{"project_id": "proj-own", "team_id": "team-own"},
					{"project_id": "proj-donor", "team_id": "team-donor"},
				},
				"work_items": []map[string]any{
					{"work_item_id": "source-1", "project_id": "proj-own"},
					{"work_item_id": "donor-1", "project_id": "proj-donor"},
				},
				"dependencies": []map[string]any{
					{"source_work_item_id": "source-1", "target_work_item_id": "donor-1", "relationship_type": "relates_to"},
				},
				"source_work_item_id": "source-1",
			},
		},
		{
			ID: "inheritable_relates_to_transfers",
			Input: map[string]any{
				"project_ownership": []map[string]any{
					{"project_id": "proj-donor", "team_id": "team-donor"},
				},
				"work_items": []map[string]any{
					{"work_item_id": "source-1", "project_id": ""},
					{"work_item_id": "donor-1", "project_id": "proj-donor"},
				},
				"dependencies": []map[string]any{
					{"source_work_item_id": "source-1", "target_work_item_id": "donor-1", "relationship_type": "relates_to"},
				},
				"source_work_item_id": "source-1",
			},
		},
		{
			ID: "blocking_relationship_never_inherits",
			Input: map[string]any{
				"project_ownership": []map[string]any{
					{"project_id": "proj-donor", "team_id": "team-donor"},
				},
				"work_items": []map[string]any{
					{"work_item_id": "source-1", "project_id": ""},
					{"work_item_id": "donor-1", "project_id": "proj-donor"},
				},
				"dependencies": []map[string]any{
					{"source_work_item_id": "source-1", "target_work_item_id": "donor-1", "relationship_type": "blocked_by"},
				},
				"source_work_item_id": "source-1",
			},
		},
		{
			ID: "latest_edge_by_last_synced_wins",
			Input: map[string]any{
				"project_ownership": []map[string]any{
					{"project_id": "proj-donor", "team_id": "team-donor"},
				},
				"work_items": []map[string]any{
					{"work_item_id": "source-1", "project_id": ""},
					{"work_item_id": "donor-1", "project_id": "proj-donor"},
				},
				"dependencies": []map[string]any{
					{"source_work_item_id": "source-1", "target_work_item_id": "donor-1", "relationship_type": "relates_to", "last_synced": "2026-08-01T00:00:00Z"},
					{"source_work_item_id": "source-1", "target_work_item_id": "donor-1", "relationship_type": "blocked_by", "last_synced": "2026-08-20T00:00:00Z"},
				},
				"source_work_item_id": "source-1",
			},
		},
		{
			ID: "extkey_target_resolves_cross_provider",
			Input: map[string]any{
				"project_ownership": []map[string]any{
					{"project_id": "proj-donor", "team_id": "team-donor"},
				},
				"work_items": []map[string]any{
					{"work_item_id": "source-1", "project_id": ""},
					{"work_item_id": "linear:PLAT-9", "project_id": "proj-donor"},
				},
				"dependencies": []map[string]any{
					{"source_work_item_id": "source-1", "target_work_item_id": "extkey:PLAT-9", "relationship_type": "relates_to"},
				},
				"source_work_item_id": "source-1",
			},
		},
		{
			ID: "ambiguous_extkey_never_guessed",
			Input: map[string]any{
				"project_ownership": []map[string]any{
					{"project_id": "proj-donor", "team_id": "team-donor"},
				},
				"work_items": []map[string]any{
					{"work_item_id": "source-1", "project_id": ""},
					{"work_item_id": "linear:PLAT-9", "project_id": "proj-donor"},
					{"work_item_id": "jira:PLAT-9", "project_id": "proj-donor"},
				},
				"dependencies": []map[string]any{
					{"source_work_item_id": "source-1", "target_work_item_id": "extkey:PLAT-9", "relationship_type": "relates_to"},
				},
				"source_work_item_id": "source-1",
			},
		},
		{
			ID: "multiple_donor_candidates_pick_lexicographically_smallest",
			Input: map[string]any{
				"project_ownership": []map[string]any{
					{"project_id": "proj-1", "team_id": "team-platform"},
					{"project_id": "proj-2", "team_id": "team-growth"},
				},
				"work_items": []map[string]any{
					{"work_item_id": "source-1", "project_id": ""},
					{"work_item_id": "linear:PLAT-9", "project_id": "proj-1"},
					{"work_item_id": "linear:ZETA-1", "project_id": "proj-2"},
				},
				"dependencies": []map[string]any{
					{"source_work_item_id": "source-1", "target_work_item_id": "linear:ZETA-1", "relationship_type": "relates_to"},
					{"source_work_item_id": "source-1", "target_work_item_id": "linear:PLAT-9", "relationship_type": "relates_to"},
				},
				"source_work_item_id": "source-1",
			},
		},
		{
			// codex adversarial review, 2026-08-28, confirmed finding: an
			// unowned donor ("linear:AAA-1", no project_ownership entry at
			// all) sorts before the owned donor ("linear:ZETA-1") but must
			// never suppress it -- only donors that themselves resolve to a
			// team are valid tie-break candidates.
			ID: "unowned_donor_never_suppresses_an_owned_donor",
			Input: map[string]any{
				"project_ownership": []map[string]any{
					{"project_id": "proj-owned", "team_id": "team-platform"},
				},
				"work_items": []map[string]any{
					{"work_item_id": "source-1", "project_id": ""},
					{"work_item_id": "linear:AAA-1", "project_id": "proj-unowned"},
					{"work_item_id": "linear:ZETA-1", "project_id": "proj-owned"},
				},
				"dependencies": []map[string]any{
					{"source_work_item_id": "source-1", "target_work_item_id": "linear:AAA-1", "relationship_type": "relates_to"},
					{"source_work_item_id": "source-1", "target_work_item_id": "linear:ZETA-1", "relationship_type": "relates_to"},
				},
				"source_work_item_id": "source-1",
			},
		},
	}
}

func TestTeamRepoOwnershipDonorWalkMatchesFrozenPythonProduction(t *testing.T) {
	compareRowsAgainstPythonOracle(
		t,
		"sync/team-repo-ownership/donor-walk",
		teamRepoOwnershipDonorWalkOracleCases(),
		buildTeamRepoOwnershipDonorWalkOracleRow,
		nil,
	)
}

// buildTeamRepoOwnershipDonorWalkOracleRow is the Go side of the frozen pair. DECLARED DIVERGENCE: the frozen Python
// has no rule on the donor's Type, and the pair's inputs carry none; Go refuses a donor whose Type is empty
// (teamattribution.LinkedIssueDonorRefusal). So the pair reads a work item with no Type as an issue, as Python
// does, and the divergence itself is asserted by
// TestTeamRepoOwnershipDonorWalkDeclaredDivergenceGoRefusesAnEmptyTypeDonor below.
func buildTeamRepoOwnershipDonorWalkOracleRow(
	t *testing.T,
	input map[string]any,
) teamRepoOwnershipDonorWalkOracleRow {
	t.Helper()
	return buildTeamRepoOwnershipDonorWalkOracleRowWith(t, input, true)
}

// teamRepoOwnershipDonorWalkEmptyTypeDonorCases are the pair's cases whose Python answer is a donor's team.
var teamRepoOwnershipDonorWalkEmptyTypeDonorCases = []string{
	"inheritable_relates_to_transfers",
	"extkey_target_resolves_cross_provider",
	"multiple_donor_candidates_pick_lexicographically_smallest",
	"unowned_donor_never_suppresses_an_owned_donor",
}

// Go refuses where the frozen Python accepts: the same input, read with the real rule (no Type = empty_type),
// gives no team, while the Python-shaped read gives the frozen donor team. A case that stops resting on a
// donor with no Type must leave the list.
func TestTeamRepoOwnershipDonorWalkDeclaredDivergenceGoRefusesAnEmptyTypeDonor(t *testing.T) {
	byID := map[string]map[string]any{}
	for _, c := range teamRepoOwnershipDonorWalkOracleCases() {
		byID[c.ID] = c.Input
	}
	for _, id := range teamRepoOwnershipDonorWalkEmptyTypeDonorCases {
		input, ok := byID[id]
		if !ok {
			t.Fatalf("case %q is not in the pair", id)
		}
		if python := buildTeamRepoOwnershipDonorWalkOracleRowWith(t, input, true); python.TeamID == nil {
			t.Fatalf("%s: the Python-shaped read gives no team, want the frozen donor team", id)
		}
		if strict := buildTeamRepoOwnershipDonorWalkOracleRowWith(t, input, false); strict.TeamID != nil {
			t.Fatalf("%s: Go with the donor-type rule still gives team %q", id, *strict.TeamID)
		}
	}
}

func buildTeamRepoOwnershipDonorWalkOracleRowWith(
	t *testing.T,
	input map[string]any,
	readEmptyTypeAsIssue bool,
) teamRepoOwnershipDonorWalkOracleRow {
	t.Helper()
	encoded, err := json.Marshal(input)
	if err != nil {
		t.Fatal(err)
	}
	var decoded teamRepoOwnershipDonorWalkOracleInput
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		t.Fatal(err)
	}

	links := make([]TeamRepoOwnershipProjectLink, 0, len(decoded.ProjectOwnership))
	for _, link := range decoded.ProjectOwnership {
		links = append(links, TeamRepoOwnershipProjectLink{
			// Every oracle work item is provider="linear" (see below); the
			// ownership rows backing them must match, now that resolution is
			// keyed by (provider, project_id) not bare project_id.
			Provider: "linear", ProjectID: link.ProjectID, TeamID: link.TeamID, IsPrimary: true,
		})
	}
	workItems := make([]TeamRepoOwnershipWorkItem, 0, len(decoded.WorkItems))
	byID := make(map[string]TeamRepoOwnershipWorkItem, len(decoded.WorkItems))
	for _, item := range decoded.WorkItems {
		// Every oracle work item is provider="linear", matching the Python
		// side's hardcoded _PROVIDER (testdata/oracle_pairs/sync_team-repo-
		// ownership_donor-walk.py) -- required so buildIssueKeyIndex indexes
		// extkey donor candidates the same way Python's _work_item does.
		row := TeamRepoOwnershipWorkItem{WorkItemID: item.WorkItemID, Provider: "linear", ProjectID: item.ProjectID}
		if readEmptyTypeAsIssue {
			row.Type = "issue"
		}
		workItems = append(workItems, row)
		byID[item.WorkItemID] = row
	}
	edges := make([]TeamRepoOwnershipDependencyEdge, 0, len(decoded.Dependencies))
	for _, dep := range decoded.Dependencies {
		edge := TeamRepoOwnershipDependencyEdge{
			SourceWorkItemID: dep.SourceWorkItemID,
			TargetWorkItemID: dep.TargetWorkItemID,
			RelationshipType: dep.RelationshipType,
		}
		if dep.LastSynced != "" {
			parsed, err := time.Parse(time.RFC3339, dep.LastSynced)
			if err != nil {
				t.Fatal(err)
			}
			edge.LastSynced = parsed
		}
		edges = append(edges, edge)
	}

	projectToTeam := resolveProjectToTeam(links)
	// CHAOS-4537 renamed buildDonorProjectIDResolver to buildDonorTeamIDResolver
	// and changed its return from a team_project_ownership lookup key to the
	// resolved team_id directly -- see that function's doc comment. None of
	// these oracle cases exercise the Linear-only linear_team_key arm (no
	// NativeTeamKey field in this oracle's input shape at all), so this
	// change is a pure simplification here: the extra projectToTeam[ref]
	// re-lookup this test used to need is gone.
	// CHAOS-4537 codex review P1 added a knownLinearTeamKeys param; nil here
	// since no oracle case in this file exercises the Linear-only arm.
	donorTeamID := buildDonorTeamIDResolver(byID, edges, projectToTeam, nil, nil, nil)
	teamID := donorTeamID(decoded.SourceWorkItemID).teamID
	row := teamRepoOwnershipDonorWalkOracleRow{}
	if teamID != "" {
		row.TeamID = &teamID
	}
	return row
}
