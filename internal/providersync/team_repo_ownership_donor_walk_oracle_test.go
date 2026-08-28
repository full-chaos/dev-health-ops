package providersync

import (
	"encoding/json"
	"testing"
	"time"
)

// team_repo_ownership_donor_walk_oracle_test.go is CHAOS-4365 item 1b's
// differential-oracle pair: it proves buildDonorProjectIDResolver's edge
// gating (docs/contribute/architecture/team-attribution.md Sec 0.6) matches
// the LIVE, production compute_work_items.py::build_linked_issue_team_resolver
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
	}
}

func TestTeamRepoOwnershipDonorWalkMatchesLivePythonProduction(t *testing.T) {
	compareRowsAgainstPythonOracle(
		t,
		"sync/team-repo-ownership/donor-walk",
		teamRepoOwnershipDonorWalkOracleCases(),
		buildTeamRepoOwnershipDonorWalkOracleRow,
		nil,
	)
}

func buildTeamRepoOwnershipDonorWalkOracleRow(
	t *testing.T,
	input map[string]any,
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
	donorProjectID := buildDonorProjectIDResolver(byID, edges, projectToTeam)
	ref := donorProjectID(decoded.SourceWorkItemID)
	row := teamRepoOwnershipDonorWalkOracleRow{}
	if teamID, ok := projectToTeam[ref]; ok {
		row.TeamID = &teamID
	}
	return row
}
