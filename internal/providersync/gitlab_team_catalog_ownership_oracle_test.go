package providersync

import (
	"testing"
	"time"
)

// gitlabTeamCatalogOwnershipProducerRow mirrors TeamProjectOwnershipRecord's
// dataclass fields (metrics/schemas.py) exactly.
type gitlabTeamCatalogOwnershipProducerRow struct {
	OrgID       string     `json:"org_id"`
	Provider    string     `json:"provider"`
	TeamID      string     `json:"team_id"`
	ProjectID   string     `json:"project_id"`
	ProjectKey  *string    `json:"project_key"`
	Source      string     `json:"source"`
	IsPrimary   int        `json:"is_primary"`
	Specificity int        `json:"specificity"`
	Priority    int        `json:"priority"`
	ValidFrom   time.Time  `json:"valid_from"`
	ValidTo     *time.Time `json:"valid_to"`
	UpdatedAt   time.Time  `json:"updated_at"`
}

func TestGitLabReferenceOwnershipMatchesLivePythonProducer(t *testing.T) {
	compareRowsAgainstPythonOracle(
		t,
		"gitlab/work-items/reference-ownership",
		[]oracleCase{
			{
				// Root-only: depth 0 -> specificity BASE_SPECIFICITY (100),
				// the case _project_ownership_rows's own docstring calls out.
				ID: "root_group_specificity",
				Input: map[string]any{
					"org_id": "org-acme", "normalized_at": "2026-08-10T12:34:56.789Z",
					"target_full_path": "full-chaos", "target_project_path": "full-chaos/api",
					"teams": []any{
						map[string]any{"full_path": "full-chaos", "repo_patterns": []any{"full-chaos/api"}},
					},
				},
			},
			{
				// A discovered subgroup's parent resolves via the SAME
				// path-prefix rule as the team row (_parent_by_team), so its
				// depth is 1 -> specificity BASE_SPECIFICITY+CHILD_SPECIFICITY_STEP (110).
				// This is the exact ladder a from-scratch Go port is most
				// likely to get subtly wrong.
				ID: "subgroup_depth_specificity",
				Input: map[string]any{
					"org_id": "org-acme", "normalized_at": "2026-08-10T12:34:56.789Z",
					"target_full_path": "full-chaos/platform", "target_project_path": "full-chaos/platform/svc",
					"teams": []any{
						map[string]any{"full_path": "full-chaos", "repo_patterns": []any{"full-chaos/api"}},
						map[string]any{"full_path": "full-chaos/platform", "repo_patterns": []any{"full-chaos/platform/svc"}},
					},
				},
			},
		},
		buildGitLabTeamCatalogOwnershipOracleRow,
		nil,
	)
}

func buildGitLabTeamCatalogOwnershipOracleRow(t *testing.T, input map[string]any) gitlabTeamCatalogOwnershipProducerRow {
	t.Helper()
	normalizedAt, err := time.Parse(time.RFC3339Nano, input["normalized_at"].(string))
	if err != nil {
		t.Fatal(err)
	}
	rawTeams := input["teams"].([]any)
	fullPaths := make([]string, 0, len(rawTeams))
	byPath := map[string][]string{}
	for _, raw := range rawTeams {
		entry := raw.(map[string]any)
		fullPath := entry["full_path"].(string)
		fullPaths = append(fullPaths, fullPath)
		patterns := make([]string, 0)
		for _, pattern := range entry["repo_patterns"].([]any) {
			patterns = append(patterns, pattern.(string))
		}
		byPath[fullPath] = patterns
	}
	parentByTeam := gitlabTeamCatalogParentByTeam(fullPaths)
	targetFullPath := input["target_full_path"].(string)
	targetProjectPath := input["target_project_path"].(string)
	teamID := gitlabTeamID(targetFullPath)
	specificity := uint16(gitlabTeamCatalogBaseSpecificity + gitlabTeamDepth(teamID, parentByTeam)*gitlabTeamCatalogChildSpecificityStep)
	row := normalizeGitLabOwnershipRow(input["org_id"].(string), teamID, targetProjectPath, specificity, normalizedAt)
	return gitlabTeamCatalogOwnershipProducerRow{
		OrgID: row.OrgID, Provider: row.Provider, TeamID: row.TeamID, ProjectID: row.ProjectID,
		ProjectKey: row.ProjectKey, Source: row.Source, IsPrimary: int(row.IsPrimary),
		Specificity: int(row.Specificity), Priority: int(row.Priority),
		ValidFrom: row.ValidFrom, ValidTo: row.ValidTo, UpdatedAt: row.UpdatedAt,
	}
}
