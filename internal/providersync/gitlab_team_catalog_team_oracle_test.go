package providersync

import (
	"testing"
	"time"
)

// gitlabTeamCatalogTeamProducerRow mirrors _gitlab_team_row's twelve dict
// keys exactly (team_autoimport_gitlab.py) -- the boundary this oracle
// compares against. It deliberately omits team_uuid and members_
// authoritative: both are Go-only (ClickHouse derives the former; the
// latter is this port's own roster-preservation bookkeeping), neither
// exists in Python's dict, and normalizeGitLabTeamRow's full row is
// re-asserted against BOTH (via the real gitlabTeamCatalogTeamRow type) in
// gitlab_team_catalog_test.go's unit tests.
type gitlabTeamCatalogTeamProducerRow struct {
	ID            string    `json:"id"`
	Name          string    `json:"name"`
	Description   *string   `json:"description"`
	Members       []string  `json:"members"`
	ProjectKeys   []string  `json:"project_keys"`
	RepoPatterns  []string  `json:"repo_patterns"`
	IsActive      bool      `json:"is_active"`
	UpdatedAt     time.Time `json:"updated_at"`
	OrgID         string    `json:"org_id"`
	Provider      string    `json:"provider"`
	NativeTeamKey string    `json:"native_team_key"`
	ParentTeamID  *string   `json:"parent_team_id"`
}

func TestGitLabReferenceTeamCatalogMatchesLivePythonProducer(t *testing.T) {
	compareRowsAgainstPythonOracle(
		t,
		"gitlab/work-items/reference-team",
		[]oracleCase{
			{
				ID: "root_group",
				Input: map[string]any{
					"org_id": "org-acme", "full_path": "full-chaos",
					"name": "Full Chaos", "description": "Root group",
					"project_keys":  []any{"full-chaos/api", "full-chaos/web"},
					"normalized_at": "2026-08-10T12:34:56.789Z",
				},
			},
			{
				ID: "subgroup_derives_parent_from_path",
				Input: map[string]any{
					"org_id": "org-acme", "full_path": "full-chaos/platform",
					"name": "Platform", "description": nil,
					"project_keys":  []any{"full-chaos/platform/svc"},
					"normalized_at": "2026-08-10T12:34:56.789Z",
				},
			},
		},
		buildGitLabTeamCatalogTeamOracleRow,
		nil,
	)
}

func buildGitLabTeamCatalogTeamOracleRow(t *testing.T, input map[string]any) gitlabTeamCatalogTeamProducerRow {
	t.Helper()
	normalizedAt, err := time.Parse(time.RFC3339Nano, input["normalized_at"].(string))
	if err != nil {
		t.Fatal(err)
	}
	var description *string
	if value, ok := input["description"].(string); ok {
		description = &value
	}
	projectKeys := make([]string, 0)
	for _, raw := range input["project_keys"].([]any) {
		projectKeys = append(projectKeys, raw.(string))
	}
	group := gitlabTeamCatalogGroupPayload{
		FullPath: input["full_path"].(string), Name: input["name"].(string), Description: description,
	}
	row := normalizeGitLabTeamRow(input["org_id"].(string), group, projectKeys, normalizedAt)
	return gitlabTeamCatalogTeamProducerRow{
		ID: row.ID, Name: row.Name, Description: row.Description, Members: row.Members,
		ProjectKeys: row.ProjectKeys, RepoPatterns: row.RepoPatterns, IsActive: row.IsActive == 1,
		UpdatedAt: row.UpdatedAt, OrgID: row.OrgID, Provider: row.Provider,
		NativeTeamKey: *row.NativeTeamKey, ParentTeamID: row.ParentTeamID,
	}
}
