package providersync

import (
	"encoding/json"
	"testing"
	"time"
)

// gitlabTeamCatalogProjectProducerRow mirrors ProjectRecord's dataclass
// fields (metrics/schemas.py) exactly.
type gitlabTeamCatalogProjectProducerRow struct {
	ID         string    `json:"id"`
	Provider   string    `json:"provider"`
	Name       string    `json:"name"`
	IsActive   int       `json:"is_active"`
	UpdatedAt  time.Time `json:"updated_at"`
	LastSynced time.Time `json:"last_synced"`
	ProjectKey *string   `json:"project_key"`
	OrgID      string    `json:"org_id"`
	State      string    `json:"state"`
	TargetDate *string   `json:"target_date"`
	URL        string    `json:"url"`
	TeamIDs    []string  `json:"team_ids"`
	TeamKeys   []string  `json:"team_keys"`
	LeadID     *string   `json:"lead_id"`
	LeadName   *string   `json:"lead_name"`
	LeadEmail  *string   `json:"lead_email"`
}

func TestGitLabReferenceProjectCatalogMatchesLivePythonProducer(t *testing.T) {
	compareRowsAgainstPythonOracle(
		t,
		"gitlab/work-items/reference-project",
		[]oracleCase{
			{
				ID: "active_project",
				Input: map[string]any{
					"org_id": "org-acme", "normalized_at": "2026-08-10T12:34:56.789Z",
					"native_id": "42", "path_with_namespace": "full-chaos/platform/svc",
					"name": "svc", "archived": false,
					"web_url": "https://gitlab.example.com/full-chaos/platform/svc",
				},
			},
			{
				// CHAOS-3380: archived -> is_active=0, the rename/reuse
				// docstring's own worked case.
				ID: "archived_project_falls_back_to_path_for_name",
				Input: map[string]any{
					"org_id": "org-acme", "normalized_at": "2026-08-10T12:34:56.789Z",
					"native_id": "43", "path_with_namespace": "full-chaos/legacy/tool",
					"name": "", "archived": true, "web_url": "",
				},
			},
		},
		buildGitLabTeamCatalogProjectOracleRow,
		nil,
	)
}

func buildGitLabTeamCatalogProjectOracleRow(t *testing.T, input map[string]any) gitlabTeamCatalogProjectProducerRow {
	t.Helper()
	normalizedAt, err := time.Parse(time.RFC3339Nano, input["normalized_at"].(string))
	if err != nil {
		t.Fatal(err)
	}
	payload := gitlabTeamCatalogProjectPayload{
		ID: json.Number(input["native_id"].(string)), PathWithNamespace: input["path_with_namespace"].(string),
		Name: input["name"].(string), Archived: input["archived"].(bool), WebURL: input["web_url"].(string),
	}
	row, ok := normalizeGitLabProjectCatalogRow(input["org_id"].(string), payload, normalizedAt)
	if !ok {
		t.Fatal("normalizeGitLabProjectCatalogRow rejected a valid oracle case")
	}
	return gitlabTeamCatalogProjectProducerRow{
		ID: row.ID, Provider: row.Provider, Name: row.Name, IsActive: int(row.IsActive),
		UpdatedAt: row.UpdatedAt, LastSynced: row.LastSynced, ProjectKey: row.ProjectKey,
		OrgID: row.OrgID, State: row.State, URL: row.URL, TeamIDs: row.TeamIDs, TeamKeys: row.TeamKeys,
		LeadID: row.LeadID, LeadName: row.LeadName, LeadEmail: row.LeadEmail,
	}
}
