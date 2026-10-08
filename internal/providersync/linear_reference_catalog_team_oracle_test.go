package providersync

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/identityalias"
	"github.com/full-chaos/dev-health-ops/internal/teamid"
)

type linearReferenceTeamProducerRow struct {
	ID            string    `json:"id"`
	TeamUUID      string    `json:"team_uuid"`
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
	// GoTeamID is the team id the Go writer stores (see the goOnlyFields
	// reason below).
	GoTeamID string `json:"go_team_id_chaos_8939"`
}

func TestLinearReferenceTeamCatalogMatchesFrozenPythonProducer(t *testing.T) {
	// team_autoimport_linear.py is deleted -- native Go providersync is the
	// only producer now. Frozen under the last live comparison (see
	// testdata/oracle_frozen/README.md).
	compareRowsAgainstFrozenOracle(
		t,
		"linear_work-items_reference-team",
		[]oracleCase{
			{
				ID: "team_with_project_key",
				Input: map[string]any{
					"org_id":        "org-acme",
					"team_id":       "ENG",
					"name":          "Engineering",
					"description":   "Platform team",
					"project_keys":  []any{"ENG"},
					"normalized_at": "2026-08-10T12:34:56.789Z",
				},
			},
			{
				ID: "team_without_description",
				Input: map[string]any{
					"org_id":        "org-acme",
					"team_id":       "OPS",
					"name":          "Operations",
					"description":   nil,
					"project_keys":  []any{"OPS", "SUPPORT"},
					"normalized_at": "2026-08-10T12:34:56.789Z",
				},
			},
		},
		buildLinearReferenceTeamCatalogOracleRow,
		map[string]string{
			"team_uuid": "ClickHouse derives this UUID while Python's dict sink row leaves it to the sink default",
			"go_team_id_chaos_8939": "CHAOS-8939 (intentional divergence): the Go writer stores linear:<key> as the " +
				"team id, the frozen Python producer stored the bare key. The compared id is the Go id without its " +
				"prefix, and the row builder fails unless the Go id is exactly linear:<that key>.",
		},
	)
}

func buildLinearReferenceTeamCatalogOracleRow(t *testing.T, input map[string]any) linearReferenceTeamProducerRow {
	t.Helper()
	rawKeys, err := json.Marshal(input["project_keys"])
	if err != nil {
		t.Fatal(err)
	}
	var projectKeys []string
	if err := json.Unmarshal(rawKeys, &projectKeys); err != nil {
		t.Fatal(err)
	}
	normalizedAt, err := time.Parse(time.RFC3339Nano, input["normalized_at"].(string))
	if err != nil {
		t.Fatal(err)
	}
	var description *string
	if value, ok := input["description"].(string); ok {
		description = &value
	}
	claim := linearOracleClaim(input)
	team, err := normalizeLinearReferenceTeam(claim, linearReferenceCatalogTeamPayload{
		ID: input["team_id"].(string), Key: input["team_id"].(string), Name: input["name"].(string),
		Description: description,
	}, identityalias.Load(""), normalizedAt)
	if err != nil {
		t.Fatal(err)
	}
	nativeKey, own := teamid.NativeKey("linear", team.ID)
	if !own || team.ID != "linear:"+nativeKey || nativeKey != input["team_id"].(string) {
		t.Fatalf("Go team id %q is not linear:<key> for key %q", team.ID, input["team_id"])
	}
	return linearReferenceTeamProducerRow{
		ID: nativeKey, GoTeamID: team.ID, TeamUUID: team.TeamUUID, Name: team.Name, Description: team.Description, Members: team.Members,
		ProjectKeys: projectKeys, RepoPatterns: team.RepoPatterns, IsActive: team.IsActive == 1,
		UpdatedAt: team.UpdatedAt, OrgID: team.OrgID, Provider: team.Provider,
		NativeTeamKey: *team.NativeTeamKey, ParentTeamID: team.ParentTeamID,
	}
}
