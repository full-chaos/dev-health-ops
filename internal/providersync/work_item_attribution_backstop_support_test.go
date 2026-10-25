package providersync

import (
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/teamattribution"
	"github.com/google/uuid"
)

const githubDerivedOracleOrg = "org-acme"

// githubTeamAttributionColumns is the column-wise projection of the
// work_item_team_attributions rows the frozen oracle compares.
type githubTeamAttributionColumns struct {
	WorkItemID []string     `json:"work_item_id"`
	Provider   []string     `json:"provider"`
	Source     []string     `json:"source"`
	IsPrimary  []int        `json:"is_primary"`
	Confidence []string     `json:"confidence"`
	Evidence   []string     `json:"evidence"`
	ComputedAt []time.Time  `json:"computed_at"`
	RepoID     []*uuid.UUID `json:"repo_id"`
	TeamID     []*string    `json:"team_id"`
	TeamName   []*string    `json:"team_name"`
	OrgID      []string     `json:"org_id"`
}

func githubWorkItemDerivationSubjectFromRow(row githubWorkItemRow) teamattribution.GithubWorkItemDerivationSubject {
	var repoID *string
	if row.RepoID != nil {
		value := row.RepoID.String()
		repoID = &value
	}
	return teamattribution.GithubWorkItemDerivationSubject{
		WorkItemID: row.WorkItemID, Provider: row.Provider, Type: row.Type, RepoID: repoID,
		NativeTeamKey: row.NativeTeamKey, ProjectKey: row.ProjectKey,
		ProjectID: row.ProjectID, ProjectName: row.ProjectName,
		Assignees: append([]string(nil), row.Assignees...),
		Reporter:  row.Reporter, OrgID: row.OrgID,
	}
}

func githubDerivedOracleGoItem(t *testing.T, raw map[string]any) githubWorkItemRow {
	t.Helper()
	row := githubWorkItemRow{
		WorkItemID: raw["work_item_id"].(string),
		Provider:   raw["provider"].(string),
		Title:      raw["title"].(string),
		Type:       githubDerivedOracleString(raw, "type", "issue"),
		Status:     githubDerivedOracleString(raw, "status", "todo"),
		ProjectID:  stringPointer(raw["project_id"].(string)),
		CreatedAt:  githubDerivedOracleTime(t, raw["created_at"].(string)),
		UpdatedAt:  githubDerivedOracleTime(t, raw["updated_at"].(string)),
		OrgID:      raw["org_id"].(string),
	}
	if values, ok := raw["assignees"].([]any); ok {
		for _, value := range values {
			row.Assignees = append(row.Assignees, value.(string))
		}
	}
	if value, ok := raw["repo_id"].(string); ok {
		parsed, err := uuid.Parse(value)
		if err != nil {
			t.Fatal(err)
		}
		row.RepoID = &parsed
	}
	return row
}

func githubDerivedOracleString(raw map[string]any, key, fallback string) string {
	if value, ok := raw[key].(string); ok && value != "" {
		return value
	}
	return fallback
}

func githubDerivedOracleTime(t *testing.T, value string) time.Time {
	t.Helper()
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		t.Fatal(err)
	}
	return parsed.UTC()
}

func toDerivationDependencyEdges(rows []githubWorkItemDependencyRow) []teamattribution.GithubWorkItemDerivationDependencyEdge {
	edges := make([]teamattribution.GithubWorkItemDerivationDependencyEdge, len(rows))
	for index, row := range rows {
		edges[index] = teamattribution.GithubWorkItemDerivationDependencyEdge{
			SourceWorkItemID: row.SourceWorkItemID,
			TargetWorkItemID: row.TargetWorkItemID,
			RelationshipType: row.RelationshipType,
			LastSynced:       row.LastSynced,
		}
	}
	return edges
}
