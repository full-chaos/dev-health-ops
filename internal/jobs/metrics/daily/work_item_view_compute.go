package daily

import (
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/workitemmetrics"
)

// WorkItemViewItem is one stored work item with its primary team, as the
// request-time read of a repository's linked items hands it to the compute
// (CHAOS-9094). It carries the columns the work_item family reads and nothing
// else; the team is the row of work_item_team_attributions the family reads
// (HasAttribution false = no row, which resolves to unassigned as it does there).
type WorkItemViewItem struct {
	WorkItemID    string
	Provider      string
	Status        string
	ProjectKey    string
	ProjectID     string
	NativeTeamKey string
	ProjectName   string
	CreatedAt     time.Time
	CompletedAt   *time.Time
	Type          string
	Assignees     []string
	StartedAt     *time.Time
	ClosedAt      *time.Time
	StoryPoints   *float64

	HasAttribution bool
	TeamID         string
	TeamName       string
}

// WorkItemViewScopeID is the work scope of an item: the one derivation
// (workitemmetrics.WorkScopeID).
func WorkItemViewScopeID(item WorkItemViewItem) string {
	return workitemmetrics.WorkScopeID(item.Provider, item.ProjectKey, item.ProjectID, item.NativeTeamKey, item.ProjectName)
}

// ComputeWorkItemMetricsDay is the work_item family's compute for one UTC day over
// the given items, by the same function the family calls
// (workitemmetrics.ComputeStoredItemsDay). Transitions are not read: they only feed
// the flow breakdown of the cycle-time record, which none of the rows returned here
// depends on.
func ComputeWorkItemMetricsDay(day time.Time, items []WorkItemViewItem) []workitemmetrics.MetricsDailyRow {
	rows := make([]workitemmetrics.StoredItem, 0, len(items))
	attributions := make(map[string]workitemmetrics.PrimaryAttribution, len(items))
	for _, item := range items {
		rows = append(rows, workitemmetrics.StoredItem{
			WorkItemID: item.WorkItemID, Provider: item.Provider, Status: item.Status,
			ProjectKey: item.ProjectKey, ProjectID: item.ProjectID, NativeTeamKey: item.NativeTeamKey, ProjectName: item.ProjectName,
			CreatedAt: item.CreatedAt, CompletedAt: item.CompletedAt,
			Type: item.Type, Assignees: item.Assignees, StartedAt: item.StartedAt, ClosedAt: item.ClosedAt, StoryPoints: item.StoryPoints,
		})
		if item.HasAttribution {
			attributions[item.WorkItemID] = workitemmetrics.PrimaryAttribution{TeamID: item.TeamID, TeamName: item.TeamName}
		}
	}
	return workitemmetrics.ComputeStoredItemsDay(day, rows, nil, attributions).MetricsDaily
}
