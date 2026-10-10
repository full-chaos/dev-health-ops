package workitemmetrics

import (
	"sort"
	"time"
)

// StoredItem is one stored work item row as the work_item family reads it, with the
// columns the work-item metrics are computed from. It lives here, with the compute,
// because two readers need the same definition of a day's metrics: the daily job,
// which loads every item of a work scope, and a request-time read that loads the
// items linked to a repository's pull requests (CHAOS-9094). A serving binary must
// not import the job package, so the pure compute is shared from this package; the
// I/O stays with each caller.
type StoredItem struct {
	WorkItemID    string
	Provider      string
	Status        string
	ProjectKey    string
	ProjectID     string
	NativeTeamKey string
	ProjectName   string
	CreatedAt     time.Time
	CompletedAt   *time.Time

	Type        string
	Assignees   []string
	StartedAt   *time.Time
	ClosedAt    *time.Time
	StoryPoints *float64
}

// PrimaryAttribution is the primary team row of work_item_team_attributions for an
// item; an item with no row has none (ComputeStoredItemsDay resolves it to
// unassigned, as the cascade does when every source declines).
type PrimaryAttribution struct {
	TeamID   string
	TeamName string
}

// WorkScopeID ports WorkItem.work_scope_id
// (src/dev_health_ops/models/work_items.py:88-119) byte-for-byte: jira uses
// project_key when present; every other provider prefers project_id, then
// project_name, then native_team_key (a team-only Linear issue keeps a non-empty
// scope key), then falls back to project_key too (covers a non-jira item whose
// project_key is set but the three fields above are all empty). May return "".
func WorkScopeID(provider, projectKey, projectID, nativeTeamKey, projectName string) string {
	if provider == "jira" && projectKey != "" {
		return projectKey
	}
	if projectID != "" {
		return projectID
	}
	if projectName != "" {
		return projectName
	}
	if nativeTeamKey != "" {
		return nativeTeamKey
	}
	if projectKey != "" {
		return projectKey
	}
	return ""
}

// WorkScopeID is the work scope of the item.
func (item StoredItem) WorkScopeID() string {
	return WorkScopeID(item.Provider, item.ProjectKey, item.ProjectID, item.NativeTeamKey, item.ProjectName)
}

// SortStoredItems makes the compute reproducible run to run regardless of
// ClickHouse row-return order (see ComputeStoredItemsDay).
func SortStoredItems(items []StoredItem) []StoredItem {
	sorted := make([]StoredItem, len(items))
	copy(sorted, items)
	sort.SliceStable(sorted, func(i, j int) bool { return sorted[i].WorkItemID < sorted[j].WorkItemID })
	return sorted
}

// ProjectStoredItems reduces the rows to the Items the compute reads, in order.
func ProjectStoredItems(rows []StoredItem) []Item {
	items := make([]Item, 0, len(rows))
	for index, row := range rows {
		items = append(items, Item{
			SourceIndex: index,
			WorkItemID:  row.WorkItemID,
			Provider:    row.Provider,
			Type:        row.Type,
			Status:      row.Status,
			Assignee:    FirstAssignee(row.Assignees),
			CreatedAt:   row.CreatedAt,
			StartedAt:   row.StartedAt,
			CompletedAt: row.CompletedAt,
			ClosedAt:    row.ClosedAt,
			StoryPoints: row.StoryPoints,
		})
	}
	return items
}

// StoredItemResolver answers from the primary attributions, applying the same
// normalize_team_id/normalize_team_name defaults Python applies to a nil resolver
// result. A work item with no row resolves to unassigned/Unassigned, which is
// what resolve_team_attribution returns when every cascade source declines.
func StoredItemResolver(rows []StoredItem, attributions map[string]PrimaryAttribution) Resolver {
	return func(index int) Attribution {
		row := rows[index]
		attribution := attributions[row.WorkItemID]
		return Attribution{
			WorkScopeID: row.WorkScopeID(),
			TeamID:      NormalizeTeamID(&attribution.TeamID),
			TeamName:    NormalizeTeamName(&attribution.TeamName),
		}
	}
}

// ComputeStoredItemsDay is the work_item family's compute for one UTC day over
// loaded rows: sort, project, attribute, ComputeDailyTriplet. The one definition of
// the day's work-item metrics: the daily job and the request-time read of a
// repository's linked items both call it.
func ComputeStoredItemsDay(
	day time.Time, rows []StoredItem, transitions []Transition, attributions map[string]PrimaryAttribution,
) Triplet {
	sorted := SortStoredItems(rows)
	projected := ProjectStoredItems(sorted)
	return ComputeDailyTriplet(
		day,
		projected,
		transitions,
		AssertAligned(len(sorted), projected, StoredItemResolver(sorted, attributions)),
	)
}
