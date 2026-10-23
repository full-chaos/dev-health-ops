// Package linkeditems reads the work items linked to a repository's pull
// requests and computes their daily work-item metrics with the daily job's own
// compute (workitemmetrics). It is the one definition of "a repository's work"
// for the readers whose daily tables hold no item and no repository (Home,
// Quadrant).
package linkeditems

import (
	"context"
	"fmt"
	"time"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/workitemmetrics"
)

// RowQuerier is the read boundary of the loader.
type RowQuerier interface {
	Query(ctx context.Context, statement string, bindings []dhclickhouse.Binding) (dhclickhouse.RowScanner, error)
}

// Subquery is the item ids linked to the repositories' pull requests.
//
// An item is its (provider, work_item_id) in every read here. The ONE place that
// matches on work_item_id alone is the link: work_graph_issue_pr carries no
// provider (key repo_id, work_item_id, pr_number), and every producer prefixes the
// id with its provider (jira:, gh:, linear:, gitlab), so the id names the item
// across providers.
const Subquery = `(SELECT work_item_id FROM work_graph_issue_pr FINAL
        WHERE org_id = {org_id:String} AND repo_id IN {link_repo_ids:Array(UUID)})`

// Version is one stored work_items row of a linked item: the item, the repository
// id of the row and its version.
type Version struct {
	Item   workitemmetrics.StoredItem
	RepoID string
	Synced time.Time
}

func (v Version) version() workitemmetrics.ItemVersion {
	return workitemmetrics.ItemVersion{Provider: v.Item.Provider, WorkItemID: v.Item.WorkItemID, RepoID: v.RepoID, LastSynced: v.Synced}
}

// Address is the address of a stored row: the item (provider and provider-local
// id) and the repository id the row is stored under.
type Address struct{ Provider, WorkItemID, RepoID string }

// Loader reads the linked items of RepoIDs that the work_item family would load
// for some day of [WindowStart, WindowEnd). Budget is the per-statement SETTINGS
// clause (bounds are the caller's).
type Loader struct {
	Client      RowQuerier
	OrgID       string
	RepoIDs     []string
	WindowStart time.Time
	WindowEnd   time.Time
	Budget      string
}

// Bindings are the bindings of every statement that embeds Subquery.
func (l Loader) Bindings() []dhclickhouse.Binding {
	return []dhclickhouse.Binding{
		{Name: "org_id", Value: l.OrgID},
		{Name: "link_repo_ids", Value: l.RepoIDs},
		{Name: "win_start", Value: l.WindowStart.Format("2006-01-02")},
		{Name: "win_end", Value: l.WindowEnd.Format("2006-01-02")},
	}
}

// Read reads the stored rows of the linked items and each row's primary team.
func (l Loader) Read(ctx context.Context) ([]Version, map[Address]workitemmetrics.PrimaryAttribution, error) {
	versions, err := l.readItems(ctx)
	if err != nil {
		return nil, nil, err
	}
	attributions, err := l.readAttributions(ctx, versions)
	if err != nil {
		return nil, nil, err
	}
	return versions, attributions, nil
}

// readItems reads the stored rows of the linked items that the work_item family
// would load for some day of the window: the family's predicate (created before
// the window ends and either not done or completed no earlier than its start), over
// the union of the days, from work_items FINAL. Every version is kept: the version
// an item is on a day is chosen per day by RowsByDay, with the daily job's own rule.
func (l Loader) readItems(ctx context.Context) ([]Version, error) {
	rows, err := l.Client.Query(ctx, `
SELECT toString(repo_id), last_synced,
       work_item_id, provider, status, project_key, project_id, native_team_key, project_name,
       created_at, completed_at, type, assignees, started_at, closed_at, story_points
FROM work_items FINAL
WHERE org_id = {org_id:String}
  AND created_at < toDateTime64({win_end:Date}, 3, 'UTC')
  AND (status != 'done' OR completed_at >= toDateTime64({win_start:Date}, 3, 'UTC'))
  AND work_item_id IN `+Subquery+l.Budget, l.Bindings())
	if err != nil {
		return nil, fmt.Errorf("linkeditems: items: %w", err)
	}
	defer rows.Close()
	var out []Version
	for rows.Next() {
		var v Version
		var completed, started, closed *time.Time
		var points *float64
		item := &v.Item
		if err := rows.Scan(&v.RepoID, &v.Synced, &item.WorkItemID, &item.Provider, &item.Status,
			&item.ProjectKey, &item.ProjectID, &item.NativeTeamKey, &item.ProjectName,
			&item.CreatedAt, &completed, &item.Type, &item.Assignees, &started, &closed, &points); err != nil {
			return nil, fmt.Errorf("linkeditems: items scan: %w", err)
		}
		item.CompletedAt, item.StartedAt, item.ClosedAt, item.StoryPoints = completed, started, closed, points
		out = append(out, v)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("linkeditems: items: %w", err)
	}
	return out, nil
}

// readAttributions reads each item's primary team: the row of
// work_item_team_attributions the work_item family reads (is_primary = 1, the
// newest snapshot of each (repository, item)). An item with no row has none, which
// the compute resolves to unassigned as the family does.
func (l Loader) readAttributions(ctx context.Context, versions []Version) (map[Address]workitemmetrics.PrimaryAttribution, error) {
	out := map[Address]workitemmetrics.PrimaryAttribution{}
	if len(versions) == 0 {
		return out, nil
	}
	rows, err := l.Client.Query(ctx, `
SELECT toString(repo_id), provider, work_item_id, ifNull(team_id, ''), ifNull(team_name, '')
FROM work_item_team_attributions FINAL
WHERE org_id = {org_id:String} AND is_primary = 1
  AND work_item_id IN `+Subquery+`
  AND (repo_id, provider, work_item_id, computed_at) IN (
      SELECT repo_id, provider, work_item_id, max(computed_at)
      FROM work_item_team_attributions
      WHERE org_id = {org_id:String} AND work_item_id IN `+Subquery+`
      GROUP BY repo_id, provider, work_item_id)`+l.Budget, l.Bindings())
	if err != nil {
		return nil, fmt.Errorf("linkeditems: attributions: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var address Address
		var attribution workitemmetrics.PrimaryAttribution
		if err := rows.Scan(&address.RepoID, &address.Provider, &address.WorkItemID, &attribution.TeamID, &attribution.TeamName); err != nil {
			return nil, fmt.Errorf("linkeditems: attributions scan: %w", err)
		}
		// Every stored row of an item keeps its own attribution: the day's chosen
		// version (RowsByDay) takes the attribution of the row it is.
		out[address] = attribution
	}
	return out, rows.Err()
}

// RowsByDay runs the work_item family's compute for every day of [from, to). For
// each day the rows are the versions that pass the day's predicate, each item once
// by the daily job's own rule (workitemmetrics.OncePerProviderAndID: a version that
// does not pass the day's predicate does not compete), each with the attribution of
// the stored row it is. The compute itself keeps the items that are relevant to the
// day, as it does for the daily job.
func RowsByDay(from, to time.Time, versions []Version, attributions map[Address]workitemmetrics.PrimaryAttribution) map[time.Time][]workitemmetrics.MetricsDailyRow {
	out := map[time.Time][]workitemmetrics.MetricsDailyRow{}
	if len(versions) == 0 {
		return out
	}
	for day := from; day.Before(to); day = day.AddDate(0, 0, 1) {
		dayEnd := day.AddDate(0, 0, 1)
		var passing []Version
		var keys []workitemmetrics.ItemVersion
		for _, v := range versions {
			if workitemmetrics.PassesDayPredicate(v.Item.Status, v.Item.CreatedAt, v.Item.CompletedAt, day, dayEnd) {
				passing = append(passing, v)
				keys = append(keys, v.version())
			}
		}
		kept, _ := workitemmetrics.OncePerProviderAndID(keys)
		items := make([]workitemmetrics.StoredItem, 0, len(kept))
		dayAttributions := map[string]workitemmetrics.PrimaryAttribution{}
		for _, index := range kept {
			v := passing[index]
			items = append(items, v.Item)
			if attribution, found := attributions[Address{v.Item.Provider, v.Item.WorkItemID, v.RepoID}]; found {
				dayAttributions[v.Item.WorkItemID] = attribution
			}
		}
		if len(items) == 0 {
			continue
		}
		if rows := workitemmetrics.ComputeStoredItemsDay(day, items, nil, dayAttributions).MetricsDaily; len(rows) > 0 {
			out[day] = rows
		}
	}
	return out
}
