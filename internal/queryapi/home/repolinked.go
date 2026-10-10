package home

import (
	"context"
	"errors"
	"fmt"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/deltarule"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/teamscope"
	"log/slog"
	"math"
	"sort"
	"strings"
	"sync"
	"time"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/workitemmetrics"
)

// CHAOS-9094: the four work-item metrics (cycle_time, throughput,
// wip_saturation, blocked_work) under a repository filter.
//
// Their daily tables hold no item and no repository: a row is a (day, provider,
// work scope, team). A repository scopes them through the ITEMS linked to that
// repository's pull requests, and the one link of record is the table
// work_graph_issue_pr (its provenance tier: native > explicit_text >
// heuristic). Never an issue's own repository column, never an issue-key
// prefix, never a team's repositories.
//
// With a repository filter the metric is computed at request time from the
// linked items by the daily job's own compute (daily.ComputeWorkItemMetricsDay:
// the function the work_item family calls), then aggregated as the Home read
// aggregates the daily table (see aggregateLinkedRows). blocked_work reads the
// per-item rows the work_item_state family stores
// (work_item_blocked_durations_daily). With no repository filter nothing here
// runs and the daily tables serve.

const (
	// repoLinkedMaxExecutionSeconds bounds every request-time statement. A
	// statement past it is a STATED state (repoLinkTimedOut), never an empty
	// tile and never the unfiltered value.
	repoLinkedMaxExecutionSeconds = 8

	// The states MetricDelta.RepoLinkState carries.
	repoLinkLinked   = "linked"
	repoLinkNoLinks  = "no_links"
	repoLinkTimedOut = "timed_out"
	// repoLinkTooLarge: the read returned more rows than the bound
	// (RepoLinkedMaxResultRows): the four metrics have no data, the Home
	// document is served.
	repoLinkTooLarge = "too_large"
)

// RepoLinkedMaxResultRows is the ONE row bound of the repo-linked read, sent as a
// per-statement setting (max_result_rows) so REST (whose client defaults to 1,000
// rows) and GraphQL (500,000) read the same way. A repository's view holds one row
// per linked item (about 11,000 on the largest organization measured); 200,000 is
// far above that and below the GraphQL client's bound. Past it the read says
// "too_large". A variable so a test can lower it.
var RepoLinkedMaxResultRows uint64 = 200_000

// repoLinkedMetrics are the metrics a repository filter scopes through links.
var repoLinkedMetrics = map[string]bool{
	"cycle_time": true, "throughput": true, "wip_saturation": true, "blocked_work": true,
}

// RepoLinkBasis counts the items of a repository's view by the best provenance
// tier of the links that put them there: a lower tier is never counted as
// native.
type RepoLinkBasis struct {
	Native       int `json:"native"`
	ExplicitText int `json:"explicit_text"`
	Heuristic    int `json:"heuristic"`
}

// RepoLinkCoverage says how much of the work the repository view can see: an
// item with no link is in no repository's view.
type RepoLinkCoverage struct {
	LinkedItems   int `json:"linked_items"`
	ItemsInWindow int `json:"items_in_window"`
}

// repoLinkedView is everything one request reads once for the four metrics.
type repoLinkedView struct {
	state          string
	items          []workitemmetrics.StoredItem
	attributions   map[string]workitemmetrics.PrimaryAttribution
	blocked        []blockedItemRow
	basis          RepoLinkBasis
	multiRepoItems int
	coverage       RepoLinkCoverage
	// rowsByDay are the compute's rows for every day of both windows.
	rowsByDay map[time.Time][]workitemmetrics.MetricsDailyRow
}

// blockedItemRow is one (day, team) of the per-item blocked hours.
type blockedItemRow struct {
	Day      time.Time
	TeamID   string
	Duration float64 // the hours of the items with blocked hours that day
	Positive int64   // how many items had blocked hours that day
}

// repoLinkedLoader loads the view once for the request, whichever of the four
// metrics asks first.
type repoLinkedLoader struct {
	client                                     QueryClient
	f                                          Filters
	orgID                                      string
	repoIDs                                    []string
	compareStart, compareEnd, startDay, endDay time.Time

	// itemRepo is the repository of the stored row chosen for each item.
	itemRepo map[string]string

	once sync.Once
	view *repoLinkedView
	err  error
}

type repoLinkedKey struct{}

func withRepoLinkedLoader(ctx context.Context, loader *repoLinkedLoader) context.Context {
	return context.WithValue(ctx, repoLinkedKey{}, loader)
}

func repoLinkedLoaderFrom(ctx context.Context) *repoLinkedLoader {
	loader, _ := ctx.Value(repoLinkedKey{}).(*repoLinkedLoader)
	return loader
}

// repoLinkedRepoRefs are the repositories the request names.
func repoLinkedRepoRefs(f Filters) []string {
	return teamscope.NamedRepoRefs(f.Scope.Level, f.Scope.IDs, f.What.Repos)
}

func (loader *repoLinkedLoader) load(ctx context.Context) (*repoLinkedView, error) {
	loader.once.Do(func() { loader.view, loader.err = loader.read(ctx) })
	if loader.view == nil && loader.err == nil {
		// The read did not finish (its goroutine was stopped): never an empty view.
		return nil, errors.New("home: the repo-linked read did not finish")
	}
	return loader.view, loader.err
}

func (loader *repoLinkedLoader) read(ctx context.Context) (*repoLinkedView, error) {
	view := &repoLinkedView{state: repoLinkLinked}
	ids, err := resolveRepoIDs(ctx, loader.client, repoLinkedRepoRefs(loader.f), loader.orgID)
	if err != nil {
		return nil, err
	}
	loader.repoIDs = ids
	if len(ids) == 0 {
		// Named repositories that resolve to nothing: the filter matches nothing.
		view.state = repoLinkNoLinks
		return view, nil
	}
	view, err = loader.readLinked(ctx, view)
	if err != nil {
		// A bound hit in this read degrades the four metrics only, never the whole
		// Home document: a stated state, no data, never the unfiltered value.
		if reason := repoLinkBoundHit(err); reason != "" {
			slog.Warn("home: repository-linked work-item read hit a bound",
				"operation", "home.repo_linked_work_items", "org_id", loader.orgID, "state", reason,
				"max_execution_seconds", repoLinkedMaxExecutionSeconds, "max_result_rows", RepoLinkedMaxResultRows,
				"repositories", len(loader.repoIDs), "error", err)
			return &repoLinkedView{state: reason}, nil
		}
		return nil, err
	}
	return view, nil
}

// repoLinkBoundHit names the state of a read that hit its time bound (ClickHouse
// code 159) or its row bound (code 396), "" for any other error.
func repoLinkBoundHit(err error) string {
	text := strings.ToLower(err.Error())
	switch {
	case strings.Contains(text, "timeout_exceeded") || strings.Contains(text, "code: 159") || errors.Is(err, context.DeadlineExceeded):
		return repoLinkTimedOut
	case strings.Contains(text, "too_many_rows_or_bytes") || strings.Contains(text, "code: 396") || strings.Contains(text, "limit for result exceeded"):
		return repoLinkTooLarge
	}
	return ""
}

const repoLinkedSettings = "\nSETTINGS max_execution_time = %d, max_result_rows = %d"

// linkedItemsSubquery is the item ids linked to the repositories' pull requests.
const linkedItemsSubquery = `(SELECT work_item_id FROM work_graph_issue_pr FINAL
        WHERE org_id = {org_id:String} AND repo_id IN {link_repo_ids:Array(UUID)})`

func (loader *repoLinkedLoader) bindings() []dhclickhouse.Binding {
	return []dhclickhouse.Binding{
		{Name: "org_id", Value: loader.orgID},
		{Name: "link_repo_ids", Value: loader.repoIDs},
		{Name: "win_start", Value: formatDay(loader.compareStart)},
		{Name: "win_end", Value: formatDay(loader.endDay)},
	}
}

func (loader *repoLinkedLoader) readLinked(ctx context.Context, view *repoLinkedView) (*repoLinkedView, error) {
	budget := fmt.Sprintf(repoLinkedSettings, repoLinkedMaxExecutionSeconds, RepoLinkedMaxResultRows)

	items, err := loader.readItems(ctx, budget)
	if err != nil {
		return nil, err
	}
	attributions, err := loader.readAttributions(ctx, budget, items)
	if err != nil {
		return nil, err
	}
	view.items, view.attributions = items, attributions
	if err := loader.readBasis(ctx, budget, view); err != nil {
		return nil, err
	}
	if err := loader.readCoverage(ctx, budget, view); err != nil {
		return nil, err
	}
	blocked, err := loader.readBlocked(ctx, budget)
	if err != nil {
		return nil, err
	}
	view.blocked = blocked
	view.rowsByDay = computeRowsByDay(loader.compareStart, loader.endDay, items, attributions)
	if len(items) == 0 && len(blocked) == 0 {
		view.state = repoLinkNoLinks
	}
	return view, nil
}

// readItems reads the linked items the work_item family would load for some day
// of the two windows: the family's predicate (created before the window ends and
// either not done or completed no earlier than its start), over the union of
// the days, from work_items FINAL, once per (provider, id) by the newest
// last_synced (the lower repository id on a tie).
func (loader *repoLinkedLoader) readItems(ctx context.Context, budget string) ([]workitemmetrics.StoredItem, error) {
	rows, err := loader.client.Query(ctx, `
SELECT toString(repo_id), last_synced,
       work_item_id, provider, status, project_key, project_id, native_team_key, project_name,
       created_at, completed_at, type, assignees, started_at, closed_at, story_points
FROM work_items FINAL
WHERE org_id = {org_id:String}
  AND created_at < toDateTime64({win_end:Date}, 3, 'UTC')
  AND (status != 'done' OR completed_at >= toDateTime64({win_start:Date}, 3, 'UTC'))
  AND work_item_id IN `+linkedItemsSubquery+budget, loader.bindings())
	if err != nil {
		return nil, fmt.Errorf("home: repo-linked items: %w", err)
	}
	defer rows.Close()
	type versioned struct {
		item   workitemmetrics.StoredItem
		repoID string
		synced time.Time
	}
	best := map[[2]string]versioned{}
	for rows.Next() {
		var v versioned
		var completed, started, closed *time.Time
		var points *float64
		item := &v.item
		if err := rows.Scan(&v.repoID, &v.synced, &item.WorkItemID, &item.Provider, &item.Status,
			&item.ProjectKey, &item.ProjectID, &item.NativeTeamKey, &item.ProjectName,
			&item.CreatedAt, &completed, &item.Type, &item.Assignees, &started, &closed, &points); err != nil {
			return nil, fmt.Errorf("home: repo-linked items scan: %w", err)
		}
		item.CompletedAt, item.StartedAt, item.ClosedAt, item.StoryPoints = completed, started, closed, points
		key := [2]string{item.Provider, item.WorkItemID}
		if current, seen := best[key]; seen {
			newer := v.synced.After(current.synced) || (v.synced.Equal(current.synced) && v.repoID < current.repoID)
			if !newer {
				continue
			}
		}
		best[key] = v
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("home: repo-linked items: %w", err)
	}
	out := make([]workitemmetrics.StoredItem, 0, len(best))
	loader.itemRepo = make(map[string]string, len(best))
	for _, v := range best {
		out = append(out, v.item)
		loader.itemRepo[v.item.WorkItemID] = v.repoID
	}
	sort.Slice(out, func(i, j int) bool { return out[i].WorkItemID < out[j].WorkItemID })
	return out, nil
}

// readAttributions reads each item's primary team: the row of
// work_item_team_attributions the work_item family reads (is_primary = 1, the
// newest snapshot of each (repository, item)). An item with no row has none, which
// the compute resolves to unassigned as the family does.
func (loader *repoLinkedLoader) readAttributions(ctx context.Context, budget string, items []workitemmetrics.StoredItem) (map[string]workitemmetrics.PrimaryAttribution, error) {
	out := map[string]workitemmetrics.PrimaryAttribution{}
	if len(items) == 0 {
		return out, nil
	}
	rows, err := loader.client.Query(ctx, `
SELECT toString(repo_id), work_item_id, ifNull(team_id, ''), ifNull(team_name, '')
FROM work_item_team_attributions FINAL
WHERE org_id = {org_id:String} AND is_primary = 1
  AND work_item_id IN `+linkedItemsSubquery+`
  AND (repo_id, work_item_id, computed_at) IN (
      SELECT repo_id, work_item_id, max(computed_at)
      FROM work_item_team_attributions
      WHERE org_id = {org_id:String} AND work_item_id IN `+linkedItemsSubquery+`
      GROUP BY repo_id, work_item_id)`+budget, loader.bindings())
	if err != nil {
		return nil, fmt.Errorf("home: repo-linked attributions: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var repoID, itemID string
		var attribution workitemmetrics.PrimaryAttribution
		if err := rows.Scan(&repoID, &itemID, &attribution.TeamID, &attribution.TeamName); err != nil {
			return nil, fmt.Errorf("home: repo-linked attributions scan: %w", err)
		}
		// The attribution of the stored row the item was read from.
		if loader.itemRepo[itemID] == repoID {
			out[itemID] = attribution
		}
	}
	return out, rows.Err()
}

// readBasis counts the items of the view by the best tier of the links that put
// them there, and the items that are in more than one repository's view (linked
// to pull requests of more than one repository, whichever repositories).
func (loader *repoLinkedLoader) readBasis(ctx context.Context, budget string, view *repoLinkedView) error {
	rows, err := loader.client.Query(ctx, `
SELECT work_item_id,
       multiIf(maxIf(rank, repo_id IN {link_repo_ids:Array(UUID)}) = 3, 'native',
               maxIf(rank, repo_id IN {link_repo_ids:Array(UUID)}) = 2, 'explicit_text', 'heuristic') AS tier,
       toUInt8(uniqExact(repo_id) > 1) AS multi
FROM (
  SELECT work_item_id, repo_id,
         multiIf(provenance = 'native', 3, provenance = 'explicit_text', 2, 1) AS rank
  FROM work_graph_issue_pr FINAL
  WHERE org_id = {org_id:String} AND work_item_id IN `+linkedItemsSubquery+`
)
GROUP BY work_item_id`+budget, loader.bindings())
	if err != nil {
		return fmt.Errorf("home: repo-linked basis: %w", err)
	}
	defer rows.Close()
	inView := map[string]bool{}
	for _, item := range view.items {
		inView[item.WorkItemID] = true
	}
	for rows.Next() {
		var id, tier string
		var multi uint8
		if err := rows.Scan(&id, &tier, &multi); err != nil {
			return fmt.Errorf("home: repo-linked basis scan: %w", err)
		}
		if !inView[id] {
			continue
		}
		switch tier {
		case "native":
			view.basis.Native++
		case "explicit_text":
			view.basis.ExplicitText++
		default:
			view.basis.Heuristic++
		}
		if multi == 1 {
			view.multiRepoItems++
		}
	}
	return rows.Err()
}

// readCoverage is the organization's items in the window and how many of them
// have a link to any repository: an item with no link is in no repository's view.
func (loader *repoLinkedLoader) readCoverage(ctx context.Context, budget string, view *repoLinkedView) error {
	rows, err := loader.client.Query(ctx, `
SELECT toInt64(count()), toInt64(countIf(work_item_id IN (
    SELECT work_item_id FROM work_graph_issue_pr FINAL WHERE org_id = {org_id:String})))
FROM (
  SELECT provider, work_item_id
  FROM work_items FINAL
  WHERE org_id = {org_id:String}
    AND created_at < toDateTime64({win_end:Date}, 3, 'UTC')
    AND (status != 'done' OR completed_at >= toDateTime64({win_start:Date}, 3, 'UTC'))
  GROUP BY provider, work_item_id
)`+budget, loader.bindings())
	if err != nil {
		return fmt.Errorf("home: repo-linked coverage: %w", err)
	}
	defer rows.Close()
	if rows.Next() {
		var total, linked int64
		if err := rows.Scan(&total, &linked); err != nil {
			return fmt.Errorf("home: repo-linked coverage scan: %w", err)
		}
		view.coverage = RepoLinkCoverage{LinkedItems: int(linked), ItemsInWindow: int(total)}
	}
	return rows.Err()
}

// readBlocked reads the blocked hours the work_item_state family stores per item
// (newest version of each (day, item)), summed in ClickHouse per (day, team): one row
// per day and team instead of one per item and day. positive counts the items with
// blocked hours that day (a daily blocked row exists only where blocked hours do).
func (loader *repoLinkedLoader) readBlocked(ctx context.Context, budget string) ([]blockedItemRow, error) {
	rows, err := loader.client.Query(ctx, `
SELECT day, team_id, sum(duration_hours), toInt64(countIf(duration_hours > 0))
FROM (
  SELECT day, provider, work_item_id, argMax(team_id, computed_at) AS team_id,
         argMax(duration_hours, computed_at) AS duration_hours
  FROM work_item_blocked_durations_daily
  WHERE org_id = {org_id:String} AND day >= {win_start:Date} AND day < {win_end:Date}
    AND work_item_id IN `+linkedItemsSubquery+`
  GROUP BY day, provider, work_item_id
)
GROUP BY day, team_id`+budget, loader.bindings())
	if err != nil {
		return nil, fmt.Errorf("home: repo-linked blocked: %w", err)
	}
	defer rows.Close()
	var out []blockedItemRow
	for rows.Next() {
		var r blockedItemRow
		if err := rows.Scan(&r.Day, &r.TeamID, &r.Duration, &r.Positive); err != nil {
			return nil, fmt.Errorf("home: repo-linked blocked scan: %w", err)
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// computeRowsByDay runs the work_item family's compute for every day of the
// window over the items. The compute itself keeps the items that are relevant to
// the day (created before it ends and started, completed or open at its end, or
// created in it), as it does for the daily job.
func computeRowsByDay(from, to time.Time, items []workitemmetrics.StoredItem, attributions map[string]workitemmetrics.PrimaryAttribution) map[time.Time][]workitemmetrics.MetricsDailyRow {
	out := map[time.Time][]workitemmetrics.MetricsDailyRow{}
	if len(items) == 0 {
		return out
	}
	for day := from; day.Before(to); day = day.AddDate(0, 0, 1) {
		if rows := workitemmetrics.ComputeStoredItemsDay(day, items, nil, attributions).MetricsDaily; len(rows) > 0 {
			out[day] = rows
		}
	}
	return out
}

// dayValue is one value of a window's aggregate.
type linkedAggregate struct {
	value   float64
	hasData bool
	series  []dayValueRow
}

// aggregateLinkedWindow aggregates the rows of [from, to) the way fetchMetricValue
// and fetchMetricSeries aggregate the daily table: the metric's aggregator over
// the (day, provider, work scope, team) rows, a NULL skipped, no value = no data;
// the series has one point per day with a value.
func (view *repoLinkedView) aggregateLinkedWindow(metric string, from, to time.Time, teamIDs []string) linkedAggregate {
	teamOK := func(id string) bool {
		if len(teamIDs) == 0 {
			return true
		}
		for _, t := range teamIDs {
			if t == id {
				return true
			}
		}
		return false
	}
	var rowCount int
	var sum float64
	var n int
	perDay := map[time.Time]*[2]float64{} // sum, n
	add := func(day time.Time, v float64) {
		sum += v
		n++
		d := perDay[day]
		if d == nil {
			d = &[2]float64{}
			perDay[day] = d
		}
		d[0] += v
		d[1]++
	}
	average := metric == "cycle_time" || metric == "wip_saturation"
	if metric == "blocked_work" {
		for _, r := range view.blocked {
			if r.Day.Before(from) || !r.Day.Before(to) || !teamOK(normalizeTeamID(r.TeamID)) {
				continue
			}
			if r.Positive <= 0 {
				continue
			}
			rowCount++
			add(r.Day, r.Duration)
		}
		// A blocked row exists in the daily table only where blocked hours do.
		if rowCount == 0 {
			return linkedAggregate{}
		}
	} else {
		for day, rows := range view.rowsByDay {
			if day.Before(from) || !day.Before(to) {
				continue
			}
			for _, row := range rows {
				if !teamOK(row.TeamID) {
					continue
				}
				rowCount++
				switch metric {
				case "cycle_time":
					if row.CycleTimeP50Hours != nil {
						add(day, *row.CycleTimeP50Hours)
					}
				case "throughput":
					add(day, float64(row.ItemsCompleted))
				case "wip_saturation":
					add(day, row.WIPCongestionRatio)
				}
			}
		}
	}
	if rowCount == 0 || n == 0 {
		return linkedAggregate{}
	}
	value := sum
	if average {
		value = sum / float64(n)
	}
	days := make([]time.Time, 0, len(perDay))
	for day := range perDay {
		days = append(days, day)
	}
	sort.Slice(days, func(i, j int) bool { return days[i].Before(days[j]) })
	series := make([]dayValueRow, 0, len(days))
	for _, day := range days {
		d := perDay[day]
		v := d[0]
		if average {
			v = d[0] / d[1]
		}
		series = append(series, dayValueRow{Day: day, Value: v})
	}
	if math.IsNaN(value) {
		return linkedAggregate{}
	}
	return linkedAggregate{value: value, hasData: true, series: series}
}

// normalizeTeamID is the daily job's default for a blocked row's empty team.
func normalizeTeamID(id string) string {
	if id == "" {
		return "unassigned"
	}
	return id
}

// computeRepoLinkedDelta is computeMetricDelta for one of the four work-item
// metrics under a repository filter.
func computeRepoLinkedDelta(ctx context.Context, client QueryClient, spec metricSpec, startDay, endDay, compareStart, compareEnd time.Time, f Filters, orgID string) (MetricDelta, error) {
	loader := repoLinkedLoaderFrom(ctx)
	if loader == nil {
		loader = &repoLinkedLoader{
			client: client, f: f, orgID: orgID,
			compareStart: compareStart, compareEnd: compareEnd, startDay: startDay, endDay: endDay,
		}
	}
	view, err := loader.load(ctx)
	if err != nil {
		return MetricDelta{}, err
	}
	var teamIDs []string
	if f.Scope.Level == "team" {
		teamIDs = f.Scope.IDs
	}
	current := view.aggregateLinkedWindow(spec.Metric, startDay, endDay, teamIDs)
	previous := view.aggregateLinkedWindow(spec.Metric, compareStart, compareEnd, teamIDs)
	currentValue, previousValue := safeFloat(current.value), safeFloat(previous.value)
	pct := deltarule.Of(currentValue, previousValue, current.hasData, previous.hasData).Pct
	if pct != nil {
		safe := safeFloat(*pct)
		pct = &safe
	}
	// The state is a property of the LINK, not of the metric's value (D5864): "linked"
	// whenever a linked item is in the window, the same word for the four metrics of one
	// request; whether the metric has data follows the daily read's own rule.
	state := view.state
	applied := true
	basis, multi, coverage := view.basis, view.multiRepoItems, view.coverage
	return MetricDelta{
		Metric: spec.Metric, Label: spec.Label, Unit: spec.Unit,
		Value:    safeFloat(spec.Transform(currentValue)),
		DeltaPct: pct, HasData: current.hasData, HasPriorData: previous.hasData,
		Spark:             sparkPoints(current.series, spec.Transform),
		RepoFilterApplied: &applied,

		RepoLinkState:          &state,
		RepoLinkBasis:          &basis,
		RepoLinkMultiRepoItems: &multi,
		RepoLinkCoverage:       &coverage,
	}, nil
}
