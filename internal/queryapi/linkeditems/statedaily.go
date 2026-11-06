package linkeditems

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	dhclickhouse "github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/workitemblockers"
	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/workitemmetrics"
)

// StateBudget is the per-statement SETTINGS clause of the state-duration recompute: the bounds of UserBudget. A bound
// hit is an error of the read, never an answer over more items.
const StateBudget = UserBudget

// stateBlockedLimit bounds the blocking relations, and the relation ends, the recompute reads for its items (the
// max_result_rows bound of the other reads). More is workitemblockers.ErrLimitExceeded: an error of the read.
const stateBlockedLimit = 200_000

// transitionsStatement reads the status transitions of the linked items up to the end of the window, with the identity
// and dedup of the daily work_item_state family (internal/jobs/metrics/daily, LoadWorkItemStateTransitions): a
// transition belongs to its work item, and only re-synced copies of one event collapse.
const transitionsStatement = `
SELECT work_item_id, occurred_at, from_status, to_status
FROM (
	SELECT
		org_id, work_item_id, occurred_at,
		from_status, to_status, from_status_raw, to_status_raw, actor,
		max(last_synced) AS last_synced
	FROM work_item_transitions
	WHERE org_id = {org_id:String} AND occurred_at < toDateTime64({win_end:Date}, 3, 'UTC')
	  AND work_item_id IN ` + Subquery + `
	GROUP BY org_id, work_item_id, occurred_at,
		from_status, to_status, from_status_raw, to_status_raw, actor
)`

// StateDaily is the work_item_state_durations_daily rows of the items linked to a loader's repositories, recomputed
// with the daily job's own compute (workitemmetrics.ComputeStateDurationsDay): read once for the window of the loader
// and shared by every read of one request (CHAOS-13088). The table has no item and no repository, so a request held to
// a repository ceiling reads these rows where it reads the table.
type StateDaily struct {
	loader     Loader
	computedAt time.Time

	once sync.Once
	rows map[time.Time][]workitemmetrics.StateDurationRow
	err  error
}

// NewStateDaily is the state-duration recompute of loader. computedAt ends the last segment of an item that is not
// completed, as the daily job's run time does; a day that ended before it is computed as the job computes it. It
// reads nothing when loader holds no repository.
func NewStateDaily(loader Loader, computedAt time.Time) *StateDaily {
	return &StateDaily{loader: loader, computedAt: computedAt.UTC()}
}

// Rows are the recomputed rows of every day of [WindowStart, WindowEnd), by day.
func (s *StateDaily) Rows(ctx context.Context) (map[time.Time][]workitemmetrics.StateDurationRow, error) {
	s.once.Do(func() { s.rows, s.err = s.compute(ctx) })
	return s.rows, s.err
}

func (s *StateDaily) compute(ctx context.Context) (map[time.Time][]workitemmetrics.StateDurationRow, error) {
	none := map[time.Time][]workitemmetrics.StateDurationRow{}
	if len(s.loader.RepoIDs) == 0 {
		return none, nil
	}
	versions, attributions, err := s.loader.Read(ctx)
	if err != nil {
		return nil, err
	}
	if len(versions) == 0 {
		return none, nil
	}
	transitions, err := s.loader.readTransitions(ctx)
	if err != nil {
		return nil, err
	}
	// With no transition every item is skipped, as the daily job skips it: the blocked spans are not read.
	if len(transitions) == 0 {
		return none, nil
	}
	blocked, err := workitemblockers.LoadBlockedIntervalsNaming(ctx, namedQuerier{s.loader.Client}, s.loader.OrgID, blockingNames(versions), stateBlockedLimit)
	if err != nil {
		return nil, fmt.Errorf("linkeditems: blocked spans: %w", err)
	}
	return StateRowsByDay(s.loader.WindowStart, s.loader.WindowEnd, versions, attributions, transitions, s.computedAt, blocked), nil
}

// readTransitions reads transitionsStatement.
func (l Loader) readTransitions(ctx context.Context) ([]workitemmetrics.StateTransition, error) {
	rows, err := l.Client.Query(ctx, transitionsStatement+l.Budget, l.Bindings())
	if err != nil {
		return nil, fmt.Errorf("linkeditems: transitions: %w", err)
	}
	defer rows.Close()
	var out []workitemmetrics.StateTransition
	for rows.Next() {
		var transition workitemmetrics.StateTransition
		if err := rows.Scan(&transition.WorkItemID, &transition.OccurredAt, &transition.FromStatus, &transition.ToStatus); err != nil {
			return nil, fmt.Errorf("linkeditems: transitions scan: %w", err)
		}
		transition.OccurredAt = transition.OccurredAt.UTC()
		out = append(out, transition)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("linkeditems: transitions: %w", err)
	}
	return out, nil
}

// blockingNames are the names the blocking relations of the items use: each item's id and, for a jira or linear item,
// the external-key form of its issue key (workitemmetrics.ExternalKeyTarget), sorted, without repeats.
func blockingNames(versions []Version) []string {
	seen := map[string]bool{}
	var names []string
	add := func(name string) {
		if name != "" && !seen[name] {
			seen[name] = true
			names = append(names, name)
		}
	}
	for _, v := range versions {
		add(v.Item.WorkItemID)
		if key, ok := workitemmetrics.ExternalKeyTarget(v.Item.Provider, v.Item.WorkItemID); ok {
			add(key)
		}
	}
	sort.Strings(names)
	return names
}

// namedQuerier runs the statements of workitemblockers through the query API's client, which binds by name.
type namedQuerier struct{ client RowQuerier }

func (q namedQuerier) QueryNamed(ctx context.Context, statement string, params []workitemblockers.Param) (workitemblockers.Rows, error) {
	bindings := make([]dhclickhouse.Binding, 0, len(params))
	for _, param := range params {
		bindings = append(bindings, dhclickhouse.Binding{Name: param.Name, Value: param.Value})
	}
	return q.client.Query(ctx, statement, bindings)
}

// StateRowsByDay runs the work_item_state family's compute for every day of [from, to). For each day the items are
// the versions that pass the day's predicate, each item once by the daily job's own rule
// (workitemmetrics.OncePerProviderAndID), each with the attribution of the stored row it is, and the transitions of
// the day are those before its end, as the daily job reads them.
func StateRowsByDay(
	from, to time.Time, versions []Version, attributions map[Address]workitemmetrics.PrimaryAttribution,
	transitions []workitemmetrics.StateTransition, computedAt time.Time, blocked map[string][]workitemmetrics.BlockedInterval,
) map[time.Time][]workitemmetrics.StateDurationRow {
	out := map[time.Time][]workitemmetrics.StateDurationRow{}
	if len(versions) == 0 || len(transitions) == 0 {
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
		if len(kept) == 0 {
			continue
		}
		items := make([]workitemmetrics.StateItem, 0, len(kept))
		dayAttributions := map[string]workitemmetrics.PrimaryAttribution{}
		for _, index := range kept {
			v := passing[index]
			items = append(items, stateItemOf(v.Item))
			if attribution, found := attributions[Address{v.Item.Provider, v.Item.WorkItemID, v.RepoID}]; found {
				dayAttributions[v.Item.WorkItemID] = attribution
			}
		}
		var dayTransitions []workitemmetrics.StateTransition
		for _, transition := range transitions {
			if transition.OccurredAt.Before(dayEnd) {
				dayTransitions = append(dayTransitions, transition)
			}
		}
		rows, _, _ := workitemmetrics.ComputeStateDurationsDay(day, dayEnd, items, dayTransitions, dayAttributions, computedAt, blocked)
		if len(rows) > 0 {
			out[day] = rows
		}
	}
	return out
}

func stateItemOf(item workitemmetrics.StoredItem) workitemmetrics.StateItem {
	return workitemmetrics.StateItem{
		WorkItemID: item.WorkItemID, Provider: item.Provider, Status: item.Status,
		ProjectKey: item.ProjectKey, ProjectID: item.ProjectID, NativeTeamKey: item.NativeTeamKey, ProjectName: item.ProjectName,
		CreatedAt: item.CreatedAt, CompletedAt: item.CompletedAt,
	}
}
