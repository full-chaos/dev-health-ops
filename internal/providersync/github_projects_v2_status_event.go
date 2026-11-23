package providersync

import (
	"context"
	"log/slog"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/querybound"
	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/workitemmetrics"
)

// githubProjectV2StatusEvent is the board's one event for an item: GitHub
// serves no past board moves, only the current Status value and the time it
// was set. Every field comes from the payload, so a re-sync of the same value
// writes the same row. The board knows no prior status: FromStatus is
// "unknown", the token the repository chain starts with. A value with no
// status mapping is an event too (githubProjectV2UnmappedStatus). A value with
// no or an unparsable updatedAt (GitHub serves it as DateTime!) is not an
// event, and timeMissing tells the caller to count it.
func githubProjectV2StatusEvent(
	claim Claim, item gitHubProjectV2ItemPayload, workItemID string, normalizedAt time.Time,
) (event githubWorkItemTransitionRow, hasEvent bool, timeMissing bool, err error) {
	var status *gitHubProjectV2FieldValuePayload
	for index, value := range item.FieldValues.Nodes {
		if value.Typename == "ProjectV2ItemFieldSingleSelectValue" &&
			normalizeWorkItemLabel(value.Field.Name) == "status" && value.Name != nil {
			status = &item.FieldValues.Nodes[index]
		}
	}
	if status == nil {
		return githubWorkItemTransitionRow{}, false, false, nil
	}
	raw := *status.Name
	if strings.TrimSpace(raw) == "" {
		return githubWorkItemTransitionRow{}, false, false, nil
	}
	occurredAt := parseGitHubWorkItemTime(status.UpdatedAt)
	if occurredAt == nil {
		return githubWorkItemTransitionRow{}, false, true, nil
	}
	toStatus := githubProjectV2Status(raw, nil, "")
	if toStatus == "unknown" {
		toStatus = githubProjectV2UnmappedStatus(item.Content, *occurredAt)
	}
	event = githubWorkItemTransitionRow{
		WorkItemID: workItemID, Provider: "github", OccurredAt: occurredAt.UTC(),
		ToStatusRaw: &raw, FromStatus: "unknown", ToStatus: toStatus,
		OrgID: claim.OrgID, LastSynced: normalizedAt.UTC(),
	}
	if err := event.validate(claim); err != nil {
		return githubWorkItemTransitionRow{}, false, false, err
	}
	return event, true, false, nil
}

// githubProjectV2UnmappedStatus is the status of a board value the mapping
// does not know (a custom column): "unknown", unless the issue was already
// closed when the value was set (closed now, closedAt <= the value's
// updatedAt): the native close outranks an unmapped column, so the event goes
// to done, or to canceled for a not-planned close. A closed state is required
// as well as closedAt, so an issue open now never takes a close from an older
// closedAt.
func githubProjectV2UnmappedStatus(content gitHubProjectV2ContentPayload, at time.Time) string {
	closedAt := parseGitHubWorkItemTime(content.ClosedAt)
	if content.Typename != "Issue" || content.State == nil || normalizeWorkItemLabel(*content.State) != "closed" ||
		closedAt == nil || closedAt.After(at) {
		return "unknown"
	}
	if content.StateReason != nil && normalizeWorkItemLabel(*content.StateReason) == "not_planned" {
		return "canceled"
	}
	return "done"
}

// reportGitHubProjectV2StatusTimeMissing is the one record per unit of the
// board items whose Status value came with no usable updatedAt. Those items
// keep the board value as their status and add no event, so without this line
// a payload change would silently stop the board side of the history.
func reportGitHubProjectV2StatusTimeMissing(ctx context.Context, claim Claim, items int) {
	if items == 0 {
		return
	}
	slog.Default().WarnContext(ctx, "github_projects_v2.status_value_time_missing",
		"provider", claim.Provider, "dataset", claim.Dataset,
		"org_id", claim.OrgID, "unit", claim.ID, "integration", claim.IntegrationID, "items", items)
}

// githubProjectV2StoredTransitionsReader reads the transitions earlier runs
// stored for a set of items. A board item's status is the last event of its
// whole history, and the repository events of an issue arrive only in the
// runs whose window holds its update, so the batch alone is not the history.
type githubProjectV2StoredTransitionsReader interface {
	StoredTransitions(ctx context.Context, orgID string, workItemIDs []string) ([]githubWorkItemTransitionRow, error)
}

// GitHubProjectV2StoredTransitionsClickHouseReader is the production reader.
type GitHubProjectV2StoredTransitionsClickHouseReader struct{ Conn driver.Conn }

// One row per distinct event, the identity the daily loaders read
// (jobs/metrics/daily/work_item_state_native_clickhouse.go): re-synced copies
// collapse, two events at one instant do not, and a stale pre-170 board row is
// dropped (workitemmetrics.StaleBoardTieZeroSQL). Each row carries the
// tie_ordinal of every copy (one event content can sit at two keys: two boards
// of one issue, or a first label and a board column of the same name), so the
// settle step can tell which stored copies a batch row replaces.
const gitHubProjectV2StoredTransitionsQuery = `
SELECT work_item_id, occurred_at, from_status, to_status, to_status_raw, arraySort(groupUniqArray(tie_ordinal))
FROM (
	SELECT ` + workitemmetrics.StoredTransitionColumnsSQL + `
	FROM work_item_transitions
	WHERE org_id = ? AND work_item_id IN ?
)
WHERE NOT ` + workitemmetrics.StaleBoardTieZeroSQL + `
GROUP BY org_id, work_item_id, occurred_at, from_status, to_status, from_status_raw, to_status_raw, actor`

func (reader GitHubProjectV2StoredTransitionsClickHouseReader) StoredTransitions(
	ctx context.Context, orgID string, workItemIDs []string,
) ([]githubWorkItemTransitionRow, error) {
	if reader.Conn == nil || ctx == nil || strings.TrimSpace(orgID) == "" {
		return nil, ErrInvalidConfiguration
	}
	var stored []githubWorkItemTransitionRow
	for _, chunk := range querybound.ChunkStringsByRenderedBytes(workItemIDs, querybound.MaxArrayBytes) {
		rows, err := reader.Conn.Query(ctx, gitHubProjectV2StoredTransitionsQuery, orgID, chunk)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var workItemID, from, to, raw string
			var occurredAt time.Time
			var tieOrdinals []uint32
			if err := rows.Scan(&workItemID, &occurredAt, &from, &to, &raw, &tieOrdinals); err != nil {
				rows.Close()
				return nil, err
			}
			stored = append(stored, githubStoredTransitionRow(workItemID, occurredAt, from, to, raw, tieOrdinals))
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return nil, err
		}
	}
	return stored, nil
}

// githubStoredTransitionRow is one row of the stored read.
func githubStoredTransitionRow(
	workItemID string, occurredAt time.Time, from, to, raw string, tieOrdinals []uint32,
) githubWorkItemTransitionRow {
	return githubWorkItemTransitionRow{
		WorkItemID: workItemID, OccurredAt: occurredAt.UTC(), FromStatus: from, ToStatus: to,
		ToStatusRaw: nullableString(raw), storedTieOrdinals: tieOrdinals,
	}
}

// mergeGitHubProjectV2RowsWithStored is the route's board merge: the union of
// both halves, then each board item's status from the last event of its
// stored and batch history.
func mergeGitHubProjectV2RowsWithStored(
	ctx context.Context, reader githubProjectV2StoredTransitionsReader, orgID string, repository, projects githubWorkItemRows,
) (githubWorkItemRows, error) {
	merged := mergeGitHubProjectV2Rows(repository, projects)
	ids := githubProjectV2EventItemIDs(projects)
	if len(ids) == 0 {
		return merged, nil
	}
	stored, err := reader.StoredTransitions(ctx, orgID, ids)
	if err != nil {
		return githubWorkItemRows{}, err
	}
	for index := range stored {
		stored[index].OrgID = orgID
	}
	return settleGitHubProjectV2Status(merged, projects, stored), nil
}

// githubProjectV2EventItemIDs lists, once each, the items the board half
// emitted a Status event for.
func githubProjectV2EventItemIDs(projects githubWorkItemRows) []string {
	seen := make(map[string]struct{}, len(projects.StatusTransitions))
	ids := make([]string, 0, len(projects.StatusTransitions))
	for _, event := range projects.StatusTransitions {
		if _, dup := seen[event.WorkItemID]; !dup {
			seen[event.WorkItemID] = struct{}{}
			ids = append(ids, event.WorkItemID)
		}
	}
	return ids
}

// settleGitHubProjectV2Status sets the status of every board item that has a
// board event in this batch to the last event of its history: the transitions
// the table holds once the batch is written (transitionsAfterWrite), ordered by
// workitemmetrics.OrderTransitions (the order the state segments use). The
// history itself is not changed. An item with no board event keeps the status
// the board row already has.
func settleGitHubProjectV2Status(
	merged, projects githubWorkItemRows, stored []githubWorkItemTransitionRow,
) githubWorkItemRows {
	ids := githubProjectV2EventItemIDs(projects)
	if len(ids) == 0 {
		return merged
	}
	history := make(map[string][]githubWorkItemTransitionRow, len(ids))
	for _, id := range ids {
		history[id] = nil
	}
	for _, transition := range transitionsAfterWrite(stored, merged.StatusTransitions) {
		if _, wanted := history[transition.WorkItemID]; wanted {
			history[transition.WorkItemID] = append(history[transition.WorkItemID], transition)
		}
	}
	merged.WorkItems = append([]githubWorkItemRow{}, merged.WorkItems...)
	for position, row := range merged.WorkItems {
		events, wanted := history[row.WorkItemID]
		if !wanted || row.ProjectID == nil || !strings.HasPrefix(*row.ProjectID, "ghprojv2:") {
			continue
		}
		if last, ok := lastGitHubTransition(events); ok {
			row.Status = last.ToStatus
			row.StatusRaw = last.ToStatusRaw
			merged.WorkItems[position] = row
		}
	}
	return merged
}

// transitionsAfterWrite is the history work_item_transitions holds once the
// batch is written, read as its readers read it: each stored event at every
// key it holds, the writer's rows (rankTransitionTies) replacing the stored
// rows of their sorting key (the engine keeps the newer last_synced), then one
// row per event content. A stored event stays while any of its keys does.
func transitionsAfterWrite(stored, batch []githubWorkItemTransitionRow) []githubWorkItemTransitionRow {
	rows := make([]workItemTransitionStoredRow, 0, len(stored)+len(batch))
	for _, row := range stored {
		for _, tieOrdinal := range row.storedTieOrdinals {
			projected := projectWorkItemTransition(row)
			projected.TieOrdinal = tieOrdinal
			rows = append(rows, projected)
		}
	}
	rows = append(rows, rankTransitionTies(projectRows(batch, projectWorkItemTransition))...)
	rows = dedupeBySortingKey(rows, transitionSortingKey)
	rows = dedupeBySortingKey(rows, func(row workItemTransitionStoredRow) string {
		return transitionInstantKey(row) + workItemKeySeparator + transitionTieOrder(row)
	})
	out := make([]githubWorkItemTransitionRow, 0, len(rows))
	for _, row := range rows {
		out = append(out, githubWorkItemTransitionRow{
			WorkItemID: row.WorkItemID, Provider: row.Provider, OccurredAt: row.OccurredAt,
			FromStatusRaw: nullableString(row.FromStatusRaw), ToStatusRaw: nullableString(row.ToStatusRaw),
			FromStatus: row.FromStatus, ToStatus: row.ToStatus, Actor: nullableString(row.Actor),
			OrgID: row.OrgID, LastSynced: row.LastSynced,
		})
	}
	return out
}

// lastGitHubTransition is the last element of workitemmetrics.OrderTransitions,
// with every time at the stored precision (clickHouseMillis), so the batch
// orders its events the way a later read of the stored rows does.
// Two events with the same instant and statuses but different raw values are
// one step of that order; the smaller raw value is taken, so the answer does
// not depend on the read order.
func lastGitHubTransition(events []githubWorkItemTransitionRow) (githubWorkItemTransitionRow, bool) {
	if len(events) == 0 {
		return githubWorkItemTransitionRow{}, false
	}
	ordered := make([]workitemmetrics.Transition, 0, len(events))
	for _, event := range events {
		ordered = append(ordered, workitemmetrics.Transition{
			WorkItemID: event.WorkItemID, OccurredAt: clickHouseMillis(event.OccurredAt),
			FromStatus: event.FromStatus, ToStatus: event.ToStatus,
		})
	}
	ordered = workitemmetrics.OrderTransitions(ordered)
	want := ordered[len(ordered)-1]
	var last githubWorkItemTransitionRow
	found := false
	for _, event := range events {
		if !clickHouseMillis(event.OccurredAt).Equal(want.OccurredAt) || event.FromStatus != want.FromStatus || event.ToStatus != want.ToStatus {
			continue
		}
		if !found || rawLess(event.ToStatusRaw, last.ToStatusRaw) {
			last, found = event, true
		}
	}
	return last, found
}

func rawLess(left, right *string) bool {
	if left == nil || right == nil {
		return left == nil && right != nil
	}
	return *left < *right
}
