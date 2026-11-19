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
// status mapping is not an event. A value with no or an unparsable updatedAt
// (GitHub serves it as DateTime!) is not an event either, and timeMissing
// tells the caller to count it.
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
		return githubWorkItemTransitionRow{}, false, false, nil
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
// collapse, two events at one instant do not.
const gitHubProjectV2StoredTransitionsQuery = `
SELECT work_item_id, occurred_at, from_status, to_status, to_status_raw
FROM work_item_transitions
WHERE org_id = ? AND work_item_id IN ?
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
			var row githubWorkItemTransitionRow
			var raw string
			if err := rows.Scan(&row.WorkItemID, &row.OccurredAt, &row.FromStatus, &row.ToStatus, &raw); err != nil {
				rows.Close()
				return nil, err
			}
			row.OccurredAt = row.OccurredAt.UTC()
			row.ToStatusRaw = nullableString(raw)
			stored = append(stored, row)
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return nil, err
		}
	}
	return stored, nil
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
// board event in this batch to the last event of its history: the stored
// transitions plus the merged batch, ordered by workitemmetrics.OrderTransitions
// (the order the state segments use). The history itself is not changed. An
// item with no board event keeps the status the board row already has.
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
	for _, list := range [][]githubWorkItemTransitionRow{stored, merged.StatusTransitions} {
		for _, transition := range list {
			if _, wanted := history[transition.WorkItemID]; wanted {
				history[transition.WorkItemID] = append(history[transition.WorkItemID], transition)
			}
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
