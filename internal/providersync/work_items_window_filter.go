package providersync

import (
	"context"
	"encoding/json"
	"log/slog"
	"strconv"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// workItemWindowView is what the window filter reads of one fetched item: its id for the log line and its
// updated time. Known is false for an item with no parseable updated time; such an item is kept, as
// Python kept it (it cannot be compared with the window).
type workItemWindowView struct {
	ID        string
	UpdatedAt time.Time
	Known     bool
}

// dropOutsideWorkItemWindow keeps the items updated within the claim's half-open window [since, before),
// for the work-items routes whose fetch bound is wider than the window: GitLab's updated_before is
// inclusive, and the Jira JQL bounds are whole dates widened for every user time zone (up to 50 hours
// beyond the window on each side). An item outside the window belongs to the adjacent window, so it is
// written once. Dropping them is the expected case on every sync, so each call logs ONE Info line (only
// when it dropped something) with the provider, the window and the count; each dropped item is a Debug
// line. Every dropped item is counted (provider, dev_health_work_items_outside_window_dropped_total).
func dropOutsideWorkItemWindow[T any](
	ctx context.Context, metrics *providerfoundation.Metrics, claim Claim, items []T, view func(T) workItemWindowView,
) []T {
	if claim.SinceAt == nil && claim.BeforeAt == nil {
		return items
	}
	kept := make([]T, 0, len(items))
	dropped := 0
	for _, item := range items {
		seen := view(item)
		if !seen.Known || workItemUpdatedWithinWindow(claim, seen.UpdatedAt) {
			kept = append(kept, item)
			continue
		}
		dropped++
		metrics.RecordWorkItemOutsideWindow(claim.Provider)
		slog.Default().DebugContext(ctx, "providersync.work_items.dropped_outside_window_item",
			"org_id", claim.OrgID, "unit_id", claim.ID, "provider", claim.Provider,
			"source_id", claim.SourceID, "work_item", seen.ID, "reason", "outside_window",
			"updated_at", seen.UpdatedAt.UTC().Format(time.RFC3339Nano))
	}
	if dropped > 0 {
		slog.Default().InfoContext(ctx, "providersync.work_items.dropped_outside_window",
			"org_id", claim.OrgID, "unit_id", claim.ID, "provider", claim.Provider,
			"source_id", claim.SourceID, "reason", "outside_window", "dropped", dropped, "kept", len(kept),
			"window_since", formatWindowBound(claim.SinceAt), "window_before", formatWindowBound(claim.BeforeAt))
	}
	return kept
}

func workItemUpdatedWithinWindow(claim Claim, updated time.Time) bool {
	if claim.SinceAt != nil && updated.Before(claim.SinceAt.UTC()) {
		return false
	}
	if claim.BeforeAt != nil && !updated.Before(claim.BeforeAt.UTC()) {
		return false
	}
	return true
}

func formatWindowBound(bound *time.Time) string {
	if bound == nil {
		return ""
	}
	return bound.UTC().Format(time.RFC3339Nano)
}

// gitLabWorkItemWindowView reads the iid and updated_at of a raw GitLab issue or merge request.
func gitLabWorkItemWindowView(raw json.RawMessage) workItemWindowView {
	var item struct {
		IID       int    `json:"iid"`
		UpdatedAt string `json:"updated_at"`
	}
	if json.Unmarshal(raw, &item) != nil {
		return workItemWindowView{}
	}
	view := workItemWindowView{ID: strconv.Itoa(item.IID)}
	if at, err := time.Parse(time.RFC3339Nano, item.UpdatedAt); err == nil {
		view.UpdatedAt, view.Known = at, true
	}
	return view
}

// jiraWorkItemWindowView reads the key and fields.updated of a Jira search result.
func jiraWorkItemWindowView(issue map[string]any) workItemWindowView {
	view := workItemWindowView{}
	view.ID, _ = issue["key"].(string)
	fields, _ := issue["fields"].(map[string]any)
	if at := jiraTime(fields["updated"]); at != nil {
		view.UpdatedAt, view.Known = *at, true
	}
	return view
}

// WorkItemsFetchIsUnbounded reports whether a work-items dataset's own query is not bounded by the unit
// window: a Jira dataset with fetch_all, or with a JQL override, runs the same full query in every unit.
// Planned as bounded windows it would repeat that query once per window, so the planner plans it as ONE
// window, and the route does not window-filter what it returns. The planner and the route share this
// predicate so they cannot drift.
func WorkItemsFetchIsUnbounded(provider string, options map[string]any) bool {
	if provider != "jira" {
		return false
	}
	claim := Claim{Unit: Unit{DatasetOptions: options}}
	return jiraOptionString(claim, "jql_override", "") != "" || jiraOptionBool(claim, "fetch_all", false)
}

// dropJiraIssuesOutsideWindow applies the window to a Jira search result. A fetch_all or jql_override
// search has no window to hold and is left as it is.
func dropJiraIssuesOutsideWindow(
	ctx context.Context, metrics *providerfoundation.Metrics, claim Claim, issues []map[string]any,
) []map[string]any {
	if WorkItemsFetchIsUnbounded(claim.Provider, claim.DatasetOptions) {
		return issues
	}
	return dropOutsideWorkItemWindow(ctx, metrics, claim, issues, jiraWorkItemWindowView)
}
