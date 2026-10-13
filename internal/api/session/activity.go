package session

import (
	"context"
	"log/slog"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/platform/logging"
	"github.com/full-chaos/dev-health-ops/internal/storage/clickhouse/liverow"
)

// activity is OrganizationActivity.
type activity struct {
	hasData bool
	last    *time.Time
	// unknown is set when a table read failed: the activity is then partial,
	// and must not rank the organization against another one.
	unknown bool
}

// metricTables are the tables _load_org_activity reads, in its order.
var metricTables = []string{"repo_metrics_daily", "user_metrics_daily", "team_metrics_daily", "work_item_metrics_daily"}

// orgActivity is _load_org_activity: per organization, whether any metric
// row exists and the newest computed_at across the four daily tables. A
// table that cannot be read is skipped, as Python skips it, but it is counted,
// logged at WARN and marks the organization's activity unknown, so that the
// choice of an organization never rests on partial data (selectActiveMembership).
// Without a ClickHouse connection every organization has no activity.
func (h handlers) orgActivity(ctx context.Context, orgIDs []uuid.UUID) map[uuid.UUID]activity {
	out := map[uuid.UUID]activity{}
	var ordered []uuid.UUID
	for _, id := range orgIDs {
		if _, seen := out[id]; !seen {
			out[id] = activity{}
			ordered = append(ordered, id)
		}
	}
	if h.ClickHouse == nil {
		return out
	}
	for _, orgID := range ordered {
		var result activity
		for _, table := range metricTables {
			var count uint64
			var last time.Time
			err := h.ClickHouse.QueryRow(ctx, activityStatement(table),
				clickhouse.Named("org_id", orgID.String())).Scan(&count, &last)
			if err != nil {
				class := logging.ErrorClass(err)
				recordActivityReadFailed(ctx, table, class)
				h.Logger.WarnContext(ctx, "org activity table read failed, activity unknown",
					slog.String("table", table), slog.String("error_class", class))
				result.unknown = true
				continue
			}
			last = last.UTC()
			result.hasData = result.hasData || count > 0
			if result.last == nil || last.After(*result.last) {
				value := last
				result.last = &value
			}
		}
		out[orgID] = result
	}
	return out
}

// activityStatement counts the metric rows of one organization in table and
// takes their newest computed_at.
//
// A team-keyed daily table can hold retraction rows (package liverow): the
// newest row of a key the compute no longer produces, with 0 in every count.
// Such a row is not data. For those tables the statement reads the newest row
// of each key (FINAL) and keeps the measurements only, so an organization
// whose only newest rows are retraction rows has no data, and a retraction
// row does not make its metrics look newer than its last measurement.
func activityStatement(table string) string {
	source, where := table, "org_id = {org_id:String}"
	if liverow.Registered(table) {
		source += " FINAL"
		where += " AND " + liverow.Predicate(table, "")
	}
	return "SELECT count() AS row_count, max(computed_at) AS last_metrics_at FROM " + source + " WHERE " + where
}

// minTime is datetime.min in UTC, the sort value of a missing timestamp.
var minTime = time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC)

// anyActivityUnknown reports whether the activity of any membership is unknown.
func anyActivityUnknown(memberships []membershipRow, activities map[uuid.UUID]activity) bool {
	for _, m := range memberships {
		if activities[m.OrgID].unknown {
			return true
		}
	}
	return false
}

// chooseMembership is selectActiveMembership with a WARN when the choice falls
// back because an activity read failed (no ids in the line).
func (h handlers) chooseMembership(ctx context.Context, memberships []membershipRow, requested *uuid.UUID, activities map[uuid.UUID]activity) *membershipRow {
	if requested == nil && len(memberships) > 1 && anyActivityUnknown(memberships, activities) {
		recordActivityFallback(ctx)
		h.Logger.WarnContext(ctx, "org activity unknown, active organization chosen by earliest join",
			slog.Int("memberships", len(memberships)))
	}
	return selectActiveMembership(memberships, requested, activities)
}

// selectActiveMembership is _select_active_membership: the requested org's
// membership, or the one with data, then the newest metrics, then the
// earliest join (the first such, as min() keeps the first).
//
// Stricter than Python: when the activity of any membership is unknown (a
// table read failed), activity ranks nobody and the earliest join decides, so
// a failed read can never send a login to another organization.
func selectActiveMembership(memberships []membershipRow, requested *uuid.UUID, activities map[uuid.UUID]activity) *membershipRow {
	if requested != nil {
		for index := range memberships {
			if memberships[index].OrgID == *requested {
				return &memberships[index]
			}
		}
		return nil
	}
	if len(memberships) == 0 {
		return nil
	}
	fallback := anyActivityUnknown(memberships, activities)
	type key struct {
		noData bool
		last   time.Time
		joined time.Time
	}
	keyOf := func(m membershipRow) key {
		a := activities[m.OrgID]
		last := minTime
		if a.last != nil {
			last = *a.last
		}
		joined := m.CreatedAt
		if m.JoinedAt != nil {
			joined = *m.JoinedAt
		}
		if fallback {
			return key{last: minTime, joined: joined.UTC()}
		}
		return key{noData: !a.hasData, last: last, joined: joined.UTC()}
	}
	less := func(a, b key) bool {
		if a.noData != b.noData {
			return !a.noData
		}
		// -last.timestamp() ascending is last descending.
		if !a.last.Equal(b.last) {
			return a.last.After(b.last)
		}
		return a.joined.Before(b.joined)
	}
	best := 0
	bestKey := keyOf(memberships[0])
	for index := 1; index < len(memberships); index++ {
		if candidate := keyOf(memberships[index]); less(candidate, bestKey) {
			best, bestKey = index, candidate
		}
	}
	return &memberships[best]
}
