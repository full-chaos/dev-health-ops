// Package chread reads an organization's metrics activity from the four daily
// metrics tables in ClickHouse. It is the one place the statements live: go-api's
// login (internal/api/session) and the projection writer (internal/orgactivity)
// both use it, so the fact the projection holds is the fact go-api reads live.
// Only the metrics worker and go-api link it; the auth plane does not.
package chread

import (
	"context"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/full-chaos/dev-health-ops/internal/storage/clickhouse/liverow"
)

// Tables are the tables _load_org_activity reads, in its order.
var Tables = []string{"repo_metrics_daily", "user_metrics_daily", "team_metrics_daily", "work_item_metrics_daily"}

// Statement counts the metric rows of one organization in table and takes their
// newest computed_at.
//
// A team-keyed daily table can hold retraction rows (package liverow): the
// newest row of a key the compute no longer produces, with 0 in every count.
// Such a row is not data. For those tables the statement reads the newest row
// of each key (FINAL) and keeps the measurements only, so an organization
// whose only newest rows are retraction rows has no data, and a retraction
// row does not make its metrics look newer than its last measurement.
func Statement(table string) string {
	source, where := table, "org_id = {org_id:String}"
	if liverow.Registered(table) {
		source += " FINAL"
		where += " AND " + liverow.Predicate(table, "")
	}
	return "SELECT count() AS row_count, max(computed_at) AS last_metrics_at FROM " + source + " WHERE " + where
}

// Result is the activity of one organization over the tables that were read.
type Result struct {
	HasData bool
	// Last is the newest computed_at over the tables read; nil when none was read.
	Last *time.Time
	// Failed names the tables whose read failed; the Result is then partial.
	Failed []string
}

// Read reads the four tables for orgID. A table that cannot be read is skipped
// and named in Failed (onFailure, when set, sees each error), as go-api does.
func Read(ctx context.Context, conn driver.Conn, orgID string, onFailure func(table string, err error)) Result {
	var result Result
	for _, table := range Tables {
		var count uint64
		var last time.Time
		if err := conn.QueryRow(ctx, Statement(table), clickhouse.Named("org_id", orgID)).Scan(&count, &last); err != nil {
			result.Failed = append(result.Failed, table)
			if onFailure != nil {
				onFailure(table, err)
			}
			continue
		}
		last = last.UTC()
		result.HasData = result.HasData || count > 0
		if result.Last == nil || last.After(*result.Last) {
			value := last
			result.Last = &value
		}
	}
	return result
}
