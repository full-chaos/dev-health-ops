package daily

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"
)

// RetractedColumn is the UInt8 column (DEFAULT 0) of the six benchmarking
// output tables that marks a retraction row. A computed row stores 0. A reader
// takes the NEWEST row of a key (by computed_at) first and leaves the key out
// when that row holds 1.
const RetractedColumn = "retracted"

// benchmarkingKeyKind is how one key column of a benchmarking output table is
// scanned and written back.
type benchmarkingKeyKind int

const (
	benchmarkingKeyString benchmarkingKeyKind = iota
	benchmarkingKeyUInt16
	benchmarkingKeyDate
)

type benchmarkingKeyColumn struct {
	name string
	kind benchmarkingKeyKind
}

// benchmarkingOutputTable is one output table of benchmarking: the Date column
// that holds the day of a run, and the columns of a key besides org_id.
type benchmarkingOutputTable struct {
	table string
	day   string
	keys  []benchmarkingKeyColumn
}

// benchmarkingOutputTables are the six output tables in the order WriteOutputs
// writes them. The day column of each is the run's day on a normal run, except
// for an anomaly (and the insight of an anomaly): its day is the last point of
// the series at or before the run's day.
var benchmarkingOutputTables = []benchmarkingOutputTable{
	{"testops_metric_baselines", "period_end", []benchmarkingKeyColumn{
		{"metric_name", benchmarkingKeyString}, {"scope_type", benchmarkingKeyString}, {"scope_key", benchmarkingKeyString},
		{"rolling_window_days", benchmarkingKeyUInt16}}},
	{"testops_maturity_bands", "period_end", []benchmarkingKeyColumn{
		{"metric_name", benchmarkingKeyString}, {"scope_type", benchmarkingKeyString}, {"scope_key", benchmarkingKeyString}}},
	{"testops_metric_anomalies", "day", []benchmarkingKeyColumn{
		{"metric_name", benchmarkingKeyString}, {"scope_type", benchmarkingKeyString}, {"scope_key", benchmarkingKeyString},
		{"anomaly_type", benchmarkingKeyString}}},
	{"testops_period_comparisons", "current_period_end", []benchmarkingKeyColumn{
		{"metric_name", benchmarkingKeyString}, {"scope_type", benchmarkingKeyString}, {"scope_key", benchmarkingKeyString},
		{"comparison_period_end", benchmarkingKeyDate}}},
	{"testops_metric_correlations", "period_end", []benchmarkingKeyColumn{
		{"metric_name", benchmarkingKeyString}, {"paired_metric_name", benchmarkingKeyString},
		{"scope_type", benchmarkingKeyString}, {"scope_key", benchmarkingKeyString}}},
	{"testops_benchmark_insights", "period_end", []benchmarkingKeyColumn{{"insight_id", benchmarkingKeyString}}},
}

func (table benchmarkingOutputTable) keyList() string {
	names := make([]string, 0, len(table.keys))
	for _, column := range table.keys {
		names = append(names, column.name)
	}
	return strings.Join(names, ", ")
}

// liveKeysQuery reads the keys of one organization and day whose newest row is
// not a retraction, with the newest computed_at of each.
func (table benchmarkingOutputTable) liveKeysQuery() string {
	keys := table.keyList()
	return "SELECT " + keys + ", max(computed_at) FROM " + table.table +
		" WHERE org_id = ? AND " + table.day + " = ?" +
		" GROUP BY " + keys + " HAVING argMax(" + RetractedColumn + ", computed_at) = 0" +
		" ORDER BY " + keys
}

func (table benchmarkingOutputTable) retractionInsert() string {
	return "INSERT INTO " + table.table + " (org_id, " + table.day + ", " + table.keyList() + ", " +
		RetractedColumn + ", computed_at)"
}

type benchmarkingLiveKey struct {
	values []any
	stored time.Time
}

func (table benchmarkingOutputTable) loadLiveKeys(
	ctx context.Context, conn staleKeyConn, organizationID string, day time.Time,
) ([]benchmarkingLiveKey, error) {
	rows, err := conn.Query(ctx, table.liveKeysQuery(), organizationID, day)
	if err != nil {
		return nil, fmt.Errorf("load %s live keys: %w", table.table, err)
	}
	defer rows.Close()
	var live []benchmarkingLiveKey
	for rows.Next() {
		targets := make([]any, 0, len(table.keys)+1)
		for _, column := range table.keys {
			switch column.kind {
			case benchmarkingKeyUInt16:
				targets = append(targets, new(uint16))
			case benchmarkingKeyDate:
				targets = append(targets, new(time.Time))
			default:
				targets = append(targets, new(string))
			}
		}
		var stored time.Time
		targets = append(targets, &stored)
		if err := rows.Scan(targets...); err != nil {
			return nil, fmt.Errorf("scan %s live key: %w", table.table, err)
		}
		values := make([]any, 0, len(table.keys))
		for _, target := range targets[:len(table.keys)] {
			switch typed := target.(type) {
			case *uint16:
				values = append(values, *typed)
			case *time.Time:
				values = append(values, *typed)
			case *string:
				values = append(values, *typed)
			}
		}
		live = append(live, benchmarkingLiveKey{values: values, stored: stored})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate %s live keys: %w", table.table, err)
	}
	return live, nil
}

// benchmarkingRetractionVersion is strictly newer than the stored row: now in
// whole seconds (the column is DateTime), or one second after the stored row
// when now is not later.
func benchmarkingRetractionVersion(now, stored time.Time) time.Time {
	version := now.UTC().Truncate(time.Second)
	if floor := stored.UTC().Truncate(time.Second).Add(time.Second); floor.After(version) {
		version = floor
	}
	return version
}

const benchmarkingEmptyRunRepositoryPresent = "repository_present_at_finalize"

// retireEmptyRun is benchmarking on a run of the whole organization whose
// discovery found no repository. It computes nothing: the compute reads the
// history of a window and keeps going past a failed read, so "retire what was
// not produced" after it would retire on a transient failure and write new
// rows for a repository that is gone. With no repository the organization has
// no benchmark target on the day, so every live key of the day in the six
// output tables gets a retraction row.
//
// The repositories are read again first (the read of the dispatch). A
// repository that is back keeps every row: the family returns an
// emptyRunSkip, counted as skipped_empty_run, and the next run recomputes.
func (executor *BenchmarkingFinalizeExecutor) retireEmptyRun(ctx context.Context, run Run, logger *slog.Logger) (int, error) {
	discoverer, err := NewClickHouseRepositoryDiscoverer(executor.conn)
	if err != nil {
		return 0, fmt.Errorf("%w: %v", errBenchmarkingFinalizeUnavailable, err)
	}
	repositories, err := discoverer.RepositoryIDs(ctx, run.OrganizationID)
	if err != nil {
		return 0, fmt.Errorf("benchmarking: read the repositories of a run with no partition: %w", err)
	}
	day := run.TargetDay.UTC()
	day = time.Date(day.Year(), day.Month(), day.Day(), 0, 0, 0, 0, time.UTC)
	if len(repositories) > 0 {
		logger.Info("benchmarking finalize kept the rows of a run with no partition: a repository is present",
			"run_id", run.ID, "organization_id", run.OrganizationID, "target_day", day.Format("2006-01-02"),
			"partitions_discovered", len(repositories))
		return 0, &emptyRunSkip{reason: benchmarkingEmptyRunRepositoryPresent}
	}
	now := executor.nowUTC()
	written := 0
	for _, table := range benchmarkingOutputTables {
		count, err := executor.retireTable(ctx, table, run.OrganizationID, day, now)
		written += count
		if err != nil {
			if written == 0 {
				return 0, err
			}
			return written, fmt.Errorf("%w: benchmarking retraction failed after %d row(s) may have landed: %w", ErrPartialWrite, written, err)
		}
	}
	logger.Info("benchmarking finalize retired the rows of a run with no repository",
		"run_id", run.ID, "organization_id", run.OrganizationID, "target_day", day.Format("2006-01-02"),
		"rows_retired", written)
	return written, nil
}

func (executor *BenchmarkingFinalizeExecutor) retireTable(
	ctx context.Context, table benchmarkingOutputTable, organizationID string, day, now time.Time,
) (int, error) {
	live, err := table.loadLiveKeys(ctx, executor.conn, organizationID, day)
	if err != nil || len(live) == 0 {
		return 0, err
	}
	batch, err := executor.conn.PrepareBatch(ctx, table.retractionInsert())
	if err != nil {
		return 0, fmt.Errorf("prepare %s retraction batch: %w", table.table, err)
	}
	for _, key := range live {
		values := make([]any, 0, len(key.values)+4)
		values = append(values, organizationID, day)
		values = append(values, key.values...)
		values = append(values, uint8(1), benchmarkingRetractionVersion(now, key.stored))
		if err := batch.Append(values...); err != nil {
			return 0, fmt.Errorf("append %s retraction row: %w", table.table, err)
		}
	}
	// A Send error is ambiguous: the rows may be stored.
	if err := batch.Send(); err != nil {
		return len(live), fmt.Errorf("send %s retraction batch: %w", table.table, err)
	}
	return len(live), nil
}
