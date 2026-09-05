package remaining

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// ClickHouse readers for release_impact (CHAOS-4296), ported query-for-query
// from src/dev_health_ops/metrics/release_impact.py.
//
// EVERY query here is org-scoped, and that is load-bearing rather than
// defensive: release_ref + environment is NOT globally unique, so an unscoped
// read can return another tenant's deploy timestamp, repo_id or concurrent
// deploys, and write it into this org's surface-readable row (CHAOS-2381 /
// CHAOS-2397). The Python carries that reasoning in four separate docstrings;
// it is stated once here and must survive any query edit.

// dateArgument renders a Date literal the same way dateTime64Argument
// (dora_native_clickhouse.go) renders a DateTime64 one: as a plain string,
// never a bound time.Time. clickhouse-go renders a time.Time value bound to a
// {name:Date} placeholder as `toDateTime('2026-08-20 00:00:00')`, which
// ClickHouse then refuses to parse AS a Date (CANNOT_PARSE_DATE) -- measured
// live via TestReleaseImpactParityClassBattery, the same class of defect
// dora_native_clickhouse.go's comment documents for DateTime64. Every {*:Date}
// and {*:DateTime64(...)} parameter in this file must go through this or
// dateTime64Argument, never a raw time.Time.
func dateArgument(value time.Time) string {
	return value.UTC().Format("2006-01-02")
}

// ErrReleaseImpactSchemaIncompatible reports a deployed schema this executor
// cannot write against safely.
var ErrReleaseImpactSchemaIncompatible = errors.New(
	"deployed release_impact schema does not support what this executor writes")

// releaseImpactTableEngine reports the engine backing one table, the same way
// capacityTableEngine (capacity_native_clickhouse.go) does: system.tables
// rather than SHOW CREATE TABLE, since the engine column is already the bare
// family name and needs no DDL parsing.
func releaseImpactTableEngine(ctx context.Context, conn driver.Conn, table string) (string, error) {
	var engine string
	if err := conn.QueryRow(ctx, `
SELECT engine FROM system.tables
WHERE database = currentDatabase() AND name = {table:String}`,
		namedArguments(map[string]any{"table": table})...).Scan(&engine); err != nil {
		return "", fmt.Errorf("inspect %s engine: %w", table, err)
	}
	return engine, nil
}

// releaseImpactExpectedSortingKey is migration 034's ORDER BY for
// release_impact_daily, unchanged by migration 088 (088 converts the ENGINE
// only; the shadow-table rewrite explicitly asserts the sorting key survives
// the swap unchanged -- see the Python migration's own shadow_key != old_key
// check). A table with ANY other sorting key is a different table shape this
// executor's writer was never designed against, regardless of what engine it
// reports.
const releaseImpactExpectedSortingKey = "org_id, release_ref, environment, day"

// releaseImpactTableEngineFull reports the FULL engine expression (including
// constructor arguments, e.g. "ReplacingMergeTree(computed_at)") -- unlike
// releaseImpactTableEngine's system.tables.engine, which is only ever the
// bare family name and cannot reveal the version column.
func releaseImpactTableEngineFull(ctx context.Context, conn driver.Conn, table string) (string, error) {
	var engineFull string
	if err := conn.QueryRow(ctx, `
SELECT engine_full FROM system.tables
WHERE database = currentDatabase() AND name = {table:String}`,
		namedArguments(map[string]any{"table": table})...).Scan(&engineFull); err != nil {
		return "", fmt.Errorf("inspect %s engine_full: %w", table, err)
	}
	return engineFull, nil
}

// releaseImpactSortingKey reports a table's sorting key, normalized the same
// way dora_native_clickhouse.go's classifySortingKey does (backticks and
// ClickHouse's own rendering variance stripped) -- ORDER MATTERS and is not
// sorted away, matching classifySortingKey's own reasoning: a differently
// ORDERED sorting key is a different table, not an equivalent one.
func releaseImpactSortingKey(ctx context.Context, conn driver.Conn, table string) (string, error) {
	var sortingKey string
	if err := conn.QueryRow(ctx, `
SELECT sorting_key FROM system.tables
WHERE database = currentDatabase() AND name = {table:String}`,
		namedArguments(map[string]any{"table": table})...).Scan(&sortingKey); err != nil {
		return "", fmt.Errorf("inspect %s sorting_key: %w", table, err)
	}
	normalized := strings.Join(strings.Fields(strings.ReplaceAll(sortingKey, "`", " ")), " ")
	return strings.ReplaceAll(normalized, " ,", ","), nil
}

// verifyReleaseImpactSchema is checked at CONSTRUCTION, matching the sibling
// native executors' discipline (dora/capacity): a database this code cannot
// write against safely refuses the kind once and loudly, rather than
// recomputing partitions job after job that quietly double (triple, ...)
// every metric, or -- codex r3's finding (CHAOS-4296/#2262) -- silently
// collapse DISTINCT rows into each other under a wrong sorting key or version
// column.
//
// Migration 088 converts release_impact_daily from MergeTree to
// ReplacingMergeTree(computed_at) specifically so a RECOMPUTED day's rows
// collapse to the latest version instead of accumulating -- this executor's
// day loop (ComputePartition -> RecomputationWindow) deliberately
// re-processes the same (org_id, release_ref, environment, day) on every run
// within the recomputation window, by design (CHAOS-4258's degraded-signal
// window needs the re-read). On a stale plain-MergeTree table, nothing else
// in this file's write path (writeReleaseImpactRows does a plain INSERT, no
// reader applies FINAL/argMax to release_impact_daily itself -- that table is
// write-only from this executor's own perspective) would ever catch or
// collapse the resulting duplicates. Checking ONLY the engine family name
// (codex r2's fix) is not enough on its own, per r3: a table converted to
// `ReplacingMergeTree()` with the WRONG version column, or with a sorting key
// that does not include environment/day, would pass an engine-only check
// while silently collapsing DISTINCT rows -- exactly the failure mode
// dora_native_clickhouse.go's classifySortingKey exists to catch for its own
// table, applied here to release_impact_daily.
func verifyReleaseImpactSchema(ctx context.Context, conn driver.Conn) error {
	const table = "release_impact_daily"
	engine, err := releaseImpactTableEngine(ctx, conn, table)
	if err != nil {
		return err
	}
	if !strings.Contains(engine, "ReplacingMergeTree") {
		return fmt.Errorf(
			"%w: %s is %s, expected ReplacingMergeTree -- migration 088 has "+
				"not been applied, and every recomputed day would append "+
				"duplicate rows instead of collapsing them",
			ErrReleaseImpactSchemaIncompatible, table, engine)
	}

	engineFull, err := releaseImpactTableEngineFull(ctx, conn, table)
	if err != nil {
		return err
	}
	if !strings.Contains(engineFull, "ReplacingMergeTree(computed_at)") &&
		!strings.Contains(engineFull, "ReplacingMergeTree(`computed_at`)") {
		return fmt.Errorf(
			"%w: %s is %s, expected the ReplacingMergeTree version column to "+
				"be computed_at -- a different (or missing) version column "+
				"collapses rows by the WRONG field, silently discarding "+
				"distinct metric rows instead of superseding stale ones",
			ErrReleaseImpactSchemaIncompatible, table, engineFull)
	}

	sortingKey, err := releaseImpactSortingKey(ctx, conn, table)
	if err != nil {
		return err
	}
	if sortingKey != releaseImpactExpectedSortingKey {
		return fmt.Errorf(
			"%w: %s is ordered by (%s), expected (%s) -- refusing rather "+
				"than guessing, because a shorter or reordered sorting key "+
				"collapses distinct (org_id, release_ref, environment, day) "+
				"rows into each other",
			ErrReleaseImpactSchemaIncompatible, table, sortingKey,
			releaseImpactExpectedSortingKey)
	}
	return nil
}

// ReleaseImpactReader owns the ClickHouse side of the family.
type ReleaseImpactReader struct {
	conn   driver.Conn
	logger *slog.Logger
}

// NewReleaseImpactReader builds a reader over an existing connection. logger
// is optional; when set, an absence-tolerant reader (DeployTimestamp,
// FirstFrictionSpike, RepoIDForRelease) logs any error OTHER than
// sql.ErrNoRows before folding it into the "no row" return -- codex r1 finding
// 2 (CHAOS-4296/#2262): the original `if err != nil { return nil, nil }` shape
// could not tell "no matching row" (Python's None, silent) apart from a real
// query/scan failure (a dropped connection, a malformed result), so a genuine
// failure read as an ordinary absence with nothing in the logs to find it by.
func NewReleaseImpactReader(conn driver.Conn, logger *slog.Logger) *ReleaseImpactReader {
	return &ReleaseImpactReader{conn: conn, logger: logger}
}

// logNonAbsenceError logs err with the caller's identifiers UNLESS it is
// sql.ErrNoRows (the expected "no matching row" case, which every caller
// below folds into its Python-matching nil/zero-value return). Returns true
// when err is the genuine failure case, so callers can decide whether to
// still propagate it.
func (r *ReleaseImpactReader) logNonAbsenceError(err error, msg string, args ...any) bool {
	if errors.Is(err, sql.ErrNoRows) {
		return false
	}
	if r.logger != nil {
		r.logger.Error(msg, append(args, "error", err)...)
	}
	return true
}

// ReleaseEnvPair is one (release_ref, environment) scope entry.
type ReleaseEnvPair struct {
	ReleaseRef  string
	Environment string
}

// FindReleaseEnvPairs ports _find_release_env_pairs (release_impact.py:125-137).
//
// SCOPE COMES FROM TELEMETRY ONLY -- deliberately unchanged in PR1. CHAOS-4258
// observes that this makes "no releases happened" and "telemetry ingestion is
// broken" indistinguishable, both reporting rows_written=0. PR1 keeps scope
// derivation identical to Python so the parity oracle stays exact, and reports
// the divergence through DeploymentsWithoutTelemetry below instead of by
// inventing rows. Changing THIS function's result set is a row-semantics change
// and belongs to the follow-up ticket, not here.
func (r *ReleaseImpactReader) FindReleaseEnvPairs(
	ctx context.Context, orgID string, day time.Time,
) ([]ReleaseEnvPair, error) {
	rows, err := r.conn.Query(ctx, `
SELECT DISTINCT release_ref, environment
FROM telemetry_signal_bucket
WHERE org_id = {org_id:String}
  AND toDate(bucket_start) = {day:Date}
  AND release_ref != ''
ORDER BY release_ref, environment`,
		namedArguments(map[string]any{"org_id": orgID, "day": dateArgument(day)})...)
	if err != nil {
		return nil, err
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil && r.logger != nil {
			r.logger.Error("release_impact: FindReleaseEnvPairs rows.Close failed",
				"org_id", orgID, "day", day.Format("2006-01-02"), "error", closeErr)
		}
	}()

	var out []ReleaseEnvPair
	for rows.Next() {
		var p ReleaseEnvPair
		if err := rows.Scan(&p.ReleaseRef, &p.Environment); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

// CountTotalReleases ports _count_total_releases (release_impact.py:140-161):
// the coverage_ratio DENOMINATOR, distinct release_refs deployed that day.
func (r *ReleaseImpactReader) CountTotalReleases(
	ctx context.Context, orgID string, day time.Time,
) (int, error) {
	var cnt uint64
	err := r.conn.QueryRow(ctx, `
SELECT count(DISTINCT release_ref) AS cnt
FROM deployments
WHERE release_ref != ''
  AND toDate(coalesce(deployed_at, started_at)) = {day:Date}
  AND org_id = {org_id:String}`,
		namedArguments(map[string]any{"org_id": orgID, "day": dateArgument(day)})...).Scan(&cnt)
	if err != nil {
		return 0, err
	}
	return int(cnt), nil
}

// DeploymentsWithoutTelemetry answers CHAOS-4258's question directly: were
// there deployments on this day, when telemetry produced no scope at all?
//
// This is the DEGRADED SIGNAL input. It adds no rows and changes no existing
// value -- the caller emits a counter and a structured log when this returns
// >0 alongside an empty scope. That combination is the state CHAOS-4258
// describes: a real deployment whose telemetry never arrived, previously
// reported as an ordinary zero-row success and indistinguishable from a day
// with no releases.
//
// MEASURED on the local stack 2026-09-05: 29 deployments in the last 7 days
// against 0 telemetry rows, i.e. this returns 29 where scope is empty.
func (r *ReleaseImpactReader) DeploymentsWithoutTelemetry(
	ctx context.Context, orgID string, day time.Time,
) (int, error) {
	return r.CountTotalReleases(ctx, orgID, day)
}

// DeployTimestamp ports _get_deploy_timestamp (release_impact.py:164-197).
// Returns nil where Python returns None.
func (r *ReleaseImpactReader) DeployTimestamp(
	ctx context.Context, orgID, releaseRef, environment string,
) (*time.Time, error) {
	var ts *time.Time
	err := r.conn.QueryRow(ctx, `
SELECT coalesce(deployed_at, started_at) AS deploy_ts
FROM deployments
WHERE release_ref = {release_ref:String}
  AND environment = {environment:String}
  AND org_id = {org_id:String}
ORDER BY deploy_ts DESC
LIMIT 1`,
		namedArguments(map[string]any{
			"release_ref": releaseRef, "environment": environment, "org_id": orgID,
		})...).Scan(&ts)
	if err != nil {
		if r.logNonAbsenceError(err, "release_impact: DeployTimestamp query/scan failed",
			"org_id", orgID, "release_ref", releaseRef, "environment", environment) {
			return nil, err
		}
		// No row is not an error condition in Python -- it returns None.
		return nil, nil
	}
	return ts, nil
}

// SignalRateRaw ports _signal_rate's QUERY half (release_impact.py:216-247).
// The null semantics live in NewSignalRate so they are unit-testable without
// a database; this returns the raw sums plus whether any row came back.
func (r *ReleaseImpactReader) SignalRateRaw(
	ctx context.Context, orgID, releaseRef, environment, signalPattern string,
	windowStart, windowEnd time.Time,
) (totalSignals, totalSessions int, hadRows bool, err error) {
	var sig, sess *uint64
	err = r.conn.QueryRow(ctx, `
SELECT sum(signal_count) AS total_signals,
       sum(session_count) AS total_sessions
FROM telemetry_signal_bucket
WHERE org_id = {org_id:String}
  AND release_ref = {release_ref:String}
  AND environment = {environment:String}
  AND signal_type LIKE {signal_pattern:String}
  AND bucket_start >= {window_start:DateTime64(3)}
  AND bucket_end <= {window_end:DateTime64(3)}`,
		namedArguments(map[string]any{
			"org_id": orgID, "release_ref": releaseRef, "environment": environment,
			"signal_pattern": signalPattern,
			"window_start":   dateTime64Argument(windowStart, millisecondPrecision),
			"window_end":     dateTime64Argument(windowEnd, millisecondPrecision),
		})...).Scan(&sig, &sess)
	if err != nil {
		return 0, 0, false, err
	}
	if sig == nil && sess == nil {
		return 0, 0, false, nil
	}
	if sig != nil {
		totalSignals = int(*sig)
	}
	if sess != nil {
		totalSessions = int(*sess)
	}
	return totalSignals, totalSessions, true, nil
}

// FirstFrictionSpike ports _time_to_first_friction_spike's QUERY half
// (release_impact.py:317-341). Returns nil when no spike row exists.
func (r *ReleaseImpactReader) FirstFrictionSpike(
	ctx context.Context, orgID, releaseRef, environment string,
	deployTS, spikeEnd time.Time,
) (*time.Time, error) {
	var first *time.Time
	err := r.conn.QueryRow(ctx, `
SELECT min(bucket_start) AS first_friction_ts
FROM telemetry_signal_bucket
WHERE org_id = {org_id:String}
  AND release_ref = {release_ref:String}
  AND environment = {environment:String}
  AND signal_type LIKE 'friction.%'
  AND bucket_start >= {deploy_ts:DateTime64(3)}
  AND bucket_start <= {spike_end:DateTime64(3)}
  AND signal_count > 0`,
		namedArguments(map[string]any{
			"org_id": orgID, "release_ref": releaseRef, "environment": environment,
			"deploy_ts": dateTime64Argument(deployTS, millisecondPrecision),
			"spike_end": dateTime64Argument(spikeEnd, millisecondPrecision),
		})...).Scan(&first)
	if err != nil {
		if r.logNonAbsenceError(err, "release_impact: FirstFrictionSpike query/scan failed",
			"org_id", orgID, "release_ref", releaseRef, "environment", environment) {
			return nil, err
		}
		return nil, nil // absence is a value here, matching Python's None
	}
	return first, nil
}

// ConcurrentDeployCount ports _concurrent_deploy_count (release_impact.py:356-394).
func (r *ReleaseImpactReader) ConcurrentDeployCount(
	ctx context.Context, orgID, releaseRef, environment string,
	windowStart, windowEnd time.Time,
) (int, error) {
	var cnt uint64
	err := r.conn.QueryRow(ctx, `
SELECT count(DISTINCT release_ref) AS cnt
FROM deployments
WHERE environment = {environment:String}
  AND release_ref != {release_ref:String}
  AND release_ref != ''
  AND coalesce(deployed_at, started_at) >= {window_start:DateTime64(3)}
  AND coalesce(deployed_at, started_at) <= {window_end:DateTime64(3)}
  AND org_id = {org_id:String}`,
		namedArguments(map[string]any{
			"environment": environment, "release_ref": releaseRef,
			"window_start": dateTime64Argument(windowStart, millisecondPrecision),
			"window_end":   dateTime64Argument(windowEnd, millisecondPrecision),
			"org_id":       orgID,
		})...).Scan(&cnt)
	if err != nil {
		return 0, err
	}
	return int(cnt), nil
}

// BucketHours ports _data_completeness's QUERY half (release_impact.py:412-424):
// distinct hourly buckets present for this release on this day.
func (r *ReleaseImpactReader) BucketHours(
	ctx context.Context, orgID, releaseRef, environment string, day time.Time,
) (int, error) {
	var hours uint64
	err := r.conn.QueryRow(ctx, `
SELECT count(DISTINCT toStartOfHour(bucket_start)) AS bucket_hours
FROM telemetry_signal_bucket
WHERE org_id = {org_id:String}
  AND release_ref = {release_ref:String}
  AND environment = {environment:String}
  AND toDate(bucket_start) = {day:Date}`,
		namedArguments(map[string]any{
			"org_id": orgID, "release_ref": releaseRef,
			"environment": environment, "day": dateArgument(day),
		})...).Scan(&hours)
	if err != nil {
		return 0, err
	}
	return int(hours), nil
}

// RepoIDForRelease ports _get_repo_id_for_release (release_impact.py:587-620).
func (r *ReleaseImpactReader) RepoIDForRelease(
	ctx context.Context, orgID, releaseRef, environment string,
) (string, error) {
	var repoID string
	err := r.conn.QueryRow(ctx, `
SELECT repo_id
FROM deployments
WHERE release_ref = {release_ref:String}
  AND environment = {environment:String}
  AND org_id = {org_id:String}
ORDER BY coalesce(deployed_at, started_at) DESC
LIMIT 1`,
		namedArguments(map[string]any{
			"release_ref": releaseRef, "environment": environment, "org_id": orgID,
		})...).Scan(&repoID)
	if err != nil {
		if r.logNonAbsenceError(err, "release_impact: RepoIDForRelease query/scan failed",
			"org_id", orgID, "release_ref", releaseRef, "environment", environment) {
			return "", err
		}
		return "", nil // absence is a value here, matching Python's None
	}
	return repoID, nil
}

// releaseImpactRow is one release_impact_daily row, matching migration 034's
// column list and write_release_impact_daily's serialization (ci.py:380-407)
// exactly -- including which fields are always non-nil (confidence, coverage,
// data_completeness, missing_required_fields_count, concurrent_deploy_count)
// versus genuinely nullable (the delta/rate fields, time-to-first-issue, every
// flag_* column, which PR1 does not populate).
type releaseImpactRow struct {
	OrgID                 string
	Day                   time.Time
	ReleaseRef            string
	Environment           string
	RepoID                string // "" when unknown, matching Python's str(repo_id) if repo_id else ""
	FrictionDelta         *float64
	PostFrictionRate      *float64
	ErrorDelta            *float64
	PostErrorRate         *float64
	TimeToFirstIssue      *float64
	Confidence            float64
	CoverageRatioTop      float64 // release_impact_coverage_ratio (Python passes coverage_ratio to both columns)
	MissingRequiredFields uint32
	DataCompleteness      float64
	ConcurrentDeployCount uint32
	ComputedAt            time.Time
}

// writeReleaseImpactRows ports write_release_impact_daily's INSERT (ci.py:375-440).
// flag_*, issue_to_release_impact_link_rate stay NULL (out of PR1 scope);
// rollback_or_disable_after_impact_spike stays 0 -- both match ci.py's current
// behaviour verbatim (release_impact.py:558-576), not a Go-side simplification.
func (r *ReleaseImpactReader) writeReleaseImpactRows(
	ctx context.Context, rows []releaseImpactRow,
) (int, error) {
	if len(rows) == 0 {
		return 0, nil
	}
	batch, err := r.conn.PrepareBatch(ctx, `INSERT INTO release_impact_daily (
org_id, day, release_ref, environment, repo_id,
release_user_friction_delta, release_post_friction_rate,
release_error_rate_delta, release_post_error_rate,
time_to_first_user_issue_after_release,
release_impact_confidence_score, release_impact_coverage_ratio,
flag_exposure_rate, flag_activation_rate, flag_reliability_guardrail,
flag_friction_delta, flag_rollout_half_life, flag_churn_rate,
issue_to_release_impact_link_rate,
rollback_or_disable_after_impact_spike, coverage_ratio,
missing_required_fields_count, instrumentation_change_flag,
data_completeness, concurrent_deploy_count, computed_at)`)
	if err != nil {
		return 0, err
	}
	for _, row := range rows {
		// release_impact_confidence_score, release_impact_coverage_ratio,
		// coverage_ratio and data_completeness are Float32 columns (migration
		// 034) even though the compute side is deliberately float64 all the
		// way through (ComputeConfidence's FMA-barrier discipline needs the
		// full float64 path to match CPython bit-for-bit) -- the narrowing to
		// float32 happens ONLY here, at the write boundary, matching what
		// clickhouse_connect does implicitly for the Python writer.
		if err := batch.Append(
			row.OrgID, row.Day, row.ReleaseRef, row.Environment, row.RepoID,
			row.FrictionDelta, row.PostFrictionRate,
			row.ErrorDelta, row.PostErrorRate,
			row.TimeToFirstIssue,
			float32(row.Confidence), float32(row.CoverageRatioTop),
			(*float64)(nil), (*float64)(nil), (*float64)(nil),
			(*float64)(nil), (*float64)(nil), (*float64)(nil),
			(*float64)(nil),
			uint32(0), float32(row.CoverageRatioTop),
			row.MissingRequiredFields, uint8(0),
			float32(row.DataCompleteness), row.ConcurrentDeployCount, row.ComputedAt,
		); err != nil {
			return 0, err
		}
	}
	if err := batch.Send(); err != nil {
		return 0, err
	}
	return len(rows), nil
}
