package remaining

import (
	"context"
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

// ReleaseImpactReader owns the ClickHouse side of the family.
type ReleaseImpactReader struct {
	conn driver.Conn
}

// NewReleaseImpactReader builds a reader over an existing connection.
func NewReleaseImpactReader(conn driver.Conn) *ReleaseImpactReader {
	return &ReleaseImpactReader{conn: conn}
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
		namedArguments(map[string]any{"org_id": orgID, "day": day})...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

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
		namedArguments(map[string]any{"org_id": orgID, "day": day})...).Scan(&cnt)
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
		// No row is not an error condition in Python -- it returns None.
		return nil, nil //nolint:nilerr // absence is a value here, matching _get_deploy_timestamp
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
			"signal_pattern": signalPattern, "window_start": windowStart, "window_end": windowEnd,
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
			"deploy_ts": deployTS, "spike_end": spikeEnd,
		})...).Scan(&first)
	if err != nil {
		return nil, nil //nolint:nilerr // absence is a value here, matching Python's None
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
			"window_start": windowStart, "window_end": windowEnd, "org_id": orgID,
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
			"environment": environment, "day": day,
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
		return "", nil //nolint:nilerr // absence is a value here, matching Python's None
	}
	return repoID, nil
}
