package numerical

import (
	"sort"
	"strings"
	"time"
)

// DeployRow mirrors compute_deployments.py's DeploymentRow input shape
// (src/dev_health_ops/metrics/schemas.py:128) for the ONE family
// ComputeDeployMetrics ports: compute_deploy_metrics_daily
// (compute_deployments.py:53, CHAOS-4293). It reuses failedDeploymentStatuses
// (parity.go, DORA's own compute) since both families read the same
// `deployments` table and Python's DEPLOYMENT_FAILURE_STATUSES constant is
// the shared, provider-agnostic source of truth (compute_deployments.py:20)
// -- one Go set instead of two avoids exactly the silent-provider-bias
// divergence that constant's own docstring warns about.
//
// Distinct from the pre-existing Deployment type above (DORA's own input
// shape, job_dora.py -> parity.go): same table, different family, different
// field set -- this one also carries FinishedAt, which DORA's compute never
// reads. Zero time.Time{} is the same "unset" sentinel Deployment already
// uses in this package.
type DeployRow struct {
	RepoID     string
	Status     string
	StartedAt  time.Time
	FinishedAt time.Time
	DeployedAt time.Time
	MergedAt   time.Time
}

// DeployMetric mirrors DeployMetricsDailyRecord (schemas.py:637). The two
// percentile fields are nil exactly when Python's
// `... if durations else None` / `... if lead_times else None` branch would
// leave them None -- an EMPTY durations/lead_times slice, never a zero
// value standing in for "no data".
type DeployMetric struct {
	RepoID                 string
	Day                    time.Time
	DeploymentsCount       int
	FailedDeploymentsCount int
	DeployTimeP50Hours     *float64
	LeadTimeP50Hours       *float64
	ComputedAt             time.Time
}

type deployMetricsBucket struct {
	deployments int
	failed      int
	durations   []float64
	leadTimes   []float64
}

// ComputeDeployMetrics ports compute_deploy_metrics_daily
// (compute_deployments.py:53) byte-for-byte:
//   - deployed_at falls back to started_at when unset (both zero = the row
//     is skipped entirely, never a synthesized "now" value);
//   - the day-window compare is a plain UTC [start, start+24h), NOT the
//     four-way coalesce DORA's own loader query prefilters on
//     (dora_native_clickhouse.go:238) -- that is a DIFFERENT Python
//     function's SQL over the SAME table, and the two are not required to
//     (and in production do not) agree;
//   - a negative duration or lead time (clock skew / bad data) is silently
//     dropped from the percentile input, never clamped to zero -- matching
//     `if duration >= 0` / `if lead_time >= 0` in the Python;
//   - repos are emitted in sorted (ascending, string-compare) repo_id
//     order, matching Python's `sorted(by_repo.items(), key=lambda kv: kv[0])`.
func ComputeDeployMetrics(day time.Time, deployments []DeployRow, computedAt time.Time) []DeployMetric {
	start := time.Date(day.UTC().Year(), day.UTC().Month(), day.UTC().Day(), 0, 0, 0, 0, time.UTC)
	end := start.Add(24 * time.Hour)
	computedAtUTC := computedAt.UTC()

	byRepo := make(map[string]*deployMetricsBucket)
	var repoOrder []string
	for _, row := range deployments {
		deployedAt := row.DeployedAt
		if deployedAt.IsZero() {
			deployedAt = row.StartedAt
		}
		if deployedAt.IsZero() {
			continue
		}
		deployedAt = deployedAt.UTC()
		if deployedAt.Before(start) || !deployedAt.Before(end) {
			continue
		}

		bucket, ok := byRepo[row.RepoID]
		if !ok {
			bucket = &deployMetricsBucket{}
			byRepo[row.RepoID] = bucket
			repoOrder = append(repoOrder, row.RepoID)
		}
		bucket.deployments++
		status := strings.ToLower(strings.TrimSpace(row.Status))
		if _, failed := failedDeploymentStatuses[status]; failed {
			bucket.failed++
		}

		if !row.StartedAt.IsZero() && !row.FinishedAt.IsZero() {
			duration := row.FinishedAt.UTC().Sub(row.StartedAt.UTC()).Hours()
			if duration >= 0 {
				bucket.durations = append(bucket.durations, duration)
			}
		}
		if !row.MergedAt.IsZero() {
			leadTime := deployedAt.Sub(row.MergedAt.UTC()).Hours()
			if leadTime >= 0 {
				bucket.leadTimes = append(bucket.leadTimes, leadTime)
			}
		}
	}

	sort.Strings(repoOrder)
	records := make([]DeployMetric, 0, len(repoOrder))
	for _, repoID := range repoOrder {
		bucket := byRepo[repoID]
		record := DeployMetric{
			RepoID:                 repoID,
			Day:                    start,
			DeploymentsCount:       bucket.deployments,
			FailedDeploymentsCount: bucket.failed,
			ComputedAt:             computedAtUTC,
		}
		if len(bucket.durations) > 0 {
			value := deployPercentile(bucket.durations, 50.0)
			record.DeployTimeP50Hours = &value
		}
		if len(bucket.leadTimes) > 0 {
			value := deployPercentile(bucket.leadTimes, 50.0)
			record.LeadTimeP50Hours = &value
		}
		records = append(records, record)
	}
	return records
}

// deployPercentile ports _percentile (compute_deployments.py:36): linear
// interpolation between the two nearest ranks over a sorted copy of values,
// clamped at the ends exactly like the Python (percentile<=0 -> min,
// percentile>=100 -> max).
func deployPercentile(values []float64, percentile float64) float64 {
	if len(values) == 0 {
		return 0
	}
	if percentile <= 0 {
		return deploySliceMin(values)
	}
	if percentile >= 100 {
		return deploySliceMax(values)
	}
	sorted := append([]float64(nil), values...)
	sort.Float64s(sorted)
	if len(sorted) == 1 {
		return sorted[0]
	}
	rank := float64(len(sorted)-1) * (percentile / 100.0)
	lo := int(rank)
	hi := lo + 1
	if hi > len(sorted)-1 {
		hi = len(sorted) - 1
	}
	frac := rank - float64(lo)
	return sorted[lo]*(1-frac) + sorted[hi]*frac
}

func deploySliceMin(values []float64) float64 {
	m := values[0]
	for _, v := range values[1:] {
		if v < m {
			m = v
		}
	}
	return m
}

func deploySliceMax(values []float64) float64 {
	m := values[0]
	for _, v := range values[1:] {
		if v > m {
			m = v
		}
	}
	return m
}
