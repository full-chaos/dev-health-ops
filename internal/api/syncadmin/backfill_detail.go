package syncadmin

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

// getBackfillJob is sync.py's get_backfill_job: the org's job
// (BackfillJobService.get_job: an id uuid.UUID() refuses raises, the api's
// unhandled 500; none is 404), its linked sync run's counts, and, once the
// job is known to exist and only when the api has a ClickHouse login, the
// metrics diagnostics of its date window.
func (h *handlers) getBackfillJob(w http.ResponseWriter, r *http.Request) {
	id, err := pythonparity.ParseUUID(r.PathValue("job_id"))
	if err != nil {
		h.fail(w, r, "parse_job_id", err)
		return
	}
	ctx := r.Context()
	org := orgID(r)
	job, err := h.store.backfillJobByID(ctx, org, id)
	if err != nil {
		h.fail(w, r, "get_backfill_job", err)
		return
	}
	if job == nil {
		policy.WriteDetail(w, http.StatusNotFound, "Backfill job not found", nil)
		return
	}
	counts, err := h.backfillRunCounts(r, job)
	if err != nil {
		h.fail(w, r, "backfill_run_counts", err)
		return
	}
	out := backfillJobResponse(job, counts)
	if h.diagnostics != nil {
		diagnostics, err := h.diagnostics.backfillDiagnostics(ctx, org, job.SinceDate, job.BeforeDate)
		if err != nil {
			h.fail(w, r, "metrics_diagnostics", err)
			return
		}
		out.Set("metrics_diagnostics", diagnostics)
	}
	policy.WriteJSON(w, http.StatusOK, out, nil)
}

// diagnosticsReader is build_backfill_metrics_diagnostics' ClickHouse
// reads.
type diagnosticsReader interface {
	backfillDiagnostics(ctx context.Context, orgID string, start, end time.Time) (*pyjson.Object, error)
}

// clickhouseDiagnostics reads over the api's own ClickHouse login.
type clickhouseDiagnostics struct{ conn driver.Conn }

// reasonKeys is metrics/compounding_risk.py's reason vocabulary, in
// _REASON_KEYS order.
var reasonKeys = []string{"missing_rework_churn", "missing_complexity_delta", "missing_review_latency", "missing_ownership_signal"}

const rowsPerDayQuery = `
SELECT day, count(DISTINCT repo_id) AS row_count
FROM %s
WHERE org_id = {org_id:String}
  AND day >= {start:Date} AND day <= {end:Date}
GROUP BY day`

const compoundingRiskQuery = `
WITH latest AS (
    SELECT
        day,
        scope,
        scope_id,
        argMax(compounding_risk, computed_at) AS compounding_risk,
        argMax(severity, computed_at) AS severity,
        argMax(rework_churn, computed_at) AS rework_churn,
        argMax(complexity_delta, computed_at) AS complexity_delta,
        argMax(review_latency_p90h, computed_at) AS review_latency_p90h,
        argMax(single_owner_ratio, computed_at) AS single_owner_ratio,
        argMax(ownership_gini, computed_at) AS ownership_gini
    FROM compounding_risk_daily
    WHERE org_id = {org_id:String}
      AND day >= {start:Date} AND day <= {end:Date}
      AND scope = 'repo'
    GROUP BY day, scope, scope_id
)
SELECT
    day,
    count() AS total_rows,
    countIf(compounding_risk IS NOT NULL) AS non_null_rows,
    countIf(severity = 'unknown') AS unknown_rows,
    countIf(rework_churn IS NULL) AS missing_rework_churn,
    countIf(complexity_delta IS NULL) AS missing_complexity_delta,
    countIf(review_latency_p90h IS NULL) AS missing_review_latency,
    countIf(single_owner_ratio IS NULL AND ownership_gini IS NULL) AS missing_ownership_signal
FROM latest
GROUP BY day`

// bucket is one BackfillMetricsDiagnosticsBucket.
type bucket struct {
	metrics, complexity, risk, nonNull, unknown int64
	reasons                                     [4]int64
}

func (b bucket) object() *pyjson.Object {
	out := pyjson.NewObject()
	out.Set("repo_metrics_rows", pyjson.IntOf(b.metrics))
	out.Set("repo_complexity_rows", pyjson.IntOf(b.complexity))
	out.Set("compounding_risk_rows", pyjson.IntOf(b.risk))
	out.Set("compounding_risk_non_null_rows", pyjson.IntOf(b.nonNull))
	out.Set("compounding_risk_unknown_rows", pyjson.IntOf(b.unknown))
	reasons := pyjson.NewObject()
	for index, key := range reasonKeys {
		reasons.Set(key, pyjson.IntOf(b.reasons[index]))
	}
	out.Set("reason_counts", reasons)
	return out
}

func dayKey(at time.Time) string { return at.Format(time.DateOnly) }

// backfillDiagnostics is build_backfill_metrics_diagnostics: repos with
// data per day in repo_metrics_daily and repo_complexity_daily, the latest
// repo-scope compounding-risk row counts and missing-input reasons per day,
// one entry per day of the window (a day with no rows is zeros), and their
// sum.
func (c clickhouseDiagnostics) backfillDiagnostics(ctx context.Context, orgID string, start, end time.Time) (*pyjson.Object, error) {
	params := []any{
		clickhouse.Named("org_id", orgID), clickhouse.Named("start", dayKey(start)), clickhouse.Named("end", dayKey(end)),
	}
	metrics, err := c.rowsPerDay(ctx, "repo_metrics_daily", params)
	if err != nil {
		return nil, err
	}
	complexity, err := c.rowsPerDay(ctx, "repo_complexity_daily", params)
	if err != nil {
		return nil, err
	}
	rows, err := c.conn.Query(ctx, compoundingRiskQuery, params...)
	if err != nil {
		return nil, fmt.Errorf("query compounding_risk_daily diagnostics: %w", err)
	}
	risk := map[string]bucket{}
	for rows.Next() {
		var day time.Time
		var total, nonNull, unknown, rework, complexityDelta, latency, ownership uint64
		if err := rows.Scan(&day, &total, &nonNull, &unknown, &rework, &complexityDelta, &latency, &ownership); err != nil {
			rows.Close()
			return nil, fmt.Errorf("scan compounding_risk_daily diagnostics: %w", err)
		}
		risk[dayKey(day)] = bucket{risk: int64(total), nonNull: int64(nonNull), unknown: int64(unknown),
			reasons: [4]int64{int64(rework), int64(complexityDelta), int64(latency), int64(ownership)}}
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}
	rows.Close()

	return assembleDiagnostics(start, end, metrics, complexity, risk), nil
}

// assembleDiagnostics is build_backfill_metrics_diagnostics after its
// three reads: one entry per day of [start, end] (none when end is before
// start; a day with no rows is zeros), and their sum.
func assembleDiagnostics(start, end time.Time, metrics, complexity map[string]int64, risk map[string]bucket) *pyjson.Object {
	var aggregate bucket
	perDay := []pyjson.Value{}
	for day := start; !day.After(end); day = day.AddDate(0, 0, 1) {
		key := dayKey(day)
		entry := risk[key]
		entry.metrics, entry.complexity = metrics[key], complexity[key]
		aggregate.metrics += entry.metrics
		aggregate.complexity += entry.complexity
		aggregate.risk += entry.risk
		aggregate.nonNull += entry.nonNull
		aggregate.unknown += entry.unknown
		for index := range aggregate.reasons {
			aggregate.reasons[index] += entry.reasons[index]
		}
		// BackfillMetricsDiagnosticsDay: the bucket's fields, then day.
		object := entry.object()
		object.Set("day", key)
		perDay = append(perDay, object)
	}
	out := pyjson.NewObject()
	out.Set("range_start", dayKey(start))
	out.Set("range_end", dayKey(end))
	out.Set("aggregate", aggregate.object())
	out.Set("per_day", perDay)
	return out
}

func (c clickhouseDiagnostics) rowsPerDay(ctx context.Context, table string, params []any) (map[string]int64, error) {
	rows, err := c.conn.Query(ctx, fmt.Sprintf(rowsPerDayQuery, table), params...)
	if err != nil {
		return nil, fmt.Errorf("query %s diagnostics: %w", table, err)
	}
	defer rows.Close()
	out := map[string]int64{}
	for rows.Next() {
		var day time.Time
		var count uint64
		if err := rows.Scan(&day, &count); err != nil {
			return nil, fmt.Errorf("scan %s diagnostics: %w", table, err)
		}
		out[dayKey(day)] = int64(count)
	}
	return out, rows.Err()
}
