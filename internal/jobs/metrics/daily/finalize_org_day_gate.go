package daily

import (
	"context"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

const (
	// FinalizeOrgDayDeferredCounter counts finalize claims refused because another run of the same organization and
	// day holds a live finalize lease.
	FinalizeOrgDayDeferredCounter = "daily_finalize_org_day_deferred_total"
	// FinalizeOrgDayDeferHistogram records, in seconds, how long a refused claim is told to wait.
	FinalizeOrgDayDeferHistogram = "daily_finalize_org_day_defer_seconds"

	// finalizeOrgDayMaxWait bounds a refused claim's snooze: the other run usually finishes well before its lease
	// ends, so waiting out the whole lease would idle the day.
	finalizeOrgDayMaxWait = 30 * time.Second
	finalizeOrgDayMinWait = time.Second
)

// deferToOtherRunOfOrgDay refuses a finalize claim while ANOTHER run of the same organization and target day holds a
// live finalize lease: the finalize families of a day write the same keys, and overlapping computes tie on computed_at
// (CHAOS-9945). The lease is the lock: it is taken by one short statement, renewed by the holder's lease renewal, and
// its expiry is how a lost holder stops blocking the day. No connection is held during the compute.
//
// A transaction-scoped advisory lock on (organization, day), held for the length of the claim transaction only, makes the
// check and the claim one step: two runs cannot both see no holder and both claim. It returns nil when no other run holds
// the day.
func (store *PostgresStore) deferToOtherRunOfOrgDay(
	ctx context.Context, tx pgx.Tx, runID, orgID string, targetDay, now time.Time,
) (*LeaseActiveError, error) {
	day := targetDay.UTC().Format("2006-01-02")
	if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))",
		"daily_finalize_org_day:"+orgID+":"+day); err != nil {
		return nil, ErrUnavailable
	}
	var holderID string
	var holderLeaseEnds time.Time
	err := tx.QueryRow(ctx, `
SELECT other.id::text, other.finalization_lease_expires_at
FROM public.daily_metrics_runs AS other
WHERE other.org_id = $1::uuid AND other.target_day = $2::date AND other.id <> $3::uuid
  AND other.status = 'running' AND other.finalization_status = 'running'
  AND other.finalization_lease_expires_at > $4
ORDER BY other.finalization_lease_expires_at DESC
LIMIT 1`, orgID, day, runID, now).Scan(&holderID, &holderLeaseEnds)
	if err == pgx.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, ErrUnavailable
	}
	wait := holderLeaseEnds.Sub(now)
	if wait > finalizeOrgDayMaxWait {
		wait = finalizeOrgDayMaxWait
	}
	if wait < finalizeOrgDayMinWait {
		wait = finalizeOrgDayMinWait
	}
	slog.WarnContext(ctx, "daily finalize deferred: another run of the same organization and day holds the finalize lease",
		"organization_id", orgID, "day", day, "run_id", runID, "holder_run_id", holderID, "retry_after", wait)
	meter := otel.GetMeterProvider().Meter("github.com/full-chaos/dev-health-ops/internal/jobs/metrics/daily")
	if counter, counterErr := meter.Int64Counter(FinalizeOrgDayDeferredCounter, metric.WithUnit("{claim}"),
		metric.WithDescription("finalize claims refused because another run of the same organization and day holds a live finalize lease")); counterErr == nil {
		counter.Add(context.WithoutCancel(ctx), 1)
	}
	if histogram, histogramErr := meter.Float64Histogram(FinalizeOrgDayDeferHistogram, metric.WithUnit("s"),
		metric.WithDescription("seconds a refused finalize claim is told to wait for the run that holds its organization-day")); histogramErr == nil {
		histogram.Record(context.WithoutCancel(ctx), wait.Seconds(), metric.WithAttributes(attribute.String("outcome", "deferred")))
	}
	return &LeaseActiveError{RetryAfter: wait}, nil
}
