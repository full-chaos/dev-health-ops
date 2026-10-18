package syncreconciler

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// StrandedKindCount is the number of deliveries of one kind that sit at another route generation than the route's,
// with a missing or terminal River job, on a run that is not terminal. They are counted, never re-armed: the re-arm
// rule of each kind is its own decision.
type StrandedKindCount struct {
	Kind     string `json:"kind"`
	Stranded int    `json:"stranded"`
}

// GenerationReconcileReport is what a route command prints about the deliveries its generation change left behind.
type GenerationReconcileReport struct {
	Kind             string              `json:"kind"`
	Rearmed          int                 `json:"rearmed"`
	SkippedNotReady  int                 `json:"skipped_not_ready"`
	SkippedLiveJob   int                 `json:"skipped_live_job"`
	SkippedLockedJob int                 `json:"skipped_locked_job"`
	OtherKinds       []StrandedKindCount `json:"other_kinds"`
}

// GenerationReconcileCounter counts what a route command did to deliveries at another generation, by kind and action
// (rearmed, counted_not_rearmed).
const GenerationReconcileCounter = "sync_route_generation_stranded_total"

// countOtherGenerationStrandedSQL counts, per kind other than finalize_sync_run, the deliveries a generation change
// can strand: dispatched through river at a generation other than the route's, on a route that is river and not
// paused, with a river job that is missing or terminal, on a run that is not terminal.
const countOtherGenerationStrandedSQL = `
SELECT outbox.kind, count(*)
FROM public.sync_dispatch_outbox AS outbox
JOIN public.sync_runs AS run ON run.id=outbox.sync_run_id
JOIN public.sync_dispatch_transport_routes AS route ON route.kind=outbox.kind
LEFT JOIN %s AS job ON job.id=CASE
    WHEN outbox.transport_job_id ~ '^[1-9][0-9]{0,17}$' THEN outbox.transport_job_id::bigint END
WHERE outbox.kind <> 'finalize_sync_run'
    AND outbox.status='dispatched'
    AND outbox.dispatched_transport='river'
    AND outbox.dispatched_route_generation IS DISTINCT FROM route.generation
    AND route.transport='river' AND NOT route.paused
    AND ` + nonterminalSyncRunStatusPredicate + `
    AND (job.id IS NULL OR job.state IN ('completed','discarded','cancelled'))
GROUP BY outbox.kind
ORDER BY outbox.kind`

// countOtherGenerationLiveFinalizeSQL counts the finalize deliveries at another generation that the reconcile must
// not touch because their River job is live (available, pending, retryable, running, scheduled). They are candidates of
// no re-arm, so without this count an operator could not see them.
const countOtherGenerationLiveFinalizeSQL = `
SELECT count(*)
FROM public.sync_dispatch_outbox AS outbox
JOIN public.sync_runs AS run ON run.id=outbox.sync_run_id
JOIN public.sync_dispatch_transport_routes AS route ON route.kind=outbox.kind
JOIN %s AS job ON job.id=CASE
    WHEN outbox.transport_job_id ~ '^[1-9][0-9]{0,17}$' THEN outbox.transport_job_id::bigint END
WHERE outbox.kind='finalize_sync_run'
    AND outbox.status='dispatched'
    AND outbox.dispatched_transport='river'
    AND outbox.dispatched_route_generation IS DISTINCT FROM route.generation
    AND route.transport='river' AND NOT route.paused
    AND ` + nonterminalSyncRunStatusPredicate + `
    AND job.state NOT IN ('completed','discarded','cancelled')`

// ReconcileOtherGenerations is run by the route commands (apply, pause, resume) after they changed a route's
// generation, so the command does not leave deliveries of the old generation behind (chris, CHAOS-9964: "A command
// shouldn't leave garbage around."). For finalize_sync_run it re-arms every delivery dispatched at another generation
// whose River job is missing or completed and whose run is finalize-ready, with the guards and the code of the
// scheduled ready-finalize repair; only the equality of the delivery's generation with the route's is dropped, and
// only here: the scheduled repair keeps it (CHAOS-5462). A live job is never touched. Deliveries of the other kinds
// are counted and warned about, not re-armed.
func (repair *TerminalDeliveryRepair) ReconcileOtherGenerations(
	ctx context.Context, now time.Time, kind string,
) (GenerationReconcileReport, error) {
	report := GenerationReconcileReport{Kind: kind, OtherKinds: []StrandedKindCount{}}
	if repair == nil || repair.begin == nil || ctx == nil || now.IsZero() {
		return report, ErrInvalidConfiguration
	}
	meter := otel.GetMeterProvider().Meter("github.com/full-chaos/dev-health-ops/internal/syncreconciler")
	counter, counterErr := meter.Int64Counter(GenerationReconcileCounter, metric.WithUnit("{delivery}"),
		metric.WithDescription("deliveries left at another route generation that a route command handled, by kind and action"))
	record := func(kindName, action string, count int) {
		if counterErr == nil && count > 0 {
			counter.Add(ctx, int64(count), metric.WithAttributes(attribute.String("kind", kindName), attribute.String("action", action)))
		}
	}

	if kind == "finalize_sync_run" {
		// The walk is by keyset (dispatched_at, id): a page of deliveries it skips never hides the ones behind it, and
		// every delivery is seen once, so the skip counts add up. There is no pass ceiling. A cancelled context fails the
		// next pass.
		cursor := &finalizeCursor{at: time.Unix(0, 0).UTC(), id: "00000000-0000-0000-0000-000000000000"}
		for {
			outcome, err := repair.reconcileFinalizePass(ctx, now, cursor)
			if err != nil {
				return report, err
			}
			report.Rearmed += outcome.Recovered
			report.SkippedNotReady += outcome.SkippedNotReady
			report.SkippedLiveJob += outcome.SkippedLiveJob
			report.SkippedLockedJob += outcome.SkippedLockedJob
			if outcome.Candidates < maximumStepLimit {
				break
			}
			cursor = &finalizeCursor{at: outcome.LastDispatchedAt, id: outcome.LastID}
		}
		live, err := repair.countOtherGenerationLive(ctx)
		if err != nil {
			return report, err
		}
		report.SkippedLiveJob += live
		record(kind, "rearmed", report.Rearmed)
		record(kind, "skipped_live_job", report.SkippedLiveJob)
		record(kind, "skipped_not_ready", report.SkippedNotReady)
		record(kind, "skipped_locked_job", report.SkippedLockedJob)
		slog.InfoContext(ctx, "syncreconciler.generation_reconcile",
			"kind", kind, "rearmed", report.Rearmed, "skipped_not_ready", report.SkippedNotReady,
			"skipped_live_job", report.SkippedLiveJob, "skipped_locked_job", report.SkippedLockedJob)
	}

	tx, err := repair.begin(ctx)
	if err != nil || tx == nil {
		return report, ErrUnavailable
	}
	defer func() { _ = tx.Rollback(ctx) }()
	rows, err := tx.Query(ctx, repair.otherGenerationCountQuery)
	if err != nil {
		return report, fmt.Errorf("count deliveries at another generation: %w", ErrUnavailable)
	}
	for rows.Next() {
		var count StrandedKindCount
		if err := rows.Scan(&count.Kind, &count.Stranded); err != nil {
			rows.Close()
			return report, fmt.Errorf("count deliveries at another generation: %w", ErrUnavailable)
		}
		report.OtherKinds = append(report.OtherKinds, count)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return report, fmt.Errorf("count deliveries at another generation: %w", ErrUnavailable)
	}
	if err := tx.Commit(ctx); err != nil {
		return report, ErrUnavailable
	}
	for _, count := range report.OtherKinds {
		slog.WarnContext(ctx, "syncreconciler.generation_reconcile_other_kind_not_rearmed",
			"kind", count.Kind, "stranded", count.Stranded)
		record(count.Kind, "counted_not_rearmed", count.Stranded)
	}
	return report, nil
}

func (repair *TerminalDeliveryRepair) reconcileFinalizePass(ctx context.Context, now time.Time, cursor *finalizeCursor) (readyFinalizeOutcome, error) {
	tx, err := repair.begin(ctx)
	if err != nil || tx == nil {
		return readyFinalizeOutcome{}, ErrUnavailable
	}
	defer func() { _ = tx.Rollback(ctx) }()
	outcome, err := repair.repairReadyFinalizersWith(ctx, tx, now.UTC(), maximumStepLimit, repair.otherGenerationQuery, repair.otherGenerationLockSQL, now.UTC(), cursor, true)
	if err != nil {
		return readyFinalizeOutcome{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return readyFinalizeOutcome{}, ErrUnavailable
	}
	return outcome, nil
}

func (repair *TerminalDeliveryRepair) countOtherGenerationLive(ctx context.Context) (int, error) {
	tx, err := repair.begin(ctx)
	if err != nil || tx == nil {
		return 0, ErrUnavailable
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var live int
	if err := tx.QueryRow(ctx, repair.otherGenerationLiveQuery).Scan(&live); err != nil {
		return 0, fmt.Errorf("count finalize deliveries at another generation with a live job: %w", ErrUnavailable)
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, ErrUnavailable
	}
	return live, nil
}
