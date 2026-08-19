//go:build integration

package fixed

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/platform/health"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestDegradedStateIsSharedByReplicasAndSurvivesRestart(t *testing.T) {
	pool := startScheduledReportPostgres(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	schedule := scheduleByID(t, scheduledReportsScheduleID)

	raisedAt := time.Date(2026, time.July, 25, 6, 20, 0, 0, time.UTC)
	seedDegradedEvaluation(
		t, ctx, pool, NewOccurrence(schedule, raisedAt, raisedAt),
		DegradedScheduledReportsUndeliverable,
	)
	baselineAt := raisedAt.Add(schedule.Cadence.Period())
	seedBaseline(t, ctx, pool, NewOccurrence(schedule, baselineAt, baselineAt))

	first := newReportLoop(t, pool, schedule)
	second := newReportLoop(t, pool, schedule)
	for replica, loop := range []*Loop{first, second} {
		if err := loop.step(ctx, baselineAt.Add(time.Minute)); err != nil {
			t.Fatalf("replica %d read-back window: %v", replica+1, err)
		}
		assertDegradedGauge(t, loop, DegradedScheduledReportsUndeliverable, 1)
	}

	// A new loop models a process restart. It did not observe the evaluation
	// that raised the condition and must still export the shared ledger truth.
	restarted := newReportLoop(t, pool, schedule)
	if err := restarted.step(ctx, baselineAt.Add(2*time.Minute)); err != nil {
		t.Fatalf("restart read-back window: %v", err)
	}
	assertDegradedGauge(t, restarted, DegradedScheduledReportsUndeliverable, 1)

	clearedAt := baselineAt.Add(schedule.Cadence.Period())
	seedClearEvaluation(t, ctx, pool, NewOccurrence(schedule, clearedAt, clearedAt))
	for replica, loop := range []*Loop{first, second, restarted} {
		if err := loop.step(ctx, clearedAt.Add(time.Minute)); err != nil {
			t.Fatalf("replica %d clear read-back window: %v", replica+1, err)
		}
		assertDegradedGauge(t, loop, "none", 0)
	}
}

func seedBaseline(
	t *testing.T,
	ctx context.Context,
	pool *pgxpool.Pool,
	occurrence Occurrence,
) {
	t.Helper()
	if _, err := pool.Exec(ctx, `
INSERT INTO public.fixed_schedule_occurrences
    (occurrence_key, identity_version, schedule_id, target_kind, scheduled_for, observed_at,
     status, handoff_count, skip_reason, degraded_reason, completed_at, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5, $6, 'skipped', 0, 'cold_start_baseline', NULL, $6, $6, $6)`,
		occurrence.Key, occurrence.IdentityVersion, occurrence.ScheduleID,
		occurrence.TargetKind, occurrence.ScheduledFor, occurrence.ObservedAt,
	); err != nil {
		t.Fatal(err)
	}
}

func TestRepeatedClaimLosersLoadTheWinnersDurableDegradedVerdict(t *testing.T) {
	pool := startScheduledReportPostgres(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	schedule := scheduleByID(t, scheduledReportsScheduleID)
	anchorAt := time.Date(2026, time.July, 25, 6, 20, 0, 0, time.UTC)
	seedRecordedOccurrence(t, ctx, pool, NewOccurrence(schedule, anchorAt, anchorAt))

	var lastReplicas []*Loop
	for round := 1; round <= 2; round++ {
		arrivals := &sync.WaitGroup{}
		arrivals.Add(2)
		ledger := claimBarrierLedger{Ledger: NewPostgresLedger(), arrivals: arrivals}
		replicas := []*Loop{
			newVerdictLoop(t, pool, schedule, ledger, DegradedScheduledReportsUndeliverable),
			newVerdictLoop(t, pool, schedule, ledger, DegradedScheduledReportsUndeliverable),
		}
		observedAt := anchorAt.Add(time.Duration(round) * schedule.Cadence.Period())
		decision, err := DueOccurrence(
			schedule,
			observedAt,
			&Anchor{
				ScheduledFor: anchorAt.Add(time.Duration(round-1) * schedule.Cadence.Period()),
				ObservedAt:   anchorAt.Add(time.Duration(round-1) * schedule.Cadence.Period()),
			},
		)
		if err != nil || decision.Occurrence == nil || decision.SkippedStale {
			t.Fatalf("round %d premise has no runnable occurrence: %+v err=%v", round, decision, err)
		}
		errs := make([]error, len(replicas))
		var finished sync.WaitGroup
		finished.Add(len(replicas))
		for index, loop := range replicas {
			go func(index int, loop *Loop) {
				defer finished.Done()
				errs[index] = loop.step(ctx, observedAt)
			}(index, loop)
		}
		finished.Wait()
		assertStoredDegradedReason(t, pool, DegradedScheduledReportsUndeliverable)
		for index, err := range errs {
			if err != nil {
				t.Fatalf("round %d replica %d: %v", round, index+1, err)
			}
			assertDegradedGauge(t, replicas[index], DegradedScheduledReportsUndeliverable, 1)
		}
		assertOneClaimAndOneDuplicate(t, replicas)
		lastReplicas = replicas
	}

	// A clean later evaluation must clear the shared verdict for both prior
	// replicas and for a new process that never saw the clear happen.
	clearAt := anchorAt.Add(3 * schedule.Cadence.Period())
	clearer := newVerdictLoop(t, pool, schedule, NewPostgresLedger(), "")
	if err := clearer.step(ctx, clearAt); err != nil {
		t.Fatalf("clear evaluation: %v", err)
	}
	readers := append(lastReplicas, newVerdictLoop(t, pool, schedule, NewPostgresLedger(), ""))
	for index, loop := range readers {
		if err := loop.step(ctx, clearAt.Add(time.Minute)); err != nil {
			t.Fatalf("clear reader %d: %v", index+1, err)
		}
		assertDegradedGauge(t, loop, "none", 0)
	}
}

func assertStoredDegradedReason(t *testing.T, pool *pgxpool.Pool, want string) {
	t.Helper()
	var got string
	if err := pool.QueryRow(context.Background(), `
SELECT COALESCE(degraded_reason, '')
FROM public.fixed_schedule_occurrences
WHERE schedule_id = $1
ORDER BY scheduled_for DESC
LIMIT 1`, scheduledReportsScheduleID).Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("durable degraded reason = %q, want %q", got, want)
	}
}

type claimBarrierLedger struct {
	Ledger
	arrivals *sync.WaitGroup
}

func (ledger claimBarrierLedger) Claim(
	ctx context.Context,
	tx pgx.Tx,
	occurrence Occurrence,
) (ClaimResult, error) {
	ledger.arrivals.Done()
	ledger.arrivals.Wait()
	return ledger.Ledger.Claim(ctx, tx, occurrence)
}

type durableVerdictProducer struct{ degraded string }

func (durableVerdictProducer) ID() string { return ProducerScheduledReports }

func (producer durableVerdictProducer) Produce(
	context.Context,
	pgx.Tx,
	Schedule,
	Occurrence,
) (Outcome, error) {
	return Outcome{Handoffs: 1, Degraded: producer.degraded}, nil
}

func newVerdictLoop(
	t *testing.T,
	pool *pgxpool.Pool,
	schedule Schedule,
	ledger Ledger,
	degraded string,
) *Loop {
	t.Helper()
	producers, err := NewProducerSet(durableVerdictProducer{degraded: degraded})
	if err != nil {
		t.Fatal(err)
	}
	engine, err := NewEngine(EngineConfig{
		Schedules: []Schedule{schedule},
		Producers: producers,
		Ledger:    ledger,
		Publisher: &recordingPublisher{},
		Registry:  testRegistry(t),
		Pool:      pool,
	})
	if err != nil {
		t.Fatal(err)
	}
	loop, err := NewLoop(engine, DefaultLoopConfig(health.NewRegistry(100*time.Millisecond)))
	if err != nil {
		t.Fatal(err)
	}
	return loop
}

func assertOneClaimAndOneDuplicate(t *testing.T, loops []*Loop) {
	t.Helper()
	claimed, duplicate := 0, 0
	outputs := make([]string, 0, len(loops))
	for _, loop := range loops {
		var exported strings.Builder
		if err := loop.WritePrometheus(&exported); err != nil {
			t.Fatal(err)
		}
		outputs = append(outputs, exported.String())
		if strings.Contains(exported.String(), `result="claimed"} 1`) {
			claimed++
		}
		if strings.Contains(exported.String(), `result="duplicate"} 1`) {
			duplicate++
		}
	}
	if claimed != 1 || duplicate != 1 {
		t.Fatalf("claimed replicas=%d duplicate replicas=%d, want one of each; metrics:\n%s",
			claimed, duplicate, strings.Join(outputs, "\n---\n"))
	}
}

func newReportLoop(t *testing.T, pool *pgxpool.Pool, schedule Schedule) *Loop {
	t.Helper()
	engine, _ := newReportEngine(t, pool, schedule)
	loop, err := NewLoop(engine, DefaultLoopConfig(health.NewRegistry(100*time.Millisecond)))
	if err != nil {
		t.Fatal(err)
	}
	return loop
}

func seedDegradedEvaluation(
	t *testing.T,
	ctx context.Context,
	pool *pgxpool.Pool,
	occurrence Occurrence,
	reason string,
) {
	t.Helper()
	if _, err := pool.Exec(ctx, `
INSERT INTO public.fixed_schedule_occurrences
    (occurrence_key, identity_version, schedule_id, target_kind, scheduled_for, observed_at,
     status, handoff_count, skip_reason, degraded_reason, completed_at, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5, $6, 'materialized', 1, NULL, $7, $6, $6, $6)`,
		occurrence.Key, occurrence.IdentityVersion, occurrence.ScheduleID,
		occurrence.TargetKind, occurrence.ScheduledFor, occurrence.ObservedAt, reason,
	); err != nil {
		t.Fatal(err)
	}
}

func seedClearEvaluation(
	t *testing.T,
	ctx context.Context,
	pool *pgxpool.Pool,
	occurrence Occurrence,
) {
	t.Helper()
	if _, err := pool.Exec(ctx, `
INSERT INTO public.fixed_schedule_occurrences
    (occurrence_key, identity_version, schedule_id, target_kind, scheduled_for, observed_at,
     status, handoff_count, skip_reason, degraded_reason, completed_at, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5, $6, 'skipped', 0, 'no_due_scheduled_reports', NULL, $6, $6, $6)`,
		occurrence.Key, occurrence.IdentityVersion, occurrence.ScheduleID,
		occurrence.TargetKind, occurrence.ScheduledFor, occurrence.ObservedAt,
	); err != nil {
		t.Fatal(err)
	}
}

func assertDegradedGauge(t *testing.T, loop *Loop, reason string, value int) {
	t.Helper()
	var exported strings.Builder
	if err := loop.WritePrometheus(&exported); err != nil {
		t.Fatal(err)
	}
	want := `fixed_scheduler_schedule_degraded{schedule="` + scheduledReportsScheduleID +
		`",reason="` + reason + `"} `
	for _, line := range strings.Split(exported.String(), "\n") {
		if strings.HasPrefix(line, want) {
			if strings.HasSuffix(line, string(rune('0'+value))) {
				return
			}
			t.Fatalf("degraded gauge = %q, want value %d", line, value)
		}
	}
	t.Fatalf("missing degraded gauge %q in:\n%s", want, exported.String())
}
