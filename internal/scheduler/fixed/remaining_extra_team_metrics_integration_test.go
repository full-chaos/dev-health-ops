//go:build integration

package fixed

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobs/metrics/remaining"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// remainingMetricTablesDDL mirrors the minimal shape
// internal/jobs/metrics/remaining/postgres_integration_test.go's
// createRemainingTables uses, plus the organizations table the real
// PostgresOrganizationLister reads. Repeated here (rather than shared) because
// nothing in this repo currently exports a cross-package integration DDL
// helper -- every existing integration suite hand-rolls the same minimal
// subset, and this test follows that convention.
const remainingMetricTablesDDL = `
CREATE TABLE public.organizations (
    id uuid PRIMARY KEY,
    is_active boolean NOT NULL DEFAULT TRUE
);
CREATE TABLE remaining_metric_runs (
 id uuid PRIMARY KEY, org_id uuid NOT NULL, family text NOT NULL, generation text NOT NULL,
 scope_key text NOT NULL, generation_seed bigint NULL, status text NOT NULL,
 canceled_at timestamptz NULL, created_at timestamptz NOT NULL, updated_at timestamptz NOT NULL,
 UNIQUE(org_id,family,generation,scope_key)
);
CREATE TABLE remaining_metric_partitions (
 id uuid PRIMARY KEY, run_id uuid NOT NULL REFERENCES remaining_metric_runs(id), ordinal integer NOT NULL CHECK (ordinal >= 1),
 scope jsonb NOT NULL, status text NOT NULL, claim_token uuid NULL, lease_expires_at timestamptz NULL,
 attempt_count integer NOT NULL, output_evidence text NULL, completed_at timestamptz NULL,
 created_at timestamptz NOT NULL, updated_at timestamptz NOT NULL, UNIQUE(run_id,ordinal)
);
CREATE TABLE worker_job_completion_fences (
 completion_key text PRIMARY KEY,
 completed_at timestamptz NOT NULL DEFAULT statement_timestamp()
)`

// startRemainingMetricsPostgres provisions a real PostgreSQL instance with the
// fixed-schedule occurrence ledger plus the durable remaining-metrics tables,
// so the reachability assertion below runs the real Engine, the real
// PostgresLedger, the real RemainingMetricsFanoutProducer, and the real
// remaining.PostgresStore against real transactions -- not test doubles for
// any of those layers. Only the OUTBOX->River hop is a recording fake
// (recordingPublisher, already used by every other engine integration test in
// this package): standing up the full joboutbox/River schema as well is out
// of scope for this reachability proof, and that hop is shared, already-
// trusted plumbing every other working remaining-metrics family goes through
// unchanged.
func startRemainingMetricsPostgres(t *testing.T) *pgxpool.Pool {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	t.Cleanup(cancel)
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer closeCancel()
		if err := instance.Close(closeCtx); err != nil {
			t.Errorf("terminate PostgreSQL: %v", err)
		}
	})
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	if _, err := pool.Exec(ctx, fixedScheduleOccurrenceDDL); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, remainingMetricTablesDDL); err != nil {
		t.Fatal(err)
	}
	return pool
}

// remainingPartitionScope reads back the durable scope JSON for the single
// partition a one-organization fan-out occurrence creates.
func remainingPartitionScope(t *testing.T, ctx context.Context, pool *pgxpool.Pool, partitionID string) map[string]any {
	t.Helper()
	var raw []byte
	if err := pool.QueryRow(ctx,
		"SELECT scope FROM remaining_metric_partitions WHERE id = $1::uuid", partitionID,
	).Scan(&raw); err != nil {
		t.Fatalf("read partition scope: %v", err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatalf("decode partition scope: %v", err)
	}
	return decoded
}

// recordingRemainingPartitionPublisher captures every partition the durable
// remaining-metrics store publishes. RemainingMetricsFanoutProducer routes its
// job handoff through remaining.PartitionPublisher (called from inside
// PostgresStore.StartRunTx), NOT through the engine's generic
// Publisher/Outcome.Requests path that HeartbeatProducer/RetentionProducer/
// DailyMetricsFanoutProducer use -- so the engine's own recordingPublisher
// never sees these publishes, and this double is what actually proves one
// happened.
type recordingRemainingPartitionPublisher struct {
	mu    sync.Mutex
	calls []recordingRemainingPartitionPublisherCall
}

type recordingRemainingPartitionPublisherCall struct {
	Run       remaining.Run
	Partition remaining.Partition
}

func (publisher *recordingRemainingPartitionPublisher) PublishPartitionTx(
	_ context.Context, _ pgx.Tx, run remaining.Run, partition remaining.Partition, _ string,
) error {
	publisher.mu.Lock()
	defer publisher.mu.Unlock()
	publisher.calls = append(publisher.calls, recordingRemainingPartitionPublisherCall{Run: run, Partition: partition})
	return nil
}

func (publisher *recordingRemainingPartitionPublisher) count() int {
	publisher.mu.Lock()
	defer publisher.mu.Unlock()
	return len(publisher.calls)
}

// TestExtraMetricsAndTeamMetricsFanoutsAreReachableFromAFixedScheduleTick is
// the CHAOS-4243 acceptance case: before this family's producer bindings
// existed, extra_metrics_daily_fanout / team_metrics_daily_fanout had zero
// producer, so this test (with the byScheduleID entries removed) fails at the
// NewRemainingMetricsFanoutProducer/Produce step with
// ErrProducerUnavailable -- the schedule declaration alone is not reachability
// proof, per AGENTS.md's "a cited constructor is not proof of capability."
func TestExtraMetricsAndTeamMetricsFanoutsAreReachableFromAFixedScheduleTick(t *testing.T) {
	for _, scheduleID := range []string{"extra_metrics_daily_fanout", "team_metrics_daily_fanout"} {
		t.Run(scheduleID, func(t *testing.T) {
			ctx := context.Background()
			pool := startRemainingMetricsPostgres(t)
			schedule := scheduleByID(t, scheduleID)

			orgID := uuid.NewString()
			if _, err := pool.Exec(ctx,
				"INSERT INTO public.organizations (id, is_active) VALUES ($1::uuid, TRUE)", orgID,
			); err != nil {
				t.Fatal(err)
			}

			store, err := remaining.NewPostgresStore(pool)
			if err != nil {
				t.Fatal(err)
			}
			partitionPublisher := &recordingRemainingPartitionPublisher{}
			producer, err := NewRemainingMetricsFanoutProducer(
				store, partitionPublisher, NewPostgresOrganizationLister(), &recordingGraphWriter{},
			)
			if err != nil {
				t.Fatal(err)
			}
			producers, err := NewProducerSet(producer)
			if err != nil {
				t.Fatal(err)
			}
			engine, err := NewEngine(EngineConfig{
				Schedules: []Schedule{schedule},
				Producers: producers,
				Ledger:    NewPostgresLedger(),
				Publisher: &recordingPublisher{},
				Registry:  testRegistry(t),
				Pool:      pool,
			})
			if err != nil {
				t.Fatal(err)
			}

			location, err := schedule.Location()
			if err != nil {
				t.Fatal(err)
			}
			observedAt := time.Date(2026, time.August, 25, 6, 0, 0, 0, time.UTC)
			dueAt, ok := schedule.Cadence.Previous(observedAt, location)
			if !ok {
				t.Fatal("schedule cadence resolved no due time")
			}
			// Seed the anchor one full interval back so this window is real
			// work rather than a cold-start baseline (mirrors
			// newReportEngine's TestScheduledReportsDispatch pattern).
			previous := NewOccurrence(schedule, dueAt.Add(-schedule.Cadence.Period()), dueAt.Add(-schedule.Cadence.Period()))
			seedRecordedOccurrence(t, ctx, pool, previous)

			result, err := engine.Step(ctx, observedAt)
			if err != nil {
				t.Fatalf("Step(): %v", err)
			}
			scheduleResult := resultFor(t, result, schedule.ID)
			if scheduleResult.Err != nil {
				t.Fatalf("schedule failed: %v", scheduleResult.Err)
			}
			if scheduleResult.Claimed != 1 || scheduleResult.Handoffs != 1 {
				var skipReason, degradedReason *string
				_ = pool.QueryRow(ctx, "SELECT skip_reason, degraded_reason FROM public.fixed_schedule_occurrences WHERE schedule_id=$1", scheduleID).Scan(&skipReason, &degradedReason)
				var orgCount int
				_ = pool.QueryRow(ctx, "SELECT count(*) FROM public.organizations WHERE is_active").Scan(&orgCount)
				sr, dr := "<nil>", "<nil>"
				if skipReason != nil {
					sr = *skipReason
				}
				if degradedReason != nil {
					dr = *degradedReason
				}
				t.Fatalf("schedule result = %+v, want one claim and one handoff (skip_reason=%s degraded_reason=%s active_orgs=%d)", scheduleResult, sr, dr, orgCount)
			}
			if partitionPublisher.count() != 1 {
				t.Fatalf("published %d partitions, want one", partitionPublisher.count())
			}

			published := partitionPublisher.calls[0]
			wantFamily := map[string]string{
				"extra_metrics_daily_fanout": "extra_metrics",
				"team_metrics_daily_fanout":  "team_metrics",
			}[scheduleID]
			if published.Run.Family != wantFamily {
				t.Fatalf("published run family = %q, want %q", published.Run.Family, wantFamily)
			}
			gotKind, ok := remaining.JobKindForFamily(published.Run.Family)
			if !ok {
				t.Fatalf("family %q has no registered job kind", published.Run.Family)
			}
			if gotKind != schedule.TargetKind {
				t.Fatalf("resolved job kind = %q, want schedule.TargetKind %q", gotKind, schedule.TargetKind)
			}
			if published.Run.OrganizationID != orgID {
				t.Fatalf("published run org = %q, want %q", published.Run.OrganizationID, orgID)
			}
			if published.Partition.ID == "" {
				t.Fatal("published partition carries no id")
			}

			scope := remainingPartitionScope(t, ctx, pool, published.Partition.ID)
			if scope["version"] != float64(1) {
				t.Fatalf("partition scope version = %v, want 1", scope["version"])
			}
			if scope["day"] != dueAt.UTC().Format("2006-01-02") {
				t.Fatalf("partition scope day = %v, want %s", scope["day"], dueAt.UTC().Format("2006-01-02"))
			}
			if scope["backfill_days"] != float64(1) {
				t.Fatalf("partition scope backfill_days = %v, want 1", scope["backfill_days"])
			}

			// Durable-readback proof (does a row land?), independent of the
			// in-memory publisher double: re-read the partition/run by id
			// straight from Postgres.
			var runFamily, runOrg string
			if err := pool.QueryRow(ctx,
				`SELECT r.family, r.org_id::text FROM remaining_metric_partitions p
				 JOIN remaining_metric_runs r ON r.id = p.run_id
				 WHERE p.id = $1::uuid`, published.Partition.ID,
			).Scan(&runFamily, &runOrg); err != nil {
				t.Fatal(err)
			}
			if runOrg != orgID {
				t.Fatalf("durable run org = %q, want %q", runOrg, orgID)
			}
			if runFamily != wantFamily {
				t.Fatalf("durable run family = %q, want %q", runFamily, wantFamily)
			}
		})
	}
}
